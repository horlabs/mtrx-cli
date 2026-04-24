"""
matrix_backend.py
-----------------
Thin async wrapper around matrix-nio that provides the primitives
the adapter layer needs:
  - login / logout
  - send text message (DM or group/room)
  - upload + send file attachment
  - receive messages (sync loop → asyncio Queue)
  - list joined rooms (= "groups")
  - create / invite / leave rooms
  - set display name
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import mimetypes
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import aiofiles  # type: ignore[import-untyped]
from nio import (  # type: ignore[import-untyped]
    AsyncClient,
    AsyncClientConfig,
    InviteEvent,
    JoinError,
    KeyVerificationAccept,
    KeyVerificationCancel,
    KeyVerificationEvent,
    KeyVerificationKey,
    KeyVerificationMac,
    KeyVerificationStart,
    LoginError,
    MatrixRoom,
    RoomCreateError,
    RoomMessageFile,
    RoomMessageImage,
    RoomMessageText,
    UploadError,
    UploadResponse,
)
from nio.exceptions import OlmUnverifiedDeviceError  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


@dataclass
class IncomingMessage:
    """Normalized incoming message, mirrors signal-cli's envelope format."""

    timestamp: int
    sender: str  # Matrix user-id, e.g. @alice:matrix.org
    sender_name: str
    room_id: str
    room_name: str
    body: str
    attachments: List[Dict[str, Any]] = field(
        default_factory=lambda: cast(List[Dict[str, Any]], [])
    )
    is_group: bool = False
    group_id: Optional[str] = None  # room_id used as group-id


class MatrixBackend:
    def __init__(
        self,
        homeserver: str,
        user_id: str,
        password: str | None = None,
        access_token: str | None = None,
        store_path: str = "~/.local/share/mtrx-cli",
        device_name: str = "mtrx-cli",
        enable_e2ee: bool | None = None,
    ):
        self.homeserver = homeserver
        self.user_id = user_id
        self._password = password
        self._access_token = access_token
        self.store_path = Path(store_path).expanduser()
        self.store_path.mkdir(parents=True, exist_ok=True)
        self._session_file = self.store_path / "session.json"
        self.device_name = device_name

        if enable_e2ee is None:
            try:
                import olm  # type: ignore  # noqa: F401

                encryption_enabled = True
            except Exception:  # noqa: BLE001
                encryption_enabled = False
        else:
            encryption_enabled = enable_e2ee

        config = AsyncClientConfig(
            store_sync_tokens=True,
            encryption_enabled=encryption_enabled,
        )
        if not encryption_enabled:
            logger.warning(
                "Matrix E2EE support disabled (olm not available). "
                "Encrypted room messages may not be received as plaintext callbacks."
            )
        self.client = AsyncClient(
            homeserver=self.homeserver,
            user=self.user_id,
            store_path=str(self.store_path),
            config=config,
        )
        self.message_queue: asyncio.Queue[IncomingMessage] = asyncio.Queue()
        self._sync_task: Optional[asyncio.Task[Any]] = None
        self._logged_in = False
        self._verification_events: Dict[str, Dict[str, Any]] = {}

        # Register callbacks
        self.client.add_event_callback(self._on_message_text, RoomMessageText)
        self.client.add_event_callback(self._on_message_file, RoomMessageFile)
        self.client.add_event_callback(self._on_message_image, RoomMessageImage)
        self.client.add_event_callback(self._on_invite, InviteEvent)  # type: ignore
        self.client.add_to_device_callback(
            self._on_key_verification,
            (
                KeyVerificationStart,
                KeyVerificationAccept,
                KeyVerificationKey,
                KeyVerificationMac,
                KeyVerificationCancel,
            ),
        )

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    async def login(self) -> None:
        session = self._load_session()
        session_token = str(session.get("access_token", "")).strip()
        session_device_id = str(session.get("device_id", "")).strip()

        token_to_use = (self._access_token or session_token or "").strip()
        if token_to_use:
            restored = self._restore_login(token_to_use, session_device_id)
            whoami = await self.client.whoami()
            whoami_user = getattr(whoami, "user_id", None)
            if whoami_user == self.user_id:
                current_device_id = str(getattr(self.client, "device_id", "") or "")
                if token_to_use and current_device_id:
                    self._save_session(token_to_use, current_device_id)
                self._logged_in = True
                logger.info("Logged in via restored access token")
                return

            if whoami_user and whoami_user != self.user_id:
                raise RuntimeError(
                    "Matrix access token belongs to a different user. "
                    f"Configured: {self.user_id}, token user: {whoami_user}"
                )

            detail = getattr(whoami, "message", "unknown error")
            if self._access_token and token_to_use == self._access_token:
                raise RuntimeError(
                    "Matrix access token validation failed. "
                    f"Check homeserver URL and token. Details: {detail}"
                )

            # Persisted token may be stale; clear and continue with password login.
            if restored:
                logger.warning(
                    "Stored Matrix session is invalid, falling back to password login"
                )
            self._clear_session()

        if not self._password:
            raise ValueError("Either password or access_token must be provided")

        resp = await self.client.login(
            password=self._password,
            device_name=self.device_name,
        )
        if isinstance(resp, LoginError):
            raise RuntimeError(f"Matrix login failed: {resp.message}")

        access_token = getattr(resp, "access_token", "")
        device_id = getattr(resp, "device_id", "")
        if access_token and device_id:
            self._save_session(str(access_token), str(device_id))

        self._logged_in = True
        logger.info("Logged in as %s (device: %s)", self.user_id, resp.device_id)

    def _restore_login(self, access_token: str, device_id: str) -> bool:
        """Restore a prior session token/device so E2EE identity stays stable."""
        if not access_token:
            return False

        if hasattr(self.client, "restore_login") and device_id:
            self.client.restore_login(self.user_id, device_id, access_token)
            return True

        self.client.access_token = access_token
        self.client.user_id = self.user_id
        if device_id and hasattr(self.client, "device_id"):
            setattr(self.client, "device_id", device_id)
        return True

    def _load_session(self) -> Dict[str, Any]:
        if not self._session_file.exists():
            return {}
        try:
            return cast(Dict[str, Any], json.loads(self._session_file.read_text()))
        except Exception:  # noqa: BLE001
            logger.warning(
                "Ignoring unreadable Matrix session file: %s", self._session_file
            )
            return {}

    def _save_session(self, access_token: str, device_id: str) -> None:
        payload = {
            "user_id": self.user_id,
            "access_token": access_token,
            "device_id": device_id,
        }
        self._session_file.write_text(json.dumps(payload))
        self._session_file.chmod(0o600)

    def _clear_session(self) -> None:
        if self._session_file.exists():
            self._session_file.unlink(missing_ok=True)

    async def logout(self) -> None:
        if self._logged_in:
            # never really logout, as the token will be invalid afterwards
            # await self.client.logout()
            pass
        await self.client.close()
        self._logged_in = False

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    async def send_message(
        self,
        room_id: str,
        text: str,
        attachment_paths: list[str] | None = None,
    ) -> Dict[str, Any]:
        """Send a text message (and optional attachments) to a Matrix room."""
        results: List[Any] = []

        if text:
            resp = await self._room_send_with_unverified_fallback(
                room_id=room_id,
                message_type="m.room.message",
                content={"msgtype": "m.text", "body": text},
            )
            results.append(resp)

        for path in attachment_paths or []:
            resp = await self._send_file(room_id, path)
            results.append(resp)

        return {"results": [str(r) for r in results]}

    async def _room_send_with_unverified_fallback(
        self,
        room_id: str,
        message_type: str,
        content: Dict[str, Any],
    ) -> Any:
        """Send a room event and retry once if unverified devices block encryption."""
        try:
            return await self.client.room_send(
                room_id=room_id,
                message_type=message_type,
                content=content,
            )
        except OlmUnverifiedDeviceError as exc:
            logger.warning(
                "Unverified device blocked encrypted send in room %s; "
                "retrying with ignore_unverified_devices=True (%s)",
                room_id,
                exc,
            )
            return await self.client.room_send(
                room_id=room_id,
                message_type=message_type,
                content=content,
                ignore_unverified_devices=True,
            )

    async def resolve_recipient_to_room(self, recipient: str) -> str:
        """
        Resolve signal-cli style recipient values to a Matrix room.

        Accepted recipient forms:
          - room id or alias: !... or #...
          - matrix user id: @user:server
          - localpart shorthand: user  -> resolves to @user:... if unique
        """
        value = (recipient or "").strip()
        if not value:
            raise ValueError("Recipient must not be empty")

        if value.startswith(("!", "#")):
            return value

        user_id = await self._resolve_user_id(value)
        return await self._get_or_create_direct_room(user_id)

    async def _resolve_user_id(self, recipient: str) -> str:
        """Resolve a recipient token to a Matrix user-id."""
        if recipient.startswith("@"):
            return recipient

        await self.client.sync(timeout=5000)
        matches: list[str] = []
        for room in self.client.rooms.values():
            for uid in room.users.keys():
                if uid == self.user_id:
                    continue
                if uid == recipient:
                    matches.append(uid)
                    continue

                localpart = uid.lstrip("@").split(":", 1)[0]
                if localpart == recipient:
                    matches.append(uid)

        unique_matches = sorted(set(matches))
        if len(unique_matches) == 1:
            return unique_matches[0]
        if len(unique_matches) > 1:
            raise ValueError(
                f"Recipient '{recipient}' is ambiguous: {', '.join(unique_matches)}"
            )
        raise ValueError(
            f"Recipient '{recipient}' could not be resolved. Use full user-id like @user:server"
        )

    async def _get_or_create_direct_room(self, user_id: str) -> str:
        """Find an existing DM room with user_id or create one."""
        await self.client.sync(timeout=5000)

        for room_id, room in self.client.rooms.items():
            members = set(room.users.keys())
            if self.user_id in members and user_id in members and len(members) == 2:
                return room_id

        return await self.create_room(name="", invite=[user_id], is_direct=True)

    async def _send_file(self, room_id: str, file_path: str) -> Dict[str, Any]:
        path = Path(file_path)
        mime = mimetypes.guess_type(str(path))[0] or "application/octet-stream"

        async with aiofiles.open(path, "rb") as f:
            data = await f.read()

        up_resp, _ = await self.client.upload(
            cast(Any, data),
            content_type=mime,
            filename=path.name,
            filesize=len(data),
        )
        if isinstance(up_resp, UploadError):
            raise RuntimeError(f"Upload failed: {up_resp.message}")

        assert isinstance(up_resp, UploadResponse)
        mxc_uri = up_resp.content_uri

        is_image = mime.startswith("image/")
        msgtype = "m.image" if is_image else "m.file"

        content: Dict[str, Any] = {
            "msgtype": msgtype,
            "body": path.name,
            "url": mxc_uri,
            "info": {"mimetype": mime, "size": len(data)},
        }

        return cast(
            Dict[str, Any],
            await self._room_send_with_unverified_fallback(
                room_id=room_id,
                message_type="m.room.message",
                content=content,
            ),
        )

    # ------------------------------------------------------------------
    # Rooms / Groups
    # ------------------------------------------------------------------

    async def list_rooms(self) -> List[Dict[str, Any]]:
        """Return all joined rooms as group-like dicts."""
        await self.client.sync(timeout=5000)
        rooms: List[Dict[str, Any]] = []
        for room_id, room in self.client.rooms.items():
            members = list(room.users.keys())
            is_direct = self._is_direct_room(room)
            rooms.append(
                {
                    "id": room_id,
                    "name": room.display_name or room_id,
                    "members": members,
                    "member_count": len(members),
                    "is_direct": is_direct,
                }
            )
        return rooms

    async def create_room(
        self,
        name: str,
        invite: list[str] | None = None,
        is_direct: bool = False,
    ) -> str:
        """Create a Matrix room; returns room_id."""
        resp = await self.client.room_create(
            name=name,
            invite=invite or [],
            is_direct=is_direct,
        )
        if isinstance(resp, RoomCreateError):
            raise RuntimeError(f"Room creation failed: {resp.message}")
        return resp.room_id

    async def join_room(self, room_id_or_alias: str) -> str:
        resp = await self.client.join(room_id_or_alias)
        if isinstance(resp, JoinError):
            raise RuntimeError(f"Join failed: {resp.message}")
        return resp.room_id

    async def leave_room(self, room_id: str) -> None:
        await self.client.room_leave(room_id)

    async def invite_user(self, room_id: str, user_id: str) -> None:
        await self.client.room_invite(room_id, user_id)

    async def kick_user(self, room_id: str, user_id: str, reason: str = "") -> None:
        await self.client.room_kick(room_id, user_id, reason)

    # ------------------------------------------------------------------
    # Profile
    # ------------------------------------------------------------------

    async def set_display_name(self, name: str) -> None:
        await self.client.set_displayname(name)

    async def get_display_name(self, user_id: str | None = None) -> str:
        uid = user_id or self.user_id
        resp = await self.client.get_displayname(uid)
        return getattr(resp, "displayname", uid) or uid

    # ------------------------------------------------------------------
    # E2EE Device identities / trust
    # ------------------------------------------------------------------

    async def _refresh_device_store(self) -> None:
        """Refresh known device keys so trust/list commands see current state."""
        if not getattr(self.client.config, "encryption_enabled", False):
            raise RuntimeError(
                "E2EE is disabled. Install matrix-nio[e2e]/olm and enable encryption."
            )

        keys_query = getattr(self.client, "keys_query", None)
        if callable(keys_query):
            result = keys_query()
            if inspect.isawaitable(result):
                await result

        # Sync updates the in-memory store state and callbacks.
        await self.client.sync(timeout=0, full_state=False)

    def _get_device_by_id(self, user_id: str, device_id: str) -> Any | None:
        device_store = getattr(self.client, "device_store", None)
        if device_store is None:
            return None

        try:
            user_devices = device_store[user_id]
        except Exception:  # noqa: BLE001
            return None

        if isinstance(user_devices, dict):
            return user_devices.get(device_id)

        try:
            return user_devices[device_id]
        except Exception:  # noqa: BLE001
            return None

    def _iter_user_devices(self, user_id: str) -> List[tuple[str, Any]]:
        device_store = getattr(self.client, "device_store", None)
        if device_store is None:
            return []

        try:
            user_devices = device_store[user_id]
        except Exception:  # noqa: BLE001
            return []

        if isinstance(user_devices, dict):
            return list(user_devices.items())

        items = getattr(user_devices, "items", None)
        if callable(items):
            try:
                return cast(List[tuple[str, Any]], list(cast(Any, items)()))
            except Exception:  # noqa: BLE001
                return []
        return []

    async def list_identities(self, user_id: str | None = None) -> List[Dict[str, Any]]:
        await self._refresh_device_store()

        device_store = getattr(self.client, "device_store", None)
        if device_store is None:
            return []

        if user_id:
            users = [user_id]
        else:
            raw_users = getattr(device_store, "users", [])
            users = sorted(str(u) for u in raw_users)

        identities: List[Dict[str, Any]] = []
        for uid in users:
            for device_id, device in self._iter_user_devices(uid):
                identities.append(
                    {
                        "recipient": uid,
                        "deviceId": str(device_id),
                        "name": getattr(device, "display_name", "") or "",
                        "identityKey": getattr(device, "ed25519", "") or "",
                        "isVerified": bool(getattr(device, "verified", False)),
                        "isBlacklisted": bool(getattr(device, "blacklisted", False)),
                        "isIgnored": bool(getattr(device, "ignored", False)),
                    }
                )

        return identities

    async def trust_device(self, user_id: str, device_id: str) -> Dict[str, Any]:
        await self._refresh_device_store()

        device = self._get_device_by_id(user_id, device_id)
        if device is None:
            raise ValueError(f"Unknown device '{device_id}' for '{user_id}'")

        verify_device = getattr(self.client, "verify_device", None)
        if not callable(verify_device):
            raise RuntimeError(
                "Current matrix-nio client does not support verify_device"
            )

        result = verify_device(device)
        if inspect.isawaitable(result):
            await result

        return {
            "recipient": user_id,
            "deviceId": device_id,
            "isVerified": bool(getattr(device, "verified", True)),
            "identityKey": getattr(device, "ed25519", "") or "",
        }

    async def trust_all_devices(self, user_id: str) -> List[Dict[str, Any]]:
        await self._refresh_device_store()

        results: List[Dict[str, Any]] = []
        for device_id, _device in self._iter_user_devices(user_id):
            trusted = await self.trust_device(user_id, str(device_id))
            results.append(trusted)

        return results

    async def _flush_to_device_messages(self) -> None:
        send_to_device_messages = getattr(self.client, "send_to_device_messages", None)
        if not callable(send_to_device_messages):
            return

        result = send_to_device_messages()
        if inspect.isawaitable(result):
            await result

    def _verification_to_dict(self, transaction_id: str, sas: Any) -> Dict[str, Any]:
        device = getattr(sas, "other_olm_device", None)
        user_id = getattr(device, "user_id", "") or ""
        device_id = getattr(device, "id", "") or ""

        info: Dict[str, Any] = {
            "transactionId": transaction_id,
            "recipient": user_id,
            "deviceId": device_id,
            "weStarted": bool(getattr(sas, "we_started_it", False)),
            "isCanceled": bool(getattr(sas, "canceled", False)),
            "isTimedOut": bool(getattr(sas, "timed_out", False)),
            "isVerified": bool(getattr(sas, "verified", False)),
            "sasAccepted": bool(getattr(sas, "sas_accepted", False)),
            "state": str(getattr(getattr(sas, "state", None), "name", "unknown")),
        }

        try:
            if bool(getattr(sas, "other_key_set", False)):
                info["emoji"] = [
                    {"symbol": symbol, "description": description}
                    for symbol, description in sas.get_emoji()
                ]
                info["decimals"] = list(sas.get_decimals())
        except Exception:  # noqa: BLE001
            # Emoji/decimal are not always available at every SAS state.
            pass

        tx_meta = self._verification_events.get(transaction_id)
        if tx_meta:
            info["lastEvent"] = tx_meta

        return info

    def _get_sas(self, transaction_id: str) -> Any:
        verifications = getattr(self.client, "key_verifications", {})
        sas = verifications.get(transaction_id)
        if sas is None:
            raise ValueError(f"Unknown verification transaction: {transaction_id}")
        return sas

    async def list_verifications(
        self, recipient: str | None = None
    ) -> List[Dict[str, Any]]:
        await self.client.sync(timeout=0, full_state=False)
        await self._flush_to_device_messages()

        verifications = getattr(self.client, "key_verifications", {})
        out: List[Dict[str, Any]] = []
        for transaction_id, sas in verifications.items():
            item = self._verification_to_dict(str(transaction_id), sas)
            if recipient and item.get("recipient") != recipient:
                continue
            out.append(item)
        return out

    async def start_verification(self, user_id: str, device_id: str) -> Dict[str, Any]:
        await self._refresh_device_store()

        device = self._get_device_by_id(user_id, device_id)
        if device is None:
            raise ValueError(f"Unknown device '{device_id}' for '{user_id}'")

        start_key_verification = getattr(self.client, "start_key_verification", None)
        if not callable(start_key_verification):
            raise RuntimeError(
                "Current matrix-nio client does not support interactive key verification"
            )

        result = start_key_verification(device)
        if inspect.isawaitable(result):
            await result

        await self._flush_to_device_messages()

        get_active_sas = getattr(self.client, "get_active_sas", None)
        sas = None
        if callable(get_active_sas):
            sas = get_active_sas(user_id, device_id)

        if sas is None:
            raise RuntimeError(
                "Verification started but no active SAS session found. Try listVerifications."
            )

        transaction_id = str(getattr(sas, "transaction_id", "") or "")
        if not transaction_id:
            raise RuntimeError("Failed to determine verification transaction id")

        return self._verification_to_dict(transaction_id, sas)

    async def accept_verification(self, transaction_id: str) -> Dict[str, Any]:
        await self.client.sync(timeout=0, full_state=False)

        accept_key_verification = getattr(self.client, "accept_key_verification", None)
        if not callable(accept_key_verification):
            raise RuntimeError(
                "Current matrix-nio client does not support interactive key verification"
            )

        result = accept_key_verification(transaction_id)
        if inspect.isawaitable(result):
            await result

        await self._flush_to_device_messages()
        await self.client.sync(timeout=0, full_state=False)
        await self._flush_to_device_messages()

        sas = self._get_sas(transaction_id)
        return self._verification_to_dict(transaction_id, sas)

    async def confirm_verification(self, transaction_id: str) -> Dict[str, Any]:
        await self.client.sync(timeout=0, full_state=False)

        confirm_short_auth_string = getattr(
            self.client, "confirm_short_auth_string", None
        )
        if not callable(confirm_short_auth_string):
            raise RuntimeError(
                "Current matrix-nio client does not support interactive key verification"
            )

        result = confirm_short_auth_string(transaction_id)
        if inspect.isawaitable(result):
            await result

        await self._flush_to_device_messages()
        await self.client.sync(timeout=0, full_state=False)
        await self._flush_to_device_messages()

        sas = self._get_sas(transaction_id)
        return self._verification_to_dict(transaction_id, sas)

    async def cancel_verification(
        self,
        transaction_id: str,
        reject: bool = False,
    ) -> Dict[str, Any]:
        cancel_key_verification = getattr(self.client, "cancel_key_verification", None)
        if not callable(cancel_key_verification):
            raise RuntimeError(
                "Current matrix-nio client does not support interactive key verification"
            )

        result = cancel_key_verification(transaction_id, reject=reject)
        if inspect.isawaitable(result):
            await result

        await self._flush_to_device_messages()

        try:
            sas = self._get_sas(transaction_id)
            return self._verification_to_dict(transaction_id, sas)
        except ValueError:
            return {
                "transactionId": transaction_id,
                "isCanceled": True,
                "reject": bool(reject),
            }

    # ------------------------------------------------------------------
    # Receive (daemon sync loop)
    # ------------------------------------------------------------------

    async def start_daemon(self) -> None:
        """Start background sync loop, feeding IncomingMessages into queue."""
        if self._sync_task and not self._sync_task.done():
            logger.debug("Sync daemon already running")
            return

        if self._sync_task and self._sync_task.done():
            if self._sync_task.cancelled():
                logger.warning("Sync daemon task was cancelled; restarting")
            else:
                exc = self._sync_task.exception()
                if exc is not None:
                    logger.warning(
                        "Sync daemon task ended with error; restarting: %s", exc
                    )
                else:
                    logger.warning("Sync daemon task ended unexpectedly; restarting")

        logger.info("Starting sync daemon")
        self._sync_task = asyncio.create_task(self._sync_loop())
        logger.info("Matrix sync daemon started")

    async def stop_daemon(self) -> None:
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

    async def _sync_loop(self) -> None:
        logger.info("Sync loop starting")
        while True:
            try:
                await self.client.sync_forever(timeout=30_000, full_state=True)
                logger.warning("sync_forever returned unexpectedly; restarting in 2s")
            except asyncio.CancelledError:
                logger.info("Sync loop cancelled")
                raise
            except Exception as e:
                logger.error("Sync loop crashed, retrying in 2s: %s", e, exc_info=True)

            await asyncio.sleep(2)

    async def sync_once(
        self,
        timeout_ms: int = 0,
        full_state: bool = True,
        reset_since: bool = False,
    ) -> None:
        """Run a single sync request to feed callbacks and message_queue.

        reset_since=True clears the current sync token and performs a one-time
        backfill sync, which can recover messages that were missed by queue-only
        polling across short-lived CLI invocations.
        """
        if reset_since:
            setattr(self.client, "next_batch", None)
        await self.client.sync(timeout=timeout_ms, full_state=full_state)

    # ------------------------------------------------------------------
    # Event callbacks (internal)
    # ------------------------------------------------------------------

    def _room_has_explicit_group_identity(self, room: MatrixRoom) -> bool:
        room_name = str(getattr(room, "name", "") or "").strip()
        if room_name:
            return True
        canonical_alias = str(getattr(room, "canonical_alias", "") or "").strip()
        return bool(canonical_alias)

    def _is_direct_room(self, room: MatrixRoom, sender: str | None = None) -> bool:
        if bool(getattr(room, "is_direct", False)):
            return True
        members = set(getattr(room, "users", {}).keys())
        if self.user_id not in members:
            return False
        other_members = members - {self.user_id}
        if len(other_members) != 1:
            return False
        if sender is not None and sender not in other_members:
            return False
        if self._room_has_explicit_group_identity(room):
            return False
        return True

    def _make_incoming(
        self,
        room: MatrixRoom,
        sender: str,
        body: str,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> IncomingMessage:
        is_group = not self._is_direct_room(room, sender)
        return IncomingMessage(
            timestamp=int(time.time() * 1000),
            sender=sender,
            sender_name=(
                (room.user_name(sender) or sender) if sender in room.users else sender
            ),
            room_id=room.room_id,
            room_name=room.display_name or room.room_id,
            body=body,
            attachments=attachments or [],
            is_group=is_group,
            group_id=room.room_id if is_group else None,
        )

    async def _on_message_text(self, room: MatrixRoom, event: Any) -> None:
        if event.sender == self.user_id:
            return  # skip own messages
        msg = self._make_incoming(room, event.sender, event.body)
        logger.debug(
            "Message callback: queuing text from %s in room %s: %s",
            event.sender,
            room.room_id,
            event.body[:50],
        )
        await self.message_queue.put(msg)

    async def _on_message_file(self, room: MatrixRoom, event: Any) -> None:
        if event.sender == self.user_id:
            return
        mxc = getattr(event, "url", "")
        src = cast(Dict[str, Any], getattr(event, "source", {}))
        content = cast(Dict[str, Any], src.get("content", {}))
        info = cast(Dict[str, Any], content.get("info", {}))
        att: Dict[str, Any] = {
            "contentType": info.get("mimetype", "application/octet-stream"),
            "filename": event.body,
            "url": mxc,
        }
        msg = self._make_incoming(room, event.sender, event.body, [att])
        logger.debug(
            "Message callback: queuing file from %s: %s", event.sender, event.body
        )
        await self.message_queue.put(msg)

    async def _on_message_image(self, room: MatrixRoom, event: Any) -> None:
        if event.sender == self.user_id:
            return
        mxc = getattr(event, "url", "")
        att: Dict[str, Any] = {
            "contentType": "image/jpeg",
            "filename": event.body,
            "url": mxc,
        }
        msg = self._make_incoming(room, event.sender, event.body, [att])
        logger.debug(
            "Message callback: queuing image from %s: %s", event.sender, event.body
        )
        await self.message_queue.put(msg)

    async def _on_invite(self, room: MatrixRoom, event: Any) -> None:
        logger.info("Auto-joining invited room %s", room.room_id)
        await self.client.join(room.room_id)

    def _on_key_verification(self, event: Any) -> None:
        transaction_id = str(getattr(event, "transaction_id", "") or "")
        if not transaction_id:
            return

        event_type = event.__class__.__name__
        self._verification_events[transaction_id] = {
            "event": event_type,
            "sender": str(getattr(event, "sender", "") or ""),
            "timestamp": int(time.time() * 1000),
        }

        logger.info(
            "Verification event %s for tx %s from %s",
            event_type,
            transaction_id,
            getattr(event, "sender", ""),
        )
