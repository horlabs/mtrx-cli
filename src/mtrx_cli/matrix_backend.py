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

        # Register callbacks
        self.client.add_event_callback(self._on_message_text, RoomMessageText)
        self.client.add_event_callback(self._on_message_file, RoomMessageFile)
        self.client.add_event_callback(self._on_message_image, RoomMessageImage)
        self.client.add_event_callback(self._on_invite, InviteEvent) # type: ignore

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    async def login(self) -> None:
        if self._access_token:
            self.client.access_token = self._access_token
            self.client.user_id = self.user_id
            whoami = await self.client.whoami()
            whoami_user = getattr(whoami, "user_id", None)
            if not whoami_user:
                detail = getattr(whoami, "message", "unknown error")
                raise RuntimeError(
                    "Matrix access token validation failed. "
                    f"Check homeserver URL and token. Details: {detail}"
                )
            if whoami_user != self.user_id:
                raise RuntimeError(
                    "Matrix access token belongs to a different user. "
                    f"Configured: {self.user_id}, token user: {whoami_user}"
                )
            self._logged_in = True
            logger.info("Logged in via access token")
            return

        if not self._password:
            raise ValueError("Either password or access_token must be provided")

        resp = await self.client.login(
            password=self._password,
            device_name=self.device_name,
        )
        if isinstance(resp, LoginError):
            raise RuntimeError(f"Matrix login failed: {resp.message}")

        self._logged_in = True
        logger.info("Logged in as %s (device: %s)", self.user_id, resp.device_id)

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
            rooms.append(
                {
                    "id": room_id,
                    "name": room.display_name or room_id,
                    "members": members,
                    "member_count": len(members),
                    "is_direct": bool(getattr(room, "is_direct", False)),
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
                    logger.warning("Sync daemon task ended with error; restarting: %s", exc)
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

    def _make_incoming(
        self,
        room: MatrixRoom,
        sender: str,
        body: str,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> IncomingMessage:
        members = list(room.users.keys())
        is_group = len(members) > 2
        return IncomingMessage(
            timestamp=int(time.time() * 1000),
            sender=sender,
            sender_name=(room.user_name(sender) or sender) if sender in room.users else sender,
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
