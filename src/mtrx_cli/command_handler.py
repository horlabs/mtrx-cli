"""
command_handler.py
------------------
Maps every signal-cli JSON-RPC / CLI command to its Matrix equivalent.

Supported commands (same names as signal-cli):
  send            → room_send text + attachments
  receive         → drain queue (manual mode)
  listGroups      → list joined rooms
  createGroup     → create room
  updateGroup     → rename room / invite members
  quitGroup       → leave room
  listContacts    → list room members across all rooms
  sendGroupMessage → alias for send with groupId
  joinGroup       → join room by alias or id
  updateProfile   → set display name
  getAvatar       → (stub)
  listDevices     → (stub)
  getUserStatus   → (stub, always "registered")
  deleteContact   → (stub)
  block / unblock → (stub)
  sendReceipt     → (stub, no-op)
  sendTyping      → send typing notification
  sendReaction    → send reaction (m.reaction)
  deleteMessage   → redact event
  listNumbers     → alias for listContacts
  version         → return adapter version
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Union, cast

from .matrix_backend import MatrixBackend
from .utils import msg_to_envelope

logger = logging.getLogger(__name__)

VERSION = "0.1.0"
ADAPTER_NAME = "mtrx-cli"

# Alias for RPC-style parameter mappings
Params = Mapping[str, Any]


def _recipient_to_room(params: Params) -> Optional[str]:
    """Extract room-id from signal-cli params (recipient, groupId, etc.)."""
    # Try common keys and coerce to str when present.
    for key in ("groupId", "room", "recipient"):
        val = params.get(key)
        if isinstance(val, str) and val:
            return val
    return None


def _normalize_recipients(params: Params) -> List[str]:
    """Collect recipients from signal-cli style params."""
    raw = params.get("recipients")
    recipients: List[str] = []
    if isinstance(raw, str):
        recipients = [raw]
    elif isinstance(raw, list):
        raw_list = cast(List[Any], raw)
        recipients = [str(x) for x in raw_list if x is not None]

    single = params.get("recipient")
    if isinstance(single, str) and single not in recipients:
        recipients = [single, *recipients]

    return recipients


class CommandHandler:
    """Dispatch Signal-CLI commands to Matrix backend implementations."""

    def __init__(self, backend: MatrixBackend, account: str):
        self.backend = backend
        self.account = account

        self._dispatch: Dict[str, Callable[[Params], Awaitable[Any]]] = {
            # Messaging
            "send": self._send,
            "sendGroupMessage": self._send,
            "sendMessage": self._send,
            # Receive
            "receive": self._receive,
            "subscribe": self._subscribe,
            "unsubscribe": self._unsubscribe,
            # Groups / Rooms
            "listGroups": self._list_groups,
            "createGroup": self._create_group,
            "updateGroup": self._update_group,
            "quitGroup": self._quit_group,
            "joinGroup": self._join_group,
            # Contacts / Users
            "listContacts": self._list_contacts,
            "listNumbers": self._list_contacts,
            "updateContact": self._update_contact_stub,
            "deleteContact": self._stub,
            "getUserStatus": self._get_user_status,
            "block": self._stub,
            "unblock": self._stub,
            # Profile
            "updateProfile": self._update_profile,
            "getAvatar": self._stub,
            # Reactions / Receipts / Typing
            "sendReaction": self._send_reaction,
            "sendReceipt": self._stub,
            "sendReadReceipt": self._stub,
            "sendTyping": self._send_typing,
            # Message management
            "deleteMessage": self._delete_message,
            # Device / Account
            "listDevices": self._list_devices,
            "version": self._version,
            # Misc
            "listIdentities": self._stub_list,
            "trust": self._stub,
            "setPin": self._stub,
            "removePin": self._stub,
        }

    # ------------------------------------------------------------------
    # Main dispatch entry point
    # ------------------------------------------------------------------

    async def handle(self, method: str, params: Params, _account: str) -> Any:
        """Invoke a mapped command handler by `method` name with `params`."""
        fn = self._dispatch.get(method)
        if fn is None:
            raise NotImplementedError(f"Command '{method}' not implemented")
        return await fn(params)

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    async def _send(self, params: Params) -> Dict[str, Any]:
        """
        signal-cli params:
          recipient / groupId  → room id / alias
          message              → text body
          attachment           → list of local file paths
        """
        group_id = params.get("groupId") or params.get("room")
        recipients = _normalize_recipients(params)

        if group_id and recipients:
            raise ValueError("Use either groupId or recipient(s), not both")

        if len(recipients) > 1:
            raise ValueError(
                "Only one recipient is currently supported for direct sends"
            )

        if group_id:
            if group_id.startswith(("!", "#")):
                room_id = group_id
            else:
                # Some clients pass DM recipients via groupId. Resolve those to a DM room.
                room_id = await self.backend.resolve_recipient_to_room(group_id)
        elif recipients:
            room_id = await self.backend.resolve_recipient_to_room(recipients[0])
        else:
            raise ValueError("No recipient or groupId specified")

        text = params.get("message") or params.get("body") or ""
        raw_attachments = params.get("attachments") or params.get("attachment")
        attachments_list: List[str] = []
        if raw_attachments is None:
            attachments_list = []
        elif isinstance(raw_attachments, str):
            attachments_list = [raw_attachments]
        elif isinstance(raw_attachments, list):
            attachments_list = [
                str(a) for a in cast(List[Any], raw_attachments) if a is not None
            ]
        else:
            attachments_list = [str(raw_attachments)]

        result = await self.backend.send_message(room_id, text, attachments_list)
        return {"timestamp": result.get("timestamp", 0), **result}

    # ------------------------------------------------------------------
    # Receive
    # ------------------------------------------------------------------

    async def _receive(self, params: Params) -> List[Dict[str, Any]]:
        """Drain the queue and return all pending envelopes (manual mode)."""
        # _msg_to_envelope is imported from utils at module level.

        envelopes: List[Dict[str, Any]] = []
        timeout = float(params.get("timeout", 3))

        # First pull: fetch immediately available server-side updates.
        await self.backend.sync_once(timeout_ms=0, full_state=True)

        while True:
            try:
                msg = self.backend.message_queue.get_nowait()
                envelopes.append(msg_to_envelope(msg, self.account))
            except asyncio.QueueEmpty:
                break

        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(
                    self.backend.message_queue.get(), timeout=min(remaining, 1.0)
                )
                envelopes.append(msg_to_envelope(msg, self.account))
            except asyncio.TimeoutError:
                # Keep polling until the full timeout has elapsed.
                continue

        # Fallback: one-time backfill when normal sync returned nothing.
        if not envelopes:
            await self.backend.sync_once(
                timeout_ms=0, full_state=True, reset_since=True
            )
            while True:
                try:
                    msg = self.backend.message_queue.get_nowait()
                    envelopes.append(msg_to_envelope(msg, self.account))
                except asyncio.QueueEmpty:
                    break
        return envelopes

    async def _subscribe(self, _params: Params) -> int:
        """Daemon-mode subscription (returns a subscription id)."""
        return 1  # single global subscription for now

    async def _unsubscribe(self, _params: Params) -> None:
        return None

    # ------------------------------------------------------------------
    # Groups / Rooms
    # ------------------------------------------------------------------

    async def _list_groups(self, _params: Params) -> List[Dict[str, Any]]:
        rooms: List[Dict[str, Any]] = await self.backend.list_rooms()
        result: List[Dict[str, Any]] = []
        for r in rooms:
            result.append(
                {
                    "id": r["id"],
                    "name": r["name"],
                    "isMember": True,
                    "isBlocked": False,
                    "memberCount": r["member_count"],
                    "members": r["members"],
                    "admins": [],
                    "pendingMembers": [],
                    "requestingMembers": [],
                    "inviteLink": None,
                    "messageExpirationTime": 0,
                }
            )
        return result

    async def _create_group(self, params: Params) -> Dict[str, str]:
        name = params.get("name", "")
        raw_members = cast(
            Union[str, List[Any]], params.get("members") or params.get("member") or []
        )
        members: List[str] = []
        if isinstance(raw_members, str):
            members = [raw_members]
        members = [str(m) for m in raw_members if m is not None]
        room_id = await self.backend.create_room(name=name, invite=members)
        return {"groupId": room_id}

    async def _update_group(self, params: Params) -> Dict[str, str]:
        """
        Supports:
          groupId       the room to update
          name          new display name  (TODO: room rename API)
          members       list of user_ids to invite
          removeMembers list of user_ids to kick
        """
        room_id = params.get("groupId") or params.get("room")
        if not room_id:
            raise ValueError("groupId required")

        # Invite new members
        raw_members = cast(
            Union[str, List[Any]], params.get("members") or params.get("member") or []
        )
        members: List[str] = []
        if isinstance(raw_members, str):
            members = [raw_members]
        members = [str(m) for m in raw_members if m is not None]
        for uid in members:
            await self.backend.invite_user(room_id, uid)

        # Kick removed members
        raw_remove = cast(List[Any], params.get("removeMembers") or [])
        remove_members: List[str] = []
        remove_members = [str(m) for m in raw_remove if m is not None]
        for uid in remove_members:
            await self.backend.kick_user(room_id, uid)

        return {"groupId": room_id}

    async def _quit_group(self, params: Params) -> None:
        room_id = params.get("groupId") or params.get("room")
        if not room_id:
            raise ValueError("groupId required")
        await self.backend.leave_room(room_id)

    async def _join_group(self, params: Params) -> Dict[str, str]:
        """Join by invite-link (= room alias) or room_id."""
        uri = params.get("uri") or params.get("inviteLink") or params.get("groupId")
        if not uri:
            raise ValueError("uri / inviteLink / groupId required")
        room_id = await self.backend.join_room(uri)
        return {"groupId": room_id}

    # ------------------------------------------------------------------
    # Contacts
    # ------------------------------------------------------------------

    async def _list_contacts(self, _params: Params) -> List[Dict[str, Any]]:
        rooms = await self.backend.list_rooms()
        seen: set[str] = set()
        contacts: List[Dict[str, Any]] = []
        for r in rooms:
            for member in r["members"]:
                if member not in seen and member != self.backend.user_id:
                    seen.add(member)
                    contacts.append(
                        {
                            "number": member,
                            "uuid": member,
                            "name": member,
                            "color": None,
                            "blocked": False,
                            "messageExpirationTime": 0,
                        }
                    )
        return contacts

    async def _update_contact_stub(self, _params: Params) -> None:
        return None  # contact name changes are server-side in Matrix

    async def _get_user_status(self, params: Params) -> List[Dict[str, Any]]:
        recipients = _normalize_recipients(params)
        return [{"recipient": r, "isRegistered": True} for r in recipients]

    # ------------------------------------------------------------------
    # Profile
    # ------------------------------------------------------------------

    async def _update_profile(self, params: Params) -> None:
        name = params.get("name") or params.get("givenName")
        if name:
            await self.backend.set_display_name(name)

    # ------------------------------------------------------------------
    # Reactions / Typing / Redact
    # ------------------------------------------------------------------

    async def _send_reaction(self, params: Params) -> None:
        """Send an m.reaction event."""
        room_id = _recipient_to_room(params)
        if room_id and not room_id.startswith(("!", "#")):
            room_id = await self.backend.resolve_recipient_to_room(room_id)
        if not room_id:
            return
        emoji = params.get("emoji", "👍")
        target_event_id = params.get("targetTimestamp") or params.get("targetEventId")

        content: Dict[str, Any] = {
            "m.relates_to": {
                "rel_type": "m.annotation",
                "event_id": str(target_event_id or ""),
                "key": emoji,
            }
        }
        await self.backend.client.room_send(
            room_id=room_id,
            message_type="m.reaction",
            content=content,
        )

    async def _send_typing(self, params: Params) -> None:
        room_id = _recipient_to_room(params)
        if room_id and not room_id.startswith(("!", "#")):
            room_id = await self.backend.resolve_recipient_to_room(room_id)
        if not room_id:
            return
        typing = not params.get("stop", False)
        await self.backend.client.room_typing(room_id, typing_state=typing)

    async def _delete_message(self, params: Params) -> None:
        room_id = _recipient_to_room(params)
        if room_id and not room_id.startswith(("!", "#")):
            room_id = await self.backend.resolve_recipient_to_room(room_id)
        event_id = params.get("targetTimestamp") or params.get("eventId")
        if room_id and event_id:
            await self.backend.client.room_redact(room_id, str(event_id))

    # ------------------------------------------------------------------
    # Device / Account stubs
    # ------------------------------------------------------------------

    async def _list_devices(self, _params: Params) -> List[Dict[str, Any]]:
        return [
            {
                "id": 1,
                "name": self.backend.device_name,
                "createdTimestamp": 0,
                "lastSeenTimestamp": 0,
            }
        ]

    async def _version(self, _params: Params) -> Dict[str, str]:
        return {
            "version": VERSION,
            "name": ADAPTER_NAME,
            "backend": "matrix",
        }

    # ------------------------------------------------------------------
    # Generic stubs
    # ------------------------------------------------------------------

    async def _stub(self, _params: Params) -> None:
        return None

    async def _stub_list(self, _params: Params) -> List[Any]:
        return []
