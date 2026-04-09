"""Utility helpers shared across modules.

Extracted helpers that would otherwise create cyclic imports when
imported at module level (e.g. between `command_handler` and
`jsonrpc_server`).
"""

from __future__ import annotations

from typing import Any, Dict

from .matrix_backend import IncomingMessage


def msg_to_envelope(msg: IncomingMessage, account: str) -> Dict[str, Any]:
    """Convert IncomingMessage → signal-cli envelope notification format."""
    envelope: dict[str, Any] = {
        "timestamp": msg.timestamp,
        "source": msg.sender,
        "sourceName": msg.sender_name,
        "sourceDevice": 1,
    }
    data_message: dict[str, Any] = {
        "timestamp": msg.timestamp,
        "message": msg.body,
        "expiresInSeconds": 0,
        "attachments": msg.attachments,
    }
    if msg.is_group:
        data_message["groupInfo"] = {
            "groupId": msg.group_id,
            "groupName": msg.room_name,
            "type": "DELIVER",
        }
    envelope["dataMessage"] = data_message
    return {"envelope": envelope, "account": account}
