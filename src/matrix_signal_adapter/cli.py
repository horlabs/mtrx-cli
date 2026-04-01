#!/usr/bin/env python3
"""
signal-cli compatible CLI wrapper backed by Matrix.

Usage (mirrors signal-cli):
  matrix-signal-adapter [--config CONFIG] [-a ACCOUNT] <command> [options]

Commands:
    send        -m TEXT [RECIPIENT ...] [-g GROUP] [--attachment FILE ...]
  receive     [--timeout SECONDS]
  listGroups  [-d]
  createGroup -n NAME [-m MEMBER ...]
  updateGroup -g GROUP [-n NAME] [-m MEMBER ...]
  quitGroup   -g GROUP
  joinGroup   --uri URI
  listContacts
  updateProfile --name NAME
  sendTyping  (-r ROOM | -g GROUP) [--stop]
  sendReaction (-r ROOM | -g GROUP) -e EMOJI --target-timestamp TS
  deleteMessage (-r ROOM | -g GROUP) --target-timestamp TS
  version
  daemon      [--socket [PATH]] [--tcp [HOST:PORT]] [--http [HOST:PORT]]
  jsonRpc     (reads JSON-RPC from stdin, writes to stdout)

Account flags (analogous to signal-cli):
  --config  path to config directory (default: ~/.config/matrix-signal-adapter)
  -a/--account  Matrix user-id, e.g. @user:matrix.org

Configuration file  (~/.config/matrix-signal-adapter/<account>.json):
  {
    "homeserver": "https://matrix.org",
    "user_id":    "@user:matrix.org",
    "password":   "secret",          // or use access_token
    "access_token": "syt_..."        // preferred: avoids storing password
  }
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from .command_handler import CommandHandler
from .jsonrpc_server import JsonRpcServer
from .matrix_backend import MatrixBackend

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_DIR = Path("~/.config/matrix-signal-adapter").expanduser()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _load_config(config_dir: Path, account: str) -> dict:
    """Load JSON config file for the given account."""
    safe = account.lstrip("@").replace(":", "_").replace("/", "_")
    path = config_dir / f"{safe}.json"
    if not path.exists():
        sys.exit(
            f"Config file not found: {path}\n"
            "Create it with:\n"
            '  {"homeserver":"https://matrix.org","user_id":"@you:matrix.org",'
            '"password":"secret"}'
        )
    with open(path) as f:
        return json.load(f)


def _build_backend(cfg: dict) -> MatrixBackend:
    return MatrixBackend(
        homeserver=cfg["homeserver"],
        user_id=cfg["user_id"],
        password=cfg.get("password"),
        access_token=cfg.get("access_token"),
        enable_e2ee=cfg.get("enable_e2ee"),
    )


# ---------------------------------------------------------------------------
# Argument parser (mirrors signal-cli options)
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="matrix-signal-adapter",
        description="signal-cli compatible interface backed by Matrix",
    )
    p.add_argument("--config", default=str(DEFAULT_CONFIG_DIR), metavar="CONFIG")
    p.add_argument("-a", "--account", metavar="ACCOUNT", help="Matrix user-id")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--output", choices=["plain-text", "json"], default="plain-text")

    sub = p.add_subparsers(dest="command", metavar="COMMAND")

    # ---- send ----
    sp = sub.add_parser("send", help="Send a message")
    sp.add_argument("-m", "--message", metavar="TEXT")
    sp.add_argument("-r", "--recipient", metavar="RECIPIENT")
    sp.add_argument("recipients", nargs="*", metavar="RECIPIENT")
    sp.add_argument("-g", "--group-id", metavar="GROUP")
    sp.add_argument("-a", "--attachment", metavar="FILE", action="append", default=[])
    sp.add_argument("--message-from-stdin", action="store_true")

    # ---- receive ----
    sp = sub.add_parser("receive", help="Receive pending messages")
    sp.add_argument("--timeout", type=float, default=3.0, metavar="SECONDS")

    # ---- listGroups ----
    sp = sub.add_parser("listGroups", help="List Matrix rooms")
    sp.add_argument("-d", "--detailed", action="store_true")

    # ---- createGroup ----
    sp = sub.add_parser("createGroup", help="Create a Matrix room")
    sp.add_argument("-n", "--name", required=True)
    sp.add_argument("-m", "--member", action="append", default=[], metavar="MEMBER")

    # ---- updateGroup ----
    sp = sub.add_parser("updateGroup", help="Update room members / name")
    sp.add_argument("-g", "--group-id", required=True, metavar="GROUP")
    sp.add_argument("-n", "--name", metavar="NAME")
    sp.add_argument("-m", "--member", action="append", default=[], metavar="MEMBER")
    sp.add_argument("--remove-member", action="append", default=[], metavar="MEMBER")

    # ---- quitGroup ----
    sp = sub.add_parser("quitGroup", help="Leave a Matrix room")
    sp.add_argument("-g", "--group-id", required=True, metavar="GROUP")

    # ---- joinGroup ----
    sp = sub.add_parser("joinGroup", help="Join a room by alias or invite-link")
    sp.add_argument("--uri", required=True)

    # ---- listContacts ----
    sub.add_parser("listContacts", help="List known Matrix users")

    # ---- updateProfile ----
    sp = sub.add_parser("updateProfile", help="Update display name")
    sp.add_argument("--name", required=True)

    # ---- sendTyping ----
    sp = sub.add_parser("sendTyping", help="Send typing notification")
    sp.add_argument("-r", "--recipient", metavar="ROOM")
    sp.add_argument("-g", "--group-id", metavar="GROUP")
    sp.add_argument("--stop", action="store_true")

    # ---- sendReaction ----
    sp = sub.add_parser("sendReaction", help="Send a reaction")
    sp.add_argument("-r", "--recipient", metavar="ROOM")
    sp.add_argument("-g", "--group-id", metavar="GROUP")
    sp.add_argument("-e", "--emoji", required=True)
    sp.add_argument("--target-timestamp", required=True, metavar="TS")

    # ---- deleteMessage ----
    sp = sub.add_parser("deleteMessage", help="Delete (redact) a message")
    sp.add_argument("-r", "--recipient", metavar="ROOM")
    sp.add_argument("-g", "--group-id", metavar="GROUP")
    sp.add_argument("--target-timestamp", required=True, metavar="TS")

    # ---- version ----
    sub.add_parser("version", help="Print version")

    # ---- daemon ----
    sp = sub.add_parser("daemon", help="Run as daemon")
    sp.add_argument("--socket", nargs="?", const=True, metavar="PATH")
    sp.add_argument("--tcp", nargs="?", const="localhost:7583", metavar="HOST:PORT")
    sp.add_argument("--http", nargs="?", const="localhost:8080", metavar="HOST:PORT")

    # ---- jsonRpc ----
    sub.add_parser("jsonRpc", help="JSON-RPC via stdin/stdout")

    return p


# ---------------------------------------------------------------------------
# Run helpers
# ---------------------------------------------------------------------------


def _output(obj: Any, mode: str = "plain-text") -> None:
    if mode == "json":
        print(json.dumps(obj, indent=2))
    else:
        if isinstance(obj, list):
            for item in obj:
                print(item)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                print(f"{k}: {v}")
        else:
            print(obj)


async def _run(args: argparse.Namespace) -> None:
    config_dir = Path(args.config)
    if not config_dir.exists():
        config_dir.mkdir(parents=True, exist_ok=True)

    # --- version is config-free ---
    if args.command == "version":
        from .command_handler import ADAPTER_NAME, VERSION

        print(f"{ADAPTER_NAME} {VERSION} (Matrix backend)")
        return

    if not args.account:
        sys.exit(
            "Please specify an account with -a / --account  (e.g. @you:matrix.org)"
        )

    cfg = _load_config(config_dir, args.account)
    backend = _build_backend(cfg)

    await backend.login()

    handler = CommandHandler(backend, args.account)
    output_mode = args.output

    try:
        cmd = args.command

        if cmd == "send":
            msg = args.message or ""
            if args.message_from_stdin:
                msg = sys.stdin.read()
            recipients = list(getattr(args, "recipients", []) or [])
            if args.recipient and args.recipient not in recipients:
                recipients.insert(0, args.recipient)
            result = await handler.handle(
                "send",
                {
                    "recipient": recipients[0] if recipients else None,
                    "recipients": recipients,
                    "groupId": getattr(args, "group_id", None),
                    "message": msg,
                    "attachments": args.attachment,
                },
                args.account,
            )
            _output(result, output_mode)

        elif cmd == "receive":
            await backend.start_daemon()
            try:
                result = await handler.handle(
                    "receive", {"timeout": args.timeout}, args.account
                )
            finally:
                await backend.stop_daemon()
            _output(result, "json")  # receive always outputs JSON envelopes

        elif cmd == "listGroups":
            result = await handler.handle("listGroups", {}, args.account)
            _output(result, output_mode)

        elif cmd == "createGroup":
            result = await handler.handle(
                "createGroup",
                {
                    "name": args.name,
                    "members": args.member,
                },
                args.account,
            )
            _output(result, output_mode)

        elif cmd == "updateGroup":
            result = await handler.handle(
                "updateGroup",
                {
                    "groupId": args.group_id,
                    "name": getattr(args, "name", None),
                    "members": args.member,
                    "removeMembers": args.remove_member,
                },
                args.account,
            )

        elif cmd == "quitGroup":
            await handler.handle("quitGroup", {"groupId": args.group_id}, args.account)

        elif cmd == "joinGroup":
            result = await handler.handle("joinGroup", {"uri": args.uri}, args.account)
            _output(result, output_mode)

        elif cmd == "listContacts":
            result = await handler.handle("listContacts", {}, args.account)
            _output(result, output_mode)

        elif cmd == "updateProfile":
            await handler.handle("updateProfile", {"name": args.name}, args.account)

        elif cmd == "sendTyping":
            room = getattr(args, "group_id", None) or args.recipient
            await handler.handle(
                "sendTyping", {"recipient": room, "stop": args.stop}, args.account
            )

        elif cmd == "sendReaction":
            room = getattr(args, "group_id", None) or args.recipient
            await handler.handle(
                "sendReaction",
                {
                    "recipient": room,
                    "emoji": args.emoji,
                    "targetTimestamp": args.target_timestamp,
                },
                args.account,
            )

        elif cmd == "deleteMessage":
            room = getattr(args, "group_id", None) or args.recipient
            await handler.handle(
                "deleteMessage",
                {
                    "recipient": room,
                    "targetTimestamp": args.target_timestamp,
                },
                args.account,
            )

        elif cmd == "daemon":
            server = JsonRpcServer(backend, args.account)

            if args.socket:
                socket_path = (
                    args.socket
                    if isinstance(args.socket, str)
                    else f"/tmp/matrix-signal-{args.account.lstrip('@').replace(':', '_')}.sock"
                )
                await server.run_socket(socket_path)

            elif args.tcp:
                host, _, port_str = (
                    args.tcp if args.tcp != True else "localhost:7583"
                ).partition(":")
                await server.run_tcp(host, int(port_str or 7583))

            elif args.http:
                host, _, port_str = (
                    args.http if args.http != True else "localhost:8080"
                ).partition(":")
                await server.run_http(host, int(port_str or 8080))

            else:
                # Default: UNIX socket
                socket_path = f"/tmp/matrix-signal-{args.account.lstrip('@').replace(':', '_')}.sock"
                await server.run_socket(socket_path)

        elif cmd == "jsonRpc":
            server = JsonRpcServer(backend, args.account)
            await server.run_stdio()

        else:
            sys.exit(f"Unknown command: {cmd}")

    finally:
        await backend.logout()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
