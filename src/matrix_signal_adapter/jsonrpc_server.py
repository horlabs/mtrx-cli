"""
jsonrpc_server.py
-----------------
Implements the signal-cli JSON-RPC daemon interface.

Modes (identical to signal-cli):
  --socket [PATH]   UNIX-domain socket
  --tcp [HOST:PORT] TCP socket           (default localhost:7583)
  --http [HOST:PORT] HTTP endpoint at /api/v1/rpc  (default localhost:8080)

Wire format: newline-delimited JSON (same as signal-cli):
  Request:  {"jsonrpc":"2.0","id":1,"method":"send","params":{...}}
  Response: {"jsonrpc":"2.0","id":1,"result":{...}}
  Notification: {"jsonrpc":"2.0","method":"receive","params":{...}}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any, Callable, Coroutine

from aiohttp import web

from .matrix_backend import MatrixBackend, IncomingMessage
from .command_handler import CommandHandler

logger = logging.getLogger(__name__)

JSONRPC = "2.0"


def _ok(id_: Any, result: Any) -> dict:
    return {"jsonrpc": JSONRPC, "id": id_, "result": result}


def _err(id_: Any, code: int, message: str, data: Any = None) -> dict:
    err: dict = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": JSONRPC, "id": id_, "error": err}


def _notification(method: str, params: Any) -> dict:
    return {"jsonrpc": JSONRPC, "method": method, "params": params}


def _msg_to_envelope(msg: IncomingMessage) -> dict:
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
    return {"envelope": envelope, "account": "matrix"}


class JsonRpcServer:
    def __init__(self, backend: MatrixBackend, account: str | None = None):
        self.backend = backend
        self.account = account or backend.user_id
        self.handler = CommandHandler(backend, self.account)
        self._writer_lock = asyncio.Lock()
        self._subscriptions: dict[int, asyncio.Task] = {}

    # ------------------------------------------------------------------
    # Dispatch a single request dict → result dict
    # ------------------------------------------------------------------

    async def dispatch(self, req: dict) -> dict | None:
        method = req.get("method", "")
        params = req.get("params") or {}
        req_id = req.get("id")

        # Notification (no id) → fire and forget
        if req_id is None and method:
            asyncio.create_task(self.handler.handle(method, params, self.account))
            return None

        try:
            result = await self.handler.handle(method, params, self.account)
            return _ok(req_id, result)
        except NotImplementedError as e:
            return _err(req_id, -32601, f"Method not found: {method}", str(e))
        except Exception as e:  # noqa: BLE001
            logger.exception("Error handling %s", method)
            return _err(req_id, -32000, str(e))

    # ------------------------------------------------------------------
    # STDIN / STDOUT mode  (signal-cli jsonRpc)
    # ------------------------------------------------------------------

    async def run_stdio(self) -> None:
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        proto = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: proto, sys.stdin)
        await self.backend.start_daemon()

        # Background: forward incoming messages to stdout
        asyncio.create_task(self._forward_notifications_stdio())

        while True:
            try:
                line = await reader.readline()
            except Exception:
                break
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except json.JSONDecodeError as e:
                resp = _err(None, -32700, f"Parse error: {e}")
                sys.stdout.write(json.dumps(resp) + "\n")
                sys.stdout.flush()
                continue

            resp = await self.dispatch(req)
            if resp is not None:
                sys.stdout.write(json.dumps(resp) + "\n")
                sys.stdout.flush()

    async def _forward_notifications_stdio(self) -> None:
        while True:
            msg: IncomingMessage = await self.backend.message_queue.get()
            notif = _notification("receive", _msg_to_envelope(msg))
            sys.stdout.write(json.dumps(notif) + "\n")
            sys.stdout.flush()

    # ------------------------------------------------------------------
    # TCP / UNIX socket daemon
    # ------------------------------------------------------------------

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        async def send(obj: dict) -> None:
            writer.write((json.dumps(obj) + "\n").encode())
            await writer.drain()

        # Push incoming Matrix messages to this client
        async def push_loop() -> None:
            while True:
                msg = await self.backend.message_queue.get()
                notif = _notification("receive", _msg_to_envelope(msg))
                try:
                    await send(notif)
                except Exception:
                    break

        push_task = asyncio.create_task(push_loop())

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    req = json.loads(line)
                except json.JSONDecodeError as e:
                    await send(_err(None, -32700, f"Parse error: {e}"))
                    continue
                resp = await self.dispatch(req)
                if resp is not None:
                    await send(resp)
        finally:
            push_task.cancel()
            writer.close()

    async def run_socket(self, socket_path: str) -> None:
        """UNIX domain socket mode."""
        if os.path.exists(socket_path):
            os.unlink(socket_path)
        await self.backend.start_daemon()
        server = await asyncio.start_unix_server(self._handle_client, path=socket_path)
        logger.info("Listening on UNIX socket %s", socket_path)
        async with server:
            await server.serve_forever()

    async def run_tcp(self, host: str = "localhost", port: int = 7583) -> None:
        """TCP socket mode."""
        await self.backend.start_daemon()
        server = await asyncio.start_server(self._handle_client, host, port)
        logger.info("Listening on TCP %s:%d", host, port)
        async with server:
            await server.serve_forever()

    # ------------------------------------------------------------------
    # HTTP mode  (/api/v1/rpc)
    # ------------------------------------------------------------------

    async def run_http(self, host: str = "localhost", port: int = 8080) -> None:
        """HTTP JSON-RPC endpoint mode."""
        await self.backend.start_daemon()

        app = web.Application()
        app.router.add_post("/api/v1/rpc", self._http_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        logger.info("HTTP JSON-RPC listening on http://%s:%d/api/v1/rpc", host, port)

        # Keep running until interrupted
        stop_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        await stop_event.wait()
        await runner.cleanup()

    async def _http_handler(self, request: web.Request) -> web.Response:
        try:
            req = await request.json()
        except Exception as e:
            return web.json_response(_err(None, -32700, f"Parse error: {e}"), status=400)

        resp = await self.dispatch(req)
        if resp is None:
            return web.Response(status=204)
        return web.json_response(resp)
