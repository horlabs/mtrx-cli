"""
jsonrpc_server.py
-----------------
Implements the signal-cli JSON-RPC daemon interface.

Modes (identical to signal-cli):
  --socket [PATH]   UNIX-domain socket
  --tcp [HOST:PORT] TCP socket           (default localhost:7583)
    --http [HOST:PORT] HTTP endpoints:
                                        POST /api/v1/rpc    JSON-RPC (single + batch)
                                        GET  /api/v1/events SSE stream for incoming messages
                                        GET  /api/v1/check  health check (200 when running)
                                        default localhost:8080

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
from typing import Any, Dict, List, cast

from aiohttp import web

from .command_handler import CommandHandler
from .matrix_backend import IncomingMessage, MatrixBackend
from .utils import msg_to_envelope

logger = logging.getLogger(__name__)

JSONRPC = "2.0"


def _log_wire_out(channel: str, target: str, text: str) -> None:
    """Log exact outgoing wire payload (including framing/newlines)."""
    logger.debug("OUT [%s -> %s] %r", channel, target, text)


def _ok(id_: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": JSONRPC, "id": id_, "result": result}


def _err(id_: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": JSONRPC, "id": id_, "error": err}


def _notification(method: str, params: Any) -> Dict[str, Any]:
    return {"jsonrpc": JSONRPC, "method": method, "params": params}


# _msg_to_envelope is provided by utils to avoid import cycles.


class JsonRpcServer:
    def __init__(self, backend: MatrixBackend, account: str | None = None):
        self.backend = backend
        self.account = account or backend.user_id
        self.handler = CommandHandler(backend, self.account)
        self._writer_lock = asyncio.Lock()
        self._subscriptions: Dict[int, asyncio.Task[Any]] = {}

    # ------------------------------------------------------------------
    # Dispatch a single request dict → result dict
    # ------------------------------------------------------------------

    async def dispatch(self, req: Dict[str, Any]) -> Dict[str, Any] | None:
        method = req.get("method", "")
        params = cast(Dict[str, Any], req.get("params") or {})
        req_id = req.get("id")

        # Notification (no id) → fire and forget
        if req_id is None and method:
            logger.debug("JSON-RPC notification: %s", method)
            asyncio.create_task(self.handler.handle(method, params, self.account))
            return None

        try:
            logger.debug("JSON-RPC request: id=%s method=%s", req_id, method)
            result = await self.handler.handle(method, params, self.account)
            logger.debug("JSON-RPC response: id=%s", req_id)
            return _ok(req_id, result)
        except NotImplementedError as e:
            logger.warning("JSON-RPC method not found: %s", method)
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
                parse_error = _err(None, -32700, f"Parse error: {e}")
                line_out = json.dumps(parse_error) + "\n"
                _log_wire_out("stdio", "stdout", line_out)
                sys.stdout.write(line_out)
                sys.stdout.flush()
                continue

            response = await self.dispatch(cast(Dict[str, Any], req))
            if response is not None:
                line_out = json.dumps(response) + "\n"
                _log_wire_out("stdio", "stdout", line_out)
                sys.stdout.write(line_out)
                sys.stdout.flush()

    async def _forward_notifications_stdio(self) -> None:
        while True:
            msg: IncomingMessage = await self.backend.message_queue.get()
            notification = _notification("receive", msg_to_envelope(msg, self.account))
            line_out = json.dumps(notification) + "\n"
            _log_wire_out("stdio", "stdout", line_out)
            sys.stdout.write(line_out)
            sys.stdout.flush()

    # ------------------------------------------------------------------
    # TCP / UNIX socket daemon
    # ------------------------------------------------------------------

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        peer_name = str(peer) if peer is not None else "unknown"

        async def send(obj: Dict[str, Any]) -> None:
            line_out = json.dumps(obj) + "\n"
            _log_wire_out("socket", peer_name, line_out)
            writer.write(line_out.encode())
            await writer.drain()

        # Push incoming Matrix messages to this client
        async def push_loop() -> None:
            while True:
                msg = await self.backend.message_queue.get()
                notification = _notification(
                    "receive", msg_to_envelope(msg, self.account)
                )
                try:
                    await send(notification)
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
                resp = await self.dispatch(cast(Dict[str, Any], req))
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
        # signal-cli-compatible HTTP endpoints + compatibility aliases.
        app.router.add_post("/api/v1/rpc", self._http_handler)
        app.router.add_post("/api/v1/rpc/", self._http_handler)
        app.router.add_post("/", self._http_handler)
        app.router.add_get("/api/v1/events", self._http_events)
        app.router.add_get("/api/v1/check", self._http_check)
        app.router.add_get("/", self._http_index)

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
            logger.debug("JSON parse error: %s", e)
            return web.json_response(
                _err(None, -32700, f"Parse error: {e}"), status=400
            )

        # Support JSON-RPC single and batch requests.
        if isinstance(req, list):
            if not req:
                return web.json_response(
                    _err(None, -32600, "Invalid Request"), status=400
                )

            logger.debug(
                "JSON-RPC batch request: %s", json.dumps(req, separators=(",", ":"))
            )
            responses: List[Dict[str, Any]] = []
            req_list = cast(List[Dict[str, Any]], req)
            for item in req_list:
                response = await self.dispatch(item)
                if response is not None:
                    responses.append(response)

            if not responses:
                return web.Response(status=204)
            resp_json = json.dumps(responses, separators=(",", ":"))
            logger.debug("JSON-RPC batch response: %s", resp_json)
            return web.json_response(responses)

        if not isinstance(req, dict):
            return web.json_response(_err(None, -32600, "Invalid Request"), status=400)

        req_json = json.dumps(req, separators=(",", ":"))
        logger.debug("JSON-RPC request: %s", req_json)
        response = await self.dispatch(cast(Dict[str, Any], req))
        if response is None:
            return web.Response(status=204)
        resp_json = json.dumps(response, separators=(",", ":"))
        logger.debug("JSON-RPC response: %s", resp_json)
        return web.json_response(response)

    async def _http_check(self, request: web.Request) -> web.Response:
        return web.Response(status=200, text="OK")

    async def _http_events(self, request: web.Request) -> web.StreamResponse:
        client_addr = request.remote or "unknown"
        logger.debug(
            "SSE client connected: %s, queue size: %d",
            client_addr,
            self.backend.message_queue.qsize(),
        )

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)

        # signal-cli style: keepalive comment line.
        frame = ":\n"
        _log_wire_out("sse", client_addr, frame)
        await response.write(frame.encode())
        logger.debug("SSE initial keepalive sent to %s", client_addr)

        keepalive_count = 0
        try:
            while True:
                try:
                    msg: IncomingMessage = await asyncio.wait_for(
                        self.backend.message_queue.get(), timeout=15.0
                    )
                    logger.debug(
                        "SSE sending message to %s from %s: %s",
                        client_addr,
                        msg.sender,
                        msg.body[:50],
                    )
                except asyncio.TimeoutError:
                    # Keep the SSE stream active and visible for curl -N.
                    keepalive_count += 1
                    if keepalive_count % 4 == 0:  # Log every 4th keepalive (60s)
                        logger.debug(
                            "SSE keepalive #%d to %s, queue size: %d",
                            keepalive_count,
                            client_addr,
                            self.backend.message_queue.qsize(),
                        )
                    frame = ":\n"
                    _log_wire_out("sse", client_addr, frame)
                    await response.write(frame.encode())
                    continue

                payload_obj = msg_to_envelope(msg, self.account)
                payload = json.dumps(payload_obj, separators=(",", ":"))
                frame = f"event: receive\ndata: {payload}\n\n"
                _log_wire_out("sse", client_addr, frame)
                await response.write(frame.encode())
        except (asyncio.CancelledError, ConnectionResetError, RuntimeError) as e:
            logger.debug(
                "SSE client %s disconnected: %s", client_addr, type(e).__name__
            )
        finally:
            try:
                await response.write_eof()
            except Exception:
                pass
            logger.debug("SSE stream ended for %s", client_addr)
        return response

    async def _http_index(self, request: web.Request) -> web.Response:
        payload: Dict[str, Any] = {
            "name": "mtrx-cli",
            "mode": "json-rpc",
            "endpoints": {
                "post": ["/api/v1/rpc", "/api/v1/rpc/", "/"],
                "get": ["/api/v1/check", "/api/v1/events", "/"],
            },
        }
        return web.json_response(payload)
