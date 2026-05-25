"""``POST /internal/v1/stop`` — accept manager stop and return STOPPING status update JSON."""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from runtime.domain.errors import WorkerInternalCallerNotAllowedError
from runtime.transport.internal.auth import (
    parse_internal_auth_metadata,
    require_internal_control,
)

_LOG = logging.getLogger(__name__)

_STOP_PATH = "/internal/v1/stop"


class StopControlHttpServer:
    """
      Internal HTTP server for workload stop.

      ``POST /internal/v1/stop`` body: ``{"reason": "<string>"}``.
      Response body: SRM status update JSON for ``runtime_status=STOPPING``.
    Shutdown continues asynchronously; final STOPPED/FAILED is reported to SRM when complete.
    """

    def __init__(
        self,
        *,
        bind_address: str,
        on_stop_requested: Callable[[str], dict[str, Any]],
        on_stop_rejected: Callable[[str, str], dict[str, Any]] | None = None,
    ) -> None:
        self._bind_address = bind_address.strip()
        self._on_stop_requested = on_stop_requested
        self._on_stop_rejected = on_stop_rejected
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._host = ""
        self._port = 0

    @property
    def listen_address(self) -> str:
        if self._port > 0:
            return f"{self._host}:{self._port}"
        return self._bind_address

    def start(self) -> None:
        if self._server is not None:
            return
        handler_factory = self._build_handler()
        self._server = ThreadingHTTPServer(
            self._parse_bind(self._bind_address),
            handler_factory,
        )
        self._host, self._port = self._server.server_address
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="swr-stop-http",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        server = self._server
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=timeout_seconds)
        self._server = None
        self._thread = None

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        on_stop = self._on_stop_requested
        on_reject = self._on_stop_rejected

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                _LOG.debug("stop_http " + format, *args)

            def do_POST(self) -> None:  # noqa: N802
                if self.path.rstrip("/") != _STOP_PATH.rstrip("/"):
                    self._json_response(404, {"error": "not_found"})
                    return
                try:
                    auth = parse_internal_auth_metadata(dict(self.headers))
                    require_internal_control(auth, operation="POST /internal/v1/stop")
                except WorkerInternalCallerNotAllowedError as exc:
                    self._json_response(
                        403,
                        {
                            "error": "forbidden",
                            "message": str(exc),
                        },
                    )
                    return
                length = int(self.headers.get("Content-Length", "0") or 0)
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    self._json_response(
                        400,
                        {
                            "error": "invalid_json",
                            "message": "body must be JSON object",
                        },
                    )
                    return
                if not isinstance(payload, dict):
                    self._json_response(
                        400,
                        {
                            "error": "invalid_body",
                            "message": "body must be JSON object",
                        },
                    )
                    return
                reason = str(payload.get("reason", "") or "")
                try:
                    body = on_stop(reason)
                except StopAlreadyInProgress as exc:
                    reject_body = (
                        on_reject("conflict", str(exc))
                        if on_reject is not None
                        else {"error": "conflict", "message": str(exc)}
                    )
                    self._json_response(409, reject_body)
                    return
                except WorkerAlreadyStopped as exc:
                    reject_body = (
                        on_reject("already_stopped", str(exc))
                        if on_reject is not None
                        else {"error": "already_stopped", "message": str(exc)}
                    )
                    self._json_response(409, reject_body)
                    return
                self._json_response(200, body)

            def _json_response(self, status: int, body: dict[str, Any]) -> None:
                raw = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode(
                    "utf-8"
                )
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        return _Handler

    @staticmethod
    def _parse_bind(bind_address: str) -> tuple[str, int]:
        host, sep, port_str = bind_address.rpartition(":")
        if not sep:
            raise ValueError(
                f"Invalid bind address (expected host:port): {bind_address!r}"
            )
        port = int(port_str)
        return host, port


class StopAlreadyInProgress(RuntimeError):
    """Stop request received while shutdown is already in progress."""


class WorkerAlreadyStopped(RuntimeError):
    """Stop request received after worker shutdown completed."""
