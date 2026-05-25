from __future__ import annotations

import json
import threading
from http.client import HTTPConnection
from runtime.transport.http.stop_control_server import StopControlHttpServer
from runtime.transport.internal.auth import (
    TRUST_CLASS_CONTROL,
)


def test_post_internal_v1_stop_returns_stopping_body() -> None:
    stopping_body = {
        "runtime_id": "rt-http-1",
        "runtime_status": "STOPPING",
        "health_status": "UNKNOWN",
        "metadata": {"reason_code": "SWR_STOP_REQUESTED", "message": "Stopping."},
    }
    accepted = threading.Event()

    def _on_stop(reason: str) -> dict[str, object]:
        assert reason == "scale_in"
        accepted.set()
        return stopping_body

    server = StopControlHttpServer(
        bind_address="127.0.0.1:0",
        on_stop_requested=_on_stop,
    )
    server.start()
    try:
        _host, port = server._server.server_address  # noqa: SLF001
        conn = HTTPConnection("127.0.0.1", port, timeout=5)
        payload = json.dumps({"reason": "scale_in"}).encode("utf-8")
        conn.request(
            "POST",
            "/internal/v1/stop",
            body=payload,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
                "x-internal-caller": "strategy-runtime-manager",
                "x-internal-trust-class": TRUST_CLASS_CONTROL,
            },
        )
        response = conn.getresponse()
        raw = response.read()
        conn.close()
        assert response.status == 200
        assert json.loads(raw.decode("utf-8")) == stopping_body
        assert accepted.wait(timeout=2.0)
    finally:
        server.stop()
