from __future__ import annotations

import json
from datetime import datetime, timezone
from io import BytesIO
from unittest import mock
from urllib.error import HTTPError

from runtime.domain.enums import WorkerMode
from runtime.infrastructure.http.srm.heartbeat import (
    SrmHeartbeatHttpClient,
    build_srm_heartbeat_body,
    build_srm_heartbeat_headers,
    build_srm_status_body_from_signal_envelope,
    health_status_for_local_state,
    join_heartbeat_url,
)


def test_health_status_mapping() -> None:
    assert health_status_for_local_state("RUNNING") == "HEALTHY"
    assert health_status_for_local_state("INITIALIZING") == "HEALTHY"
    assert health_status_for_local_state("FAILED") == "UNHEALTHY"
    assert health_status_for_local_state("DEGRADED") == "UNHEALTHY"


def test_build_srm_heartbeat_body_matches_contract() -> None:
    observed = datetime(2026, 5, 19, 8, 39, 15, 60684, tzinfo=timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id="f83b8db3-3584-441a-9564-a3dfc7415d97",
        mode="PAPER",
        owner_resource_id="8248a6bd-0143-4ddd-9206-a7f1581dda64",
        local_state="RUNNING",
        observed_at=observed,
    )
    assert body["runtime_type"] == "STRATEGY_WORKER_RUNTIME"
    assert body["runtime_status"] == "RUNNING"
    assert body["owner_resource_id"] == "8248a6bd-0143-4ddd-9206-a7f1581dda64"
    assert body["health_status"] == "HEALTHY"
    assert body["source"] == "HEARTBEAT"
    assert body["metadata"] == {"reason_code": "", "message": ""}
    assert "deployment_id" not in body
    assert "worker_status" not in body


def test_build_srm_heartbeat_headers_use_uuid_and_service_name() -> None:
    headers = build_srm_heartbeat_headers()
    assert headers["x-service-name"] == "strategy-worker-runtime"
    assert headers["x-request-id"] == headers["x-correlation-id"]
    assert len(headers["x-request-id"]) == 36


def test_join_heartbeat_url() -> None:
    url = join_heartbeat_url(
        "http://strategy-runtime-manager.alphovex.svc.cluster.local:8080",
        "f83b8db3-3584-441a-9564-a3dfc7415d97",
    )
    assert url.endswith(
        "/internal/v1/runtimes/f83b8db3-3584-441a-9564-a3dfc7415d97/status"
    )


def test_srm_heartbeat_client_posts_json_without_authorization() -> None:
    client = SrmHeartbeatHttpClient(
        base_url="http://127.0.0.1:8080",
        owner_resource_id="dep-1",
    )
    observed = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
    envelope = {
        "signal_type": "heartbeat",
        "identity": {
            "runtime_id": "rt-1",
            "mode": WorkerMode.PAPER.value,
            "launch_attempt": 3,
        },
        "payload": {"local_state": "RUNNING", "observed_at": observed},
    }

    captured: dict[str, object] = {}

    def fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.header_items())
        captured["body"] = json.loads(req.data.decode("utf-8"))
        response = mock.Mock(status=200, read=lambda: b'{"success":true}')
        response.__enter__ = mock.Mock(return_value=response)
        response.__exit__ = mock.Mock(return_value=False)
        return response

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        result = client.emit_signal(envelope)

    assert result["accepted"] is True
    assert "Authorization" not in {k.title() for k in captured["headers"]}
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["owner_resource_id"] == "dep-1"
    assert body["metadata"] == {"reason_code": "", "message": ""}
    assert captured["url"] == join_heartbeat_url("http://127.0.0.1:8080", "rt-1")


def test_srm_heartbeat_skipped_for_backtest_mode() -> None:
    client = SrmHeartbeatHttpClient(
        base_url="http://127.0.0.1:8080",
        owner_resource_id="dep-1",
    )
    observed = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
    with mock.patch("urllib.request.urlopen") as urlopen:
        result = client.emit_signal(
            {
                "signal_type": "heartbeat",
                "identity": {"runtime_id": "rt-1", "mode": "BACKTEST"},
                "payload": {"local_state": "RUNNING", "observed_at": observed},
            }
        )
    urlopen.assert_not_called()
    assert result.get("skipped") is True


def test_srm_heartbeat_maps_http_error() -> None:
    client = SrmHeartbeatHttpClient(
        base_url="http://127.0.0.1:8080",
        owner_resource_id="dep-1",
    )
    observed = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
    err = HTTPError(
        url="http://127.0.0.1:8080/internal/v1/runtimes/rt-1/heartbeat",
        code=404,
        msg="Not Found",
        hdrs=None,
        fp=BytesIO(b'{"message":"missing"}'),
    )

    with mock.patch("urllib.request.urlopen", side_effect=err):
        result = client.emit_signal(
            {
                "signal_type": "heartbeat",
                "identity": {"runtime_id": "rt-1", "mode": "PAPER"},
                "payload": {"local_state": "RUNNING", "observed_at": observed},
            }
        )

    assert result["accepted"] is False
    assert result["reason_code"] == "RUNTIME_NOT_FOUND"


def test_bootstrap_succeeded_posts_runtime_status_report_shape() -> None:
    client = SrmHeartbeatHttpClient(
        base_url="http://127.0.0.1:8080",
        owner_resource_id="dep-1",
    )
    observed = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
    envelope = {
        "signal_type": "bootstrap_succeeded",
        "identity": {
            "runtime_id": "rt-1",
            "mode": WorkerMode.PAPER.value,
            "launch_attempt": 1,
        },
        "payload": {
            "local_state": "READY",
            "occurred_at": observed,
            "details": {"entrypoint": "sma_crossover:SMACrossOver"},
        },
    }
    captured: dict[str, object] = {}

    def fake_urlopen(req, timeout=0):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        response = mock.Mock(status=200, read=lambda: b'{"success":true}')
        response.__enter__ = mock.Mock(return_value=response)
        response.__exit__ = mock.Mock(return_value=False)
        return response

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        result = client.emit_signal(envelope)

    assert result["accepted"] is True
    body = build_srm_status_body_from_signal_envelope(
        envelope, owner_resource_id="dep-1"
    )
    assert captured["body"] == body
    assert body["runtime_type"] == "STRATEGY_WORKER_RUNTIME"
    assert body["runtime_status"] == "RUNNING"
    assert body["health_status"] == "HEALTHY"
    assert body["source"] == "UPDATE"
    assert body["owner_resource_id"] == "dep-1"
    assert body["metadata"] == {}
    assert "event_name" not in body
    assert "payload" not in body
