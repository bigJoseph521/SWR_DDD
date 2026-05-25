from __future__ import annotations

from datetime import datetime, timezone
from unittest import mock

from runtime.transport.manager_client import SrmHttpManagerClient, build_manager_client


def _heartbeat_envelope() -> dict[str, object]:
    observed = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
    return {
        "signal_type": "heartbeat",
        "identity": {"runtime_id": "rt-1", "mode": "PAPER", "launch_attempt": 1},
        "payload": {"local_state": "RUNNING", "observed_at": observed},
    }


def test_build_manager_client_without_http_is_noop() -> None:
    client = build_manager_client()
    result = client.emit_signal(_heartbeat_envelope())
    assert result["accepted"] is True


def test_http_manager_client_routes_heartbeat_to_heartbeat_client() -> None:
    heartbeat_client = mock.Mock()
    heartbeat_client.emit_signal.return_value = {
        "accepted": True,
        "signal_type": "heartbeat",
    }
    lifecycle_client = mock.Mock()
    client = SrmHttpManagerClient(
        heartbeat_client=heartbeat_client,
        lifecycle_client=lifecycle_client,
    )
    envelope = _heartbeat_envelope()
    result = client.emit_signal(envelope)
    assert result["accepted"] is True
    heartbeat_client.emit_signal.assert_called_once_with(envelope)
    lifecycle_client.emit_signal.assert_not_called()


def test_http_manager_client_routes_lifecycle_to_lifecycle_client() -> None:
    heartbeat_client = mock.Mock()
    lifecycle_client = mock.Mock()
    lifecycle_client.emit_signal.return_value = {
        "accepted": True,
        "signal_type": "bootstrap_succeeded",
    }
    client = SrmHttpManagerClient(
        heartbeat_client=heartbeat_client,
        lifecycle_client=lifecycle_client,
    )
    envelope = {
        "signal_type": "bootstrap_succeeded",
        "identity": {"runtime_id": "rt-1", "launch_attempt": 1},
        "occurred_at": "2026-05-19T12:00:00Z",
        "payload": {},
    }
    result = client.emit_signal(envelope)
    assert result["accepted"] is True
    lifecycle_client.emit_signal.assert_called_once_with(envelope)
    heartbeat_client.emit_signal.assert_not_called()
