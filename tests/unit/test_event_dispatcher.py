from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest
from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
)
from runtime.integration.manager_gateway import ManagerGateway
from runtime.runtime.mode_policy import get_mode_policy


class _FakeManagerClient:
    def __init__(self) -> None:
        self.signals: list[dict[str, Any]] = []

    def emit_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.signals.append(payload)
        return {"accepted": True, "signal_type": payload["signal_type"]}


@dataclass(frozen=True, slots=True)
class _Identity:
    runtime_id: str
    tenant_id: str
    strategy_version_id: str
    mode: RuntimeMode
    launch_attempt: int
    account_id: str = "acct-1"
    trader_id: str | None = None


def _utc(second: int) -> datetime:
    return datetime(2026, 3, 29, 0, 0, second, tzinfo=timezone.utc)


def test_event_dispatcher_emits_execution_plane_signals_only() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-evt-1",
            tenant_id="tenant-1",
            strategy_version_id="sv-1",
            mode=RuntimeMode.PAPER,
            launch_attempt=2,
        ),
    )

    gateway.emit_bootstrap_succeeded(
        occurred_at=_utc(1), details={"entrypoint": "pkg.main:Strategy"}
    )
    gateway.emit_bootstrap_failed(
        occurred_at=_utc(2), reason_code="sdk_marker_incompatible"
    )
    gateway.emit_terminated(
        occurred_at=_utc(3),
        local_state="STOPPED",
        reason_code="MANUAL_STOP_REQUESTED",
        message="Worker shutdown complete.",
    )

    assert [item["signal_type"] for item in client.signals] == [
        "bootstrap_succeeded",
        "bootstrap_failed",
        "terminated",
    ]
    assert not hasattr(gateway, "mark_runtime_started")
    assert not hasattr(gateway, "mark_runtime_degraded")
    assert not hasattr(gateway, "mark_runtime_failed")


def test_event_dispatcher_payloads_never_use_manager_owned_lifecycle_truth() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.LIVE),
        client,
        _Identity(
            runtime_id="rt-evt-2",
            tenant_id="tenant-2",
            strategy_version_id="sv-1",
            mode=RuntimeMode.LIVE,
            launch_attempt=5,
        ),
    )
    gateway.emit_bootstrap_succeeded(occurred_at=_utc(4))

    emitted = client.signals[0]
    assert emitted["signal_type"] == "bootstrap_succeeded"
    assert "runtime.started" not in str(emitted["payload"])
    assert "runtime.degraded" not in str(emitted["payload"])
    assert "runtime.failed" not in str(emitted["payload"])


def test_bootstrap_failed_message_includes_mypy_validation_results() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-evt-mypy",
            tenant_id="tenant-1",
            strategy_version_id="sv-1",
            mode=RuntimeMode.PAPER,
            launch_attempt=1,
        ),
    )

    gateway.emit_bootstrap_failed(
        occurred_at=_utc(6),
        reason_code="sdk_protocol_mismatch",
        details={
            "mypy_result_path": "C:/repo/mypy_result.txt",
            "mypy_exit_code": 1,
            "mypy_output": "strategy.py:10: error: Name 'x' is not defined  [name-defined]",
        },
    )

    emitted = client.signals[0]["payload"]
    assert (
        emitted["reason_code"] == RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED.value
    )
    assert "missed_fields" in emitted
    assert "invalid_fields" in emitted
    assert "empty_fields" in emitted
    assert "passed" in emitted
    assert "failed" in emitted
    assert "not_checked" in emitted
    assert "details" in emitted
    assert "mypy_validation" in emitted["details"]
    assert (
        emitted["details"]["mypy_validation"]["result_path"]
        == "C:/repo/mypy_result.txt"
    )
    assert emitted["details"]["mypy_validation"]["exit_code"] == 1
    assert "name-defined" in emitted["details"]["mypy_validation"]["output"]


def test_manager_signal_wire_heartbeat_payload_is_local_state_and_observed_at_only() -> (
    None
):
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-hb-wire",
            tenant_id="tenant-hb",
            strategy_version_id="sv-hb",
            mode=RuntimeMode.PAPER,
            launch_attempt=1,
        ),
    )
    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(5))
    wire = client.signals[-1]
    p = wire["payload"]
    assert set(p.keys()) == {"local_state", "observed_at", "source"}
    assert p["local_state"] == "RUNNING"
    assert p["observed_at"] == _utc(5)
    assert p["source"] == "HEARTBEAT"


def test_manager_signal_wire_termination_runtime_job_completed_is_minimal() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-job-done",
            tenant_id="tenant-wire",
            strategy_version_id="sv-wire",
            mode=RuntimeMode.PAPER,
            launch_attempt=1,
        ),
    )
    gateway.emit_terminated(
        occurred_at=_utc(22),
        local_state="STOPPED",
        reason_code="RUNTIME_JOB_COMPLETED",
    )
    wire = client.signals[-1]
    p = wire["payload"]
    assert set(p.keys()) == {"local_state", "reason_code", "error_code"}
    assert p["local_state"] == "STOPPED"
    assert p["reason_code"] == RuntimeWorkerReasonCode.LOCAL_TERMINATION_OBSERVED.value
    assert p["error_code"] == WorkerErrorCode.RUNTIME_COMPLETED.value
    # Envelope occurred_at comes from factory (payload has no occurred_at for minimal JOB_COMPLETED).
    assert isinstance(wire["occurred_at"], datetime)


def test_manager_signal_wire_payload_omits_envelope_duplicate_fields() -> None:
    """Protobuf Struct payload must not repeat fields already on the envelope / identity."""
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-wire-1",
            tenant_id="tenant-wire",
            strategy_version_id="sv-wire",
            mode=RuntimeMode.PAPER,
            launch_attempt=3,
        ),
    )
    gateway.emit_terminated(
        occurred_at=_utc(20),
        local_state="STOPPED",
        reason_code="MANUAL_STOP_REQUESTED",
        message="Worker shutdown complete.",
        observed_at=_utc(21),
    )
    wire = client.signals[-1]
    p = wire["payload"]
    assert "accepted" not in p
    assert "accepted_at" not in p
    assert p["local_state"] == "STOPPED"
    assert p["reason_code"] == RuntimeWorkerReasonCode.STOP_REQUESTED.value
    assert p["message"] == "Worker shutdown complete."
    assert p["observed_at"] == _utc(21)
    assert "occurred_at" not in p
    for k in (
        "runtime_id",
        "launch_attempt",
        "worker_identity",
        "strategy_version_id",
        "tenant_id",
        "account_id",
    ):
        assert k not in p
    assert wire["occurred_at"] == _utc(20)


def test_event_dispatcher_requires_manager_client_emit_signal() -> None:
    with pytest.raises(TypeError, match="emit_signal"):
        ManagerGateway(
            get_mode_policy(RuntimeMode.BACKTEST),
            object(),
            _Identity(
                runtime_id="rt-evt-3",
                tenant_id="tenant-3",
                strategy_version_id="sv-1",
                mode=RuntimeMode.BACKTEST,
                launch_attempt=1,
            ),
        ).emit_bootstrap_succeeded(occurred_at=_utc(5))


def test_manager_gateway_calls_on_event_emitted_callback() -> None:
    client = _FakeManagerClient()
    captured: list[object] = []
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-evt-cb",
            tenant_id="tenant-cb",
            strategy_version_id="sv-cb",
            mode=RuntimeMode.PAPER,
            launch_attempt=1,
        ),
        on_event_emitted=lambda evt: captured.append(evt),
    )
    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(6))
    assert len(captured) == 1


def test_manager_gateway_calls_on_event_callback_even_when_deduped() -> None:
    client = _FakeManagerClient()
    captured: list[object] = []
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-evt-dedupe",
            tenant_id="tenant-cb",
            strategy_version_id="sv-cb",
            mode=RuntimeMode.PAPER,
            launch_attempt=1,
        ),
        on_event_emitted=lambda evt: captured.append(evt),
    )
    first = gateway.emit_bootstrap_failed(
        occurred_at=_utc(7),
        reason_code="artifact_not_found",
        details={"x": 1},
    )
    second = gateway.emit_bootstrap_failed(
        occurred_at=_utc(8),
        reason_code="artifact_not_found",
        details={"x": 2},
    )
    assert first.get("accepted") is True
    assert second.get("suppressed") is True
    assert len(captured) == 2


def test_bootstrap_failed_derives_field_buckets_from_field_errors() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-evt-buckets",
            tenant_id="tenant-1",
            strategy_version_id="sv-1",
            mode=RuntimeMode.PAPER,
            launch_attempt=1,
        ),
    )
    gateway.emit_bootstrap_failed(
        occurred_at=_utc(9),
        reason_code="launch_spec_invalid",
        details={
            "field_errors": {
                "runtime_id": "required_field_missing",
                "mode": "must_not_be_empty",
                "payload": "unknown_fields:runtime",
            }
        },
    )
    payload = client.signals[-1]["payload"]
    assert "runtime_id" in payload["missed_fields"]
    assert "mode" in payload["empty_fields"]
    assert "runtime" in payload["invalid_fields"]


def test_bootstrap_failed_artifact_verify_keeps_actual_digest_in_details() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-evt-digest",
            tenant_id="tenant-1",
            strategy_version_id="sv-1",
            mode=RuntimeMode.PAPER,
            launch_attempt=1,
        ),
    )
    gateway.emit_bootstrap_failed(
        occurred_at=_utc(10),
        reason_code="digest_mismatch",
        details={
            "failed": ["artifact_verify"],
            "actual_digest": "sha256:abcd",
            "expected_digest": "sha256:efgh",
        },
    )
    payload = client.signals[-1]["payload"]
    assert payload["details"]["actual_digest"] == "sha256:abcd"
