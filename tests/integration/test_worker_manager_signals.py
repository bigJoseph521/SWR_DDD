from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from runtime.domain.enums import WorkerMode
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.http.srm.manager_gateway import ManagerGateway
from runtime.domain.policies.mode_policy import get_mode_policy


class _FakeManagerClient:
    def __init__(self) -> None:
        self.signals: list[dict[str, Any]] = []

    def emit_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.signals.append(payload)
        return {"accepted": True, "signal_type": payload["signal_type"]}


def _identity(mode: WorkerMode) -> WorkerIdentity:
    return WorkerIdentity(
        runtime_id=f"rt-{mode.value.lower()}",
        tenant_id="tenant-1",
        account_id="acct-1",
        strategy_version_id="sv-1",
        mode=mode,
        validated_parameter_identity="vp-1",
        artifact_reference="artifact://strategy",
        artifact_digest="sha256:abc",
        entrypoint="strategy.main:Strategy",
        launch_attempt=3,
    )


def _utc(second: int) -> datetime:
    return datetime(2026, 3, 29, 12, 0, second, tzinfo=timezone.utc)


def test_all_signals_carry_runtime_identity_and_mode() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(WorkerMode.PAPER), client, _identity(WorkerMode.PAPER)
    )

    gateway.emit_bootstrap_succeeded(
        occurred_at=_utc(1), details={"entrypoint": "strategy.main:Strategy"}
    )
    gateway.emit_bootstrap_failed(occurred_at=_utc(2), reason_code="artifact_missing")
    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(4))
    gateway.emit_unhealthy(
        occurred_at=_utc(5),
        observed_at=_utc(6),
        reason_code="dependency_unhealthy",
    )
    gateway.emit_controlled_stop(
        occurred_at=_utc(7), reason_code="requested_by_manager"
    )
    gateway.emit_terminated(
        occurred_at=_utc(8),
        local_state="STOPPED",
        reason_code="exit_0",
        message="Process exit.",
        observed_at=_utc(9),
    )

    assert len(client.signals) == 6
    for emitted in client.signals:
        identity = emitted["identity"]
        assert identity["runtime_id"] == "rt-paper"
        assert identity["tenant_id"] == "tenant-1"
        assert identity["account_id"] == "acct-1"
        assert identity["strategy_version_id"] == "sv-1"
        assert identity["mode"] == WorkerMode.PAPER.value
        assert identity["launch_attempt"] == 3


def test_duplicate_heartbeat_is_idempotent_on_worker_side() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(WorkerMode.LIVE), client, _identity(WorkerMode.LIVE)
    )

    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(2))
    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(2))

    assert len(client.signals) == 1
    assert client.signals[0]["signal_type"] == "heartbeat"


@pytest.mark.parametrize(
    "mode", [WorkerMode.PAPER, WorkerMode.LIVE, WorkerMode.BACKTEST]
)
def test_all_modes_can_emit_manager_supervision_signals(mode: WorkerMode) -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(get_mode_policy(mode), client, _identity(mode))

    gateway.emit_unhealthy(
        occurred_at=_utc(11),
        observed_at=_utc(12),
        reason_code="runtime_degraded",
        details={"metric": "latency"},
    )

    assert len(client.signals) == 1
    assert client.signals[0]["identity"]["mode"] == mode.value
    assert client.signals[0]["signal_type"] == "unhealthy"


def test_gateway_exposes_source_signal_methods_only() -> None:
    gateway = ManagerGateway(
        get_mode_policy(WorkerMode.BACKTEST),
        _FakeManagerClient(),
        _identity(WorkerMode.BACKTEST),
    )

    assert callable(getattr(gateway, "emit_bootstrap_succeeded"))
    assert callable(getattr(gateway, "emit_bootstrap_failed"))
    assert callable(getattr(gateway, "emit_heartbeat"))
    assert callable(getattr(gateway, "emit_unhealthy"))
    assert callable(getattr(gateway, "emit_controlled_stop"))
    assert callable(getattr(gateway, "emit_terminated"))
    assert not hasattr(gateway, "set_manager_lifecycle_state")
    assert not hasattr(gateway, "mark_runtime_started")
