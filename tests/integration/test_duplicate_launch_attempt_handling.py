from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from runtime.domain.enums import WorkerMode
from runtime.infrastructure.http.srm.manager_gateway import ManagerGateway
from runtime.domain.policies.mode_policy import get_mode_policy


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
    mode: WorkerMode
    launch_attempt: int
    account_id: str = "acct-1"
    trader_id: str | None = None
    correlation_id: str = "corr-dup"


def _utc(second: int) -> datetime:
    return datetime(2026, 3, 29, 14, 0, second, tzinfo=timezone.utc)


def test_duplicate_launch_success_is_noop_single_transition_path() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(WorkerMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-dup-success",
            tenant_id="tenant-1",
            strategy_version_id="sv-1",
            mode=WorkerMode.PAPER,
            launch_attempt=1,
        ),
    )

    first = gateway.emit_bootstrap_succeeded(
        occurred_at=_utc(1),
        details={"entrypoint": "pkg.main:Strategy"},
    )
    duplicate = gateway.emit_bootstrap_succeeded(
        occurred_at=_utc(1),
        details={"entrypoint": "pkg.main:Strategy"},
    )

    assert first["accepted"] is True
    assert duplicate["suppressed"] is True
    assert duplicate["decision"] == "DROP_DUPLICATE"
    assert [item["signal_type"] for item in client.signals] == ["bootstrap_succeeded"]


def test_duplicate_launch_failure_is_noop() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(WorkerMode.LIVE),
        client,
        _Identity(
            runtime_id="rt-dup-failure",
            tenant_id="tenant-1",
            strategy_version_id="sv-2",
            mode=WorkerMode.LIVE,
            launch_attempt=9,
        ),
    )

    gateway.emit_bootstrap_failed(
        occurred_at=_utc(2),
        reason_code="artifact_missing",
        details={"digest": "sha256:bad"},
    )
    duplicate = gateway.emit_bootstrap_failed(
        occurred_at=_utc(2),
        reason_code="artifact_missing",
        details={"digest": "sha256:bad"},
    )

    assert duplicate["suppressed"] is True
    assert duplicate["decision"] == "DROP_DUPLICATE"
    assert len(client.signals) == 1
    assert client.signals[0]["signal_type"] == "bootstrap_failed"


def test_launch_failure_variants_still_emit_single_manager_signal() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(WorkerMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-launch-fail-variants",
            tenant_id="tenant-1",
            strategy_version_id="sv-variants",
            mode=WorkerMode.PAPER,
            launch_attempt=1,
        ),
    )

    first = gateway.emit_bootstrap_failed(
        occurred_at=_utc(20),
        reason_code="entrypoint_invalid",
        details={"entrypoint": "bad_entrypoint"},
    )
    second = gateway.emit_bootstrap_failed(
        occurred_at=_utc(21),
        reason_code="sdk_marker_incompatible",
        details={"required_sdk_major": 1.0, "sdk_version_marker": "2.0.0"},
    )

    assert first["accepted"] is True
    assert second["suppressed"] is True
    assert second["decision"] == "DROP_DUPLICATE"
    assert len(client.signals) == 1
    assert client.signals[0]["signal_type"] == "bootstrap_failed"


def test_old_attempt_signal_cannot_supersede_current_attempt() -> None:
    client = _FakeManagerClient()
    older_attempt_gateway = ManagerGateway(
        get_mode_policy(WorkerMode.BACKTEST),
        client,
        _Identity(
            runtime_id="rt-stale-attempt",
            tenant_id="tenant-1",
            strategy_version_id="sv-3",
            mode=WorkerMode.BACKTEST,
            launch_attempt=1,
        ),
    )
    current_attempt_gateway = ManagerGateway(
        get_mode_policy(WorkerMode.BACKTEST),
        client,
        _Identity(
            runtime_id="rt-stale-attempt",
            tenant_id="tenant-1",
            strategy_version_id="sv-3",
            mode=WorkerMode.BACKTEST,
            launch_attempt=2,
        ),
    )

    current_attempt_gateway.emit_bootstrap_succeeded(occurred_at=_utc(10))
    stale = older_attempt_gateway.emit_bootstrap_failed(
        occurred_at=_utc(11),
        reason_code="late_old_failure",
    )

    assert stale["suppressed"] is True
    assert stale["decision"] == "DROP_STALE_ATTEMPT"
    assert len(client.signals) == 1
    assert client.signals[0]["identity"]["launch_attempt"] == 2
    assert client.signals[0]["signal_type"] == "bootstrap_succeeded"
