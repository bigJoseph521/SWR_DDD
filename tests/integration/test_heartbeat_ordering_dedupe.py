from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from runtime.domain.enums import RuntimeMode
from runtime.integration.manager_gateway import ManagerGateway
from runtime.runtime.mode_policy import get_mode_policy


class _FakeManagerClient:
    def __init__(self) -> None:
        self.signals: list[dict[str, Any]] = []

    def emit_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.signals.append(payload)
        return {"accepted": True}


@dataclass(frozen=True, slots=True)
class _Identity:
    runtime_id: str
    tenant_id: str
    strategy_version_id: str
    mode: RuntimeMode
    launch_attempt: int
    account_id: str = "acct-1"
    trader_id: str | None = None
    correlation_id: str = "corr-hb"
    causation_id: str | None = "cause-launch"


def _utc(second: int) -> datetime:
    return datetime(2026, 3, 29, 15, 0, second, tzinfo=timezone.utc)


def test_duplicate_heartbeat_is_noop() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-hb-dupe",
            tenant_id="tenant-1",
            strategy_version_id="sv-1",
            mode=RuntimeMode.PAPER,
            launch_attempt=3,
        ),
    )

    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(2))
    duplicate = gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(2))

    assert duplicate["suppressed"] is True
    assert duplicate["decision"] == "DROP_DUPLICATE"
    assert len(client.signals) == 1
    assert client.signals[0]["signal_type"] == "heartbeat"


def test_heartbeat_from_stale_attempt_is_ignored_after_newer_attempt() -> None:
    client = _FakeManagerClient()
    old_gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.LIVE),
        client,
        _Identity(
            runtime_id="rt-hb-stale",
            tenant_id="tenant-2",
            strategy_version_id="sv-2",
            mode=RuntimeMode.LIVE,
            launch_attempt=1,
        ),
    )
    current_gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.LIVE),
        client,
        _Identity(
            runtime_id="rt-hb-stale",
            tenant_id="tenant-2",
            strategy_version_id="sv-2",
            mode=RuntimeMode.LIVE,
            launch_attempt=2,
        ),
    )

    current_gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(6))
    stale = old_gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(8))

    assert stale["suppressed"] is True
    assert stale["decision"] == "DROP_STALE_ATTEMPT"
    assert len(client.signals) == 1
    assert client.signals[0]["identity"]["launch_attempt"] == 2


def test_correlation_and_causation_lineage_is_deterministic() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.BACKTEST),
        client,
        _Identity(
            runtime_id="rt-hb-lineage",
            tenant_id="tenant-3",
            strategy_version_id="sv-3",
            mode=RuntimeMode.BACKTEST,
            launch_attempt=4,
            correlation_id="corr-fixed",
            causation_id="cause-fixed",
        ),
    )

    gateway.emit_heartbeat(
        local_state="RUNNING",
        observed_at=_utc(12),
        triggering_event_id="evt-trigger-1",
    )

    emitted = client.signals[0]
    assert emitted["event_name"] == "runtime.heartbeat"
    assert emitted["event_version"] == 1
    assert emitted["correlation_id"] == "corr-fixed"
    assert emitted["causation_id"] == "evt-trigger-1"
    assert emitted["producer"] == "strategy-worker-runtime"
