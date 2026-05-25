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


def _utc(second: int) -> datetime:
    return datetime(2026, 3, 29, 6, 0, second, tzinfo=timezone.utc)


def test_heartbeat_emission_is_idempotent_for_same_runtime_attempt_key() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.PAPER),
        client,
        _Identity(
            runtime_id="rt-hb-1",
            tenant_id="tenant-1",
            strategy_version_id="sv-1",
            mode=RuntimeMode.PAPER,
            launch_attempt=7,
        ),
    )

    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(2))
    gateway.emit_heartbeat(local_state="RUNNING", observed_at=_utc(2))

    assert len(client.signals) == 1
    assert client.signals[0]["signal_type"] == "heartbeat"
    assert client.signals[0]["identity"]["runtime_id"] == "rt-hb-1"
    assert client.signals[0]["identity"]["launch_attempt"] == 7


def test_heartbeat_dedupe_is_scoped_per_launch_attempt() -> None:
    client = _FakeManagerClient()
    first = ManagerGateway(
        get_mode_policy(RuntimeMode.LIVE),
        client,
        _Identity(
            runtime_id="rt-hb-2",
            tenant_id="tenant-2",
            strategy_version_id="sv-2",
            mode=RuntimeMode.LIVE,
            launch_attempt=1,
        ),
    )
    second = ManagerGateway(
        get_mode_policy(RuntimeMode.LIVE),
        client,
        _Identity(
            runtime_id="rt-hb-2",
            tenant_id="tenant-2",
            strategy_version_id="sv-2",
            mode=RuntimeMode.LIVE,
            launch_attempt=2,
        ),
    )

    first.emit_heartbeat(local_state="RUNNING", observed_at=_utc(4))
    second.emit_heartbeat(local_state="RUNNING", observed_at=_utc(4))

    assert len(client.signals) == 2
    assert {item["identity"]["launch_attempt"] for item in client.signals} == {1, 2}


def test_supervision_unhealthy_signal_carries_reason_and_timestamps() -> None:
    client = _FakeManagerClient()
    gateway = ManagerGateway(
        get_mode_policy(RuntimeMode.BACKTEST),
        client,
        _Identity(
            runtime_id="rt-hb-3",
            tenant_id="tenant-3",
            strategy_version_id="sv-3",
            mode=RuntimeMode.BACKTEST,
            launch_attempt=4,
        ),
    )
    gateway.emit_unhealthy(
        occurred_at=_utc(10),
        observed_at=_utc(11),
        reason_code="heartbeat_lagging",
        details={"source": "heartbeat_service"},
    )

    assert len(client.signals) == 1
    signal = client.signals[0]
    assert signal["signal_type"] == "unhealthy"
    assert signal["payload"]["reason_code"] == "UNHEALTHY_EXECUTION_DETECTED"
    assert signal["payload"]["details"]["source"] == "heartbeat_service"
