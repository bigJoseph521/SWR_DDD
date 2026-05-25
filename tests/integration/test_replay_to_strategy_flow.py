from __future__ import annotations

from datetime import datetime, timezone

import pytest
from runtime.domain.enums import WorkerMode, ServiceTarget
from runtime.domain.errors import UnsupportedDependencyExpansion
from runtime.infrastructure.clock.clock import SimulatedClock
from runtime.infrastructure.grpc.replay.replay_gateway import ReplayGateway
from runtime.domain.policies.mode_policy import get_mode_policy


def test_backtest_replay_ingress_is_accepted_and_updates_simulated_time() -> None:
    simulated_clock = SimulatedClock()
    gateway = ReplayGateway(
        get_mode_policy(WorkerMode.BACKTEST),
        simulated_clock=simulated_clock,
    )

    seen: dict[str, object] = {}
    simulated_time = datetime(2026, 3, 29, 10, 15, tzinfo=timezone.utc)
    tick = {"event_id": "evt-1", "symbol": "BTC-USD"}

    def _callback(mapped_tick: object) -> str:
        seen["tick"] = dict(mapped_tick)  # type: ignore[arg-type]
        seen["clock_time"] = simulated_clock.now()
        return "callback_ok"

    result = gateway.ingest_replay_tick(
        tick,
        simulated_time=simulated_time,
        strategy_callback=_callback,
    )

    assert result == "callback_ok"
    assert seen["tick"] == tick
    assert seen["clock_time"] == simulated_time


@pytest.mark.parametrize("mode", [WorkerMode.PAPER, WorkerMode.LIVE])
def test_paper_live_replay_ingress_is_rejected(mode: WorkerMode) -> None:
    with pytest.raises(UnsupportedDependencyExpansion) as exc_info:
        ReplayGateway(get_mode_policy(mode))
    assert exc_info.value.details["mode"] == mode.value
    assert exc_info.value.details["capability"] == "REPLAY_INGRESS"


def test_gateway_has_no_direct_replay_chunk_retrieval_api() -> None:
    gateway = ReplayGateway(get_mode_policy(WorkerMode.BACKTEST))
    assert callable(getattr(gateway, "ingest_replay_tick"))
    assert not hasattr(gateway, "fetch_replay_chunk")
    assert not hasattr(gateway, "fetch_replay_window")


def test_first_data_callback_is_emitted_once() -> None:
    seen: list[str] = []
    gateway = ReplayGateway(
        get_mode_policy(WorkerMode.BACKTEST),
        on_first_data=lambda: seen.append("first_data"),
    )
    gateway.ingest_replay_tick({"event_id": "evt-1"})
    gateway.ingest_replay_tick({"event_id": "evt-2"})
    assert seen == ["first_data"]


def test_backtest_order_intent_route_is_risk_service() -> None:
    policy = get_mode_policy(WorkerMode.BACKTEST)
    assert policy.route_order_intent() is ServiceTarget.RISK_SERVICE
