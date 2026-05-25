from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from runtime.application.dependency_container import (
    build_dependency_container,
)
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from runtime.domain.enums import ServiceTarget
from tests.e2e._helpers import (
    FakeManagerClient,
    FakeOmsClient,
    build_bundle_dict,
    write_strategy_package,
)


def test_backtest_replay_runtime_routes_replay_and_order_intent_to_risk_service(
    tmp_path: Path,
) -> None:
    source_root = write_strategy_package(tmp_path / "source")
    manager = FakeManagerClient()
    oms = FakeOmsClient()

    settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-backtest",
            mode="BACKTEST",
            job_id="bt-1",
        ),
        base_dir=tmp_path,
    )
    container = build_dependency_container(
        settings,
        manager_client=manager,
        risk_order_intent_client=oms,
    )
    container.worker_app.start()

    deps = container.runtime_dependencies_initializer()
    assert deps.risk_order_intent is not None
    assert deps.replay is not None
    assert container.mode_policy.route_order_intent() is ServiceTarget.RISK_SERVICE

    callback_seen: dict[str, object] = {}
    tick = {"event_id": "evt-1", "symbol": "BTC-USD"}
    replay_gateway = deps.replay
    assert replay_gateway is not None
    replay_result = replay_gateway.ingest_replay_tick(
        tick,
        simulated_time=datetime(2026, 3, 29, 11, 0, tzinfo=timezone.utc),
        strategy_callback=lambda mapped: callback_seen.setdefault("tick", dict(mapped)),
    )

    assert callback_seen["tick"] == tick
    assert replay_result == tick
    assert oms.intent_calls == []
