from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from runtime.bootstrap.runtime_spec_builder import (
    build_runtime_specs_from_settings,
)
from runtime.application.event_handling.event_dispatcher import EventDispatcher
from runtime.application.event_handling.market_event_handler import MarketEventHandler
from runtime.application.event_handling.runtime_event_handler import RuntimeEventHandler
from runtime.application.runtime_state.runtime_state import RuntimeState
from runtime.application.strategy_execution.strategy_execution_service import (
    StrategyExecutionService,
)
from runtime.application.strategy_execution.event_mapper import EventMapper
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.application.strategy_execution.strategy_error_boundary import StrategyErrorBoundary
from runtime.infrastructure.config.settings import Settings
from runtime.domain.enums import WorkerMode, WorkerPhase
from runtime.domain.model.normalized_events import MarketBarEvent
from runtime.domain.model.strategy_calculation_spec import StrategyCalculationSpec


class _BarStrategy:
    def on_bar(self, bar: object) -> None:
        self.last_bar = bar

    def on_event(self, event: object) -> None:
        self.last_event = event


def test_strategy_calculation_spec_excludes_platform_metadata() -> None:
    spec = StrategyCalculationSpec(
        params={"fast": 10, "slow": 30},
        symbol="AAPL",
        bar_timeframe="1m",
    )
    fields = set(spec.__dataclass_fields__)
    assert "strategy_id" not in fields
    assert "strategy_version_id" not in fields
    assert spec.symbol == "AAPL"
    assert spec.bar_timeframe == "1m"


def test_runtime_specs_from_settings_separates_concerns() -> None:
    launch = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "strategy_version_id": "sv-9",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "registry-local:///bundle.zip",
            "entrypoint": "sma_crossover:Strategy",
            "account_id": "acct-1",
            "parameters": {"symbol": "AAPL", "bar_timeframe": "1m"},
        }
    )
    settings = Settings(
        launch_spec=launch,
        launch_payload={
            "parameters": {"symbol": "AAPL"},
            "strategy_version_id": "sv-9",
        },
        work_root=__import__("pathlib").Path("/tmp"),
        bundle_resolve_base_dir=__import__("pathlib").Path("/tmp"),
        artifact_local_base_path=None,
        state_journal_enabled=False,
        state_journal_sqlite_path=__import__("pathlib").Path("/tmp/j.sqlite"),
        state_journal_txt_path=__import__("pathlib").Path("/tmp/j.txt"),
        strategy_runtime_manager_base_url="http://127.0.0.1:8080",
        runtime_manager_heartbeat_timeout_seconds=30.0,
        heartbeat_log_enabled=False,
        heartbeat_interval_seconds=10.0,
        deployment_id="dep-1",
        worker_control_http_bind="127.0.0.1:0",
        replay_ingress_grpc_bind="127.0.0.1:0",
        replay_ingress_grpc_fallback_ports="",
        replay_ingress_grpc_no_fallback=True,
        oms_grpc_target="127.0.0.1:50053",
        oms_grpc_timeout_seconds=3.0,
        risk_grpc_target="127.0.0.1:50054",
        risk_grpc_timeout_seconds=3.0,
        replay_bar_timeframe="1m",
        replay_ingress_trace_payload=False,
        replay_tick_logging_quiet=True,
        order_intent_correlation_id="",
        oms_correlation_id="",
        replay_session_id="",
        disable_order_intent_grpc=True,
    )
    built = build_runtime_specs_from_settings(settings)
    assert built.artifact.strategy_uri.endswith("bundle.zip")
    assert built.artifact.entrypoint == "sma_crossover:Strategy"
    assert built.calculation.symbol == "AAPL"
    assert "strategy_version_id" not in built.calculation.__dataclass_fields__
    assert built.trace.strategy_version_id == "sv-9"
    assert built.trace.request_id == "rt-1:1"
    assert built.channels.market_data_channel == "md:stream:am"
    assert built.identity.deployment_id == "dep-1"


def test_event_dispatcher_routes_market_bar_to_strategy_execution() -> None:
    calls: list[str] = []

    class _RecordingAdapter(StrategyAdapter):
        def on_event(self, raw_event):  # type: ignore[no-untyped-def]
            calls.append(str(raw_event.get("type")))
            return super().on_event(raw_event)

    strategy = _BarStrategy()
    adapter = _RecordingAdapter(
        strategy,
        boundary=StrategyErrorBoundary(),
        mapper=EventMapper(),
    )
    runtime_state = RuntimeState(initial_phase=WorkerPhase.RUNNING)
    execution = StrategyExecutionService(adapter=adapter)
    market = MarketEventHandler(
        strategy_execution=execution,
        runtime_state=runtime_state,
        mode=WorkerMode.BACKTEST,
    )
    handler = RuntimeEventHandler(market_handler=market)
    dispatcher = EventDispatcher(runtime_handler=handler, strategy_execution=execution)

    raw = {
        "type": "market.bar",
        "symbol": "AAPL",
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 100.0,
        "ts_ms": 1710000000000,
    }
    dispatcher.dispatch_raw(raw)
    assert calls == ["market.bar"]

    domain_bar = MarketBarEvent(
        symbol="AAPL",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=100.0,
        ts_ms=1710000000000,
    )
    dispatcher.dispatch(domain_bar, raw=raw)
    assert len(calls) == 2


def test_backtest_and_redis_paths_share_strategy_execution_service() -> None:
    """Normalized bar from mapper-backed raw dict uses same StrategyExecutionService."""
    strategy = MagicMock()
    adapter = StrategyAdapter(strategy, mapper=EventMapper())
    execution = StrategyExecutionService(adapter=adapter)
    runtime_state = RuntimeState(initial_phase=WorkerPhase.RUNNING)
    market = MarketEventHandler(
        strategy_execution=execution,
        runtime_state=runtime_state,
        mode=WorkerMode.PAPER,
    )
    dispatcher = EventDispatcher(
        runtime_handler=RuntimeEventHandler(market_handler=market),
        strategy_execution=execution,
    )

    redis_tick = {
        "type": "market.bar",
        "symbol": "MSFT",
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 50.0,
        "ts_ms": 1710000001000,
        "event_time": datetime(2024, 3, 10, 12, 0, tzinfo=timezone.utc),
    }
    replay_tick = dict(redis_tick)
    replay_tick["event_type"] = "market.bar"

    dispatcher.dispatch_raw(redis_tick)
    dispatcher.dispatch_raw(replay_tick)
    assert strategy.on_event.call_count == 2


def test_event_dispatcher_source_does_not_introspect_private_handler_attrs() -> None:
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[2]
        / "runtime"
        / "application"
        / "event_handling"
        / "event_dispatcher.py"
    ).read_text(encoding="utf-8")
    assert 'getattr(self._runtime_handler, "_market_handler"' not in source
    assert 'getattr(self._runtime_handler, "_portfolio_handler"' not in source
    assert "._market_handler" not in source
    assert "._portfolio_handler" not in source


def test_dispatch_raw_routes_through_public_runtime_handler_api() -> None:
    class _PublicRuntimeHandler:
        def __init__(self) -> None:
            self.raw_ticks: list[dict[str, object]] = []

        def has_market_handler(self) -> bool:
            return True

        def has_portfolio_handler(self) -> bool:
            return False

        def handle_raw_tick(self, raw_event: dict[str, object]) -> None:
            self.raw_ticks.append(dict(raw_event))

        def handle_portfolio_update(self, event: object) -> None:
            raise AssertionError("portfolio path not expected")

        def handle(self, event: object, *, raw: dict[str, object] | None = None) -> None:
            raise AssertionError("normalized handle path not expected for raw tick")

    handler = _PublicRuntimeHandler()
    dispatcher = EventDispatcher(runtime_handler=handler)  # type: ignore[arg-type]
    tick = {"type": "market.tick", "symbol": "AAPL", "price": 1.0, "size": 1.0, "ts_ms": 1}

    dispatcher.dispatch_raw(tick)

    assert handler.raw_ticks == [tick]


def test_dispatch_portfolio_routes_through_public_runtime_handler_api() -> None:
    from decimal import Decimal

    from runtime.domain.model.normalized_events import PortfolioUpdatedEvent

    class _PublicRuntimeHandler:
        def __init__(self) -> None:
            self.portfolio_events: list[PortfolioUpdatedEvent] = []

        def has_market_handler(self) -> bool:
            return False

        def has_portfolio_handler(self) -> bool:
            return True

        def handle_raw_tick(self, raw_event: dict[str, object]) -> None:
            raise AssertionError("market path not expected")

        def handle_portfolio_update(self, event: PortfolioUpdatedEvent) -> None:
            self.portfolio_events.append(event)

        def handle(self, event: object, *, raw: dict[str, object] | None = None) -> None:
            raise AssertionError("normalized handle path not expected for portfolio update")

    handler = _PublicRuntimeHandler()
    dispatcher = EventDispatcher(runtime_handler=handler)  # type: ignore[arg-type]
    event = PortfolioUpdatedEvent(
        job_id="job-1",
        timestamp=datetime(2024, 3, 10, 12, 0, tzinfo=timezone.utc),
        cash_balance=Decimal("1000"),
        buying_power=Decimal("1000"),
        equity=Decimal("1000"),
    )

    dispatcher.dispatch_portfolio(event)

    assert handler.portfolio_events == [event]


def test_dispatch_raw_logs_event_mapping_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    class _NoMarketHandler:
        def has_market_handler(self) -> bool:
            return False

        def has_portfolio_handler(self) -> bool:
            return False

        def handle_raw_tick(self, raw_event: dict[str, object]) -> None:
            pass

        def handle_portfolio_update(self, event: object) -> None:
            pass

        def handle(self, event: object, *, raw: dict[str, object] | None = None) -> None:
            pass

    dispatcher = EventDispatcher(
        runtime_handler=_NoMarketHandler(),  # type: ignore[arg-type]
        strategy_execution=None,
    )

    with caplog.at_level(logging.DEBUG):
        dispatcher.dispatch_raw({"unexpected": "payload"})

    assert any(
        "event_dispatch_raw_mapping_failed" in record.message for record in caplog.records
    )

