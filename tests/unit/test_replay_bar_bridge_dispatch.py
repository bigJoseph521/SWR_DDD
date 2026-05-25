from __future__ import annotations

from datetime import datetime, timezone

from alphovex_sdk.strategy.base import Strategy as SdkStrategy
from runtime.application.strategy_execution.event_mapper import MarketBarEvent
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.infrastructure.sdk.replay_sdk_bridge import build_replay_sdk_bridge
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.domain.enums import WorkerMode
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.clock.clock import SimulatedClock
from runtime.infrastructure.sdk.sdk_runtime_types import (
    AssetClass,
    ParameterSchema,
    StrategyMetadata,
)


def test_replay_bridge_dispatches_market_bar_to_on_bar() -> None:
    class _Strategy:
        @classmethod
        def get_metadata(cls) -> StrategyMetadata:
            return StrategyMetadata(
                name="t",
                description="",
                version="1",
                author="",
                supported_asset_classes=(AssetClass.EQUITY,),
                parameter_schema=ParameterSchema(parameters={}),
            )

        @classmethod
        def build_parameter_schema(cls) -> ParameterSchema:
            return ParameterSchema(parameters={})

        def __init__(self) -> None:
            self.bar_closes: list[float] = []

        def on_bar(self, bar, context) -> None:  # noqa: ANN001
            self.bar_closes.append(float(bar.close))

    launch = LaunchSpec(
        runtime_id="r1",
        tenant_id="t1",
        strategy_version_id="sv1",
        mode=WorkerMode.BACKTEST,
        launch_attempt=1,
        artifact_uri="uri",
        entrypoint="ep",
        artifact_digest="digest",
        account_id="acc1",
    )
    ident = WorkerIdentity(
        runtime_id="r1",
        tenant_id="t1",
        strategy_version_id="sv1",
        mode=WorkerMode.BACKTEST,
        account_id="acc1",
        trader_id=None,
        artifact_uri="uri",
        entrypoint="ep",
    )
    clock = SimulatedClock()
    clock.set_time(datetime.now(timezone.utc))

    strat = _Strategy()
    bridge = build_replay_sdk_bridge(
        strategy=strat,
        launch_spec=launch,
        worker_identity=ident,
        simulated_clock=clock,
    )
    adapter = StrategyAdapter(strategy=strat, replay_sdk_bridge=bridge)

    result = adapter.on_event(
        {
            "type": "market.bar",
            "symbol": "AAPL",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100.0,
            "ts_ms": 1_700_000_000_000,
        }
    )
    assert result.ok is True
    assert strat.bar_closes == [1.5]


def test_replay_bridge_dispatches_market_quote_to_on_quote() -> None:
    class _Strategy:
        @classmethod
        def get_metadata(cls) -> StrategyMetadata:
            return StrategyMetadata(
                name="t",
                description="",
                version="1",
                author="",
                supported_asset_classes=(AssetClass.EQUITY,),
                parameter_schema=ParameterSchema(parameters={}),
            )

        @classmethod
        def build_parameter_schema(cls) -> ParameterSchema:
            return ParameterSchema(parameters={})

        def __init__(self) -> None:
            self.mids: list[float] = []

        def on_quote(self, quote, context) -> None:  # noqa: ANN001
            self.mids.append(float(quote.mid_price))

    launch = LaunchSpec(
        runtime_id="r1",
        tenant_id="t1",
        strategy_version_id="sv1",
        mode=WorkerMode.BACKTEST,
        launch_attempt=1,
        artifact_uri="uri",
        entrypoint="ep",
        artifact_digest="digest",
        account_id="acc1",
    )
    ident = WorkerIdentity(
        runtime_id="r1",
        tenant_id="t1",
        strategy_version_id="sv1",
        mode=WorkerMode.BACKTEST,
        account_id="acc1",
        trader_id=None,
        artifact_uri="uri",
        entrypoint="ep",
    )
    clock = SimulatedClock()
    clock.set_time(datetime.now(timezone.utc))

    strat = _Strategy()
    bridge = build_replay_sdk_bridge(
        strategy=strat,
        launch_spec=launch,
        worker_identity=ident,
        simulated_clock=clock,
    )
    adapter = StrategyAdapter(strategy=strat, replay_sdk_bridge=bridge)

    result = adapter.on_event(
        {
            "type": "market.quote",
            "symbol": "AAPL",
            "bid": 100.0,
            "ask": 100.5,
            "bid_size": 1.0,
            "ask_size": 2.0,
            "ts_ms": 1_700_000_000_000,
        }
    )
    assert result.ok is True
    assert strat.mids == [100.25]


def _launch_and_identity() -> tuple[LaunchSpec, WorkerIdentity]:
    launch = LaunchSpec(
        runtime_id="r1",
        tenant_id="t1",
        strategy_version_id="sv1",
        mode=WorkerMode.BACKTEST,
        launch_attempt=1,
        artifact_uri="uri",
        entrypoint="ep",
        artifact_digest="digest",
        account_id="acc1",
    )
    ident = WorkerIdentity(
        runtime_id="r1",
        tenant_id="t1",
        strategy_version_id="sv1",
        mode=WorkerMode.BACKTEST,
        account_id="acc1",
        trader_id=None,
        artifact_uri="uri",
        entrypoint="ep",
    )
    return launch, ident


def test_bind_and_start_runs_once_for_sdk_strategy() -> None:
    class _RecordingStrategy(SdkStrategy):
        def __init__(self) -> None:
            super().__init__()
            self.inits = 0
            self.starts = 0

        def on_init(self) -> None:
            self.inits += 1

        def on_start(self) -> None:
            self.starts += 1

        def on_bar(self, bar) -> None:  # noqa: ANN001
            _ = bar

    launch, ident = _launch_and_identity()
    clock = SimulatedClock()
    clock.set_time(datetime.now(timezone.utc))

    strat = _RecordingStrategy()
    bridge = build_replay_sdk_bridge(
        strategy=strat,
        launch_spec=launch,
        worker_identity=ident,
        simulated_clock=clock,
    )
    adapter = StrategyAdapter(strategy=strat, replay_sdk_bridge=bridge)

    first = adapter.bind_and_start()
    assert first.ok is True
    assert strat.inits == 1
    assert strat.starts == 1

    second = adapter.bind_and_start()
    assert second.ok is True
    assert second.diagnostics.get("skipped") == "already_started"
    assert strat.inits == 1
    assert strat.starts == 1


def test_dispatch_on_bar_single_arg_sees_data_and_clock_after_state_apply() -> None:
    ts_ms = 1_700_000_000_000
    expected_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

    class _DataStrategy(SdkStrategy):
        def __init__(self) -> None:
            super().__init__()
            self.observed: list[tuple[float, datetime]] = []

        def on_bar(self, bar) -> None:  # noqa: ANN001
            lb = self.data.latest_bar(bar.symbol, bar.timeframe)
            assert lb is not None
            self.observed.append((float(lb.close), self.time.now()))

    launch, ident = _launch_and_identity()
    clock = SimulatedClock()
    clock.set_time(datetime(2020, 1, 1, tzinfo=timezone.utc))

    strat = _DataStrategy()
    bridge = build_replay_sdk_bridge(
        strategy=strat,
        launch_spec=launch,
        worker_identity=ident,
        simulated_clock=clock,
    )
    adapter = StrategyAdapter(strategy=strat, replay_sdk_bridge=bridge)
    assert adapter.bind_and_start().ok is True

    result = adapter.on_event(
        {
            "type": "market.bar",
            "symbol": "AAPL",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100.0,
            "ts_ms": ts_ms,
        }
    )
    assert result.ok is True
    assert strat.observed == [(1.5, expected_ts)]


def test_bar_instrument_id_matches_symbol_not_venue_numeric_id() -> None:
    """Venue-style ``instrument_id`` on the raw event must not override the ticker ``symbol``."""

    class _IdsStrategy(SdkStrategy):
        def __init__(self) -> None:
            super().__init__()
            self.last_ids: tuple[str, str] | None = None

        def on_bar(self, bar) -> None:  # noqa: ANN001
            self.last_ids = (str(bar.instrument_id), str(bar.symbol))

    launch, ident = _launch_and_identity()
    clock = SimulatedClock()
    clock.set_time(datetime(2020, 1, 1, tzinfo=timezone.utc))

    strat = _IdsStrategy()
    bridge = build_replay_sdk_bridge(
        strategy=strat,
        launch_spec=launch,
        worker_identity=ident,
        simulated_clock=clock,
    )
    adapter = StrategyAdapter(strategy=strat, replay_sdk_bridge=bridge)
    assert adapter.bind_and_start().ok is True

    ts_ms = 1_700_000_000_000
    result = adapter.on_event(
        {
            "type": "market.bar",
            "symbol": "AAPL",
            "instrument_id": "29",
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
            "ts_ms": ts_ms,
        }
    )
    assert result.ok is True
    assert strat.last_ids == ("AAPL", "AAPL")


def test_bar_instrument_id_prefers_wire_id_for_backtest_nested_bar_payload() -> None:
    """Backtest-runner stdio MARKET_DATA_EVENT uses nested ``bar`` + venue ``instrument_id``."""

    class _IdsStrategy(SdkStrategy):
        def __init__(self) -> None:
            super().__init__()
            self.last_ids: tuple[str, str] | None = None

        def on_bar(self, bar) -> None:  # noqa: ANN001
            self.last_ids = (str(bar.instrument_id), str(bar.symbol))

    launch, ident = _launch_and_identity()
    clock = SimulatedClock()
    clock.set_time(datetime(2020, 1, 1, tzinfo=timezone.utc))

    strat = _IdsStrategy()
    bridge = build_replay_sdk_bridge(
        strategy=strat,
        launch_spec=launch,
        worker_identity=ident,
        simulated_clock=clock,
    )
    adapter = StrategyAdapter(strategy=strat, replay_sdk_bridge=bridge)
    assert adapter.bind_and_start().ok is True

    ts_ms = 1_700_000_000_000
    bar_event = MarketBarEvent(
        event_type="market.bar",
        symbol="AAPL",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1.0,
        ts_ms=ts_ms,
    )
    bridge.dispatch_on_bar(
        {
            "instrument_id": "28",
            "symbol": "AAPL",
            "bar": {
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1.0,
            },
        },
        bar_event,
        strat.on_bar,
    )
    assert strat.last_ids == ("28", "AAPL")
