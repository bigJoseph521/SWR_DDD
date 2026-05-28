from __future__ import annotations

import inspect
from collections import defaultdict
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Optional, cast

from alphovex_sdk.enums.session import MarketStateEnum, SessionTypeEnum
from alphovex_sdk.models import Bar, Quote, Tick
from alphovex_sdk.typedefs.aliases import Timeframe
from alphovex_sdk.models.timer_event import TimerEvent as SdkTimerEvent
from alphovex_sdk.models.timer_event import TimerEventData, TimerEventTypeEnum
from alphovex_sdk.strategy.base import Strategy as SdkStrategy
from runtime.application.strategy_execution.event_mapper import (
    MarketBarEvent,
    MarketQuoteEvent,
    MarketTickEvent,
)
from runtime.application.strategy_execution.event_mapper import (
    TimerEvent as WireTimerEvent,
)
from runtime.domain.launch_spec import LaunchSpec
from runtime.infrastructure.strategy_loader.runtime_stub_support import (
    BarHistory,
    CashBalance,
    DefaultOrderService,
    DefaultRiskService,
    Exposure,
    IndicatorService,
    LoggerBackend,
    MarginState,
    PnL,
    PortfolioSnapshot,
    RuntimeOrderIntent,
    SnapshotPortfolioService,
)
from runtime.domain.model.strategy_calculation_spec import StrategyCalculationSpec
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.clock.clock import SimulatedClock, default_today
from runtime.infrastructure.clock.epoch_time import utc_from_epoch_millis
from runtime.application.order_intents.sdk_order_intent_submission import (
    safe_submit_sdk_order_intent,
)
from runtime.infrastructure.sdk.runtime_account_context import (
    RuntimeAccountContext,
)
from runtime.infrastructure.sdk.runtime_data_context import (
    RuntimeDataContext,
)
from runtime.infrastructure.sdk.runtime_indicator_context import (
    RuntimeIndicatorContext,
)
from runtime.infrastructure.sdk.runtime_logging_context import (
    RuntimeLoggingContext,
)
from runtime.infrastructure.sdk.runtime_orders_context import (
    RuntimeOrdersContext,
)
from runtime.infrastructure.sdk.runtime_params_context import (
    RuntimeParamsContext,
)
from runtime.infrastructure.sdk.runtime_strategy_context import (
    RuntimeStrategyContext,
)
from runtime.infrastructure.sdk.runtime_time_context import (
    RuntimeTimeContext,
)
from runtime.infrastructure.sdk.sdk_runtime_types import (
    AssetClass,
    ParameterSchema,
    StrategyMetadata,
)
from runtime.infrastructure.sdk.strategy_params_yaml import (
    merge_parameter_schema_with_adjacent_params_yaml,
)

_VALID_TIMEFRAMES = frozenset(
    {"1m", "5m", "10m", "15m", "30m", "1h", "1w", "1mo", "1d"}
)

_NON_STRATEGY_PARAMETER_KEYS = frozenset(
    {
        "bar_timeframe",
        "data_source",
        "symbol",
        "instrument_id",
        "ts_start",
        "ts_end",
    }
)


def _wire_instrument_id_str(raw: object) -> str:
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return str(raw).strip()
    return ""


def strategy_parameter_overrides_from_launch_payload(
    launch_payload: Mapping[str, object],
) -> dict[str, Any]:
    """
    Overrides for :class:`ParameterSchema` / :class:`RuntimeParamsContext`.

    Prefer top-level ``strategy_params`` (e.g. SDS ``runtime-context``). Otherwise derive
    from ``parameters`` excluding keys reserved for runtime / market-data wiring.
    """
    sp = launch_payload.get("strategy_params")
    if isinstance(sp, dict) and sp:
        out: dict[str, Any] = {}
        for k, v in sp.items():
            if isinstance(k, str) and k.strip():
                out[k.strip()] = v
        return out
    params = launch_payload.get("parameters")
    if not isinstance(params, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(k, str) and k.strip() and k not in _NON_STRATEGY_PARAMETER_KEYS:
            out[k] = v
    return out


def merge_parameter_schema_with_strategy_overrides(
    schema: ParameterSchema,
    overrides: Mapping[str, Any] | None,
) -> ParameterSchema:
    """Apply launch-time strategy parameter values on top of schema defaults (YAML / hooks)."""
    if not overrides:
        return schema
    merged = dict(schema.parameters)
    for key, value in dict(overrides).items():
        if not isinstance(key, str) or not key.strip():
            continue
        k = key.strip()
        existing = merged.get(k)
        if isinstance(existing, dict):
            spec = dict(existing)
            spec["default"] = value
            merged[k] = spec
        else:
            merged[k] = {"default": value}
    return ParameterSchema(parameters=merged)


def _legacy_positional_params(
    handler: Callable[..., object],
) -> list[inspect.Parameter]:
    """Positional parameters visible on a bound user method (``self`` is already applied)."""
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return []
    return [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]


def _dispatch_hook_payload(
    handler: Callable[..., object],
    payload: object,
    legacy_context: RuntimeStrategyContext,
) -> object:
    """
    Call ``handler(payload)`` for the modern contract.

    If the handler still declares a second positional parameter (legacy
    ``on_*(..., strategy_context)``), pass ``legacy_context`` as that argument.
    """
    params = _legacy_positional_params(handler)
    if len(params) >= 2:
        # TODO(STP-1012): remove legacy context arg once all strategies use bound context only.
        return handler(payload, legacy_context)
    return handler(payload)


class RuntimeSubmittingOrderService(DefaultOrderService):
    """
    DefaultOrderService that forwards each constructed replay intent to the
    runtime order-intent submitter (Risk gRPC or BACKTEST stdout).
    """

    __slots__ = ("_submit_order_intent",)

    def __init__(
        self,
        *,
        strategy_id: str | None,
        user_id: str | None,
        submit_order_intent: Callable[[RuntimeOrderIntent], dict[str, Any]],
        submission_time: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(
            strategy_id=strategy_id, user_id=user_id, submission_time=submission_time
        )
        self._submit_order_intent = submit_order_intent

    def _deliver_runtime_intent(self, intent: RuntimeOrderIntent) -> None:
        safe_submit_sdk_order_intent(self._submit_order_intent, intent)


class _ReplayMetrics:
    def total_pnl(self) -> float:
        return 0.0

    def realized_pnl(self) -> float:
        return 0.0

    def unrealized_pnl(self) -> float:
        return 0.0

    def return_pct(self) -> float:
        return 0.0

    def win_rate(self) -> float:
        return 0.0

    def profit_factor(self) -> float:
        return 0.0

    def max_drawdown_pct(self) -> float:
        return 0.0

    def average_trade_pnl(self) -> float:
        return 0.0

    def sharpe_ratio(self) -> float:
        return 0.0

    def exposure_pct(self) -> float:
        return 0.0


class _ReplayLogger(LoggerBackend):
    def emit_log_record(self, record: object) -> None:
        _ = record


class _SimulatedClockService:
    __slots__ = ("_clock",)

    def __init__(self, clock: SimulatedClock) -> None:
        self._clock = clock

    def now(self) -> datetime:
        return self._clock.now()

    def today(self):
        return default_today(self.now())

    def timestamp(self) -> float:
        return self.now().timestamp()

    def register_timer(self, *args: object, **kwargs: object) -> None:
        return None

    def schedule(self, *args: object, **kwargs: object) -> None:
        return None

    def schedule_every(self, *args: object, **kwargs: object) -> None:
        return None


class InMemoryReplayDataService:
    """
    In-memory market data for replay/backtest: bars (indicator history), latest quote/tick.
    """

    __slots__ = ("_by_key", "_primary_tf", "_quotes", "_ticks")

    def __init__(self, *, primary_timeframe: str) -> None:
        self._primary_tf = primary_timeframe
        self._by_key: dict[tuple[str, str], list[Bar]] = defaultdict(list)
        self._quotes: dict[str, Quote] = {}
        self._ticks: dict[str, Tick] = {}

    def append_bar(self, bar: Bar) -> None:
        iid = bar.instrument_id or ""
        key = (iid, bar.timeframe)
        self._by_key[key].append(bar)

    def record_quote(self, quote: Quote) -> None:
        iid = quote.instrument_id or ""
        self._quotes[iid] = quote

    def record_tick(self, tick: Tick) -> None:
        iid = tick.instrument_id or ""
        self._ticks[iid] = tick

    def history(self, instrument_id: str, timeframe: str, lookback: int) -> BarHistory:
        key = (instrument_id, timeframe)
        series = self._by_key.get(key, [])
        if not series or lookback <= 0:
            return BarHistory(instrument_id=instrument_id, timeframe=timeframe, bars=())
        tail = series[-lookback:]
        return BarHistory(
            instrument_id=instrument_id, timeframe=timeframe, bars=tuple(tail)
        )

    def bars(self, instrument_id: str, timeframe: str) -> BarHistory:
        key = (instrument_id, timeframe)
        series = self._by_key.get(key, [])
        return BarHistory(
            instrument_id=instrument_id, timeframe=timeframe, bars=tuple(series)
        )

    def latest_quote(self, instrument_id: str) -> Optional[Quote]:
        return self._quotes.get(instrument_id)

    def latest_bar(self, instrument_id: str, timeframe: str) -> Bar | None:
        key = (instrument_id, timeframe)
        series = self._by_key.get(key, [])
        return series[-1] if series else None

    def latest_price(self, instrument_id: str) -> float | None:
        tick = self._ticks.get(instrument_id)
        if tick is not None:
            return float(tick.price)
        quote = self._quotes.get(instrument_id)
        if quote is not None:
            return float(quote.mid_price)
        bar = self.latest_bar(instrument_id, self._primary_tf)
        return float(bar.close) if bar is not None else None

    def latest_tick(self, instrument_id: str) -> Tick | None:
        return self._ticks.get(instrument_id)


class ReplayStateCoordinator:
    """
    Worker-local coordinator for replay/backtest state. All market and clock updates
    from the worker go through this type.

    The bound :class:`~strategy_worker_runtime.infrastructure.sdk.runtime_strategy_context.RuntimeStrategyContext`
    is built with the same ``InMemoryReplayDataService`` and simulated clock backend
    as held here, so mutating this coordinator updates what strategies read via
    ``context.data``, ``context.time``, etc.—without modifying ``alphovex_sdk``.
    """

    __slots__ = ("_clock", "_data", "_sdk_context", "_default_tf")

    def __init__(
        self,
        *,
        simulated_clock: SimulatedClock,
        data: InMemoryReplayDataService,
        sdk_context: RuntimeStrategyContext,
        default_timeframe: str,
    ) -> None:
        self._clock = simulated_clock
        self._data = data
        self._sdk_context = sdk_context
        self._default_tf = default_timeframe

    @property
    def strategy_context(self) -> RuntimeStrategyContext:
        """SDK context whose services alias the same objects this coordinator mutates."""
        return self._sdk_context

    @property
    def simulated_clock(self) -> SimulatedClock:
        return self._clock

    @property
    def data_service(self) -> InMemoryReplayDataService:
        return self._data

    def resolve_timeframe(self, raw_event: Mapping[str, object]) -> str:
        raw = raw_event.get("timeframe") or raw_event.get("bar_timeframe")
        if isinstance(raw, str) and raw in _VALID_TIMEFRAMES:
            return raw
        return self._default_tf

    def resolve_instrument_id(
        self, raw_event: Mapping[str, object], symbol: str
    ) -> str:
        """Canonical instrument id for SDK bars, history keys, and order intents.

        **Backtest-runner stdio** sends a nested ``bar`` OHLC object with separate top-level
        ``instrument_id`` (venue id, often numeric) and ``symbol`` (ticker). Prefer the wire
        ``instrument_id`` in that shape.

        **Live / Redis ``market.bar``** events are flat (no nested ``bar``); the ticker
        ``symbol`` remains canonical so venue numeric ids do not replace launch symbols.
        """
        sym = str(symbol).strip()
        wire_id = _wire_instrument_id_str(raw_event.get("instrument_id"))
        if isinstance(raw_event.get("bar"), Mapping):
            if wire_id:
                return wire_id
            return sym
        if sym:
            return sym
        return wire_id or sym

    def set_event_time(self, ts_event: datetime) -> None:
        """Advance simulated time before applying market state for this event."""
        self._clock.set_time(ts_event)

    def apply_bar(
        self, raw_event: Mapping[str, object], bar_event: MarketBarEvent
    ) -> Bar:
        sym = bar_event.symbol
        iid = self.resolve_instrument_id(raw_event, sym)
        tf = self.resolve_timeframe(raw_event)
        ts_event = utc_from_epoch_millis(int(bar_event.ts_ms))
        self.set_event_time(ts_event)
        bar = Bar(
            symbol=sym,
            timeframe=cast(Timeframe, tf),
            open=bar_event.open,
            high=bar_event.high,
            low=bar_event.low,
            close=bar_event.close,
            volume=bar_event.volume,
            timestamp=ts_event,
            instrument_id=iid,
        )
        self._data.append_bar(bar)
        return bar

    def apply_quote(
        self, raw_event: Mapping[str, object], quote_event: MarketQuoteEvent
    ) -> Quote:
        sym = quote_event.symbol
        iid = self.resolve_instrument_id(raw_event, sym)
        ts_event = utc_from_epoch_millis(int(quote_event.ts_ms))
        self.set_event_time(ts_event)
        bid = float(quote_event.bid)
        ask = float(quote_event.ask)
        if ask < bid:
            ask = bid
        quote = Quote(
            symbol=sym,
            bid_price=bid,
            ask_price=ask,
            bid_size=float(quote_event.bid_size),
            ask_size=float(quote_event.ask_size),
            timestamp=ts_event,
            instrument_id=iid,
        )
        self._data.record_quote(quote)
        return quote

    def apply_tick(
        self, raw_event: Mapping[str, object], tick_event: MarketTickEvent
    ) -> Tick:
        sym = tick_event.symbol
        iid = self.resolve_instrument_id(raw_event, sym)
        ts_event = utc_from_epoch_millis(int(tick_event.ts_ms))
        self.set_event_time(ts_event)
        tick = Tick(
            symbol=sym,
            timestamp=ts_event,
            price=float(tick_event.price),
            size=float(tick_event.size),
            instrument_id=iid,
        )
        self._data.record_tick(tick)
        return tick

    def apply_timer(
        self, raw_event: Mapping[str, object], wire: WireTimerEvent
    ) -> SdkTimerEvent:
        ts_event = utc_from_epoch_millis(int(wire.scheduled_at_ms))
        self.set_event_time(ts_event)
        _ = raw_event
        data: TimerEventData = {
            "market_status": MarketStateEnum.OPEN,
            "session_info": {
                "start_time": ts_event,
                "end_time": ts_event,
                "session_type": SessionTypeEnum.REGULAR,
            },
            "event_duration": 0,
        }
        return SdkTimerEvent(
            timestamp=ts_event,
            event_type=TimerEventTypeEnum.MARKET_OPEN,
            data=data,
        )


class RuntimeSdkBridge:
    """
    SDK :class:`~strategy_worker_runtime.infrastructure.sdk.runtime_strategy_context.RuntimeStrategyContext`
    plus market-data dispatch for replay ticks mapped by EventMapper.

    State mutations run through :class:`ReplayStateCoordinator`, which shares the same
    data service and clock as the bound strategy context facades.

    Dispatches to strategy hooks: ``on_bar(bar)`` (preferred), ``on_quote``, ``on_tick``,
    ``on_timer`` when implemented on the strategy class (not the SDK default
    :class:`NotImplementedError` stubs). Legacy handlers that still take a second
    positional context argument receive the bound :class:`RuntimeStrategyContext`.
    """

    __slots__ = ("_replay_coord",)

    def __init__(
        self,
        *,
        launch_spec: LaunchSpec,
        worker_identity: WorkerIdentity,
        parameter_schema: ParameterSchema,
        metadata: StrategyMetadata,
        simulated_clock: SimulatedClock,
        default_timeframe: str,
        launch_payload: Mapping[str, object] | None = None,
        submit_sdk_order_intent: (
            Callable[[RuntimeOrderIntent], dict[str, Any]] | None
        ) = None,
    ) -> None:
        default_tf = default_timeframe
        data = InMemoryReplayDataService(primary_timeframe=default_tf)
        indicators = IndicatorService(data=data, timeframe=default_tf)
        clock_svc = _SimulatedClockService(simulated_clock)

        if submit_sdk_order_intent is not None:
            orders: DefaultOrderService | RuntimeSubmittingOrderService = (
                RuntimeSubmittingOrderService(
                    strategy_id=worker_identity.strategy_version_id,
                    user_id=launch_spec.account_id or launch_spec.trader_id,
                    submit_order_intent=submit_sdk_order_intent,
                    submission_time=clock_svc.now,
                )
            )
        else:
            orders = DefaultOrderService(
                strategy_id=worker_identity.strategy_version_id,
                submission_time=clock_svc.now,
            )

        account = launch_spec.account_id or launch_spec.trader_id or "replay"
        seed_currency, seed_cash = _seed_cash_from_launch_payload(launch_payload)
        snap = PortfolioSnapshot(
            account_id=account,
            run_id=launch_spec.runtime_id,
            ts_event=datetime.now(timezone.utc),
            cash_balance=CashBalance(
                currency=seed_currency,
                free=seed_cash,
                locked=0.0,
                buying_power=seed_cash,
                equity=seed_cash,
            ),
            pnl=PnL(),
            exposure=Exposure(),
            margin_state=MarginState(),
            positions={},
        )
        deployment = SimpleNamespace(metadata=metadata, settings=None)
        log_backend = _ReplayLogger()
        metrics = _ReplayMetrics()  # type: ignore[arg-type]
        risk = DefaultRiskService()
        portfolio_svc = SnapshotPortfolioService(snap)
        logging_facade = RuntimeLoggingContext(logger_backend=log_backend)
        sdk_context = RuntimeStrategyContext(
            account=RuntimeAccountContext(portfolio_service=portfolio_svc),
            data=RuntimeDataContext(data_service=data),
            orders=RuntimeOrdersContext(order_service=orders),
            logging_facade=logging_facade,
            params=RuntimeParamsContext(schema=parameter_schema),
            time_ctx=RuntimeTimeContext(clock_service=clock_svc),
            indicator=RuntimeIndicatorContext(indicator_service=indicators),
            risk=risk,
            metrics=metrics,
            deployment=deployment,
            run_id=worker_identity.runtime_id,
            strategy_id=worker_identity.strategy_version_id,
            clock=clock_svc,
            log_backend=log_backend,
            indicators_service=indicators,
        )
        self._replay_coord = ReplayStateCoordinator(
            simulated_clock=simulated_clock,
            data=data,
            sdk_context=sdk_context,
            default_timeframe=default_tf,
        )

    @property
    def replay_state_coordinator(self) -> ReplayStateCoordinator:
        return self._replay_coord

    @property
    def strategy_context(self) -> RuntimeStrategyContext:
        return self._replay_coord.strategy_context

    def dispatch_on_bar(
        self,
        raw_event: Mapping[str, object],
        bar_event: MarketBarEvent,
        on_bar: Callable[..., object],
    ) -> object:
        bar = self._replay_coord.apply_bar(raw_event, bar_event)
        ind_svc = self.strategy_context.indicators
        advance = getattr(ind_svc, "advance_on_bar", None)
        if callable(advance):
            advance(bar)
        return _dispatch_hook_payload(on_bar, bar, self.strategy_context)

    def dispatch_on_quote(
        self,
        raw_event: Mapping[str, object],
        quote_event: MarketQuoteEvent,
        on_quote: Callable[..., object],
    ) -> object:
        quote = self._replay_coord.apply_quote(raw_event, quote_event)
        ind_svc = self.strategy_context.indicators
        advance = getattr(ind_svc, "advance_on_quote", None)
        if callable(advance):
            advance(quote)
        return _dispatch_hook_payload(on_quote, quote, self.strategy_context)

    def dispatch_on_tick(
        self,
        raw_event: Mapping[str, object],
        tick_event: MarketTickEvent,
        on_tick: Callable[..., object],
    ) -> object:
        tick = self._replay_coord.apply_tick(raw_event, tick_event)
        ind_svc = self.strategy_context.indicators
        advance = getattr(ind_svc, "advance_on_tick", None)
        if callable(advance):
            advance(tick)
        return _dispatch_hook_payload(on_tick, tick, self.strategy_context)

    def dispatch_on_timer(
        self,
        raw_event: Mapping[str, object],
        wire: WireTimerEvent,
        on_timer: Callable[..., object],
    ) -> object:
        sdk = self._replay_coord.apply_timer(raw_event, wire)
        return _dispatch_hook_payload(on_timer, sdk, self.strategy_context)


def build_strategy_metadata(strategy: object) -> StrategyMetadata:
    gm = getattr(type(strategy), "get_metadata", None)
    if callable(gm):
        meta = gm()
        if isinstance(meta, StrategyMetadata):
            return meta
    return StrategyMetadata(
        name="strategy",
        description="",
        version="0.0.0",
        author="",
        supported_asset_classes=(AssetClass.EQUITY,),
        parameter_schema=ParameterSchema(parameters={}),
    )


def replay_sdk_hook_overridden(strategy: object, name: str) -> bool:
    """True if the concrete strategy class defines its own handler (not the SDK default stub)."""
    cls = type(strategy)
    impl = getattr(cls, name, None)
    if not callable(impl):
        return False
    if isinstance(strategy, SdkStrategy):
        base_impl = getattr(SdkStrategy, name, None)
        if base_impl is not None and impl is base_impl:
            return False
    return True


def build_runtime_sdk_bridge(
    *,
    strategy: object,
    launch_spec: LaunchSpec,
    worker_identity: WorkerIdentity,
    simulated_clock: SimulatedClock,
    launch_payload: Mapping[str, object] | None = None,
    submit_sdk_order_intent: (
        Callable[[RuntimeOrderIntent], dict[str, Any]] | None
    ) = None,
    default_timeframe: str = "1m",
    calculation_spec: StrategyCalculationSpec | None = None,
) -> RuntimeSdkBridge:
    schema_fn = getattr(type(strategy), "build_parameter_schema", None)
    if callable(schema_fn):
        schema = schema_fn()
    else:
        schema = ParameterSchema(parameters={})
    if not isinstance(schema, ParameterSchema):
        schema = ParameterSchema(parameters={})

    schema = merge_parameter_schema_with_adjacent_params_yaml(strategy, schema)

    if calculation_spec is not None:
        overrides = dict(calculation_spec.params)
        default_tf = (calculation_spec.bar_timeframe or default_timeframe).strip()
    else:
        overrides = (
            strategy_parameter_overrides_from_launch_payload(launch_payload)
            if launch_payload is not None
            else {}
        )
        default_tf = default_timeframe.strip() or "1m"
    schema = merge_parameter_schema_with_strategy_overrides(schema, overrides)

    default_tf = default_tf or "1m"
    if default_tf not in _VALID_TIMEFRAMES:
        default_tf = "1m"

    return RuntimeSdkBridge(
        launch_spec=launch_spec,
        worker_identity=worker_identity,
        parameter_schema=schema,
        metadata=build_strategy_metadata(strategy),
        simulated_clock=simulated_clock,
        default_timeframe=default_tf,
        launch_payload=launch_payload,
        submit_sdk_order_intent=submit_sdk_order_intent,
    )


def _seed_cash_from_launch_payload(
    launch_payload: Mapping[str, object] | None,
) -> tuple[str, float]:
    """Resolve opening cash from SDS runtime-context ``initial_cash`` (or legacy default)."""
    default_cash = 1_000_000.0
    default_currency = "USD"
    if launch_payload is None:
        return default_currency, default_cash
    raw = launch_payload.get("initial_cash")
    if not isinstance(raw, dict):
        return default_currency, default_cash
    currency = (
        str(raw.get("currency") or default_currency).strip().upper() or default_currency
    )
    amount_raw = raw.get("amount")
    if amount_raw is None:
        return currency, default_cash
    try:
        cash = float(str(amount_raw).strip())
    except ValueError:
        return currency, default_cash
    return currency, max(0.0, cash)
