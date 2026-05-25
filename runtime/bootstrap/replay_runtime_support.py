"""
Replay/backtest helpers that replace SDK modules not present in this repository's
alphovex_sdk tree (no ``services`` package, consolidated ``models``, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import threading
from typing import Any, Callable, List, NamedTuple

from alphovex_sdk.context.indicator_context import DataSourceEnum
from alphovex_sdk.enums.order import OrderSide, OrderType, TimeInForce
from alphovex_sdk.models.order import OrderIntent


class LoggerBackend:
    """Minimal logger backend protocol used by :class:`RuntimeLoggingContext`."""

    def emit_log_record(self, record: object) -> None:
        raise NotImplementedError


class BarHistory(NamedTuple):
    instrument_id: str
    timeframe: str
    bars: tuple[Any, ...]


@dataclass
class CashBalance:
    currency: str
    free: float
    locked: float = 0.0
    buying_power: float | None = None
    equity: float | None = None


@dataclass
class PnL:
    pass


@dataclass
class Exposure:
    pass


@dataclass
class MarginState:
    pass


@dataclass
class PortfolioSnapshot:
    """Shape consumed by :func:`runtime_account_context._snapshot_to_portfolio`."""

    account_id: str
    run_id: str
    ts_event: datetime
    cash_balance: CashBalance
    pnl: PnL
    exposure: Exposure
    margin_state: MarginState
    positions: dict[str, Any]


class SnapshotPortfolioService:
    __slots__ = ("_lock", "_snapshot")

    def __init__(self, snapshot: PortfolioSnapshot) -> None:
        self._snapshot = snapshot
        self._lock = threading.Lock()

    def get_snapshot(self) -> PortfolioSnapshot:
        with self._lock:
            return self._snapshot

    def apply_pls_balance_update(
        self,
        *,
        cash_balance: float,
        buying_power: float,
        equity: float,
        timestamp: datetime,
    ) -> None:
        """Apply portfolio-ledger Pub/Sub balance fields (does not touch positions)."""
        with self._lock:
            cur = self._snapshot.cash_balance
            self._snapshot.ts_event = timestamp
            self._snapshot.cash_balance = CashBalance(
                currency=cur.currency,
                free=cash_balance,
                locked=cur.locked,
                buying_power=buying_power,
                equity=equity,
            )


@dataclass
class ReplayOrderIntent:
    """Duck-compatible order intent for gRPC mapping (SDK :class:`OrderIntent` is frozen/brittle)."""

    instrument_id: str
    side: OrderSide
    quantity: float
    price: float
    order_type: OrderType
    limit_price: float | None
    stop_price: float | None
    time_in_force: TimeInForce | None
    client_order_id: str | None = None
    created_at: datetime | None = None

    @property
    def qty(self) -> float:
        return self.quantity


class DefaultOrderService:
    """Minimal in-process order facade for replay; subclass for gRPC forwarding."""

    __slots__ = ("_strategy_id", "_user_id", "_submission_time")

    def __init__(
        self,
        *,
        strategy_id: str | None,
        user_id: str | None = None,
        submission_time: Callable[[], datetime] | None = None,
    ) -> None:
        self._strategy_id = strategy_id
        self._user_id = user_id
        self._submission_time = submission_time

    def buy(self, order_intent: OrderIntent) -> None:
        if order_intent.side is not OrderSide.BUY:
            raise ValueError("buy() requires OrderIntent with side=BUY")
        self._deliver_replay_intent(self._replay_intent_from_sdk(order_intent))

    def sell(self, order_intent: OrderIntent) -> None:
        if order_intent.side is not OrderSide.SELL:
            raise ValueError("sell() requires OrderIntent with side=SELL")
        self._deliver_replay_intent(self._replay_intent_from_sdk(order_intent))

    def cancel_all(self) -> None:
        return None

    def cancel_for_instrument(self, instrument_id: str) -> None:
        return None

    def cancel(self, order_id: object) -> None:
        return None

    def list(self) -> List[Any]:
        return []

    def get(self, order_id: object) -> None:
        return None

    def active(self) -> List[Any]:
        return []

    def done(self) -> List[Any]:
        return []

    def filled(self) -> List[Any]:
        return []

    def for_instrument(self, instrument_id: str) -> List[Any]:
        return []

    def active_for_instrument(self, instrument_id: str) -> List[Any]:
        return []

    def has_active_for_instrument(self, instrument_id: str) -> bool:
        return False

    def _replay_intent_from_sdk(self, oi: OrderIntent) -> ReplayOrderIntent:
        if self._submission_time is not None:
            created = self._submission_time()
        else:
            created = datetime.now(timezone.utc)
        if created.tzinfo is None or created.utcoffset() is None:
            created = created.replace(tzinfo=timezone.utc)
        else:
            created = created.astimezone(timezone.utc)
        return ReplayOrderIntent(
            instrument_id=str(oi.instrument_id),
            side=oi.side,
            quantity=float(oi.quantity),
            price=float(oi.price),
            order_type=oi.order_type,
            limit_price=None if oi.limit_price is None else float(oi.limit_price),
            stop_price=None if oi.stop_price is None else float(oi.stop_price),
            time_in_force=oi.time_in_force,
            client_order_id=None,
            created_at=created,
        )

    def _deliver_replay_intent(self, intent: ReplayOrderIntent) -> None:
        _ = intent


class IndicatorService:
    """
    Replay indicator backend: advances SDK :class:`~alphovex_sdk.indicators.base.Indicator`
    instances when bars/quotes/ticks arrive so :meth:`get_indicator_value` matches
    :meth:`~alphovex_sdk.strategy.base.Strategy.on_bar` ordering.
    """

    __slots__ = ("_data", "_timeframe", "_registrations", "_values")

    def __init__(self, *, data: object, timeframe: str) -> None:
        self._data = data
        self._timeframe = timeframe
        self._registrations: list[tuple[str, object, DataSourceEnum]] = []
        self._values: dict[str, float | None] = {}

    def register_indicator(
        self, indicator_name: str, indicator: object, data_source: DataSourceEnum
    ) -> None:
        self._registrations.append((indicator_name, indicator, data_source))
        self._values[indicator_name] = None

    def advance_on_bar(self, bar: object) -> None:
        for name, ind, ds in self._registrations:
            if ds is not DataSourceEnum.BAR:
                continue
            update = getattr(ind, "update", None)
            if not callable(update):
                continue
            update(bar)
            self._values[name] = getattr(ind, "value", None)

    def advance_on_quote(self, quote: object) -> None:
        for name, ind, ds in self._registrations:
            if ds is not DataSourceEnum.QUOTE:
                continue
            update = getattr(ind, "update", None)
            if not callable(update):
                continue
            update(quote)
            self._values[name] = getattr(ind, "value", None)

    def advance_on_tick(self, tick: object) -> None:
        for name, ind, ds in self._registrations:
            if ds is not DataSourceEnum.TICK:
                continue
            update = getattr(ind, "update", None)
            if not callable(update):
                continue
            update(tick)
            self._values[name] = getattr(ind, "value", None)

    def get_indicator_value(self, indicator_id: str) -> float | None:
        if indicator_id not in self._values:
            return None
        return self._values[indicator_id]


class DefaultRiskService:
    """Placeholder risk service attached to :class:`RuntimeStrategyContext`."""

    pass
