from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping

# Transport-independent runtime events consumed by application dispatch.


@dataclass(frozen=True, slots=True)
class MarketBarEvent:
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    ts_ms: int
    event_type: str = "market.bar"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MarketQuoteEvent:
    symbol: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    ts_ms: int
    event_type: str = "market.quote"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MarketTickEvent:
    symbol: str
    price: float
    size: float
    ts_ms: int
    event_type: str = "market.tick"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TimerEvent:
    timer_id: str
    scheduled_at_ms: int
    event_type: str = "timer"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PortfolioUpdatedEvent:
    """Normalized portfolio balance update (transport-independent)."""

    job_id: str
    timestamp: datetime
    cash_balance: Decimal
    buying_power: Decimal
    equity: Decimal


@dataclass(frozen=True, slots=True)
class OrderUpdatedEvent:
    order_id: str | None
    status: str | None
    observed_at: datetime | None
    payload: Mapping[str, Any] = field(default_factory=dict)


RuntimeEvent = (
    MarketBarEvent
    | MarketQuoteEvent
    | MarketTickEvent
    | TimerEvent
    | PortfolioUpdatedEvent
    | OrderUpdatedEvent
)
