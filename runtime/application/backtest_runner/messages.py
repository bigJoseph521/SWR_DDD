"""Session-layer messages for backtest-runner subprocess (application boundary)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class InitMessage:
    """Legacy stdio ``INIT`` (tests)."""

    protocol_version: str
    backtest_job_id: str
    artifact_uri: str
    entrypoint: str
    strategy_id: str = ""
    strategy_version_id: str = ""
    parameters: dict[str, Any] = field(default_factory=dict)
    time_range: dict[str, Any] = field(default_factory=dict)
    symbols: list[Any] = field(default_factory=list)
    bar_resolution: str = ""
    type: Literal["INIT"] = "INIT"


@dataclass(frozen=True, slots=True)
class PortfolioSnapshotMessage:
    portfolio: dict[str, Any] = field(default_factory=dict)
    type: Literal["PORTFOLIO_SNAPSHOT"] = "PORTFOLIO_SNAPSHOT"


@dataclass(frozen=True, slots=True)
class OpenOrdersSnapshotMessage:
    orders: list[Any] = field(default_factory=list)
    type: Literal["OPEN_ORDERS_SNAPSHOT"] = "OPEN_ORDERS_SNAPSHOT"


@dataclass(frozen=True, slots=True)
class MarketDataEventMessage:
    event: dict[str, Any]
    type: Literal["MARKET_DATA_EVENT"] = "MARKET_DATA_EVENT"


@dataclass(frozen=True, slots=True)
class OrderIntentsMessage:
    intents: list[Any] = field(default_factory=list)
    type: Literal["ORDER_INTENTS"] = "ORDER_INTENTS"


@dataclass(frozen=True, slots=True)
class NoOpMessage:
    type: Literal["NO_OP"] = "NO_OP"


@dataclass(frozen=True, slots=True)
class StrategyErrorMessage:
    code: str
    message: str
    type: Literal["STRATEGY_ERROR"] = "STRATEGY_ERROR"


InboundMessage = (
    InitMessage
    | PortfolioSnapshotMessage
    | OpenOrdersSnapshotMessage
    | MarketDataEventMessage
)

OutboundMessage = OrderIntentsMessage | NoOpMessage | StrategyErrorMessage
