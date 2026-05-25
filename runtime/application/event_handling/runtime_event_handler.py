from __future__ import annotations

from typing import Any, Mapping

from runtime.application.event_handling.market_event_handler import MarketEventHandler
from runtime.application.event_handling.portfolio_update_handler import (
    PortfolioUpdateHandler,
)
from runtime.domain.model.normalized_events import (
    MarketBarEvent,
    MarketQuoteEvent,
    MarketTickEvent,
    OrderUpdatedEvent,
    PortfolioUpdatedEvent,
    RuntimeEvent,
    TimerEvent,
)


class RuntimeEventHandler:
    """Routes normalized runtime events to market or context handlers."""

    def __init__(
        self,
        *,
        market_handler: MarketEventHandler,
        portfolio_handler: PortfolioUpdateHandler | None = None,
    ) -> None:
        self._market_handler = market_handler
        self._portfolio_handler = portfolio_handler

    def has_market_handler(self) -> bool:
        return self._market_handler is not None

    def has_portfolio_handler(self) -> bool:
        return self._portfolio_handler is not None

    def handle_raw_tick(self, raw_event: Mapping[str, Any]) -> Any:
        if self._market_handler is None:
            return None
        return self._market_handler.handle_raw_tick(raw_event)

    def handle_portfolio_update(self, event: PortfolioUpdatedEvent) -> None:
        if self._portfolio_handler is not None:
            self._portfolio_handler.handle(event)
        else:
            self.handle(event)

    def handle(
        self, event: RuntimeEvent, *, raw: Mapping[str, Any] | None = None
    ) -> None:
        raw_payload = dict(raw or {})
        if isinstance(event, MarketBarEvent):
            self._market_handler.handle_market_bar(event, raw_payload)
        elif isinstance(event, MarketTickEvent):
            self._market_handler.handle_market_tick(event, raw_payload)
        elif isinstance(event, MarketQuoteEvent):
            self._market_handler.handle_market_quote(event, raw_payload)
        elif isinstance(event, TimerEvent):
            self._market_handler.handle_timer(event, raw_payload)
        elif isinstance(event, PortfolioUpdatedEvent):
            if self._portfolio_handler is not None:
                self._portfolio_handler.handle(event)
        elif isinstance(event, OrderUpdatedEvent):
            # Order updates adjust runtime context only; strategy hooks unchanged.
            pass
