from __future__ import annotations

import logging
from typing import Any, Mapping

from runtime.application.event_handling.runtime_event_handler import RuntimeEventHandler
from runtime.application.strategy_execution.strategy_execution_service import (
    StrategyExecutionService,
)
from runtime.application.strategy_execution.event_mapper import EventMapper, EventMappingError
from runtime.application.strategy_execution.event_mapper import MarketBarEvent as WireMarketBarEvent
from runtime.application.strategy_execution.event_mapper import MarketQuoteEvent as WireMarketQuoteEvent
from runtime.application.strategy_execution.event_mapper import MarketTickEvent as WireMarketTickEvent
from runtime.application.strategy_execution.event_mapper import TimerEvent as WireTimerEvent
from runtime.domain.model import normalized_events as domain_events

_LOG = logging.getLogger(__name__)


def wire_event_to_domain(
    mapped: Any,
    raw: Mapping[str, Any],
) -> domain_events.RuntimeEvent | None:
    """Map wire EventMapper output to domain normalized events."""
    if isinstance(mapped, WireMarketBarEvent):
        return domain_events.MarketBarEvent(
            symbol=mapped.symbol,
            open=mapped.open,
            high=mapped.high,
            low=mapped.low,
            close=mapped.close,
            volume=mapped.volume,
            ts_ms=mapped.ts_ms,
            event_type=mapped.event_type,
            metadata={"raw_keys": list(raw.keys())},
        )
    if isinstance(mapped, WireMarketQuoteEvent):
        return domain_events.MarketQuoteEvent(
            symbol=mapped.symbol,
            bid=mapped.bid,
            ask=mapped.ask,
            bid_size=mapped.bid_size,
            ask_size=mapped.ask_size,
            ts_ms=mapped.ts_ms,
            event_type=mapped.event_type,
            metadata={"raw_keys": list(raw.keys())},
        )
    if isinstance(mapped, WireMarketTickEvent):
        return domain_events.MarketTickEvent(
            symbol=mapped.symbol,
            price=mapped.price,
            size=mapped.size,
            ts_ms=mapped.ts_ms,
            event_type=mapped.event_type,
            metadata={"raw_keys": list(raw.keys())},
        )
    if isinstance(mapped, WireTimerEvent):
        return domain_events.TimerEvent(
            timer_id=mapped.timer_id,
            scheduled_at_ms=mapped.scheduled_at_ms,
            event_type=mapped.event_type,
            metadata=dict(mapped.metadata),
        )
    return None


class EventDispatcher:
    """
    Application-level dispatcher for normalized runtime events.

    Raw transport payloads (Redis tick dict, replay ingress tick) are normalized
    then routed to the same strategy execution path.
    """

    def __init__(
        self,
        *,
        runtime_handler: RuntimeEventHandler,
        strategy_execution: StrategyExecutionService | None = None,
        mapper: EventMapper | None = None,
    ) -> None:
        self._runtime_handler = runtime_handler
        self._strategy_execution = strategy_execution
        self._mapper = mapper or EventMapper()

    def dispatch_raw(self, raw_event: Mapping[str, Any]) -> None:
        """Dispatch a wire-format event (PAPER/LIVE Redis or BACKTEST replay tick)."""
        payload = dict(raw_event)
        if self._runtime_handler.has_market_handler():
            self._runtime_handler.handle_raw_tick(payload)
            return
        if self._strategy_execution is not None:
            self._strategy_execution.on_raw_event(payload)
            return
        try:
            mapped = self._mapper.map_event(raw_event)
        except EventMappingError:
            _LOG.debug("event_dispatch_raw_mapping_failed", exc_info=True)
            return
        domain_event = wire_event_to_domain(mapped, raw_event)
        if domain_event is not None:
            self._runtime_handler.handle(domain_event, raw=raw_event)

    def dispatch(
        self, event: domain_events.RuntimeEvent, *, raw: Mapping[str, Any] | None = None
    ) -> None:
        self._runtime_handler.handle(event, raw=raw)

    def dispatch_portfolio(self, event: domain_events.PortfolioUpdatedEvent) -> None:
        self._runtime_handler.handle_portfolio_update(event)
