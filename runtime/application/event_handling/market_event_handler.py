from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping

from runtime.application.runtime_state.runtime_state import RuntimeState
from runtime.application.strategy_execution.strategy_execution_service import (
    StrategyExecutionService,
)
from runtime.application.strategy_execution.strategy_error_boundary import (
    StrategyCallResult,
)
from runtime.domain.enums import WorkerMode
from runtime.domain.model.normalized_events import (
    MarketBarEvent,
    MarketQuoteEvent,
    MarketTickEvent,
    TimerEvent,
)


class MarketEventHandler:
    def __init__(
        self,
        *,
        strategy_execution: StrategyExecutionService,
        runtime_state: RuntimeState,
        mode: WorkerMode,
        on_signal_generated: Callable[[Mapping[str, str]], None] | None = None,
        on_execution_failed: Callable[[StrategyCallResult[Any]], None] | None = None,
        extract_timestamp: Callable[[Mapping[str, Any]], datetime | None] | None = None,
    ) -> None:
        self._strategy_execution = strategy_execution
        self._runtime_state = runtime_state
        self._mode = mode
        self._on_signal_generated = on_signal_generated
        self._on_execution_failed = on_execution_failed
        self._extract_timestamp = extract_timestamp

    def handle_raw_tick(
        self, tick: Mapping[str, Any]
    ) -> StrategyCallResult[Any] | None:
        if not self._runtime_state.accepts_work():
            return None
        if not self._runtime_state.first_data_received:
            self._runtime_state.mark_first_data_received()
        if self._extract_timestamp is not None:
            ts = self._extract_timestamp(tick)
            self._runtime_state.record_data_event_timestamp(ts)
        result = self._strategy_execution.on_raw_event(dict(tick))
        if isinstance(result, StrategyCallResult) and result.ok:
            if (
                self._mode in (WorkerMode.PAPER, WorkerMode.LIVE)
                and self._on_signal_generated
            ):
                self._on_signal_generated(
                    {
                        "symbol": str(tick.get("symbol") or ""),
                        "instrument_id": str(tick.get("instrument_id") or ""),
                        "event_type": str(
                            tick.get("event_type") or tick.get("type") or ""
                        ),
                    }
                )
        elif isinstance(result, StrategyCallResult) and not result.ok:
            if self._on_execution_failed is not None:
                self._on_execution_failed(result)
        return result

    def handle_market_bar(self, event: MarketBarEvent, raw: Mapping[str, Any]) -> None:
        payload = dict(raw)
        payload.setdefault("type", event.event_type)
        self.handle_raw_tick(payload)

    def handle_market_tick(
        self, event: MarketTickEvent, raw: Mapping[str, Any]
    ) -> None:
        payload = dict(raw)
        payload.setdefault("type", event.event_type)
        self.handle_raw_tick(payload)

    def handle_market_quote(
        self, event: MarketQuoteEvent, raw: Mapping[str, Any]
    ) -> None:
        payload = dict(raw)
        payload.setdefault("type", event.event_type)
        self.handle_raw_tick(payload)

    def handle_timer(self, event: TimerEvent, raw: Mapping[str, Any]) -> None:
        payload = dict(raw)
        payload.setdefault("type", event.event_type)
        self.handle_raw_tick(payload)
