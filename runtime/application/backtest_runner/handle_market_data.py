"""Dispatch one stdio ``MARKET_DATA_EVENT`` to the loaded strategy."""

from __future__ import annotations

from runtime.application.backtest_runner.messages import (
    MarketDataEventMessage,
    NoOpMessage,
    OrderIntentsMessage,
    OutboundMessage,
)
from runtime.application.backtest_runner.ports import (
    MarketDataWirePort,
    OrderIntentWirePort,
)
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.domain.launch_spec import LaunchSpec


class MarketDataDispatchError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code.strip().upper()
        self.message = message


def dispatch_market_data_event(
    *,
    adapter: StrategyAdapter,
    launch_spec: LaunchSpec,
    collector: object,
    message: MarketDataEventMessage,
    market_data_wire: MarketDataWirePort,
    order_intent_wire: OrderIntentWirePort,
) -> OutboundMessage:
    try:
        raw_event = market_data_wire.normalize_event(message.event)
    except Exception as exc:
        raise MarketDataDispatchError("MARKET_DATA_EVENT_INVALID", str(exc)) from exc

    handler_result = adapter.on_event(raw_event)
    if not handler_result.ok:
        raise MarketDataDispatchError(
            "STRATEGY_EVENT_HANDLER_FAILED",
            (
                handler_result.reason_code
                or handler_result.error_code
                or "strategy event handler failed"
            ),
        )

    try:
        wire_intents = order_intent_wire.drain_to_wire_intents(
            collector, launch_spec=launch_spec
        )
    except Exception as exc:
        raise MarketDataDispatchError(
            "ORDER_INTENT_SERIALIZATION_FAILED",
            str(exc),
        ) from exc

    if wire_intents:
        return OrderIntentsMessage(intents=wire_intents)
    return NoOpMessage()
