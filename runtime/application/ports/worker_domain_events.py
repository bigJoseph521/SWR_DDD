from __future__ import annotations

from enum import StrEnum


class StrategyWorkerDomainEvent(StrEnum):
    STARTED = "strategy_worker.started"
    RUNTIME_CONTEXT_LOADING_STARTED = "strategy_worker.runtime_context_loading_started"
    RUNTIME_CONTEXT_LOADED = "strategy_worker.runtime_context_loaded"
    STRATEGY_LOADED = "strategy_worker.strategy_loaded"
    READY = "strategy_worker.ready"
    MARKET_DATA_SUBSCRIPTION_STARTED = (
        "strategy_worker.market_data_subscription_started"
    )
    SIGNAL_GENERATED = "strategy_worker.signal_generated"
    ORDER_INTENT_EMITTED = "strategy_worker.order_intent_emitted"
    HEARTBEAT_SENT = "strategy_worker.heartbeat_sent"
    SHUTDOWN_REQUESTED = "strategy_worker.shutdown_requested"
    STOPPED = "strategy_worker.stopped"
    FAILED = "strategy_worker.failed"
