from __future__ import annotations

import logging
from enum import StrEnum
from typing import Any, Mapping

from runtime.observability.logger import RuntimeBoundLogger

STRATEGY_WORKER_RUNTIME_TYPE = "STRATEGY_WORKER_RUNTIME"


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


class DomainEventSampler:
    """Emit every Nth high-volume event at INFO; intermediate events stay at DEBUG."""

    def __init__(self, *, sample_every: int = 100) -> None:
        if sample_every < 1:
            raise ValueError("sample_every must be >= 1")
        self._sample_every = sample_every
        self._counters: dict[str, int] = {}

    def next(self, key: str) -> tuple[int, bool]:
        count = self._counters.get(key, 0) + 1
        self._counters[key] = count
        return count, count == 1 or count % self._sample_every == 0


def emit_strategy_worker_domain_event(
    logger: logging.Logger,
    *,
    event_name: str,
    message: str,
    level: int = logging.INFO,
    fields: Mapping[str, Any] | None = None,
) -> None:
    """Structured domain event before :class:`RuntimeBoundLogger` is available."""
    payload: dict[str, Any] = {
        "event_name": event_name,
        "runtime_type": STRATEGY_WORKER_RUNTIME_TYPE,
    }
    for key, value in (fields or {}).items():
        if value is not None:
            payload[key] = value
    try:
        logger.log(level, message, extra={"event": payload})
    except Exception:
        return


def emit_bound_domain_event(
    bound_logger: RuntimeBoundLogger,
    *,
    event_name: StrategyWorkerDomainEvent | str,
    message: str,
    level: int = logging.INFO,
    event_extras: Mapping[str, Any] | None = None,
) -> None:
    bound_logger.emit(
        event_name=str(event_name),
        message=message,
        level=level,
        event_extras=dict(event_extras or {}),
    )
