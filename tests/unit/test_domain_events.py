from __future__ import annotations

import logging

from runtime.observability.domain_events import (
    DomainEventSampler,
    StrategyWorkerDomainEvent,
    emit_bound_domain_event,
    emit_strategy_worker_domain_event,
)
from runtime.observability.logger import RuntimeLogContext, bind_runtime_context


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _bound_logger() -> tuple[logging.Logger, _CaptureHandler, object]:
    logger = logging.getLogger("test-domain-events")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    handler = _CaptureHandler()
    logger.addHandler(handler)
    ctx = RuntimeLogContext(
        runtime_id="rt-1",
        tenant_id="tenant-1",
        worker_identity="worker:rt-1:1",
        launch_attempt=1,
        deployment_id="dep-1",
        mode="PAPER",
        runtime_type="STRATEGY_WORKER_RUNTIME",
    )
    return logger, handler, bind_runtime_context(logger, ctx)


def test_emit_strategy_worker_domain_event_includes_runtime_type() -> None:
    logger, handler, _ = _bound_logger()
    emit_strategy_worker_domain_event(
        logger,
        event_name=StrategyWorkerDomainEvent.RUNTIME_CONTEXT_LOADING_STARTED.value,
        message="loading",
        fields={"deployment_id": "dep-1"},
    )
    event = getattr(handler.records[0], "event", {})
    assert event["event_name"] == "strategy_worker.runtime_context_loading_started"
    assert event["runtime_type"] == "STRATEGY_WORKER_RUNTIME"
    assert event["deployment_id"] == "dep-1"


def test_emit_bound_domain_event_preserves_context_fields() -> None:
    logger, handler, bound = _bound_logger()
    emit_bound_domain_event(
        bound,
        event_name=StrategyWorkerDomainEvent.STARTED,
        message="started",
    )
    event = getattr(handler.records[0], "event", {})
    assert event["event_name"] == "strategy_worker.started"
    assert event["deployment_id"] == "dep-1"
    assert event["runtime_type"] == "STRATEGY_WORKER_RUNTIME"
    assert event["mode"] == "PAPER"


def test_domain_event_sampler_emits_first_and_every_nth() -> None:
    sampler = DomainEventSampler(sample_every=3)
    assert sampler.next("k") == (1, True)
    assert sampler.next("k") == (2, False)
    assert sampler.next("k") == (3, True)


def test_strategy_worker_domain_event_names_are_prefixed() -> None:
    for item in StrategyWorkerDomainEvent:
        assert item.value.startswith("strategy_worker.")
