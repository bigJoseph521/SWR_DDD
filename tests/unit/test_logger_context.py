from __future__ import annotations

import logging

import pytest
from runtime.observability.logger import (
    RuntimeLogContext,
    bind_runtime_context,
)


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _make_logger() -> tuple[logging.Logger, _CaptureHandler]:
    logger = logging.getLogger("test-runtime-logger")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = _CaptureHandler()
    logger.addHandler(handler)
    return logger, handler


def _base_context() -> RuntimeLogContext:
    return RuntimeLogContext(
        runtime_id="rt-1",
        tenant_id="tenant-1",
        worker_identity="worker:rt-1:1",
        launch_attempt=1,
        correlation_id="corr-1",
        account_id="acct-1",
    )


def test_emits_with_required_context() -> None:
    logger, handler = _make_logger()
    runtime_logger = bind_runtime_context(logger, _base_context())

    runtime_logger.emit(
        event_name="runtime.launch_succeeded",
        message="bootstrap succeeded",
        event_extras={"component": "bootstrap"},
    )

    event_payload = getattr(handler.records[0], "event", {})
    assert isinstance(event_payload, dict)
    assert event_payload["runtime_id"] == "rt-1"
    assert event_payload["tenant_id"] == "tenant-1"
    assert event_payload["worker_identity"] == "worker:rt-1:1"
    assert event_payload["launch_attempt"] == 1
    assert event_payload["correlation_id"] == "corr-1"
    assert event_payload["component"] == "bootstrap"


def test_accepts_blank_tenant_id() -> None:
    ctx = RuntimeLogContext(
        runtime_id="rt-1",
        tenant_id="  ",
        worker_identity="worker:rt-1:1",
        launch_attempt=1,
        correlation_id="corr-1",
    )
    assert ctx.tenant_id == ""


def test_rejects_missing_runtime_id() -> None:
    with pytest.raises(ValueError, match="runtime_id"):
        RuntimeLogContext(
            runtime_id="",
            tenant_id="tenant-1",
            worker_identity="worker:rt-1:1",
            launch_attempt=1,
            correlation_id="corr-1",
        )


def test_rejects_missing_launch_attempt() -> None:
    with pytest.raises(ValueError, match="launch_attempt"):
        RuntimeLogContext(
            runtime_id="rt-1",
            tenant_id="tenant-1",
            worker_identity="worker:rt-1:1",
            launch_attempt=0,
            correlation_id="corr-1",
        )


def test_child_logger_preserves_runtime_context() -> None:
    logger, handler = _make_logger()
    runtime_logger = bind_runtime_context(logger, _base_context())
    child = runtime_logger.child(component="heartbeat")

    child.emit(event_name="runtime.heartbeat", message="heartbeat emitted")

    event_payload = getattr(handler.records[0], "event", {})
    assert isinstance(event_payload, dict)
    assert event_payload["runtime_id"] == "rt-1"
    assert event_payload["launch_attempt"] == 1
    assert event_payload["component"] == "heartbeat"


def test_rejects_identity_override_from_event_extras() -> None:
    logger, _ = _make_logger()
    runtime_logger = bind_runtime_context(logger, _base_context())

    with pytest.raises(ValueError, match="canonical identity fields"):
        runtime_logger.emit(
            event_name="runtime.launch_failed",
            message="bootstrap failed",
            event_extras={"runtime_id": "rt-other"},
        )
