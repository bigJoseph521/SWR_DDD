from __future__ import annotations

import pytest
from runtime.observability.tracing import (
    InMemorySpanExporter,
    RuntimeTraceContext,
    WorkerTracer,
    WorkerTraceSpans,
)


def _context() -> RuntimeTraceContext:
    return RuntimeTraceContext(
        runtime_id="rt-1",
        worker_identity="worker:rt-1:1",
        launch_attempt=1,
        tenant_id="tenant-1",
        correlation_id="corr-1",
        account_id="acct-1",
        strategy_version_id="sv-1",
        mode="BACKTEST",
    )


def test_bootstrap_and_heartbeat_spans_exist_with_required_identity_attributes() -> (
    None
):
    exporter = InMemorySpanExporter()
    tracer = WorkerTracer(exporter)
    context = _context()

    with tracer.span(WorkerTraceSpans.BOOTSTRAP, context):
        pass
    with tracer.span(WorkerTraceSpans.RUNTIME_EMIT_HEARTBEAT, context):
        pass

    names = [span.name for span in exporter.finished_spans]
    assert WorkerTraceSpans.BOOTSTRAP in names
    assert WorkerTraceSpans.RUNTIME_EMIT_HEARTBEAT in names

    bootstrap = exporter.finished_spans[0]
    assert bootstrap.attributes["runtime_id"] == "rt-1"
    assert bootstrap.attributes["worker_identity"] == "worker:rt-1:1"
    assert bootstrap.attributes["launch_attempt"] == 1
    assert bootstrap.attributes["tenant_id"] == "tenant-1"
    assert bootstrap.attributes["correlation_id"] == "corr-1"


def test_trace_context_accepts_blank_tenant_id() -> None:
    ctx = RuntimeTraceContext(
        runtime_id="rt-1",
        worker_identity="worker:rt-1:1",
        launch_attempt=1,
        tenant_id="  ",
        correlation_id="corr-1",
    )
    assert ctx.tenant_id == ""
    assert ctx.to_attributes()["tenant_id"] == ""


def test_failed_bootstrap_records_exception_and_error_status() -> None:
    exporter = InMemorySpanExporter()
    tracer = WorkerTracer(exporter)

    with pytest.raises(RuntimeError, match="bootstrap failed"):
        with tracer.span(WorkerTraceSpans.BOOTSTRAP, _context()):
            raise RuntimeError("bootstrap failed")

    span = exporter.finished_spans[0]
    assert span.name == WorkerTraceSpans.BOOTSTRAP
    assert span.status == "error"
    assert span.exception_type == "RuntimeError"
    assert span.exception_message == "bootstrap failed"
