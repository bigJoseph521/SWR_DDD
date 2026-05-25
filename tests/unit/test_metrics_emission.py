from __future__ import annotations

from datetime import datetime, timezone

import pytest
from runtime.observability.metrics import (
    InMemoryMetricSink,
    MetricContext,
    WorkerMetricEmitter,
)


def _context() -> MetricContext:
    return MetricContext(
        runtime_id="rt-1",
        worker_identity="worker:rt-1:1",
        launch_attempt=1,
        tenant_id="tenant-1",
        account_id="acct-1",
        strategy_version_id="sv-1",
        mode="BACKTEST",
    )


def test_launch_success_emits_one_counter_with_required_labels() -> None:
    sink = InMemoryMetricSink()
    emitter = WorkerMetricEmitter(sink)

    emitter.emit_launch_succeeded(_context())

    assert len(sink.emissions) == 1
    event = sink.emissions[0]
    assert event.metric_name == "worker_launch_succeeded_total"
    assert event.kind == "counter"
    assert event.labels["runtime_id"] == "rt-1"
    assert event.labels["worker_identity"] == "worker:rt-1:1"
    assert event.labels["launch_attempt"] == "1"


def test_heartbeat_emits_counter_and_updates_last_heartbeat_gauge() -> None:
    sink = InMemoryMetricSink()
    emitter = WorkerMetricEmitter(sink)

    emitter.emit_heartbeat(
        _context(), observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )

    assert [item.metric_name for item in sink.emissions] == [
        "worker_heartbeat_emitted_total",
        "worker_last_heartbeat_unixtime",
    ]
    assert sink.emissions[0].kind == "counter"
    assert sink.emissions[1].kind == "gauge"


def test_degraded_signal_includes_reason_code() -> None:
    sink = InMemoryMetricSink()
    emitter = WorkerMetricEmitter(sink)

    emitter.emit_degraded_signal(_context(), reason_code="heartbeat_lagging")

    emission = sink.emissions[0]
    assert emission.metric_name == "worker_degraded_signals_total"
    assert emission.labels["reason_code"] == "HEARTBEAT_LAGGING"


def test_missing_required_labels_fail_fast() -> None:
    with pytest.raises(ValueError, match="runtime_id"):
        MetricContext(runtime_id="", worker_identity="worker:rt-1:1", launch_attempt=1)

    with pytest.raises(ValueError, match="launch_attempt"):
        MetricContext(
            runtime_id="rt-1", worker_identity="worker:rt-1:1", launch_attempt=0
        )


def test_repeated_heartbeat_does_not_mutate_label_set() -> None:
    sink = InMemoryMetricSink()
    emitter = WorkerMetricEmitter(sink)
    context = _context()

    emitter.emit_heartbeat(
        context, observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    first_counter_labels = dict(sink.emissions[0].labels)

    emitter.emit_heartbeat(
        context, observed_at=datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    )
    second_counter_labels = dict(sink.emissions[2].labels)

    assert first_counter_labels == second_counter_labels
    assert "reason_code" not in second_counter_labels
