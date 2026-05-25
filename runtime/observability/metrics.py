from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be blank")
    return normalized


def _normalize_optional(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be blank when provided")
    return normalized


def _to_unix_timestamp(value: datetime) -> float:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc).timestamp()


@dataclass(frozen=True, slots=True)
class MetricContext:
    runtime_id: str
    worker_identity: str
    launch_attempt: int
    tenant_id: str | None = None
    account_id: str | None = None
    strategy_version_id: str | None = None
    mode: str | None = None
    job_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "runtime_id",
            _require_non_empty(self.runtime_id, field_name="runtime_id"),
        )
        object.__setattr__(
            self,
            "worker_identity",
            _require_non_empty(self.worker_identity, field_name="worker_identity"),
        )
        if self.launch_attempt < 1:
            raise ValueError("launch_attempt must be greater than or equal to 1")
        object.__setattr__(
            self,
            "tenant_id",
            _normalize_optional(self.tenant_id, field_name="tenant_id"),
        )
        object.__setattr__(
            self,
            "account_id",
            _normalize_optional(self.account_id, field_name="account_id"),
        )
        object.__setattr__(
            self,
            "strategy_version_id",
            _normalize_optional(
                self.strategy_version_id, field_name="strategy_version_id"
            ),
        )
        object.__setattr__(
            self, "mode", _normalize_optional(self.mode, field_name="mode")
        )
        object.__setattr__(
            self,
            "job_id",
            _normalize_optional(self.job_id, field_name="job_id"),
        )

    def labels(self) -> dict[str, str]:
        output = {
            "runtime_id": self.runtime_id,
            "worker_identity": self.worker_identity,
            "launch_attempt": str(self.launch_attempt),
        }
        if self.tenant_id is not None:
            output["tenant_id"] = self.tenant_id
        if self.account_id is not None:
            output["account_id"] = self.account_id
        if self.strategy_version_id is not None:
            output["strategy_version_id"] = self.strategy_version_id
        if self.mode is not None:
            output["mode"] = self.mode
        if self.job_id is not None:
            output["job_id"] = self.job_id
        return output


@dataclass(frozen=True, slots=True)
class MetricEmission:
    metric_name: str
    value: float
    labels: dict[str, str]
    kind: str


class MetricSink(Protocol):
    def increment_counter(
        self, name: str, *, labels: dict[str, str], amount: float = 1.0
    ) -> None: ...

    def set_gauge(self, name: str, *, labels: dict[str, str], value: float) -> None: ...

    def observe_histogram(
        self, name: str, *, labels: dict[str, str], value: float
    ) -> None: ...


class InMemoryMetricSink:
    def __init__(self) -> None:
        self.emissions: list[MetricEmission] = []

    def increment_counter(
        self, name: str, *, labels: dict[str, str], amount: float = 1.0
    ) -> None:
        self.emissions.append(
            MetricEmission(
                metric_name=name, value=amount, labels=dict(labels), kind="counter"
            )
        )

    def set_gauge(self, name: str, *, labels: dict[str, str], value: float) -> None:
        self.emissions.append(
            MetricEmission(
                metric_name=name, value=value, labels=dict(labels), kind="gauge"
            )
        )

    def observe_histogram(
        self, name: str, *, labels: dict[str, str], value: float
    ) -> None:
        self.emissions.append(
            MetricEmission(
                metric_name=name, value=value, labels=dict(labels), kind="histogram"
            )
        )


class WorkerMetricEmitter:
    def __init__(self, sink: MetricSink) -> None:
        self._sink = sink

    def emit_launch_succeeded(self, context: MetricContext) -> None:
        self._sink.increment_counter(
            "worker_launch_succeeded_total",
            labels=self._base_labels(context),
        )

    def emit_launch_failed(
        self, context: MetricContext, *, reason_code: str | None = None
    ) -> None:
        labels = self._base_labels(context)
        if reason_code is not None:
            labels["reason_code"] = _require_non_empty(
                str(reason_code).strip().upper(),
                field_name="reason_code",
            )
        self._sink.increment_counter("worker_launch_failed_total", labels=labels)

    def emit_heartbeat(self, context: MetricContext, *, observed_at: datetime) -> None:
        labels = self._base_labels(context)
        self._sink.increment_counter("worker_heartbeat_emitted_total", labels=labels)
        self._sink.set_gauge(
            "worker_last_heartbeat_unixtime",
            labels=labels,
            value=_to_unix_timestamp(observed_at),
        )

    def emit_degraded_signal(self, context: MetricContext, *, reason_code: str) -> None:
        labels = self._base_labels(context)
        labels["reason_code"] = _require_non_empty(
            str(reason_code).strip().upper(),
            field_name="reason_code",
        )
        self._sink.increment_counter("worker_degraded_signals_total", labels=labels)

    def set_runtime_health(
        self, context: MetricContext, *, health_value: float
    ) -> None:
        self._sink.set_gauge(
            "worker_runtime_health",
            labels=self._base_labels(context),
            value=health_value,
        )

    def observe_bootstrap_duration(
        self, context: MetricContext, *, seconds: float
    ) -> None:
        self._sink.observe_histogram(
            "worker_bootstrap_duration_seconds",
            labels=self._base_labels(context),
            value=seconds,
        )

    def observe_heartbeat_emit_duration(
        self, context: MetricContext, *, seconds: float
    ) -> None:
        self._sink.observe_histogram(
            "worker_heartbeat_emit_duration_seconds",
            labels=self._base_labels(context),
            value=seconds,
        )

    def _base_labels(self, context: MetricContext) -> dict[str, str]:
        labels = context.labels()
        required = ("runtime_id", "worker_identity", "launch_attempt")
        for field in required:
            if field not in labels:
                raise ValueError(f"missing required metric dimension: {field}")
        return labels
