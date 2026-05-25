from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator


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


@dataclass(frozen=True, slots=True)
class RuntimeTraceContext:
    runtime_id: str
    worker_identity: str
    launch_attempt: int
    tenant_id: str
    correlation_id: str
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
        # tenant_id is optional at the manager; tracing may carry "" for user-scoped workers.
        object.__setattr__(self, "tenant_id", self.tenant_id.strip())
        object.__setattr__(
            self,
            "correlation_id",
            _require_non_empty(self.correlation_id, field_name="correlation_id"),
        )
        if self.launch_attempt < 1:
            raise ValueError("launch_attempt must be greater than or equal to 1")
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

    def to_attributes(self) -> dict[str, str | int]:
        attrs: dict[str, str | int] = {
            "runtime_id": self.runtime_id,
            "worker_identity": self.worker_identity,
            "launch_attempt": self.launch_attempt,
            "tenant_id": self.tenant_id,
            "correlation_id": self.correlation_id,
        }
        if self.account_id is not None:
            attrs["account_id"] = self.account_id
        if self.strategy_version_id is not None:
            attrs["strategy_version_id"] = self.strategy_version_id
        if self.mode is not None:
            attrs["mode"] = self.mode
        if self.job_id is not None:
            attrs["job_id"] = self.job_id
        return attrs


@dataclass(frozen=True, slots=True)
class SpanRecord:
    name: str
    attributes: dict[str, Any]
    status: str
    exception_type: str | None = None
    exception_message: str | None = None


class InMemorySpanExporter:
    def __init__(self) -> None:
        self.finished_spans: list[SpanRecord] = []

    def export(self, span: SpanRecord) -> None:
        self.finished_spans.append(span)


class WorkerSpan:
    def __init__(
        self,
        *,
        exporter: InMemorySpanExporter,
        name: str,
        attributes: dict[str, Any],
    ) -> None:
        self._exporter = exporter
        self._name = name
        self._attributes = dict(attributes)
        self._status = "ok"
        self._exception: Exception | None = None

    def set_attribute(self, key: str, value: Any) -> None:
        self._attributes[key] = value

    def set_status_error(self) -> None:
        self._status = "error"

    def record_exception(self, exc: Exception) -> None:
        self._exception = exc
        self._status = "error"

    def finish(self) -> None:
        exception_type = (
            type(self._exception).__name__ if self._exception is not None else None
        )
        exception_message = (
            str(self._exception) if self._exception is not None else None
        )
        self._exporter.export(
            SpanRecord(
                name=self._name,
                attributes=dict(self._attributes),
                status=self._status,
                exception_type=exception_type,
                exception_message=exception_message,
            )
        )


class WorkerTracer:
    def __init__(self, exporter: InMemorySpanExporter) -> None:
        self._exporter = exporter

    @contextmanager
    def span(
        self,
        name: str,
        context: RuntimeTraceContext,
        *,
        attributes: dict[str, Any] | None = None,
    ) -> Iterator[WorkerSpan]:
        all_attributes = {**context.to_attributes(), **dict(attributes or {})}
        span = WorkerSpan(exporter=self._exporter, name=name, attributes=all_attributes)
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            raise
        finally:
            span.finish()


class WorkerTraceSpans:
    BOOTSTRAP = "worker.bootstrap"
    BOOTSTRAP_VALIDATE_LAUNCH_SPEC = "worker.bootstrap.validate_launch_spec"
    BOOTSTRAP_LOAD_ARTIFACT = "worker.bootstrap.load_artifact"
    BOOTSTRAP_IMPORT_ENTRYPOINT = "worker.bootstrap.import_entrypoint"
    BOOTSTRAP_INITIALIZE_SDK = "worker.bootstrap.initialize_sdk"
    RUNTIME_EMIT_HEARTBEAT = "worker.runtime.emit_heartbeat"
    RUNTIME_EXECUTE_CYCLE = "worker.runtime.execute_cycle"
    RUNTIME_REPORT_UNHEALTHY = "worker.runtime.report_unhealthy"
    RUNTIME_SHUTDOWN = "worker.runtime.shutdown"
