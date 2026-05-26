from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

_REQUIRED_CONTEXT_FIELDS = (
    "runtime_id",
    "tenant_id",
    "worker_identity",
    "launch_attempt",
)
_CANONICAL_IDENTITY_FIELDS = frozenset(
    {
        "runtime_id",
        "tenant_id",
        "worker_identity",
        "launch_attempt",
        "account_id",
        "strategy_version_id",
        "strategy_id",
        "deployment_id",
        "portfolio_id",
        "request_id",
        "runtime_type",
        "job_id",
        "mode",
    }
)
_FORBIDDEN_MANAGER_OWNED_EVENTS = frozenset(
    {"runtime.started", "runtime.degraded", "runtime.failed"}
)
_LIFECYCLE_CRITICAL_EVENTS = frozenset(
    {
        "runtime.launch_succeeded",
        "runtime.launch_failed",
        "runtime.heartbeat",
        "worker_launch_succeeded",
        "worker_launch_failed",
        "worker_heartbeat_emitted",
    }
)


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
class RuntimeLogContext:
    runtime_id: str
    tenant_id: str
    worker_identity: str
    launch_attempt: int
    correlation_id: str | None = None
    account_id: str | None = None
    strategy_version_id: str | None = None
    strategy_id: str | None = None
    deployment_id: str | None = None
    portfolio_id: str | None = None
    request_id: str | None = None
    runtime_type: str | None = "STRATEGY_WORKER_RUNTIME"
    job_id: str | None = None
    mode: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "runtime_id",
            _require_non_empty(self.runtime_id, field_name="runtime_id"),
        )
        # tenant_id is optional at the manager; logs may carry "" for user-scoped workers.
        object.__setattr__(self, "tenant_id", self.tenant_id.strip())
        object.__setattr__(
            self,
            "worker_identity",
            _require_non_empty(self.worker_identity, field_name="worker_identity"),
        )
        if self.launch_attempt < 1:
            raise ValueError("launch_attempt must be greater than or equal to 1")
        object.__setattr__(
            self,
            "correlation_id",
            _normalize_optional(self.correlation_id, field_name="correlation_id"),
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
            self,
            "strategy_id",
            _normalize_optional(self.strategy_id, field_name="strategy_id"),
        )
        object.__setattr__(
            self,
            "deployment_id",
            _normalize_optional(self.deployment_id, field_name="deployment_id"),
        )
        object.__setattr__(
            self,
            "portfolio_id",
            _normalize_optional(self.portfolio_id, field_name="portfolio_id"),
        )
        object.__setattr__(
            self,
            "request_id",
            _normalize_optional(self.request_id, field_name="request_id"),
        )
        object.__setattr__(
            self,
            "runtime_type",
            _normalize_optional(self.runtime_type, field_name="runtime_type"),
        )
        object.__setattr__(
            self,
            "job_id",
            _normalize_optional(self.job_id, field_name="job_id"),
        )
        object.__setattr__(
            self, "mode", _normalize_optional(self.mode, field_name="mode")
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "runtime_id": self.runtime_id,
            "tenant_id": self.tenant_id,
            "worker_identity": self.worker_identity,
            "launch_attempt": self.launch_attempt,
        }
        if self.correlation_id is not None:
            payload["correlation_id"] = self.correlation_id
        if self.account_id is not None:
            payload["account_id"] = self.account_id
        if self.strategy_version_id is not None:
            payload["strategy_version_id"] = self.strategy_version_id
        if self.strategy_id is not None:
            payload["strategy_id"] = self.strategy_id
        if self.deployment_id is not None:
            payload["deployment_id"] = self.deployment_id
        if self.portfolio_id is not None:
            payload["portfolio_id"] = self.portfolio_id
        if self.request_id is not None:
            payload["request_id"] = self.request_id
        if self.runtime_type is not None:
            payload["runtime_type"] = self.runtime_type
        if self.job_id is not None:
            payload["job_id"] = self.job_id
        if self.mode is not None:
            payload["mode"] = self.mode
        return payload


class RuntimeBoundLogger:
    def __init__(
        self,
        *,
        logger: logging.Logger,
        runtime_context: RuntimeLogContext,
        bound_fields: Mapping[str, Any] | None = None,
    ) -> None:
        self._logger = logger
        self._runtime_context = runtime_context
        self._bound_fields = dict(bound_fields or {})
        self._reject_identity_override(self._bound_fields)

    def child(self, **bound_fields: Any) -> "RuntimeBoundLogger":
        self._reject_identity_override(bound_fields)
        merged = {**self._bound_fields, **bound_fields}
        return RuntimeBoundLogger(
            logger=self._logger,
            runtime_context=self._runtime_context,
            bound_fields=merged,
        )

    def emit(
        self,
        *,
        event_name: str,
        message: str,
        level: int = logging.INFO,
        event_extras: Mapping[str, Any] | None = None,
    ) -> None:
        normalized_event_name = _require_non_empty(event_name, field_name="event_name")
        if normalized_event_name in _FORBIDDEN_MANAGER_OWNED_EVENTS:
            raise ValueError(
                f"{normalized_event_name} is manager-owned canonical lifecycle truth"
            )
        extras = dict(event_extras or {})
        self._reject_identity_override(extras)
        self._ensure_required_identity_for_event(normalized_event_name)

        event_payload = {
            "event_name": normalized_event_name,
            **self._runtime_context.to_dict(),
            **self._bound_fields,
            **extras,
        }
        self._logger.log(level, message, extra={"event": event_payload})

    def _ensure_required_identity_for_event(self, event_name: str) -> None:
        if event_name not in _LIFECYCLE_CRITICAL_EVENTS:
            return
        context = self._runtime_context.to_dict()
        missing = [field for field in _REQUIRED_CONTEXT_FIELDS if field not in context]
        if missing:
            raise ValueError(
                f"missing required runtime identity fields: {sorted(missing)}"
            )

    def _reject_identity_override(self, values: Mapping[str, Any]) -> None:
        overlapping = _CANONICAL_IDENTITY_FIELDS.intersection(values.keys())
        if overlapping:
            raise ValueError(
                "event extras must not override canonical identity fields: "
                + ", ".join(sorted(overlapping))
            )


def bind_runtime_context(
    logger: logging.Logger,
    runtime_context: RuntimeLogContext,
) -> RuntimeBoundLogger:
    return RuntimeBoundLogger(logger=logger, runtime_context=runtime_context)
