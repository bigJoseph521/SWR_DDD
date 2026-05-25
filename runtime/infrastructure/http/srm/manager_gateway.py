from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping

from runtime.domain.errors import (
    normalize_runtime_reason_code,
    worker_error_code_for_reason,
)
from runtime.domain.events.dedupe import DedupeDecision, LifecycleSignalDedupe
from runtime.domain.events.event_envelope import LifecycleEventEnvelope
from runtime.domain.events.event_factory import LifecycleEventFactory
from runtime.domain.policies.mode_policy import (
    Capability,
    ModePolicy,
    require_capability,
)
from runtime.infrastructure.grpc.control_plane_envelope_log import (
    BANNER_WR_TO_RM_HTTP,
    write_control_plane_envelope,
)

_GLOBAL_LIFECYCLE_DEDUPE = LifecycleSignalDedupe()

# Fields carried on the manager signal envelope / identity must not be repeated in the protobuf
# Struct payload (event envelope top-level + identity; see _print_runtime_manager_message).
_WIRE_PAYLOAD_EXCLUDE: frozenset[str] = frozenset(
    {
        "occurred_at",
        "runtime_id",
        "launch_attempt",
        "worker_identity",
        "strategy_version_id",
        "tenant_id",
        "account_id",
    }
)


def _wire_payload_only(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in payload.items() if k not in _WIRE_PAYLOAD_EXCLUDE}


def _derive_field_buckets_from_field_errors(
    field_errors: Mapping[str, Any],
) -> tuple[list[str], list[str], list[str]]:
    missed: set[str] = set()
    invalid: set[str] = set()
    empty: set[str] = set()
    for field, code_raw in field_errors.items():
        key = str(field)
        code = str(code_raw)
        lower = code.lower()
        if key == "payload" and code.startswith("unknown_fields:"):
            invalid.update([x for x in code.split(":", 1)[1].split(",") if x])
            continue
        if "required_field_missing" in lower or "required" in lower:
            missed.add(key)
            continue
        if (
            "must_not_be_empty" in lower
            or "must_not_be_blank" in lower
            or "blank" in lower
        ):
            empty.add(key)
            continue
        invalid.add(key)
    return (sorted(missed), sorted(invalid), sorted(empty))


class ManagerGateway:
    def __init__(
        self,
        policy: ModePolicy,
        manager_client: object,
        runtime_identity: object,
        *,
        dedupe: LifecycleSignalDedupe | None = None,
        event_factory: LifecycleEventFactory | None = None,
        on_event_emitted: Callable[[LifecycleEventEnvelope], None] | None = None,
        heartbeat_log_enabled: bool = False,
    ) -> None:
        require_capability(policy, Capability.MANAGER_SIGNAL)
        self._policy = policy
        self._manager_client = manager_client
        self._runtime_identity = runtime_identity
        self._dedupe = dedupe or _GLOBAL_LIFECYCLE_DEDUPE
        self._event_factory = event_factory or LifecycleEventFactory()
        self._on_event_emitted = on_event_emitted
        self._heartbeat_log_enabled = heartbeat_log_enabled

    def emit_bootstrap_succeeded(
        self,
        *,
        local_state: str | None = None,
        occurred_at: datetime,
        details: Mapping[str, Any] | None = None,
        triggering_event_id: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "local_state": local_state,
            "occurred_at": occurred_at,
            "details": dict(details or {}),
        }
        if triggering_event_id is not None:
            payload["triggering_event_id"] = triggering_event_id
        return self._emit(
            signal_type="bootstrap_succeeded",
            payload=payload,
        )

    def emit_bootstrap_failed(
        self,
        *,
        occurred_at: datetime,
        reason_code: str,
        details: Mapping[str, Any] | None = None,
        triggering_event_id: str | None = None,
    ) -> Any:
        details_dict = dict(details or {})
        mf_raw = details_dict.get("missed_fields")
        missed_fields = list(mf_raw) if isinstance(mf_raw, list) else []
        inv_raw = details_dict.get("invalid_fields")
        invalid_fields = list(inv_raw) if isinstance(inv_raw, list) else []
        emp_raw = details_dict.get("empty_fields")
        empty_fields = list(emp_raw) if isinstance(emp_raw, list) else []
        field_errors = details_dict.get("field_errors")
        if isinstance(field_errors, Mapping):
            m2, i2, e2 = _derive_field_buckets_from_field_errors(field_errors)
            missed_fields = sorted(set(missed_fields).union(m2))
            invalid_fields = sorted(set(invalid_fields).union(i2))
            empty_fields = sorted(set(empty_fields).union(e2))
        passed_raw = details_dict.get("passed")
        passed = list(passed_raw) if isinstance(passed_raw, list) else []
        failed_raw = details_dict.get("failed")
        failed = list(failed_raw) if isinstance(failed_raw, list) else []
        nc_raw = details_dict.get("not_checked")
        not_checked = list(nc_raw) if isinstance(nc_raw, list) else []
        details_excluded_keys = {
            "missed_fields",
            "invalid_fields",
            "empty_fields",
            "passed",
            "failed",
            "not_checked",
        }
        details_payload = {
            k: v for k, v in details_dict.items() if k not in details_excluded_keys
        }
        normalized_reason = normalize_runtime_reason_code(reason_code)
        payload: dict[str, Any] = {
            "occurred_at": occurred_at,
            "reason_code": normalized_reason,
            "error_code": worker_error_code_for_reason(normalized_reason),
            "missed_fields": missed_fields,
            "invalid_fields": invalid_fields,
            "empty_fields": empty_fields,
            "passed": passed,
            "failed": failed,
            "not_checked": not_checked,
            "details": details_payload,
        }
        mypy_result_path = details_dict.get("mypy_result_path")
        mypy_exit_code = details_dict.get("mypy_exit_code")
        mypy_output = details_dict.get("mypy_output")
        if (
            isinstance(mypy_result_path, str)
            or isinstance(mypy_exit_code, int)
            or isinstance(mypy_output, str)
        ):
            details_payload["mypy_validation"] = {
                "result_path": str(mypy_result_path or ""),
                "exit_code": (
                    int(mypy_exit_code) if isinstance(mypy_exit_code, int) else None
                ),
                "output": str(mypy_output or ""),
            }
        if triggering_event_id is not None:
            payload["triggering_event_id"] = triggering_event_id
        return self._emit(
            signal_type="bootstrap_failed",
            payload=payload,
        )

    def emit_heartbeat(
        self,
        *,
        local_state: str | None,
        observed_at: datetime,
        triggering_event_id: str | None = None,
        source: str = "HEARTBEAT",
        reason_code: str | None = None,
        message: str | None = None,
        retryable: bool | None = None,
    ) -> Any:
        """HTTP POST to SRM ``.../runtimes/{{id}}/status``.

        ``source`` is ``HEARTBEAT`` for periodic liveness or ``UPDATE`` for explicit status changes.
        """
        payload: dict[str, Any] = {
            "local_state": local_state,
            "observed_at": observed_at,
            "source": source,
        }
        if triggering_event_id is not None:
            payload["triggering_event_id"] = triggering_event_id
        if reason_code is not None:
            payload["reason_code"] = reason_code
        if message is not None:
            payload["message"] = message
        if retryable is not None:
            payload["retryable"] = retryable
        return self._emit(
            signal_type="heartbeat",
            payload=payload,
        )

    def emit_unhealthy(
        self,
        *,
        occurred_at: datetime,
        observed_at: datetime,
        reason_code: str,
        details: Mapping[str, Any] | None = None,
        triggering_event_id: str | None = None,
    ) -> Any:
        normalized_reason = normalize_runtime_reason_code(reason_code)
        payload: dict[str, Any] = {
            "reason_code": normalized_reason,
            "error_code": worker_error_code_for_reason(normalized_reason),
            "details": dict(details or {}),
            "occurred_at": occurred_at,
            "observed_at": observed_at,
        }
        if triggering_event_id is not None:
            payload["triggering_event_id"] = triggering_event_id
        return self._emit(
            signal_type="unhealthy",
            payload=payload,
        )

    def emit_controlled_stop(
        self,
        *,
        occurred_at: datetime,
        reason_code: str | None = None,
        triggering_event_id: str | None = None,
    ) -> Any:
        normalized_reason = normalize_runtime_reason_code(reason_code)
        payload: dict[str, Any] = {
            "reason_code": normalized_reason,
            "error_code": worker_error_code_for_reason(normalized_reason),
            "occurred_at": occurred_at,
        }
        if triggering_event_id is not None:
            payload["triggering_event_id"] = triggering_event_id
        return self._emit(
            signal_type="controlled_stop",
            payload=payload,
        )

    def emit_terminated(
        self,
        *,
        occurred_at: datetime,
        local_state: str,
        reason_code: str,
        message: str | None = None,
        observed_at: datetime | None = None,
        triggering_event_id: str | None = None,
    ) -> Any:
        """
        ``runtime.terminated`` payload (non-``RUNTIME_JOB_COMPLETED``): ``occurred_at``, ``local_state``,
        ``reason_code``, optional ``message`` / ``observed_at`` / ``triggering_event_id``.
        No ``accepted`` / ``accepted_at`` (those apply to StopWorker gRPC only).

        For natural job completion use ``reason_code="RUNTIME_JOB_COMPLETED"``: payload is only
        ``local_state`` and ``reason_code``. Use ``MANUAL_STOP_REQUESTED`` (e.g. Ctrl+C) and
        ``ERROR_DETECTED`` (shutdown while in ``FAILED`` phase) for other local stops.
        """
        normalized_reason = normalize_runtime_reason_code(reason_code)
        payload: dict[str, Any]
        if normalized_reason == "LOCAL_TERMINATION_OBSERVED":
            payload = {
                "local_state": local_state,
                "reason_code": normalized_reason,
                "error_code": worker_error_code_for_reason(normalized_reason),
            }
        else:
            payload = {
                "occurred_at": occurred_at,
                "local_state": local_state,
                "reason_code": normalized_reason,
                "error_code": worker_error_code_for_reason(normalized_reason),
            }
            if message is not None:
                payload["message"] = message
            if observed_at is not None:
                payload["observed_at"] = observed_at
            if triggering_event_id is not None:
                payload["triggering_event_id"] = triggering_event_id
        return self._emit(
            signal_type="terminated",
            payload=payload,
        )

    def _identity_payload(self) -> dict[str, Any]:
        runtime_id = str(getattr(self._runtime_identity, "runtime_id"))
        launch_attempt = int(getattr(self._runtime_identity, "launch_attempt"))
        strategy_version_id = str(
            getattr(self._runtime_identity, "strategy_version_id")
        )
        worker_identity = getattr(self._runtime_identity, "worker_identity", None)
        if not isinstance(worker_identity, str) or not worker_identity.strip():
            worker_identity = f"{runtime_id}:{strategy_version_id}:{launch_attempt}"
        return {
            "runtime_id": runtime_id,
            "tenant_id": getattr(self._runtime_identity, "tenant_id"),
            "trader_id": getattr(self._runtime_identity, "trader_id", None),
            "account_id": getattr(self._runtime_identity, "account_id", None),
            "strategy_version_id": strategy_version_id,
            "mode": getattr(getattr(self._runtime_identity, "mode"), "value"),
            "launch_attempt": launch_attempt,
            "worker_identity": worker_identity,
        }

    def _emit(self, *, signal_type: str, payload: Mapping[str, Any]) -> Any:
        identity = self._identity_payload()
        self._validate_required_fields(
            signal_type=signal_type, identity=identity, payload=payload
        )

        lifecycle_event = self._event_factory.create(
            signal_type=signal_type,
            identity=identity,
            payload=payload,
            correlation_id=getattr(self._runtime_identity, "correlation_id", None),
        )
        if self._on_event_emitted is not None:
            self._on_event_emitted(lifecycle_event)
        decision = self._dedupe.decide(
            envelope=lifecycle_event,
            current_launch_attempt=int(identity["launch_attempt"]),
        )
        if decision is not DedupeDecision.ACCEPT:
            return {
                "accepted": False,
                "suppressed": True,
                "decision": decision.value,
                "signal_type": signal_type,
            }

        envelope = lifecycle_event.to_manager_signal(
            signal_type=signal_type, identity=identity
        )
        wire_envelope = {
            **envelope,
            "payload": _wire_payload_only(envelope["payload"]),
        }
        emitter = getattr(self._manager_client, "emit_signal", None)
        if not callable(emitter):
            raise TypeError("Manager client must expose emit_signal(payload).")
        result = emitter(wire_envelope)
        self._print_runtime_manager_message(
            signal_type=signal_type, identity=identity, envelope=wire_envelope
        )
        return result

    def _print_runtime_manager_message(
        self,
        *,
        signal_type: str,
        identity: Mapping[str, Any],
        envelope: Mapping[str, Any],
    ) -> None:
        if signal_type == "heartbeat" and not self._heartbeat_log_enabled:
            return
        payload = envelope.get("payload")
        # Event envelope (top-level): identity + envelope metadata; domain-only fields stay in payload.
        payload_dict = dict(payload) if isinstance(payload, Mapping) else {}
        message: dict[str, Any] = {
            "event_id": envelope.get("event_id"),
            "event_name": envelope.get("event_name"),
            "event_version": envelope.get("event_version"),
            "producer": envelope.get("producer"),
            "occurred_at": envelope.get("occurred_at"),
            "correlation_id": envelope.get("correlation_id"),
            "tenant_id": identity.get("tenant_id"),
            "account_id": identity.get("account_id"),
            "runtime_id": identity.get("runtime_id"),
            "worker_identity": identity.get("worker_identity"),
            "launch_attempt": identity.get("launch_attempt"),
            "strategy_version_id": identity.get("strategy_version_id"),
            "payload": payload_dict,
        }

        banner = BANNER_WR_TO_RM_HTTP
        write_control_plane_envelope(banner=banner, message=message)

    def _validate_required_fields(
        self,
        *,
        signal_type: str,
        identity: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> None:
        runtime_id = str(identity.get("runtime_id") or "").strip()
        if not runtime_id:
            raise ValueError("runtime_id is required for lifecycle signal forwarding.")
        launch_attempt = int(identity.get("launch_attempt") or 0)
        if launch_attempt < 1:
            raise ValueError(
                "launch_attempt must be >= 1 for lifecycle signal forwarding."
            )
        worker_identity = str(identity.get("worker_identity") or "").strip()
        if not worker_identity:
            raise ValueError(
                "worker_identity is required for lifecycle signal forwarding."
            )
        if signal_type not in {
            "bootstrap_succeeded",
            "bootstrap_failed",
            "heartbeat",
            "unhealthy",
            "controlled_stop",
            "terminated",
        }:
            raise ValueError(f"Unsupported signal_type: {signal_type}")
        if signal_type == "heartbeat":
            obs = payload.get("observed_at")
            if not isinstance(obs, datetime):
                raise ValueError(
                    "payload.observed_at is required and must be datetime for heartbeat."
                )
        elif (
            signal_type == "terminated"
            and {"local_state", "reason_code"}.issubset(set(payload.keys()))
            and "occurred_at" not in payload
        ):
            if not str(payload.get("local_state") or "").strip():
                raise ValueError(
                    "payload.local_state is required for minimal termination."
                )
            if not str(payload.get("reason_code") or "").strip():
                raise ValueError(
                    "payload.reason_code is required for minimal termination."
                )
        else:
            occurred_at = payload.get("occurred_at")
            if not isinstance(occurred_at, datetime):
                raise ValueError(
                    "payload.occurred_at is required and must be datetime."
                )
