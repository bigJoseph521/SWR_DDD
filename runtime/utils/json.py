from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from runtime.utils.ids import require_canonical_identifier


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def require_json_mapping(
    value: Mapping[str, Any], *, field_name: str
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping/object")
    return value


def require_iso8601_timestamp(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    try:
        # tolerate Z
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO-8601: {value!r}") from exc
    return value


def canonical_event_envelope(
    *,
    event_id: str,
    event_name: str,
    producer: str,
    payload: Mapping[str, Any],
    event_version: int = 1,
    occurred_at: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    tenant_id: str | None = None,
    account_id: str | None = None,
    runtime_id: str | None = None,
    worker_identity: str | None = None,
    launch_attempt: int | None = None,
    strategy_version_id: str | None = None,
) -> dict[str, Any]:
    require_canonical_identifier(event_id, domain="event_ids")
    require_canonical_identifier(event_name, domain="event_names")

    if event_version < 1:
        raise ValueError("event_version must be >= 1")

    payload = dict(require_json_mapping(payload, field_name="payload"))

    envelope: dict[str, Any] = {
        "event_id": event_id,
        "event_name": event_name,
        "event_version": event_version,
        "producer": producer,
        "occurred_at": require_iso8601_timestamp(
            occurred_at or utc_now_iso(),
            field_name="occurred_at",
        ),
        "payload": payload,
    }

    if correlation_id is not None:
        envelope["correlation_id"] = correlation_id
    if causation_id is not None:
        envelope["causation_id"] = causation_id
    if tenant_id is not None:
        envelope["tenant_id"] = tenant_id
    if account_id is not None:
        envelope["account_id"] = account_id
    if runtime_id is not None:
        envelope["runtime_id"] = runtime_id
    if worker_identity is not None:
        envelope["worker_identity"] = worker_identity
    if launch_attempt is not None:
        envelope["launch_attempt"] = launch_attempt
    if strategy_version_id is not None:
        envelope["strategy_version_id"] = strategy_version_id

    return envelope


def canonical_runtime_state_payload(
    *,
    runtime_id: str,
    state: str,
    reason_code: str | None = None,
    worker_identity: str | None = None,
    launch_attempt: int | None = None,
    occurred_at: str | None = None,
    observed_at: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> dict[str, Any]:
    require_canonical_identifier(state, domain="states")
    if reason_code is not None:
        require_canonical_identifier(reason_code, domain="reasons")

    payload: dict[str, Any] = {
        "runtime_id": runtime_id,
        "state": state,
    }

    if reason_code is not None:
        payload["reason_code"] = reason_code
    if worker_identity is not None:
        payload["worker_identity"] = worker_identity
    if launch_attempt is not None:
        payload["launch_attempt"] = launch_attempt
    if occurred_at is not None:
        payload["occurred_at"] = require_iso8601_timestamp(
            occurred_at, field_name="occurred_at"
        )
    if observed_at is not None:
        payload["observed_at"] = require_iso8601_timestamp(
            observed_at, field_name="observed_at"
        )
    if correlation_id is not None:
        payload["correlation_id"] = correlation_id
    if causation_id is not None:
        payload["causation_id"] = causation_id

    return payload


def canonical_error_body(
    *,
    code: str,
    message: str,
    retryable: bool = False,
    correlation_id: str | None = None,
    details: Mapping[str, Any] | list[Any] | None = None,
) -> dict[str, Any]:
    require_canonical_identifier(code, domain="errors")

    body: dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable,
            "details": details if details is not None else {},
        }
    }

    if correlation_id is not None:
        body["error"]["correlation_id"] = correlation_id

    return body


def canonical_identifier_json(
    *,
    value: str,
    domain: str,
    field_name: str = "identifier",
) -> dict[str, str]:
    require_canonical_identifier(value, domain=domain)  # type: ignore[arg-type]
    return {field_name: value}
