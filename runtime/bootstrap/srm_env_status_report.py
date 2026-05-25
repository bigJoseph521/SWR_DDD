"""Report bootstrap status to strategy-runtime-manager (POST .../runtimes/{{id}}/status)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Final

from runtime.bootstrap.failures import BootstrapFailure, BootstrapStage
from runtime.bootstrap.launch_spec import LaunchSpecValidationError
from runtime.bootstrap.minimal_env_validation import (
    HEALTH_STATUS_HEALTHY,
    HEALTH_STATUS_UNHEALTHY,
    HEALTH_STATUS_UNKNOWN,
    RUNTIME_STATUS_FAILED,
    RUNTIME_STATUS_RUNNING,
    RUNTIME_STATUS_STARTING,
    RUNTIME_STATUS_STARTUP_FAILED,
    RUNTIME_STATUS_STOPPED,
    RUNTIME_STATUS_STOPPING,
    SWR_ARTIFACT_DIGEST_MISMATCH,
    SWR_ARTIFACT_NOT_FOUND,
    SWR_CONTEXT_FETCH_FAILED,
    SWR_CONTEXT_VALIDATION_FAILED,
    SWR_ENTRYPOINT_INVALID,
    SWR_ENV_VALIDATION_FAILED,
    SWR_KUBERNETES_TERMINATION,
    SWR_SDK_COMPATIBILITY_FAILED,
    SWR_SHUTDOWN,
    MinimalEnvSnapshot,
    MinimalEnvValidationResult,
)
from runtime.domain.errors import (
    RUNTIME_WORKER_REASON_CODES,
    normalize_runtime_reason_code,
)
from runtime.infrastructure.http.srm.heartbeat import (
    SRM_STATUS_SOURCE_UPDATE,
    build_srm_heartbeat_body,
    post_srm_runtime_status,
)

RUNTIME_CONTEXT_FETCH_FAILURE_REASONS: Final[frozenset[str]] = frozenset(
    {
        "deployment_runtime_context_unreachable",
        "deployment_runtime_context_http_error",
    }
)

RUNTIME_CONTEXT_VALIDATION_FAILURE_REASONS: Final[frozenset[str]] = frozenset(
    {
        "deployment_runtime_context_empty_body",
        "deployment_runtime_context_invalid_json",
        "deployment_runtime_context_missing_identity",
    }
)

RUNTIME_CONTEXT_FAILURE_REASONS: Final[frozenset[str]] = (
    RUNTIME_CONTEXT_FETCH_FAILURE_REASONS | RUNTIME_CONTEXT_VALIDATION_FAILURE_REASONS
)

KUBERNETES_TERMINATION_REASON: Final[str] = "KUBERNETES_TERMINATION"


def is_runtime_context_fetch_failure(exc: Exception) -> bool:
    return (
        isinstance(exc, LaunchSpecValidationError)
        and exc.reason in RUNTIME_CONTEXT_FAILURE_REASONS
    )


def runtime_context_failure_status(exc: LaunchSpecValidationError) -> tuple[str, bool]:
    if exc.reason in RUNTIME_CONTEXT_VALIDATION_FAILURE_REASONS:
        return SWR_CONTEXT_VALIDATION_FAILED, False
    return SWR_CONTEXT_FETCH_FAILED, True


def build_runtime_context_fetch_failure_message(exc: LaunchSpecValidationError) -> str:
    field_errors = dict(exc.field_errors)
    detail = field_errors.get("deployment_runtime_context", "")
    body = field_errors.get("body", "")
    if exc.reason in RUNTIME_CONTEXT_VALIDATION_FAILURE_REASONS:
        opener = f"Deployment runtime-context validation failed ({exc.reason})."
    else:
        opener = f"Deployment runtime-context fetch failed ({exc.reason})."
    parts = [opener]
    if detail:
        parts.append(str(detail))
    if body:
        parts.append(f"response_body={body}")
    missing = [
        key
        for key, code in field_errors.items()
        if key not in {"deployment_runtime_context", "body"}
        and "required_field_missing" in str(code)
    ]
    if missing:
        parts.append(f"missing={','.join(sorted(missing))}")
    return " ".join(parts)


def _report_startup_failed_to_srm(
    snapshot: MinimalEnvSnapshot,
    *,
    reason_code: str,
    message: str,
    retryable: bool,
    timeout_seconds: float,
) -> dict[str, object] | None:
    return _report_startup_failed_fields_to_srm(
        runtime_id=snapshot.runtime_id,
        srm_base_url=snapshot.srm_base_url,
        mode=snapshot.mode,
        owner_resource_id=snapshot.deployment_id,
        reason_code=reason_code,
        message=message,
        retryable=retryable,
        timeout_seconds=timeout_seconds,
    )


def _report_startup_failed_fields_to_srm(
    *,
    runtime_id: str,
    srm_base_url: str,
    mode: str,
    owner_resource_id: str,
    reason_code: str,
    message: str,
    retryable: bool,
    timeout_seconds: float,
) -> dict[str, object] | None:
    if not runtime_id.strip() or not srm_base_url.strip():
        return None
    return post_srm_runtime_status(
        base_url=srm_base_url,
        runtime_id=runtime_id,
        mode=mode,
        owner_resource_id=owner_resource_id,
        runtime_status=RUNTIME_STATUS_STARTUP_FAILED,
        health_status=HEALTH_STATUS_UNHEALTHY,
        observed_at=datetime.now(timezone.utc),
        source=SRM_STATUS_SOURCE_UPDATE,
        reason_code=reason_code,
        message=message,
        retryable=retryable,
        timeout_seconds=timeout_seconds,
    )


def bootstrap_failure_srm_reason_code(failure: BootstrapFailure) -> str:
    match failure.stage:
        case BootstrapStage.ARTIFACT_FETCH:
            return SWR_ARTIFACT_NOT_FOUND
        case BootstrapStage.ARTIFACT_VERIFY:
            return SWR_ARTIFACT_DIGEST_MISMATCH
        case BootstrapStage.ENTRYPOINT_LOAD:
            return SWR_ENTRYPOINT_INVALID
        case BootstrapStage.SDK_VALIDATE:
            return SWR_SDK_COMPATIBILITY_FAILED
        case _:
            return SWR_ARTIFACT_NOT_FOUND


def build_bootstrap_failure_message(failure: BootstrapFailure) -> str:
    parts = [
        failure.message.strip()
        or f"Bootstrap stage {failure.stage.value} failed ({failure.reason_code})."
    ]
    parts.append(f"stage={failure.stage.value}")
    parts.append(f"reason={failure.reason_code}")
    details = dict(failure.details)
    for key in (
        "entrypoint",
        "entrypoint_spec",
        "artifact_reference",
        "artifact_uri",
        "expected_digest",
        "actual_digest",
    ):
        value = details.get(key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    return " ".join(parts)


def report_bootstrap_failure_to_srm(
    failure: BootstrapFailure,
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    srm_base_url: str,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """Report artifact/entrypoint/SDK bootstrap failure via SRM HTTP status update."""
    return _report_startup_failed_fields_to_srm(
        runtime_id=runtime_id,
        srm_base_url=srm_base_url,
        mode=mode,
        owner_resource_id=owner_resource_id,
        reason_code=bootstrap_failure_srm_reason_code(failure),
        message=build_bootstrap_failure_message(failure),
        retryable=False,
        timeout_seconds=timeout_seconds,
    )


def report_bootstrap_success_to_srm(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    srm_base_url: str,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """Report successful bootstrap (artifact/entrypoint/SDK) via SRM HTTP status update."""
    if not runtime_id.strip() or not srm_base_url.strip():
        return None
    return post_srm_runtime_status(
        base_url=srm_base_url,
        runtime_id=runtime_id,
        mode=mode,
        owner_resource_id=owner_resource_id,
        runtime_status=RUNTIME_STATUS_RUNNING,
        health_status=HEALTH_STATUS_HEALTHY,
        observed_at=datetime.now(timezone.utc),
        source=SRM_STATUS_SOURCE_UPDATE,
        metadata_empty=True,
        timeout_seconds=timeout_seconds,
    )


_SHUTDOWN_UNHEALTHY_SWR_REASONS: Final[frozenset[str]] = frozenset(
    {
        "SWR_ERROR_DETECTED",
        "SWR_UNHEALTHY_EXECUTION_DETECTED",
    }
)

_FAILURE_SHUTDOWN_CANONICAL_REASONS: Final[frozenset[str]] = frozenset(
    {
        "UNHEALTHY_EXECUTION_DETECTED",
    }
)


def shutdown_swr_reason_code(canonical_reason: str) -> str:
    """Map worker shutdown reason to SRM ``metadata.reason_code`` (``SWR_`` prefix)."""
    raw = str(canonical_reason or SWR_SHUTDOWN).strip().upper()
    if raw.startswith("SWR_"):
        return raw
    return f"SWR_{raw}"


def default_shutdown_message(canonical_reason: str) -> str:
    normalized = str(canonical_reason or "").strip().upper()
    messages: dict[str, str] = {
        "STOP_REQUESTED": "Worker shutdown requested.",
        "MANUAL_STOP_REQUESTED": "Worker shutdown complete.",
        "RUNTIME_JOB_COMPLETED": "Runtime job completed.",
        "LOCAL_TERMINATION_OBSERVED": "Local termination observed.",
        "ERROR_DETECTED": "Shutdown after runtime detected failure.",
        "UNHEALTHY_EXECUTION_DETECTED": "Shutdown after unhealthy execution.",
        "CONTROLLED_SHUTDOWN_INITIATED": "Controlled shutdown initiated.",
        KUBERNETES_TERMINATION_REASON: "Worker received Kubernetes SIGTERM.",
    }
    return messages.get(normalized, f"Worker shutdown ({normalized}).")


def resolve_shutdown_report(
    *,
    canonical_reason: str,
    message: str | None,
) -> tuple[str, str]:
    swr_reason = shutdown_swr_reason_code(canonical_reason)
    text = str(message or "").strip() or default_shutdown_message(canonical_reason)
    return swr_reason, text


def shutdown_health_status(swr_reason_code: str) -> str:
    if swr_reason_code in _SHUTDOWN_UNHEALTHY_SWR_REASONS:
        return HEALTH_STATUS_UNHEALTHY
    return HEALTH_STATUS_HEALTHY


def canonical_reason_from_stop_request(reason: str) -> str:
    """Map stop request body or SIGTERM reason to canonical worker reason."""
    text = str(reason or "").strip()
    if not text:
        return normalize_runtime_reason_code("STOP_REQUESTED")
    upper = text.upper()
    if upper in (
        KUBERNETES_TERMINATION_REASON,
        SWR_KUBERNETES_TERMINATION,
        "SIGTERM",
    ):
        return KUBERNETES_TERMINATION_REASON
    lowered = text.lower()
    if lowered in ("error_detected", "error", "failed"):
        return normalize_runtime_reason_code("ERROR_DETECTED")
    if lowered == "runtime_job_completed":
        return normalize_runtime_reason_code("RUNTIME_JOB_COMPLETED")
    candidate = normalize_runtime_reason_code(text)
    if (
        candidate == "POLICY_VIOLATION"
        and text.upper() not in RUNTIME_WORKER_REASON_CODES
    ):
        return normalize_runtime_reason_code("STOP_REQUESTED")
    return candidate


def is_failure_shutdown(canonical_reason: str) -> bool:
    if canonical_reason in _FAILURE_SHUTDOWN_CANONICAL_REASONS:
        return True
    return shutdown_swr_reason_code(canonical_reason) in _SHUTDOWN_UNHEALTHY_SWR_REASONS


def _stopping_status_retryable(canonical_reason: str) -> bool | None:
    if canonical_reason == KUBERNETES_TERMINATION_REASON:
        return False
    return None


def build_stopping_status_update_body(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    canonical_reason: str,
    message: str | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """SRM status JSON for ``runtime_status=STOPPING`` (HTTP stop response body)."""
    when = observed_at if observed_at is not None else datetime.now(timezone.utc)
    reason_code, resolved_message = resolve_shutdown_report(
        canonical_reason=canonical_reason,
        message=message,
    )
    return build_srm_heartbeat_body(
        runtime_id=runtime_id,
        mode=mode,
        owner_resource_id=owner_resource_id,
        local_state=RUNTIME_STATUS_STOPPING,
        observed_at=when,
        source=SRM_STATUS_SOURCE_UPDATE,
        runtime_status=RUNTIME_STATUS_STOPPING,
        health_status=HEALTH_STATUS_UNKNOWN,
        reason_code=reason_code,
        message=resolved_message,
        retryable=_stopping_status_retryable(canonical_reason),
    )


def initiate_stop_status_update(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    srm_base_url: str,
    reason: str,
    timeout_seconds: float = 30.0,
) -> tuple[dict[str, Any], dict[str, object] | None, str, str]:
    """
    Accept stop request: POST STOPPING to SRM and return the same status body for HTTP.

    Returns ``(response_body, srm_post_result, canonical_reason, resolved_message)``.
    """
    canonical = canonical_reason_from_stop_request(reason)
    stopping_message = (
        default_shutdown_message(canonical)
        if canonical == KUBERNETES_TERMINATION_REASON
        else None
    )
    body = build_stopping_status_update_body(
        runtime_id=runtime_id,
        mode=mode,
        owner_resource_id=owner_resource_id,
        canonical_reason=canonical,
        message=stopping_message,
    )
    reason_code, resolved_message = resolve_shutdown_report(
        canonical_reason=canonical,
        message=stopping_message,
    )
    srm_response = report_stopping_to_srm(
        runtime_id=runtime_id,
        mode=mode,
        owner_resource_id=owner_resource_id,
        srm_base_url=srm_base_url,
        canonical_reason=canonical,
        message=stopping_message,
        retryable=_stopping_status_retryable(canonical),
        timeout_seconds=timeout_seconds,
    )
    return body, srm_response, canonical, resolved_message


def _report_shutdown_status_fields_to_srm(
    *,
    runtime_id: str,
    srm_base_url: str,
    mode: str,
    owner_resource_id: str,
    runtime_status: str,
    health_status: str,
    reason_code: str,
    message: str,
    retryable: bool | None = None,
    timeout_seconds: float,
) -> dict[str, object] | None:
    if not runtime_id.strip() or not srm_base_url.strip():
        return None
    if not reason_code.strip() or not message.strip():
        return None
    post_kwargs: dict[str, Any] = {
        "base_url": srm_base_url,
        "runtime_id": runtime_id,
        "mode": mode,
        "owner_resource_id": owner_resource_id,
        "runtime_status": runtime_status,
        "health_status": health_status,
        "observed_at": datetime.now(timezone.utc),
        "source": SRM_STATUS_SOURCE_UPDATE,
        "reason_code": reason_code,
        "message": message,
        "timeout_seconds": timeout_seconds,
    }
    if retryable is not None:
        post_kwargs["retryable"] = retryable
    return post_srm_runtime_status(**post_kwargs)


def report_stopping_to_srm(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    srm_base_url: str,
    canonical_reason: str,
    message: str | None,
    retryable: bool | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """Report worker entering STOPPING (mandatory reason_code + message)."""
    reason_code, resolved_message = resolve_shutdown_report(
        canonical_reason=canonical_reason,
        message=message,
    )
    return _report_shutdown_status_fields_to_srm(
        runtime_id=runtime_id,
        srm_base_url=srm_base_url,
        mode=mode,
        owner_resource_id=owner_resource_id,
        runtime_status=RUNTIME_STATUS_STOPPING,
        health_status=HEALTH_STATUS_UNKNOWN,
        reason_code=reason_code,
        message=resolved_message,
        retryable=retryable,
        timeout_seconds=timeout_seconds,
    )


def report_stopped_to_srm(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    srm_base_url: str,
    canonical_reason: str,
    message: str | None,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """Report worker STOPPED (mandatory reason_code + message; no retryable)."""
    reason_code, resolved_message = resolve_shutdown_report(
        canonical_reason=canonical_reason,
        message=message,
    )
    return _report_shutdown_status_fields_to_srm(
        runtime_id=runtime_id,
        srm_base_url=srm_base_url,
        mode=mode,
        owner_resource_id=owner_resource_id,
        runtime_status=RUNTIME_STATUS_STOPPED,
        health_status=shutdown_health_status(reason_code),
        reason_code=reason_code,
        message=resolved_message,
        timeout_seconds=timeout_seconds,
    )


def report_failed_shutdown_to_srm(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    srm_base_url: str,
    canonical_reason: str,
    message: str | None,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """Report worker FAILED after error-detected shutdown (mandatory reason_code + message)."""
    reason_code, resolved_message = resolve_shutdown_report(
        canonical_reason=canonical_reason,
        message=message,
    )
    return _report_shutdown_status_fields_to_srm(
        runtime_id=runtime_id,
        srm_base_url=srm_base_url,
        mode=mode,
        owner_resource_id=owner_resource_id,
        runtime_status=RUNTIME_STATUS_FAILED,
        health_status=HEALTH_STATUS_UNHEALTHY,
        reason_code=reason_code,
        message=resolved_message,
        timeout_seconds=timeout_seconds,
    )


def report_final_shutdown_to_srm(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    srm_base_url: str,
    canonical_reason: str,
    message: str | None,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """After successful stop: STOPPED (+ healthy) or FAILED (+ unhealthy) for error shutdown."""
    if is_failure_shutdown(canonical_reason):
        return report_failed_shutdown_to_srm(
            runtime_id=runtime_id,
            mode=mode,
            owner_resource_id=owner_resource_id,
            srm_base_url=srm_base_url,
            canonical_reason=canonical_reason,
            message=message,
            timeout_seconds=timeout_seconds,
        )
    return report_stopped_to_srm(
        runtime_id=runtime_id,
        mode=mode,
        owner_resource_id=owner_resource_id,
        srm_base_url=srm_base_url,
        canonical_reason=canonical_reason,
        message=message,
        timeout_seconds=timeout_seconds,
    )


def print_shutdown_status_outcome(
    *,
    runtime_status: str,
    reason_code: str,
    message: str,
    srm_response: dict[str, object] | None,
) -> None:
    payload: dict[str, object] = {
        "event_name": "worker_shutdown_status",
        "runtime_status": runtime_status,
        "reason_code": reason_code,
        "message": message,
    }
    if srm_response is not None:
        payload["srm_status_report"] = srm_response
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), flush=True)


def print_bootstrap_success_outcome(
    *,
    srm_response: dict[str, object] | None,
) -> None:
    payload: dict[str, object] = {
        "event_name": "worker_bootstrap_succeeded",
        "runtime_status": RUNTIME_STATUS_RUNNING,
        "health_status": HEALTH_STATUS_HEALTHY,
    }
    if srm_response is not None:
        payload["srm_status_report"] = srm_response
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), flush=True)


def print_bootstrap_failure_outcome(
    failure: BootstrapFailure,
    *,
    srm_response: dict[str, object] | None,
) -> None:
    payload: dict[str, object] = {
        "event_name": "worker_bootstrap_failed",
        "stage": failure.stage.value,
        "reason": failure.reason_code,
        "message": build_bootstrap_failure_message(failure),
        "reason_code": bootstrap_failure_srm_reason_code(failure),
        "retryable": False,
    }
    if srm_response is not None:
        payload["srm_status_report"] = srm_response
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), flush=True)


def report_minimal_env_validation_to_srm(
    result: MinimalEnvValidationResult,
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """
    Best-effort status report before full bootstrap.

    Requires ``runtime_id`` and ``STRATEGY_RUNTIME_MANAGER_BASE_URL`` on the snapshot.
    """
    snapshot = result.snapshot
    if snapshot is None:
        return None

    if result.valid:
        return post_srm_runtime_status(
            base_url=snapshot.srm_base_url,
            runtime_id=snapshot.runtime_id,
            mode=snapshot.mode,
            owner_resource_id=snapshot.deployment_id,
            runtime_status=RUNTIME_STATUS_STARTING,
            health_status=HEALTH_STATUS_UNKNOWN,
            observed_at=datetime.now(timezone.utc),
            source=SRM_STATUS_SOURCE_UPDATE,
            timeout_seconds=timeout_seconds,
        )

    return _report_startup_failed_to_srm(
        snapshot,
        reason_code=SWR_ENV_VALIDATION_FAILED,
        message=result.message,
        retryable=False,
        timeout_seconds=timeout_seconds,
    )


def report_runtime_context_fetch_failure_to_srm(
    exc: LaunchSpecValidationError,
    *,
    snapshot: MinimalEnvSnapshot,
    timeout_seconds: float = 30.0,
) -> dict[str, object] | None:
    """Report SDS runtime-context fetch/validation failure before process exit."""
    reason_code, retryable = runtime_context_failure_status(exc)
    return _report_startup_failed_to_srm(
        snapshot,
        reason_code=reason_code,
        message=build_runtime_context_fetch_failure_message(exc),
        retryable=retryable,
        timeout_seconds=timeout_seconds,
    )


def print_minimal_env_validation_outcome(
    result: MinimalEnvValidationResult,
    *,
    srm_response: dict[str, object] | None,
) -> None:
    payload: dict[str, object] = {
        "event_name": "worker_minimal_env_validation",
        "valid": result.valid,
        "message": result.message,
        "field_errors": dict(result.field_errors),
    }
    if srm_response is not None:
        payload["srm_status_report"] = srm_response
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), flush=True)


def print_runtime_context_fetch_failure_outcome(
    exc: LaunchSpecValidationError,
    *,
    srm_response: dict[str, object] | None,
) -> None:
    reason_code, retryable = runtime_context_failure_status(exc)
    payload: dict[str, object] = {
        "event_name": "worker_runtime_context_fetch_failed",
        "reason": exc.reason,
        "message": build_runtime_context_fetch_failure_message(exc),
        "field_errors": dict(exc.field_errors),
        "reason_code": reason_code,
        "retryable": retryable,
    }
    if srm_response is not None:
        payload["srm_status_report"] = srm_response
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), flush=True)
