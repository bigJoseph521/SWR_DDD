from __future__ import annotations

from typing import Any, Mapping

from runtime.domain.errors import (
    ORDER_INTENT_SUBMISSION_FAILED,
    ORDER_INTENT_SUBMISSION_INTERNAL_ERROR,
    RISK_SERVICE_TIMEOUT,
    RISK_SERVICE_UNAVAILABLE,
    OrderIntentSubmissionError,
)
from runtime.infrastructure.grpc.risk_order_intent_client import DependencyClientError


def _wire_trace_fields(wire: Mapping[str, Any] | None) -> dict[str, Any]:
    if wire is None:
        return {}
    trace_keys = (
        "correlation_id",
        "runtime_id",
        "job_id",
        "strategy_version_id",
        "account_id",
        "order_intent_id",
        "symbol",
    )
    fields: dict[str, Any] = {}
    for key in trace_keys:
        value = wire.get(key)
        if value not in (None, ""):
            fields[key] = value
    return fields


def dependency_client_error_to_submission_error(
    exc: DependencyClientError,
    *,
    wire: Mapping[str, Any] | None = None,
) -> OrderIntentSubmissionError:
    diagnostics = {**_wire_trace_fields(wire), **dict(exc.details)}
    grpc_code = str(exc.details.get("grpc_code", ""))

    if exc.code == "DEPENDENCY_UNAVAILABLE":
        if "DEADLINE_EXCEEDED" in grpc_code:
            return OrderIntentSubmissionError(
                error_code="order_intent_submission_failed",
                reason_code=RISK_SERVICE_TIMEOUT,
                diagnostics=diagnostics,
                retryable=True,
            )
        return OrderIntentSubmissionError(
            error_code="order_intent_submission_failed",
            reason_code=RISK_SERVICE_UNAVAILABLE,
            diagnostics=diagnostics,
            retryable=True,
        )

    if exc.code in {
        "DEPENDENCY_VALIDATION_FAILED",
        "DEPENDENCY_ACCESS_DENIED",
        "DEPENDENCY_NOT_FOUND",
    }:
        return OrderIntentSubmissionError(
            error_code="order_intent_submission_failed",
            reason_code=ORDER_INTENT_SUBMISSION_FAILED,
            diagnostics=diagnostics,
            retryable=False,
        )

    return OrderIntentSubmissionError(
        error_code="order_intent_submission_failed",
        reason_code=ORDER_INTENT_SUBMISSION_INTERNAL_ERROR,
        diagnostics=diagnostics,
        retryable=exc.retryable,
    )


def misconfigured_client_error(
    *,
    wire: Mapping[str, Any] | None = None,
) -> OrderIntentSubmissionError:
    return OrderIntentSubmissionError(
        error_code="order_intent_submission_failed",
        reason_code=ORDER_INTENT_SUBMISSION_INTERNAL_ERROR,
        diagnostics=_wire_trace_fields(wire),
        retryable=False,
    )
