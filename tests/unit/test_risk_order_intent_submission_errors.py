from __future__ import annotations

import grpc
from runtime.domain.errors import (
    ORDER_INTENT_SUBMISSION_FAILED,
    ORDER_INTENT_SUBMISSION_INTERNAL_ERROR,
    RISK_SERVICE_TIMEOUT,
    RISK_SERVICE_UNAVAILABLE,
)
from runtime.infrastructure.grpc.dependency_client_error import DependencyClientError
from runtime.infrastructure.grpc.risk_order_intent_client import _normalize_grpc_error
from runtime.infrastructure.grpc.risk_order_intent_submission_errors import (
    dependency_client_error_to_submission_error,
)


def test_dependency_unavailable_maps_to_risk_service_unavailable() -> None:
    exc = DependencyClientError(
        code="DEPENDENCY_UNAVAILABLE",
        message="down",
        retryable=True,
        details={"grpc_code": "StatusCode.UNAVAILABLE"},
    )
    mapped = dependency_client_error_to_submission_error(
        exc,
        wire={"runtime_id": "rt-1", "correlation_id": "corr-1"},
    )
    assert mapped.reason_code == RISK_SERVICE_UNAVAILABLE
    assert mapped.retryable is True
    assert mapped.diagnostics["runtime_id"] == "rt-1"


def test_dependency_deadline_exceeded_maps_to_risk_service_timeout() -> None:
    class _FakeRpcError(grpc.RpcError):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.DEADLINE_EXCEEDED

        def details(self) -> str:
            return "deadline exceeded"

    dep = _normalize_grpc_error(_FakeRpcError())
    mapped = dependency_client_error_to_submission_error(dep, wire={"job_id": "job-1"})
    assert mapped.reason_code == RISK_SERVICE_TIMEOUT
    assert mapped.diagnostics["job_id"] == "job-1"


def test_dependency_validation_failed_maps_to_order_intent_submission_failed() -> None:
    exc = DependencyClientError(
        code="DEPENDENCY_VALIDATION_FAILED",
        message="invalid",
        retryable=False,
        details={"grpc_code": "StatusCode.INVALID_ARGUMENT"},
    )
    mapped = dependency_client_error_to_submission_error(exc)
    assert mapped.reason_code == ORDER_INTENT_SUBMISSION_FAILED


def test_dependency_internal_failure_maps_to_internal_error_reason() -> None:
    exc = DependencyClientError(
        code="DEPENDENCY_INTERNAL_FAILURE",
        message="boom",
        retryable=False,
        details={"grpc_code": "StatusCode.INTERNAL"},
    )
    mapped = dependency_client_error_to_submission_error(exc)
    assert mapped.reason_code == ORDER_INTENT_SUBMISSION_INTERNAL_ERROR
