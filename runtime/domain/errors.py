from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping


class WorkerDomainErrorCode(StrEnum):
    INVALID_WORKER_TRANSITION = "WORKER_DOMAIN_INVALID_WORKER_TRANSITION"
    MALFORMED_WORKER_IDENTITY = "WORKER_DOMAIN_MALFORMED_WORKER_IDENTITY"
    SINGLE_ASSIGNMENT_VIOLATION = "WORKER_DOMAIN_SINGLE_ASSIGNMENT_VIOLATION"
    INVALID_LAUNCH_CONTEXT = "WORKER_DOMAIN_INVALID_LAUNCH_CONTEXT"
    UNSUPPORTED_MODE = "WORKER_DOMAIN_UNSUPPORTED_MODE"
    ORDER_INTENT_VALIDATION = "WORKER_DOMAIN_ORDER_INTENT_VALIDATION"


class WorkerDomainError(Exception):
    code: str
    retryable: bool
    details: Mapping[str, Any]

    def __init__(
        self,
        *,
        code: str,
        message: str,
        retryable: bool = False,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.details = details or {}

    @property
    def message(self) -> str:
        return str(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "retryable": self.retryable,
                "details": dict(self.details),
            }
        }


class InvalidWorkerTransitionError(WorkerDomainError):
    def __init__(self, *, from_phase: str, to_phase: str, reason: str) -> None:
        super().__init__(
            code=WorkerDomainErrorCode.INVALID_WORKER_TRANSITION.value,
            message=f"Invalid worker phase transition: {from_phase} -> {to_phase}",
            details={"from_phase": from_phase, "to_phase": to_phase, "reason": reason},
        )


class MalformedWorkerIdentityError(WorkerDomainError):
    def __init__(self, *, field_name: str, reason: str) -> None:
        super().__init__(
            code=WorkerDomainErrorCode.MALFORMED_WORKER_IDENTITY.value,
            message=f"Malformed worker identity field: {field_name}",
            details={"field_name": field_name, "reason": reason},
        )


class SingleAssignmentViolationError(WorkerDomainError):
    def __init__(self, *, reason: str) -> None:
        super().__init__(
            code=WorkerDomainErrorCode.SINGLE_ASSIGNMENT_VIOLATION.value,
            message="Worker identity violates single-assignment constraints.",
            details={"reason": reason},
        )


class InvalidLaunchContextError(WorkerDomainError):
    def __init__(self, *, field_name: str, reason: str) -> None:
        super().__init__(
            code=WorkerDomainErrorCode.INVALID_LAUNCH_CONTEXT.value,
            message=f"Invalid launch context field: {field_name}",
            details={"field_name": field_name, "reason": reason},
        )


class UnsupportedModeError(WorkerDomainError):
    def __init__(self, *, mode: str, reason: str = "mode_not_supported") -> None:
        super().__init__(
            code=WorkerDomainErrorCode.UNSUPPORTED_MODE.value,
            message=f"Unsupported runtime mode: {mode}",
            details={"mode": mode, "reason": reason},
        )


class OrderIntentValidationError(WorkerDomainError):
    def __init__(self, *, field_name: str, reason: str) -> None:
        super().__init__(
            code=WorkerDomainErrorCode.ORDER_INTENT_VALIDATION.value,
            message=f"Invalid order intent field: {field_name}",
            details={"field_name": field_name, "reason": reason},
        )


ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID = "MISSING_CORRELATION_ID"

ORDER_INTENT_SUBMISSION_INTERNAL_ERROR = "ORDER_INTENT_SUBMISSION_INTERNAL_ERROR"
ORDER_INTENT_SUBMISSION_FAILED = "ORDER_INTENT_SUBMISSION_FAILED"
ORDER_INTENT_WIRE_MAPPING_FAILED = "ORDER_INTENT_WIRE_MAPPING_FAILED"
RISK_SERVICE_TIMEOUT = "RISK_SERVICE_TIMEOUT"
RISK_SERVICE_UNAVAILABLE = "RISK_SERVICE_UNAVAILABLE"
RISK_ORDER_INTENT_REJECTED = "RISK_ORDER_INTENT_REJECTED"


class OrderIntentWireMappingError(Exception):
    """Risk order-intent wire payload could not be built (infrastructure egress)."""

    def __init__(
        self,
        *,
        reason_code: str,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.diagnostics = dict(diagnostics or {})
        super().__init__(reason_code)


class OrderIntentSubmissionError(Exception):
    """Typed order-intent submission failure (transport, policy, or configuration)."""

    def __init__(
        self,
        *,
        error_code: str,
        reason_code: str,
        diagnostics: Mapping[str, Any] | None = None,
        retryable: bool = False,
    ) -> None:
        self.error_code = error_code
        self.reason_code = reason_code
        self.diagnostics = dict(diagnostics or {})
        self.retryable = retryable
        super().__init__(reason_code)


class WorkerErrorCode(StrEnum):
    """
    Worker-owned canonical error codes.

    These come from the Canonical Catalog Pack worker domain and should be the
    primary canonical codes emitted by strategy-worker-runtime for worker-owned
    failures.
    """

    INVALID_REQUEST = "WORKER_INVALID_REQUEST"
    ARTIFACT_NOT_FOUND = "WORKER_ARTIFACT_NOT_FOUND"
    ARTIFACT_DIGEST_MISMATCH = "WORKER_ARTIFACT_DIGEST_MISMATCH"
    ENTRYPOINT_NOT_FOUND = "WORKER_ENTRYPOINT_NOT_FOUND"
    SDK_CONTRACT_INVALID = "WORKER_SDK_CONTRACT_INVALID"
    MODE_NOT_SUPPORTED = "WORKER_MODE_NOT_SUPPORTED"
    BOOTSTRAP_FAILED = "WORKER_BOOTSTRAP_FAILED"
    NOT_FOUND = "WORKER_NOT_FOUND"
    ALREADY_STOPPED = "WORKER_ALREADY_STOPPED"
    STOP_TIMEOUT = "WORKER_STOP_TIMEOUT"
    DEPENDENCY_UNHEALTHY = "WORKER_DEPENDENCY_UNHEALTHY"
    RUNTIME_COMPLETED = "WORKER_RUNTIME_COMPLETED"
    RUNTIME_FAILED = "WORKER_RUNTIME_FAILED"
    INTERNAL_CALLER_NOT_ALLOWED = "WORKER_INTERNAL_CALLER_NOT_ALLOWED"
    POLICY_VIOLATION = "WORKER_POLICY_VIOLATION"
    INTERNAL_ERROR = "WORKER_INTERNAL_ERROR"


class SharedBoundaryErrorCode(StrEnum):
    """
    Shared non-worker-owned codes that worker-runtime may need to preserve,
    surface, or translate at service boundaries.

    These are not the worker's own namespace. They are included only because
    worker-runtime communicates with runtime-manager and depends on registry
    eligibility truth.
    """

    STRATEGY_VERSION_NOT_RUNNABLE = "STRATEGY_VERSION_NOT_RUNNABLE"
    RUNTIME_START_VALIDATION_FAILED = "RUNTIME_START_VALIDATION_FAILED"
    RUNTIME_VERSION_NOT_APPROVED = "RUNTIME_VERSION_NOT_APPROVED"
    RUNTIME_WORKER_START_FAILED = "RUNTIME_WORKER_START_FAILED"
    RUNTIME_HEARTBEAT_MISSED = "RUNTIME_HEARTBEAT_MISSED"


WORKER_CANONICAL_ERROR_CODES: frozenset[str] = frozenset(
    item.value for item in WorkerErrorCode
)

SHARED_BOUNDARY_ERROR_CODES: frozenset[str] = frozenset(
    item.value for item in SharedBoundaryErrorCode
)

ALL_KNOWN_ERROR_CODES: frozenset[str] = frozenset().union(
    WORKER_CANONICAL_ERROR_CODES,
    SHARED_BOUNDARY_ERROR_CODES,
)


@dataclass(frozen=True, slots=True)
class ErrorEnvelope:
    """
    Platform-style canonical error envelope.
    """

    code: str
    message: str
    retryable: bool = False
    correlation_id: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "retryable": self.retryable,
                "correlation_id": self.correlation_id,
                "details": dict(self.details),
            }
        }


class StrategyWorkerRuntimeError(Exception):
    """
    Base class for worker-domain exceptions.

    Uses worker-owned canonical codes by default.
    """

    code: WorkerErrorCode
    message: str
    retryable: bool
    correlation_id: str | None
    details: Mapping[str, Any]

    def __init__(
        self,
        *,
        code: WorkerErrorCode,
        message: str,
        retryable: bool = False,
        correlation_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.correlation_id = correlation_id
        self.details = details or {}

    def to_envelope(self) -> ErrorEnvelope:
        return ErrorEnvelope(
            code=self.code.value,
            message=self.message,
            retryable=self.retryable,
            correlation_id=self.correlation_id,
            details=self.details,
        )

    def to_dict(self) -> dict[str, Any]:
        return self.to_envelope().as_dict()


class SharedBoundaryError(Exception):
    """
    Wrapper for non-worker-owned canonical boundary errors.

    Use when preserving a shared runtime/registry-facing code without pretending
    it is worker-owned.
    """

    code: SharedBoundaryErrorCode
    message: str
    retryable: bool
    correlation_id: str | None
    details: Mapping[str, Any]

    def __init__(
        self,
        *,
        code: SharedBoundaryErrorCode,
        message: str,
        retryable: bool = False,
        correlation_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.correlation_id = correlation_id
        self.details = details or {}

    def to_envelope(self) -> ErrorEnvelope:
        return ErrorEnvelope(
            code=self.code.value,
            message=self.message,
            retryable=self.retryable,
            correlation_id=self.correlation_id,
            details=self.details,
        )


# ---------------------------------------------------------------------------
# Shared-boundary typed exceptions
# ---------------------------------------------------------------------------


class RuntimeStartValidationFailedError(SharedBoundaryError):
    def __init__(
        self,
        *,
        reason: str,
        field_errors: Mapping[str, Any] | None = None,
        runtime_id: str | None = None,
        launch_attempt: int | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=SharedBoundaryErrorCode.RUNTIME_START_VALIDATION_FAILED,
            message="Runtime start validation failed.",
            correlation_id=correlation_id,
            details={
                "reason": reason,
                "field_errors": dict(field_errors or {}),
                "runtime_id": runtime_id,
                "launch_attempt": launch_attempt,
            },
        )


# ---------------------------------------------------------------------------
# Worker-owned typed exceptions
# ---------------------------------------------------------------------------


class WorkerInvalidRequestError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        reason: str,
        field_errors: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.INVALID_REQUEST,
            message="Worker request validation failed.",
            correlation_id=correlation_id,
            details={
                "reason": reason,
                "field_errors": dict(field_errors or {}),
            },
        )


class WorkerArtifactNotFoundError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        artifact_uri: str,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.ARTIFACT_NOT_FOUND,
            message="Worker artifact was not found.",
            correlation_id=correlation_id,
            details={"artifact_uri": artifact_uri},
        )


class WorkerArtifactDigestMismatchError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        artifact_uri: str,
        expected_digest: str | None = None,
        actual_digest: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.ARTIFACT_DIGEST_MISMATCH,
            message="Worker artifact digest did not match.",
            correlation_id=correlation_id,
            details={
                "artifact_uri": artifact_uri,
                "expected_digest": expected_digest,
                "actual_digest": actual_digest,
            },
        )


class WorkerEntrypointNotFoundError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        entrypoint: str,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.ENTRYPOINT_NOT_FOUND,
            message="Worker entrypoint could not be resolved.",
            correlation_id=correlation_id,
            details={"entrypoint": entrypoint},
        )


class WorkerSdkContractInvalidError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        entrypoint: str | None = None,
        sdk_version: str | None = None,
        correlation_id: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.SDK_CONTRACT_INVALID,
            message="Strategy artifact does not satisfy the required SDK contract.",
            correlation_id=correlation_id,
            details={
                "entrypoint": entrypoint,
                "sdk_version": sdk_version,
                "diagnostics": dict(diagnostics or {}),
            },
        )


class WorkerModeNotSupportedError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        mode: str,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.MODE_NOT_SUPPORTED,
            message="Worker mode is not supported.",
            correlation_id=correlation_id,
            details={"mode": mode},
        )


class WorkerBootstrapFailedError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        runtime_id: str | None = None,
        launch_attempt: int | None = None,
        reason: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.BOOTSTRAP_FAILED,
            message="Worker bootstrap failed.",
            retryable=True,
            correlation_id=correlation_id,
            details={
                "runtime_id": runtime_id,
                "launch_attempt": launch_attempt,
                "reason": reason,
                "diagnostics": dict(diagnostics or {}),
            },
        )


class WorkerNotFoundError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        runtime_id: str | None = None,
        worker_identity: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.NOT_FOUND,
            message="Worker instance was not found.",
            correlation_id=correlation_id,
            details={
                "runtime_id": runtime_id,
                "worker_identity": worker_identity,
            },
        )


class WorkerAlreadyStoppedError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        runtime_id: str | None = None,
        worker_identity: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.ALREADY_STOPPED,
            message="Worker is already stopped.",
            correlation_id=correlation_id,
            details={
                "runtime_id": runtime_id,
                "worker_identity": worker_identity,
            },
        )


class WorkerStopTimeoutError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        runtime_id: str | None = None,
        worker_identity: str | None = None,
        timeout_seconds: int | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.STOP_TIMEOUT,
            message="Worker stop timed out.",
            retryable=True,
            correlation_id=correlation_id,
            details={
                "runtime_id": runtime_id,
                "worker_identity": worker_identity,
                "timeout_seconds": timeout_seconds,
            },
        )


class WorkerDependencyUnhealthyError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        dependency: str,
        reason: str | None = None,
        correlation_id: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.DEPENDENCY_UNHEALTHY,
            message="An approved worker dependency is unhealthy.",
            retryable=True,
            correlation_id=correlation_id,
            details={
                "dependency": dependency,
                "reason": reason,
                "diagnostics": dict(diagnostics or {}),
            },
        )


class WorkerRuntimeFailedError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        runtime_id: str | None = None,
        worker_identity: str | None = None,
        reason: str | None = None,
        correlation_id: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.RUNTIME_FAILED,
            message="Worker runtime failed after startup.",
            retryable=True,
            correlation_id=correlation_id,
            details={
                "runtime_id": runtime_id,
                "worker_identity": worker_identity,
                "reason": reason,
                "diagnostics": dict(diagnostics or {}),
            },
        )


class WorkerInternalCallerNotAllowedError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        caller: str | None = None,
        operation: str | None = None,
        required_privilege: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.INTERNAL_CALLER_NOT_ALLOWED,
            message="Trusted internal caller is not allowed for this operation.",
            correlation_id=correlation_id,
            details={
                "caller": caller,
                "operation": operation,
                "required_privilege": required_privilege,
            },
        )


class WorkerPolicyViolationError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        policy: str,
        reason: str | None = None,
        correlation_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        merged_details = {"policy": policy, "reason": reason}
        if details:
            merged_details.update(details)

        super().__init__(
            code=WorkerErrorCode.POLICY_VIOLATION,
            message="Worker violated runtime policy.",
            correlation_id=correlation_id,
            details=merged_details,
        )


class UnsupportedDependencyExpansion(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        mode: str,
        dependency: str,
        capability: str | None = None,
        reason: str = "dependency_not_allowed_for_mode",
        correlation_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        merged_details: dict[str, Any] = {
            "policy": "runtime_mode_dependency_expansion",
            "reason": reason,
            "mode": mode,
            "dependency": dependency,
            "capability": capability,
        }
        if details:
            merged_details.update(details)

        super().__init__(
            code=WorkerErrorCode.POLICY_VIOLATION,
            message=(
                "Unsupported dependency expansion: "
                f"mode={mode}, dependency={dependency}, capability={capability}."
            ),
            correlation_id=correlation_id,
            details=merged_details,
        )


class WorkerInternalError(StrategyWorkerRuntimeError):
    def __init__(
        self,
        *,
        reason: str | None = None,
        correlation_id: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            code=WorkerErrorCode.INTERNAL_ERROR,
            message="Unclassified internal worker failure.",
            retryable=True,
            correlation_id=correlation_id,
            details={
                "reason": reason,
                "diagnostics": dict(diagnostics or {}),
            },
        )


# ---------------------------------------------------------------------------
# Shared-boundary helpers
# ---------------------------------------------------------------------------


def is_worker_error_code(value: str) -> bool:
    return value in WORKER_CANONICAL_ERROR_CODES


def is_shared_boundary_error_code(value: str) -> bool:
    return value in SHARED_BOUNDARY_ERROR_CODES


def is_known_error_code(value: str) -> bool:
    return value in ALL_KNOWN_ERROR_CODES


def require_known_error_code(value: str) -> str:
    if value not in ALL_KNOWN_ERROR_CODES:
        raise ValueError(f"Unknown canonical error code: {value}")
    return value


def to_error_envelope(
    exc: Exception,
    *,
    fallback_code: WorkerErrorCode = WorkerErrorCode.INTERNAL_ERROR,
    fallback_message: str = "Unexpected worker runtime error.",
    correlation_id: str | None = None,
) -> ErrorEnvelope:
    if isinstance(exc, StrategyWorkerRuntimeError):
        return exc.to_envelope()

    if isinstance(exc, SharedBoundaryError):
        return exc.to_envelope()

    return ErrorEnvelope(
        code=fallback_code.value,
        message=fallback_message,
        retryable=False,
        correlation_id=correlation_id,
        details={"exception_type": type(exc).__name__},
    )


def get_machine_error_code(exc: Exception) -> str:
    if isinstance(exc, WorkerDomainError):
        return exc.code
    if isinstance(exc, StrategyWorkerRuntimeError):
        return exc.code.value
    if isinstance(exc, SharedBoundaryError):
        return exc.code.value
    return WorkerErrorCode.INTERNAL_ERROR.value


class RuntimeWorkerReasonCode(StrEnum):
    STOP_REQUESTED = "STOP_REQUESTED"
    HEARTBEAT_TIMEOUT_ENFORCED = "HEARTBEAT_TIMEOUT_ENFORCED"
    POLICY_VIOLATION = "POLICY_VIOLATION"
    ARTIFACT_REFERENCE_INVALID = "ARTIFACT_REFERENCE_INVALID"
    ARTIFACT_DIGEST_MISMATCH = "ARTIFACT_DIGEST_MISMATCH"
    ENTRYPOINT_IMPORT_FAILED = "ENTRYPOINT_IMPORT_FAILED"
    SDK_COMPATIBILITY_FAILED = "SDK_COMPATIBILITY_FAILED"
    BOOTSTRAP_METADATA_MISSING = "BOOTSTRAP_METADATA_MISSING"
    BOOTSTRAP_METADATA_INCONSISTENT = "BOOTSTRAP_METADATA_INCONSISTENT"
    MODE_DEPENDENCY_NOT_ALLOWED = "MODE_DEPENDENCY_NOT_ALLOWED"
    BACKTEST_REPLAY_DIRECT_ACCESS_BLOCKED = "BACKTEST_REPLAY_DIRECT_ACCESS_BLOCKED"
    OMS_BYPASS_BLOCKED = "OMS_BYPASS_BLOCKED"
    BOUND_DEPENDENCY_UNAVAILABLE = "BOUND_DEPENDENCY_UNAVAILABLE"
    INITIALIZATION_TIMEOUT = "INITIALIZATION_TIMEOUT"
    CONTROLLED_SHUTDOWN_INITIATED = "CONTROLLED_SHUTDOWN_INITIATED"
    UNHEALTHY_EXECUTION_DETECTED = "UNHEALTHY_EXECUTION_DETECTED"
    HEARTBEAT_EMISSION_FAILED = "HEARTBEAT_EMISSION_FAILED"
    LOCAL_TERMINATION_OBSERVED = "LOCAL_TERMINATION_OBSERVED"
    KUBERNETES_TERMINATION = "KUBERNETES_TERMINATION"
    STRATEGY_STARTUP_FAILED = "STRATEGY_STARTUP_FAILED"


RUNTIME_WORKER_REASON_CODES: frozenset[str] = frozenset(
    item.value for item in RuntimeWorkerReasonCode
)


def is_runtime_worker_reason_code(value: str) -> bool:
    return value in RUNTIME_WORKER_REASON_CODES


_LEGACY_RUNTIME_REASON_MAP: dict[str, RuntimeWorkerReasonCode] = {
    "launch_spec_invalid": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT,
    "validation_runtime_error": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT,
    "bootstrap_start_failed": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT,
    "runtime_id_mismatch": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT,
    "launch_attempt_mismatch": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT,
    "strategy_version_id_mismatch": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT,
    "identity_mismatch": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT,
    "artifact_not_found": RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID,
    "artifact_access_denied": RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID,
    "artifact_fetch_failed": RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID,
    "artifact_materialization_failed": RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID,
    "artifact_provider_unsupported": RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID,
    "artifact_materialization_missing": RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID,
    "digest_unsupported": RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID,
    "expected_digest_missing": RuntimeWorkerReasonCode.ARTIFACT_DIGEST_MISMATCH,
    "digest_mismatch": RuntimeWorkerReasonCode.ARTIFACT_DIGEST_MISMATCH,
    "entrypoint_import_failed": RuntimeWorkerReasonCode.ENTRYPOINT_IMPORT_FAILED,
    "entrypoint_symbol_not_found": RuntimeWorkerReasonCode.ENTRYPOINT_IMPORT_FAILED,
    "entrypoint_symbol_invalid": RuntimeWorkerReasonCode.ENTRYPOINT_IMPORT_FAILED,
    "entrypoint_invalid": RuntimeWorkerReasonCode.ENTRYPOINT_IMPORT_FAILED,
    "sdk_protocol_mismatch": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "sdk_non_instantiable": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "sdk_invalid_signature": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "sdk_missing_required_method": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "sdk_marker_invalid": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "sdk_marker_incompatible": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "manual_stop_requested": RuntimeWorkerReasonCode.STOP_REQUESTED,
    "stop_requested": RuntimeWorkerReasonCode.STOP_REQUESTED,
    "kubernetes_termination": RuntimeWorkerReasonCode.KUBERNETES_TERMINATION,
    "sigterm": RuntimeWorkerReasonCode.KUBERNETES_TERMINATION,
    "runtime_job_completed": RuntimeWorkerReasonCode.LOCAL_TERMINATION_OBSERVED,
    "error_detected": RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
    "grpc_emit_failed": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE,
    "unsupported_signal_type": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE,
    "submit_backtest_order_intent_failed": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE,
    "submit_backtest_order_intent_not_configured": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE,
    "noop_replay_client": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE,
    "no_oms_client_configured": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE,
    "heartbeat_lagging": RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
    "strategy_bind_and_start_failed": RuntimeWorkerReasonCode.STRATEGY_STARTUP_FAILED,
    "strategy_callback_contract_violation": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "strategy_callback_unhandled_exception": RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
    "strategy_cleanup_contract_violation": RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
    "strategy_cleanup_unhandled_exception": RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
}


def normalize_runtime_reason_code(
    value: str | None,
    *,
    default: RuntimeWorkerReasonCode = RuntimeWorkerReasonCode.POLICY_VIOLATION,
) -> str:
    if isinstance(value, RuntimeWorkerReasonCode):
        return value.value
    raw = str(value or "").strip()
    if not raw:
        return default.value
    if raw in RUNTIME_WORKER_REASON_CODES:
        return raw
    mapped = _LEGACY_RUNTIME_REASON_MAP.get(raw.lower())
    if mapped is not None:
        return mapped.value
    return default.value


def worker_error_code_for_reason(reason_code: str) -> str:
    normalized = normalize_runtime_reason_code(reason_code)
    reason_enum = RuntimeWorkerReasonCode(normalized)
    mapping = {
        RuntimeWorkerReasonCode.ARTIFACT_REFERENCE_INVALID: WorkerErrorCode.ARTIFACT_NOT_FOUND,
        RuntimeWorkerReasonCode.ARTIFACT_DIGEST_MISMATCH: WorkerErrorCode.ARTIFACT_DIGEST_MISMATCH,
        RuntimeWorkerReasonCode.ENTRYPOINT_IMPORT_FAILED: WorkerErrorCode.ENTRYPOINT_NOT_FOUND,
        RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED: WorkerErrorCode.SDK_CONTRACT_INVALID,
        RuntimeWorkerReasonCode.MODE_DEPENDENCY_NOT_ALLOWED: WorkerErrorCode.MODE_NOT_SUPPORTED,
        RuntimeWorkerReasonCode.POLICY_VIOLATION: WorkerErrorCode.POLICY_VIOLATION,
        RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_MISSING: WorkerErrorCode.BOOTSTRAP_FAILED,
        RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT: WorkerErrorCode.BOOTSTRAP_FAILED,
        RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE: WorkerErrorCode.DEPENDENCY_UNHEALTHY,
        RuntimeWorkerReasonCode.HEARTBEAT_TIMEOUT_ENFORCED: WorkerErrorCode.DEPENDENCY_UNHEALTHY,
        RuntimeWorkerReasonCode.HEARTBEAT_EMISSION_FAILED: WorkerErrorCode.DEPENDENCY_UNHEALTHY,
        RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED: WorkerErrorCode.RUNTIME_FAILED,
        RuntimeWorkerReasonCode.LOCAL_TERMINATION_OBSERVED: WorkerErrorCode.RUNTIME_COMPLETED,
        RuntimeWorkerReasonCode.CONTROLLED_SHUTDOWN_INITIATED: WorkerErrorCode.RUNTIME_FAILED,
        RuntimeWorkerReasonCode.STOP_REQUESTED: WorkerErrorCode.ALREADY_STOPPED,
        RuntimeWorkerReasonCode.BACKTEST_REPLAY_DIRECT_ACCESS_BLOCKED: WorkerErrorCode.POLICY_VIOLATION,
        RuntimeWorkerReasonCode.OMS_BYPASS_BLOCKED: WorkerErrorCode.POLICY_VIOLATION,
        RuntimeWorkerReasonCode.INITIALIZATION_TIMEOUT: WorkerErrorCode.BOOTSTRAP_FAILED,
        RuntimeWorkerReasonCode.STRATEGY_STARTUP_FAILED: WorkerErrorCode.BOOTSTRAP_FAILED,
    }
    return mapping.get(reason_enum, WorkerErrorCode.INTERNAL_ERROR).value
