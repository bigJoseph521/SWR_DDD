from __future__ import annotations

import contextvars
import json
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import grpc
from runtime.domain.errors import (
    RuntimeStartValidationFailedError,
    StrategyWorkerRuntimeError,
    WorkerInternalCallerNotAllowedError,
    WorkerInvalidRequestError,
    WorkerNotFoundError,
)
from runtime.transport.internal.auth import (
    parse_internal_auth_metadata,
    require_internal_control,
    require_internal_health_trust,
    require_internal_substrate,
)

_REQUEST_METADATA: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "grpc_request_metadata",
    default={},
)

_CORRELATION_FIELDS = (
    "correlation_id",
    "causation_id",
    "tenant_id",
    "account_id",
    "runtime_id",
    "worker_identity",
    "launch_attempt",
    "strategy_version_id",
    "job_id",
)


def _normalize_metadata(metadata: Iterable[tuple[str, str]]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in metadata:
        key_str = str(key).lower().replace("-", "_")
        normalized[key_str] = str(value)
    return normalized


def get_current_request_metadata() -> dict[str, str]:
    return dict(_REQUEST_METADATA.get({}))


def get_current_correlation_id() -> str | None:
    metadata = get_current_request_metadata()
    return metadata.get("correlation_id")


class CorrelationMetadataServerInterceptor(grpc.ServerInterceptor):
    def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], grpc.RpcMethodHandler | None],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler | None:
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        invocation_metadata = tuple(handler_call_details.invocation_metadata or ())
        parsed = _normalize_metadata(
            (item.key, item.value) for item in invocation_metadata
        )
        retained = {
            key: value for key, value in parsed.items() if key in _CORRELATION_FIELDS
        }

        if handler.unary_unary:
            unary_unary = handler.unary_unary

            def wrapped_unary_unary(request: Any, context: grpc.ServicerContext) -> Any:
                token = _REQUEST_METADATA.set(retained)
                try:
                    return unary_unary(request, context)
                finally:
                    _REQUEST_METADATA.reset(token)

            return grpc.unary_unary_rpc_method_handler(
                wrapped_unary_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        return handler


class AuthServerInterceptor(grpc.ServerInterceptor):
    def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], grpc.RpcMethodHandler | None],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler | None:
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        if not handler.unary_unary:
            return handler

        method_name = handler_call_details.method
        unary_unary = handler.unary_unary

        def wrapped_unary_unary(request: Any, context: grpc.ServicerContext) -> Any:
            invocation_metadata = tuple(context.invocation_metadata() or ())
            auth = parse_internal_auth_metadata(
                (item.key, item.value) for item in invocation_metadata
            )
            if method_name.endswith("/CreateWorker"):
                require_internal_substrate(auth, operation="CreateWorker")
            elif method_name.endswith("/StopWorker"):
                require_internal_control(auth, operation="StopWorker")
            elif method_name.endswith("/CheckHealth"):
                require_internal_health_trust(auth, operation="CheckHealth")
            return unary_unary(request, context)

        return grpc.unary_unary_rpc_method_handler(
            wrapped_unary_unary,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def _details_list(details: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if details is None:
        return []
    return [{"key": key, "value": value} for key, value in details.items()]


def _normalize_exception(exc: Exception) -> tuple[grpc.StatusCode, dict[str, Any]]:
    correlation_id = get_current_correlation_id()

    if isinstance(exc, WorkerInternalCallerNotAllowedError):
        status_code = grpc.StatusCode.PERMISSION_DENIED
        code = exc.code.value
        message = exc.message
        retryable = exc.retryable
        details = exc.details
    elif isinstance(
        exc, (RuntimeStartValidationFailedError, WorkerInvalidRequestError)
    ):
        status_code = grpc.StatusCode.INVALID_ARGUMENT
        code = exc.code.value
        message = exc.message
        retryable = exc.retryable
        details = exc.details
    elif isinstance(exc, WorkerNotFoundError):
        status_code = grpc.StatusCode.NOT_FOUND
        code = exc.code.value
        message = exc.message
        retryable = exc.retryable
        details = exc.details
    elif isinstance(exc, StrategyWorkerRuntimeError):
        status_code = grpc.StatusCode.INTERNAL
        code = exc.code.value
        message = exc.message
        retryable = exc.retryable
        details = exc.details
    else:
        status_code = grpc.StatusCode.INTERNAL
        code = "WORKER_INTERNAL_ERROR"
        message = "Unclassified transport failure."
        retryable = False
        details = {"exception_type": type(exc).__name__}

    envelope = {
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable,
            "correlation_id": correlation_id,
            "details": _details_list(details),
        }
    }
    return status_code, envelope


class ErrorEnvelopeServerInterceptor(grpc.ServerInterceptor):
    def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], grpc.RpcMethodHandler | None],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler | None:
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        if not handler.unary_unary:
            return handler

        unary_unary = handler.unary_unary

        def wrapped_unary_unary(request: Any, context: grpc.ServicerContext) -> Any:
            try:
                return unary_unary(request, context)
            except grpc.RpcError:
                raise
            except Exception as exc:  # nosec B110 - normalization boundary
                status_code, envelope = _normalize_exception(exc)
                context.abort(status_code, json.dumps(envelope, ensure_ascii=True))

        return grpc.unary_unary_rpc_method_handler(
            wrapped_unary_unary,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
