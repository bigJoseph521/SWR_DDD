from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import grpc
import pytest
from runtime.domain.errors import WorkerInvalidRequestError
from runtime.transport.grpc.interceptors import (
    AuthServerInterceptor,
    CorrelationMetadataServerInterceptor,
    ErrorEnvelopeServerInterceptor,
    get_current_request_metadata,
)


class _Aborted(Exception):
    def __init__(self, status_code: grpc.StatusCode, details: str) -> None:
        super().__init__(details)
        self.status_code = status_code
        self.details = details


class _FakeContext:
    def __init__(self, metadata: list[tuple[str, str]]) -> None:
        self._metadata = tuple(SimpleNamespace(key=k, value=v) for k, v in metadata)

    def invocation_metadata(self) -> tuple[Any, ...]:
        return self._metadata

    def abort(self, status_code: grpc.StatusCode, details: str) -> None:
        raise _Aborted(status_code, details)


def _handler_details(method: str, metadata: list[tuple[str, str]]) -> Any:
    return SimpleNamespace(
        method=method,
        invocation_metadata=tuple(SimpleNamespace(key=k, value=v) for k, v in metadata),
    )


def _wrap(
    interceptor: grpc.ServerInterceptor,
    *,
    method: str,
    metadata: list[tuple[str, str]],
    func: Any,
) -> grpc.RpcMethodHandler:
    base = grpc.unary_unary_rpc_method_handler(func)
    details = _handler_details(method, metadata)
    wrapped = interceptor.intercept_service(lambda _: base, details)
    assert wrapped is not None
    return wrapped


def test_auth_allow_deny_and_missing_metadata_matrix() -> None:
    interceptor = AuthServerInterceptor()
    allow_create = _wrap(
        interceptor,
        method="/x.WorkerControlService/CreateWorker",
        metadata=[
            ("x-internal-caller", "runtime-manager"),
            ("x-internal-trust-class", "internal:runtimes:substrate"),
        ],
        func=lambda _r, _c: "ok",
    )
    assert (
        allow_create.unary_unary(
            None,
            _FakeContext(
                [
                    ("x-internal-caller", "runtime-manager"),
                    ("x-internal-trust-class", "internal:runtimes:substrate"),
                ]
            ),
        )
        == "ok"
    )

    deny_create = _wrap(
        interceptor,
        method="/x.WorkerControlService/CreateWorker",
        metadata=[
            ("x-internal-caller", "runtime-manager"),
            ("x-internal-trust-class", "internal:runtimes:control"),
        ],
        func=lambda _r, _c: "ok",
    )
    with pytest.raises(Exception):
        deny_create.unary_unary(
            None,
            _FakeContext(
                [
                    ("x-internal-caller", "runtime-manager"),
                    ("x-internal-trust-class", "internal:runtimes:control"),
                ]
            ),
        )

    missing = _wrap(
        interceptor,
        method="/x.WorkerControlService/StopWorker",
        metadata=[],
        func=lambda _r, _c: "ok",
    )
    with pytest.raises(Exception):
        missing.unary_unary(None, _FakeContext([]))


def test_error_interceptor_normalizes_domain_and_unhandled_exceptions() -> None:
    interceptor = ErrorEnvelopeServerInterceptor()

    wrapped_known = _wrap(
        interceptor,
        method="/x.WorkerControlService/CreateWorker",
        metadata=[],
        func=lambda _r, _c: (_ for _ in ()).throw(
            WorkerInvalidRequestError(reason="validation_failed")
        ),
    )
    with pytest.raises(_Aborted) as known:
        wrapped_known.unary_unary(None, _FakeContext([]))
    known_payload = json.loads(known.value.details)
    assert known.value.status_code == grpc.StatusCode.INVALID_ARGUMENT
    assert known_payload["error"]["code"] == "WORKER_INVALID_REQUEST"
    assert isinstance(known_payload["error"]["details"], list)

    wrapped_unknown = _wrap(
        interceptor,
        method="/x.WorkerControlService/CreateWorker",
        metadata=[],
        func=lambda _r, _c: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with pytest.raises(_Aborted) as unknown:
        wrapped_unknown.unary_unary(None, _FakeContext([]))
    unknown_payload = json.loads(unknown.value.details)
    assert unknown.value.status_code == grpc.StatusCode.INTERNAL
    assert unknown_payload["error"]["code"] == "WORKER_INTERNAL_ERROR"


def test_correlation_metadata_propagation() -> None:
    interceptor = CorrelationMetadataServerInterceptor()

    def _capture(_request: Any, _context: Any) -> dict[str, str]:
        return get_current_request_metadata()

    wrapped = _wrap(
        interceptor,
        method="/x.WorkerControlService/CheckHealth",
        metadata=[
            ("correlation-id", "corr-123"),
            ("causation-id", "cause-123"),
            ("tenant-id", "tenant-1"),
            ("authorization", "secret-token"),
        ],
        func=_capture,
    )

    captured = wrapped.unary_unary(
        None,
        _FakeContext(
            [
                ("correlation-id", "corr-123"),
                ("causation-id", "cause-123"),
                ("tenant-id", "tenant-1"),
                ("authorization", "secret-token"),
            ]
        ),
    )
    assert captured["correlation_id"] == "corr-123"
    assert captured["causation_id"] == "cause-123"
    assert captured["tenant_id"] == "tenant-1"
    assert "authorization" not in captured
