from __future__ import annotations

from runtime.application.strategy_execution.strategy_error_boundary import (
    StrategyErrorBoundary,
)
from runtime.domain.errors import (
    InvalidLaunchContextError,
    WorkerErrorCode,
    WorkerInternalError,
)


def test_call_success_returns_value() -> None:
    boundary = StrategyErrorBoundary()

    result = boundary.call("strategy.on_event", lambda value: value + 1, 4)

    assert result.ok is True
    assert result.value == 5
    assert result.error_code is None
    assert result.reason_code is None
    assert result.exception is None


def test_call_contains_generic_exception() -> None:
    boundary = StrategyErrorBoundary()

    def _boom() -> None:
        raise RuntimeError("unexpected")

    result = boundary.call("strategy.on_event", _boom)

    assert result.ok is False
    assert result.value is None
    assert result.error_code == WorkerErrorCode.INTERNAL_ERROR.value
    assert result.reason_code == "STRATEGY_CALLBACK_UNHANDLED_EXCEPTION"
    assert isinstance(result.exception, RuntimeError)


def test_typed_contract_exception_maps_to_stable_error_code() -> None:
    boundary = StrategyErrorBoundary()

    def _invalid_contract() -> None:
        raise InvalidLaunchContextError(field_name="entrypoint", reason="missing")

    result = boundary.call("strategy.initialize", _invalid_contract)

    assert result.ok is False
    assert result.error_code == "WORKER_DOMAIN_INVALID_LAUNCH_CONTEXT"
    assert result.reason_code == "STRATEGY_CALLBACK_CONTRACT_VIOLATION"
    assert isinstance(result.exception, InvalidLaunchContextError)


def test_cleanup_exception_is_contained() -> None:
    boundary = StrategyErrorBoundary()

    def _cleanup_failure() -> None:
        raise RuntimeError("cleanup exploded")

    result = boundary.call_cleanup(_cleanup_failure)

    assert result.ok is False
    assert result.error_code == WorkerErrorCode.INTERNAL_ERROR.value
    assert result.reason_code == "STRATEGY_CLEANUP_UNHANDLED_EXCEPTION"
    assert isinstance(result.exception, RuntimeError)


def test_diagnostics_always_include_operation() -> None:
    boundary = StrategyErrorBoundary()

    result = boundary.call("strategy.on_event", lambda: "ok")

    assert result.diagnostics["operation"] == "strategy.on_event"


def test_boundary_does_not_reraise_by_default() -> None:
    boundary = StrategyErrorBoundary()

    def _raise_worker_internal() -> None:
        raise WorkerInternalError(reason="test")

    result = boundary.call("strategy.on_event", _raise_worker_internal)

    assert result.ok is False
    assert result.reason_code == "STRATEGY_CALLBACK_CONTRACT_VIOLATION"
