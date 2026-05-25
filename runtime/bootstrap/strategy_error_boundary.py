from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Mapping, TypeVar

from runtime.domain.errors import (
    SharedBoundaryError,
    StrategyWorkerRuntimeError,
    WorkerDomainError,
    WorkerErrorCode,
    get_machine_error_code,
)

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class StrategyCallResult(Generic[T]):
    ok: bool
    value: T | None
    error_code: str | None
    reason_code: str | None
    exception: Exception | None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.reason_code is not None:
            rc = str(self.reason_code).strip().upper()
            object.__setattr__(self, "reason_code", rc or None)
        if self.error_code is not None:
            ec = str(self.error_code).strip().upper()
            object.__setattr__(self, "error_code", ec or None)


class StrategyErrorBoundary:
    _CALL_CONTRACT_REASON = "STRATEGY_CALLBACK_CONTRACT_VIOLATION"
    _CALL_UNHANDLED_REASON = "STRATEGY_CALLBACK_UNHANDLED_EXCEPTION"
    _CLEANUP_CONTRACT_REASON = "STRATEGY_CLEANUP_CONTRACT_VIOLATION"
    _CLEANUP_UNHANDLED_REASON = "STRATEGY_CLEANUP_UNHANDLED_EXCEPTION"

    def call(
        self,
        operation: str,
        func: Callable[..., T],
        *args: object,
        **kwargs: object,
    ) -> StrategyCallResult[T]:
        return self._invoke(operation, func, False, *args, **kwargs)

    def call_cleanup(
        self,
        func: Callable[..., T],
        *args: object,
        **kwargs: object,
    ) -> StrategyCallResult[T]:
        return self._invoke("strategy.cleanup", func, True, *args, **kwargs)

    def _invoke(
        self,
        operation: str,
        func: Callable[..., T],
        is_cleanup: bool,
        *args: object,
        **kwargs: object,
    ) -> StrategyCallResult[T]:
        try:
            value = func(*args, **kwargs)
            return StrategyCallResult(
                ok=True,
                value=value,
                error_code=None,
                reason_code=None,
                exception=None,
                diagnostics={"operation": operation},
            )
        except Exception as exc:  # nosec B110 - boundary intentionally contains errors
            if self._is_contract_exception(exc):
                reason_code = (
                    self._CLEANUP_CONTRACT_REASON
                    if is_cleanup
                    else self._CALL_CONTRACT_REASON
                )
                error_code = get_machine_error_code(exc)
                failure_kind = "contract"
            else:
                reason_code = (
                    self._CLEANUP_UNHANDLED_REASON
                    if is_cleanup
                    else self._CALL_UNHANDLED_REASON
                )
                error_code = WorkerErrorCode.INTERNAL_ERROR.value
                failure_kind = "unhandled"

            diagnostics: dict[str, Any] = {
                "operation": operation,
                "failure_kind": failure_kind,
                "exception_type": type(exc).__name__,
            }
            details = getattr(exc, "details", None)
            if isinstance(details, Mapping):
                diagnostics["error_details"] = dict(details)

            return StrategyCallResult(
                ok=False,
                value=None,
                error_code=error_code,
                reason_code=reason_code,
                exception=exc,
                diagnostics=diagnostics,
            )

    @staticmethod
    def _is_contract_exception(exc: Exception) -> bool:
        if isinstance(
            exc, (WorkerDomainError, StrategyWorkerRuntimeError, SharedBoundaryError)
        ):
            return True
        return isinstance(exc, (ValueError, TypeError))
