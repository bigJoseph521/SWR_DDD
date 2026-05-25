from __future__ import annotations

import pytest
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
    normalize_runtime_reason_code,
    worker_error_code_for_reason,
)


@pytest.mark.parametrize(
    ("legacy", "expected"),
    [
        (
            "strategy_bind_and_start_failed",
            RuntimeWorkerReasonCode.STRATEGY_STARTUP_FAILED,
        ),
        (
            "STRATEGY_BIND_AND_START_FAILED",
            RuntimeWorkerReasonCode.STRATEGY_STARTUP_FAILED,
        ),
        (
            "strategy_callback_contract_violation",
            RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
        ),
        (
            "strategy_callback_unhandled_exception",
            RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
        ),
        (
            "strategy_cleanup_contract_violation",
            RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
        ),
        (
            "strategy_cleanup_unhandled_exception",
            RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
        ),
        (
            "STRATEGY_CALLBACK_CONTRACT_VIOLATION",
            RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
        ),
        (
            "STRATEGY_CALLBACK_UNHANDLED_EXCEPTION",
            RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
        ),
        (
            "STRATEGY_CLEANUP_CONTRACT_VIOLATION",
            RuntimeWorkerReasonCode.SDK_COMPATIBILITY_FAILED,
        ),
        (
            "STRATEGY_CLEANUP_UNHANDLED_EXCEPTION",
            RuntimeWorkerReasonCode.UNHEALTHY_EXECUTION_DETECTED,
        ),
    ],
)
def test_legacy_strategy_reasons_map_to_canonical(
    legacy: str, expected: RuntimeWorkerReasonCode
) -> None:
    assert normalize_runtime_reason_code(legacy) == expected.value


def test_strategy_startup_failed_round_trips() -> None:
    assert (
        normalize_runtime_reason_code(
            RuntimeWorkerReasonCode.STRATEGY_STARTUP_FAILED.value
        )
        == RuntimeWorkerReasonCode.STRATEGY_STARTUP_FAILED.value
    )


def test_worker_error_code_for_strategy_startup_failed() -> None:
    assert (
        worker_error_code_for_reason(
            RuntimeWorkerReasonCode.STRATEGY_STARTUP_FAILED.value
        )
        == WorkerErrorCode.BOOTSTRAP_FAILED.value
    )
