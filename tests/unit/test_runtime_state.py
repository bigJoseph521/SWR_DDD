from __future__ import annotations

import pytest
from runtime.domain.enums import WorkerPhase
from runtime.domain.errors import InvalidWorkerTransitionError
from runtime.domain.runtime_state import (
    can_transition,
    is_terminal_phase,
    validate_transition,
)


@pytest.mark.parametrize(
    ("from_phase", "to_phase"),
    [
        (WorkerPhase.INITIALIZING, WorkerPhase.READY),
        (WorkerPhase.INITIALIZING, WorkerPhase.RUNNING),
        (WorkerPhase.READY, WorkerPhase.RUNNING),
        (WorkerPhase.RUNNING, WorkerPhase.DEGRADED),
        (WorkerPhase.DEGRADED, WorkerPhase.RUNNING),
        (WorkerPhase.RUNNING, WorkerPhase.STOPPING),
        (WorkerPhase.STOPPING, WorkerPhase.STOPPED),
        (WorkerPhase.STOPPING, WorkerPhase.COMPLETED),
    ],
)
def test_valid_transitions_are_allowed(
    from_phase: WorkerPhase, to_phase: WorkerPhase
) -> None:
    assert can_transition(from_phase, to_phase) is True
    validate_transition(from_phase, to_phase)


@pytest.mark.parametrize(
    ("from_phase", "to_phase"),
    [
        (WorkerPhase.INITIALIZING, WorkerPhase.DEGRADED),
        (WorkerPhase.READY, WorkerPhase.STOPPED),
        (WorkerPhase.RUNNING, WorkerPhase.READY),
        (WorkerPhase.STOPPED, WorkerPhase.RUNNING),
        (WorkerPhase.FAILED, WorkerPhase.READY),
    ],
)
def test_invalid_transitions_raise_typed_error(
    from_phase: WorkerPhase,
    to_phase: WorkerPhase,
) -> None:
    assert can_transition(from_phase, to_phase) is False
    with pytest.raises(
        InvalidWorkerTransitionError,
        match=f"{from_phase.value} -> {to_phase.value}",
    ) as exc_info:
        validate_transition(from_phase, to_phase)
    assert exc_info.value.code == "WORKER_DOMAIN_INVALID_WORKER_TRANSITION"


@pytest.mark.parametrize(
    "phase", [WorkerPhase.STOPPED, WorkerPhase.COMPLETED, WorkerPhase.FAILED]
)
def test_terminal_phases_are_explicit(phase: WorkerPhase) -> None:
    assert is_terminal_phase(phase) is True


@pytest.mark.parametrize(
    "phase",
    [
        WorkerPhase.INITIALIZING,
        WorkerPhase.READY,
        WorkerPhase.RUNNING,
        WorkerPhase.DEGRADED,
    ],
)
def test_non_terminal_phases_are_not_terminal(phase: WorkerPhase) -> None:
    assert is_terminal_phase(phase) is False


def test_self_transition_is_rejected() -> None:
    with pytest.raises(InvalidWorkerTransitionError) as exc_info:
        validate_transition(WorkerPhase.RUNNING, WorkerPhase.RUNNING)
    assert exc_info.value.code == "WORKER_DOMAIN_INVALID_WORKER_TRANSITION"
    assert exc_info.value.details["reason"] == "self_transition_not_allowed"
