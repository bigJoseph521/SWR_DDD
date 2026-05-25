from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Final

from runtime.domain.enums import WorkerPhase
from runtime.domain.errors import InvalidWorkerTransitionError

WorkerLocalPhase = WorkerPhase


class CanonicalRuntimeState(StrEnum):
    DRAFT = "DRAFT"
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"


class CanonicalRuntimeReason(StrEnum):
    LAUNCH_FAILED = "LAUNCH_FAILED"
    HEARTBEAT_TIMEOUT = "HEARTBEAT_TIMEOUT"
    STOP_REQUESTED = "STOP_REQUESTED"


@dataclass(frozen=True, slots=True)
class RuntimeStatus:
    local_phase: WorkerPhase
    canonical_state: CanonicalRuntimeState | None = None
    reason_code: CanonicalRuntimeReason | None = None

    def with_local_phase(self, phase: WorkerPhase) -> "RuntimeStatus":
        validate_transition(self.local_phase, phase)
        return replace(self, local_phase=phase)

    def with_canonical_state(
        self,
        state: CanonicalRuntimeState,
        reason_code: CanonicalRuntimeReason | None = None,
    ) -> "RuntimeStatus":
        validate_state_reason_pair(state, reason_code)
        return replace(self, canonical_state=state, reason_code=reason_code)


ALLOWED_PHASE_TRANSITIONS: Final[dict[WorkerPhase, frozenset[WorkerPhase]]] = {
    WorkerPhase.INITIALIZING: frozenset(
        {
            WorkerPhase.READY,
            WorkerPhase.RUNNING,
            WorkerPhase.FAILED,
            WorkerPhase.STOPPING,
        }
    ),
    WorkerPhase.READY: frozenset(
        {
            WorkerPhase.RUNNING,
            WorkerPhase.STOPPING,
            WorkerPhase.FAILED,
        }
    ),
    WorkerPhase.RUNNING: frozenset(
        {
            WorkerPhase.DEGRADED,
            WorkerPhase.STOPPING,
            WorkerPhase.FAILED,
        }
    ),
    WorkerPhase.DEGRADED: frozenset(
        {
            WorkerPhase.RUNNING,
            WorkerPhase.STOPPING,
            WorkerPhase.FAILED,
        }
    ),
    WorkerPhase.STOPPING: frozenset(
        {WorkerPhase.STOPPED, WorkerPhase.FAILED, WorkerPhase.COMPLETED}
    ),
    WorkerPhase.STOPPED: frozenset(),
    WorkerPhase.COMPLETED: frozenset(),
    WorkerPhase.FAILED: frozenset(),
}


TERMINAL_PHASES: Final[frozenset[WorkerPhase]] = frozenset(
    {WorkerPhase.STOPPED, WorkerPhase.COMPLETED, WorkerPhase.FAILED}
)

TERMINAL_LOCAL_PHASES: Final[frozenset[WorkerPhase]] = TERMINAL_PHASES

LOCAL_TO_DEFAULT_CANONICAL_STATE: Final[dict[WorkerPhase, CanonicalRuntimeState]] = {
    WorkerPhase.INITIALIZING: CanonicalRuntimeState.STARTING,
    WorkerPhase.READY: CanonicalRuntimeState.STARTING,
    WorkerPhase.RUNNING: CanonicalRuntimeState.RUNNING,
    WorkerPhase.DEGRADED: CanonicalRuntimeState.DEGRADED,
    WorkerPhase.STOPPING: CanonicalRuntimeState.STOPPING,
    WorkerPhase.STOPPED: CanonicalRuntimeState.STOPPED,
    WorkerPhase.COMPLETED: CanonicalRuntimeState.STOPPED,
    WorkerPhase.FAILED: CanonicalRuntimeState.FAILED,
}

TERMINAL_CANONICAL_STATES: Final[frozenset[CanonicalRuntimeState]] = frozenset(
    {CanonicalRuntimeState.STOPPED, CanonicalRuntimeState.FAILED}
)

VALID_STATE_REASONS: Final[
    dict[CanonicalRuntimeState, frozenset[CanonicalRuntimeReason]]
] = {
    CanonicalRuntimeState.FAILED: frozenset(
        {CanonicalRuntimeReason.LAUNCH_FAILED, CanonicalRuntimeReason.HEARTBEAT_TIMEOUT}
    ),
    CanonicalRuntimeState.STOPPING: frozenset({CanonicalRuntimeReason.STOP_REQUESTED}),
}

CANONICAL_RUNTIME_STATES: Final[frozenset[str]] = frozenset(
    state.value for state in CanonicalRuntimeState
)
CANONICAL_RUNTIME_REASONS: Final[frozenset[str]] = frozenset(
    reason.value for reason in CanonicalRuntimeReason
)


def is_terminal_phase(phase: WorkerPhase) -> bool:
    return phase in TERMINAL_PHASES


def is_terminal_local_phase(phase: WorkerPhase) -> bool:
    return is_terminal_phase(phase)


def can_transition(from_phase: WorkerPhase, to_phase: WorkerPhase) -> bool:
    if from_phase == to_phase:
        return False
    if is_terminal_phase(from_phase):
        return False
    return to_phase in ALLOWED_PHASE_TRANSITIONS[from_phase]


def validate_transition(from_phase: WorkerPhase, to_phase: WorkerPhase) -> None:
    if from_phase == to_phase:
        raise InvalidWorkerTransitionError(
            from_phase=from_phase.value,
            to_phase=to_phase.value,
            reason="self_transition_not_allowed",
        )

    if is_terminal_phase(from_phase):
        raise InvalidWorkerTransitionError(
            from_phase=from_phase.value,
            to_phase=to_phase.value,
            reason="terminal_phase_resurrection_not_allowed",
        )

    if to_phase not in ALLOWED_PHASE_TRANSITIONS[from_phase]:
        raise InvalidWorkerTransitionError(
            from_phase=from_phase.value,
            to_phase=to_phase.value,
            reason="transition_edge_not_allowed",
        )


def to_default_canonical_state(local_phase: WorkerPhase) -> CanonicalRuntimeState:
    return LOCAL_TO_DEFAULT_CANONICAL_STATE[local_phase]


def is_terminal_canonical_state(state: CanonicalRuntimeState) -> bool:
    return state in TERMINAL_CANONICAL_STATES


def is_canonical_runtime_state(value: str) -> bool:
    return value in CANONICAL_RUNTIME_STATES


def is_canonical_runtime_reason(value: str) -> bool:
    return value in CANONICAL_RUNTIME_REASONS


def require_canonical_runtime_state(value: str) -> str:
    if value not in CANONICAL_RUNTIME_STATES:
        raise ValueError(f"Non-canonical runtime state: {value}")
    return value


def require_canonical_runtime_reason(value: str) -> str:
    if value not in CANONICAL_RUNTIME_REASONS:
        raise ValueError(f"Non-canonical runtime reason: {value}")
    return value


def validate_state_reason_pair(
    state: CanonicalRuntimeState,
    reason: CanonicalRuntimeReason | None,
) -> None:
    allowed = VALID_STATE_REASONS.get(state)

    if allowed is None:
        if reason is not None:
            raise ValueError(
                f"Reason {reason.value} is not allowed for runtime state {state.value}"
            )
        return

    if reason is None:
        raise ValueError(f"Runtime state {state.value} requires a canonical reason")

    if reason not in allowed:
        raise ValueError(
            f"Reason {reason.value} is not allowed for runtime state {state.value}"
        )


def build_runtime_status(
    local_phase: WorkerPhase,
    *,
    canonical_state: CanonicalRuntimeState | None = None,
    reason_code: CanonicalRuntimeReason | None = None,
) -> RuntimeStatus:
    if canonical_state is not None:
        validate_state_reason_pair(canonical_state, reason_code)
    elif reason_code is not None:
        raise ValueError("reason_code cannot be set without canonical_state")

    return RuntimeStatus(
        local_phase=local_phase,
        canonical_state=canonical_state,
        reason_code=reason_code,
    )
