from __future__ import annotations

from enum import StrEnum
from typing import Final


# ============================================================================
# Worker execution modes
# ============================================================================


class WorkerMode(StrEnum):
    """
    Canonical execution modes for one worker runtime assignment
    """

    PAPER = "PAPER"
    LIVE = "LIVE"
    BACKTEST = "BACKTEST"


class WorkerPhase(StrEnum):
    INITIALIZING = "INITIALIZING"
    READY = "READY"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class OrderIntentSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderIntentType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


# ============================================================================
# Internal privilege / trusted caller classes
# ============================================================================


class InternalPrivilege(StrEnum):
    """
    Canonical internal privilege classes used across worker/runtime contracts.
    """

    RUNTIME_SUBSTRATE = "internal:runtimes:substrate"
    RUNTIME_CONTROL = "internal:runtimes:control"


# ============================================================================
# Worker-owned canonical internal route IDs
# Keep the IDs for traceability, tests, and shared naming.
# Do not keep HTTP path literals here if manager/worker communication is gRPC.
# ============================================================================


class WorkerRouteId(StrEnum):
    CREATE = "ROUTE-WORKER-CREATE"
    STOP = "ROUTE-WORKER-STOP"

    # Reserved / future-plan IDs
    PAUSE = "ROUTE-WORKER-PAUSE"
    RESUME = "ROUTE-WORKER-RESUME"

    HEALTH_LIVE = "ROUTE-WORKER-HEALTH-LIVE"
    HEALTH_READY = "ROUTE-WORKER-HEALTH-READY"


WORKER_ROUTE_REQUIRED_PRIVILEGE: Final[
    dict[WorkerRouteId, InternalPrivilege | None]
] = {
    WorkerRouteId.CREATE: InternalPrivilege.RUNTIME_SUBSTRATE,
    WorkerRouteId.STOP: InternalPrivilege.RUNTIME_CONTROL,
    WorkerRouteId.PAUSE: InternalPrivilege.RUNTIME_CONTROL,
    WorkerRouteId.RESUME: InternalPrivilege.RUNTIME_CONTROL,
    WorkerRouteId.HEALTH_LIVE: None,
    WorkerRouteId.HEALTH_READY: None,
}


# ============================================================================
# Runtime-manager canonical internal contract IDs used by the worker
# Keep IDs, not HTTP paths, because transport may be gRPC.
# ============================================================================


class RuntimeManagerSignalId(StrEnum):
    HEARTBEAT = "ROUTE-RUNTIME-HEARTBEAT"
    LAUNCH_SUCCEEDED = "ROUTE-RUNTIME-LAUNCH-SUCCEEDED"
    LAUNCH_FAILED = "ROUTE-RUNTIME-LAUNCH-FAILED"
    TERMINATED = "ROUTE-RUNTIME-TERMINATED"

    # Reserved / future-plan
    PAUSE_REQUESTED_INTERNAL = "ROUTE-RUNTIME-PAUSE-REQUESTED-INTERNAL"


RUNTIME_MANAGER_SIGNAL_REQUIRED_PRIVILEGE: Final[
    dict[RuntimeManagerSignalId, InternalPrivilege]
] = {
    RuntimeManagerSignalId.HEARTBEAT: InternalPrivilege.RUNTIME_SUBSTRATE,
    RuntimeManagerSignalId.LAUNCH_SUCCEEDED: InternalPrivilege.RUNTIME_SUBSTRATE,
    RuntimeManagerSignalId.LAUNCH_FAILED: InternalPrivilege.RUNTIME_SUBSTRATE,
    RuntimeManagerSignalId.TERMINATED: InternalPrivilege.RUNTIME_SUBSTRATE,
    RuntimeManagerSignalId.PAUSE_REQUESTED_INTERNAL: InternalPrivilege.RUNTIME_CONTROL,
}


# ============================================================================
# Worker-origin lifecycle signals
# These are execution-plane facts emitted by the worker.
# ============================================================================


class WorkerEventId(StrEnum):
    LAUNCH_SUCCEEDED = "EVT-WORKER-LAUNCH-SUCCEEDED"
    LAUNCH_FAILED = "EVT-WORKER-LAUNCH-FAILED"
    HEARTBEAT = "EVT-WORKER-HEARTBEAT"


class WorkerEventName(StrEnum):
    LAUNCH_SUCCEEDED = "runtime.launch_succeeded"
    LAUNCH_FAILED = "runtime.launch_failed"
    HEARTBEAT = "runtime.heartbeat"


WORKER_EVENT_ID_TO_NAME: Final[dict[WorkerEventId, WorkerEventName]] = {
    WorkerEventId.LAUNCH_SUCCEEDED: WorkerEventName.LAUNCH_SUCCEEDED,
    WorkerEventId.LAUNCH_FAILED: WorkerEventName.LAUNCH_FAILED,
    WorkerEventId.HEARTBEAT: WorkerEventName.HEARTBEAT,
}


# ============================================================================
# Manager-owned lifecycle facts
# Worker may reference them, but does not own them.
# ============================================================================


class RuntimeLifecycleEventId(StrEnum):
    STARTED = "EVT-RUNTIME-STARTED"
    DEGRADED = "EVT-RUNTIME-DEGRADED"
    FAILED = "EVT-RUNTIME-FAILED"


class RuntimeLifecycleEventName(StrEnum):
    STARTED = "runtime.started"
    DEGRADED = "runtime.degraded"
    FAILED = "runtime.failed"


RUNTIME_LIFECYCLE_EVENT_ID_TO_NAME: Final[
    dict[RuntimeLifecycleEventId, RuntimeLifecycleEventName]
] = {
    RuntimeLifecycleEventId.STARTED: RuntimeLifecycleEventName.STARTED,
    RuntimeLifecycleEventId.DEGRADED: RuntimeLifecycleEventName.DEGRADED,
    RuntimeLifecycleEventId.FAILED: RuntimeLifecycleEventName.FAILED,
}


# ============================================================================
# Worker dependency/service targets
# These help the worker enforce mode-specific communication policy.
# ============================================================================


class ServiceTarget(StrEnum):
    STRATEGY_RUNTIME_MANAGER = "strategy-runtime-manager"
    STRATEGY_REGISTRY_SERVICE = "strategy-registry-service"
    MARKET_DATA_SERVICE = "market-data-service"
    RISK_SERVICE = "risk-service"
    #: BACKTEST order intents egress to upstream runner via stdout JSONL.
    BACKTEST_RUNNER = "backtest-runner"


class DependencyAccessPattern(StrEnum):
    CONTROL_PLANE = "CONTROL_PLANE"
    INTERNAL_REPORTING = "INTERNAL_REPORTING"
    MARKET_CURRENT_STATE = "MARKET_CURRENT_STATE"
    #: SDK / strategy order intents (``risk_worker.proto``) to risk-service hot path (all modes).
    RISK_ORDER_INTENT_EGRESS = "RISK_ORDER_INTENT_EGRESS"
    BACKTEST_REPLAY_CONTEXT = "BACKTEST_REPLAY_CONTEXT"
    BACKTEST_LOOKBACK = "BACKTEST_LOOKBACK"
    BACKTEST_ORDER_INTENT = "BACKTEST_ORDER_INTENT"


MODE_ALLOWED_SERVICE_TARGETS: Final[dict[WorkerMode, frozenset[ServiceTarget]]] = {
    WorkerMode.PAPER: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER,
            ServiceTarget.MARKET_DATA_SERVICE,
            ServiceTarget.RISK_SERVICE,
        }
    ),
    WorkerMode.LIVE: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER,
            ServiceTarget.MARKET_DATA_SERVICE,
            ServiceTarget.RISK_SERVICE,
        }
    ),
    WorkerMode.BACKTEST: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER,
            ServiceTarget.BACKTEST_RUNNER,
        }
    ),
}


# ============================================================================
# Optional grouped canonical sets for validation/linting
# ============================================================================

CANONICAL_WORKER_MODES: Final[frozenset[str]] = frozenset(
    item.value for item in WorkerMode
)

CANONICAL_INTERNAL_PRIVILEGES: Final[frozenset[str]] = frozenset(
    item.value for item in InternalPrivilege
)

CANONICAL_ROUTE_IDS: Final[frozenset[str]] = frozenset(
    [item.value for item in WorkerRouteId]
    + [item.value for item in RuntimeManagerSignalId]
)

CANONICAL_EVENT_IDS: Final[frozenset[str]] = frozenset(
    [item.value for item in WorkerEventId]
    + [item.value for item in RuntimeLifecycleEventId]
)

CANONICAL_EVENT_NAMES: Final[frozenset[str]] = frozenset(
    [item.value for item in WorkerEventName]
    + [item.value for item in RuntimeLifecycleEventName]
)

ALL_CANONICAL_IDENTIFIER_VALUES: Final[frozenset[str]] = frozenset().union(
    CANONICAL_WORKER_MODES,
    CANONICAL_INTERNAL_PRIVILEGES,
    CANONICAL_ROUTE_IDS,
    CANONICAL_EVENT_IDS,
    CANONICAL_EVENT_NAMES,
)
