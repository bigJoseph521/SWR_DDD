from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from runtime.domain.enums import (
    DependencyAccessPattern,
    WorkerMode,
    ServiceTarget,
)
from runtime.domain.errors import (
    UnsupportedDependencyExpansion,
    WorkerModeNotSupportedError,
    WorkerPolicyViolationError,
)


class Capability(str, Enum):
    WALL_CLOCK = "WALL_CLOCK"
    SIMULATED_CLOCK = "SIMULATED_CLOCK"
    RISK_ORDER_INTENT_EGRESS = "RISK_ORDER_INTENT_EGRESS"
    BACKTEST_ORDER_INTENT_EGRESS = "BACKTEST_ORDER_INTENT_EGRESS"
    MANAGER_SIGNAL = "MANAGER_SIGNAL"


@dataclass(frozen=True, slots=True)
class ModePolicy:
    mode: WorkerMode
    allowed: frozenset[Capability]

    def allow_event(self, event_type: str) -> bool:
        normalized = event_type.strip()
        if not normalized:
            return False
        blocked = _MODE_BLOCKED_EVENTS.get(self.mode, frozenset())
        return normalized not in blocked

    def allow_dependency(self, dependency: str) -> bool:
        normalized = dependency.strip()
        if not normalized:
            return False
        return normalized in _MODE_ALLOWED_DEPENDENCIES[self.mode]

    def route_order_intent(self) -> ServiceTarget:
        return _ORDER_INTENT_ROUTE[self.mode]

    def allow_replay_chunk_direct(self) -> bool:
        # Worker runtime is never allowed to fetch replay chunks directly.
        return False

    def require_event_allowed(self, event_type: str) -> None:
        if self.allow_event(event_type):
            return
        raise WorkerPolicyViolationError(
            policy="runtime_mode_event_gate",
            reason="event_not_allowed_for_mode",
            details={"mode": self.mode.value, "event_type": event_type},
        )

    def require_dependency_allowed(self, dependency: str) -> None:
        if self.allow_dependency(dependency):
            return
        raise UnsupportedDependencyExpansion(
            mode=self.mode.value,
            dependency=dependency,
            reason="dependency_not_allowed_for_mode",
        )


_MODE_CAPABILITIES: dict[WorkerMode, frozenset[Capability]] = {
    WorkerMode.PAPER: frozenset(
        {
            Capability.WALL_CLOCK,
            Capability.RISK_ORDER_INTENT_EGRESS,
            Capability.MANAGER_SIGNAL,
        }
    ),
    WorkerMode.LIVE: frozenset(
        {
            Capability.WALL_CLOCK,
            Capability.RISK_ORDER_INTENT_EGRESS,
            Capability.MANAGER_SIGNAL,
        }
    ),
    WorkerMode.BACKTEST: frozenset(
        {
            Capability.SIMULATED_CLOCK,
            Capability.MANAGER_SIGNAL,
            Capability.BACKTEST_ORDER_INTENT_EGRESS,
        }
    ),
}

_MODE_ALLOWED_DEPENDENCIES: Final[dict[WorkerMode, frozenset[str]]] = {
    WorkerMode.PAPER: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER.value,
            ServiceTarget.MARKET_DATA_SERVICE.value,
            DependencyAccessPattern.RISK_ORDER_INTENT_EGRESS.value,
        }
    ),
    WorkerMode.LIVE: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER.value,
            ServiceTarget.MARKET_DATA_SERVICE.value,
            DependencyAccessPattern.RISK_ORDER_INTENT_EGRESS.value,
        }
    ),
    WorkerMode.BACKTEST: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER.value,
            DependencyAccessPattern.BACKTEST_ORDER_INTENT.value,
        }
    ),
}

_ORDER_INTENT_ROUTE: Final[dict[WorkerMode, ServiceTarget]] = {
    WorkerMode.PAPER: ServiceTarget.RISK_SERVICE,
    WorkerMode.LIVE: ServiceTarget.RISK_SERVICE,
    WorkerMode.BACKTEST: ServiceTarget.BACKTEST_RUNNER,
}

_MODE_BLOCKED_EVENTS: Final[dict[WorkerMode, frozenset[str]]] = {
    WorkerMode.PAPER: frozenset({"replay.chunk.direct.fetch"}),
    WorkerMode.LIVE: frozenset({"replay.chunk.direct.fetch"}),
    WorkerMode.BACKTEST: frozenset({"replay.chunk.direct.fetch"}),
}


def get_mode_policy(mode: WorkerMode) -> ModePolicy:
    normalized_mode = mode
    if isinstance(mode, str):
        try:
            normalized_mode = WorkerMode(mode)
        except ValueError as exc:
            raise WorkerModeNotSupportedError(mode=str(mode)) from exc

    capabilities = _MODE_CAPABILITIES.get(normalized_mode)
    if capabilities is None:
        raise WorkerModeNotSupportedError(mode=normalized_mode.value)

    return ModePolicy(mode=normalized_mode, allowed=capabilities)


def require_capability(policy: ModePolicy, capability: Capability) -> None:
    if capability in policy.allowed:
        return

    raise UnsupportedDependencyExpansion(
        mode=policy.mode.value,
        capability=capability.value,
        dependency=capability.value.lower(),
        reason="capability_not_allowed_for_mode",
    )
