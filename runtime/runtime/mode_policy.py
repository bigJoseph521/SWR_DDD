from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from runtime.domain.enums import (
    DependencyAccessPattern,
    RuntimeMode,
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
    OMS_EGRESS = "OMS_EGRESS"
    MANAGER_SIGNAL = "MANAGER_SIGNAL"
    REPLAY_INGRESS = "REPLAY_INGRESS"
    DIRECT_HISTORICAL_LOOKBACK = "DIRECT_HISTORICAL_LOOKBACK"


@dataclass(frozen=True, slots=True)
class ModePolicy:
    mode: RuntimeMode
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

    def allow_historical_window(self, *, explicitly_approved: bool) -> bool:
        if self.mode is RuntimeMode.BACKTEST:
            return False
        return explicitly_approved

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


_MODE_CAPABILITIES: dict[RuntimeMode, frozenset[Capability]] = {
    RuntimeMode.PAPER: frozenset(
        {
            Capability.WALL_CLOCK,
            Capability.OMS_EGRESS,
            Capability.MANAGER_SIGNAL,
            Capability.DIRECT_HISTORICAL_LOOKBACK,
        }
    ),
    RuntimeMode.LIVE: frozenset(
        {
            Capability.WALL_CLOCK,
            Capability.OMS_EGRESS,
            Capability.MANAGER_SIGNAL,
            Capability.DIRECT_HISTORICAL_LOOKBACK,
        }
    ),
    RuntimeMode.BACKTEST: frozenset(
        {
            Capability.SIMULATED_CLOCK,
            Capability.MANAGER_SIGNAL,
            Capability.REPLAY_INGRESS,
            Capability.OMS_EGRESS,
        }
    ),
}

_MODE_ALLOWED_DEPENDENCIES: Final[dict[RuntimeMode, frozenset[str]]] = {
    RuntimeMode.PAPER: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER.value,
            ServiceTarget.ORDER_MANAGEMENT_SERVICE.value,
            ServiceTarget.MARKET_DATA_SERVICE.value,
            ServiceTarget.HISTORICAL_DATA_SERVICE.value,
            DependencyAccessPattern.PAPER_LIVE_ORDER_INTENT.value,
            DependencyAccessPattern.RISK_ORDER_INTENT_EGRESS.value,
            DependencyAccessPattern.HISTORICAL_BOUNDED_WINDOW.value,
        }
    ),
    RuntimeMode.LIVE: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER.value,
            ServiceTarget.ORDER_MANAGEMENT_SERVICE.value,
            ServiceTarget.MARKET_DATA_SERVICE.value,
            ServiceTarget.HISTORICAL_DATA_SERVICE.value,
            DependencyAccessPattern.PAPER_LIVE_ORDER_INTENT.value,
            DependencyAccessPattern.RISK_ORDER_INTENT_EGRESS.value,
            DependencyAccessPattern.HISTORICAL_BOUNDED_WINDOW.value,
        }
    ),
    RuntimeMode.BACKTEST: frozenset(
        {
            ServiceTarget.STRATEGY_RUNTIME_MANAGER.value,
            ServiceTarget.BACKTEST_SERVICE.value,
            DependencyAccessPattern.BACKTEST_ORDER_INTENT.value,
            DependencyAccessPattern.BACKTEST_REPLAY_CONTEXT.value,
            DependencyAccessPattern.RISK_ORDER_INTENT_EGRESS.value,
        }
    ),
}

_ORDER_INTENT_ROUTE: Final[dict[RuntimeMode, ServiceTarget]] = {
    RuntimeMode.PAPER: ServiceTarget.RISK_SERVICE,
    RuntimeMode.LIVE: ServiceTarget.RISK_SERVICE,
    RuntimeMode.BACKTEST: ServiceTarget.RISK_SERVICE,
}

_MODE_BLOCKED_EVENTS: Final[dict[RuntimeMode, frozenset[str]]] = {
    RuntimeMode.PAPER: frozenset({"replay.chunk.direct.fetch"}),
    RuntimeMode.LIVE: frozenset({"replay.chunk.direct.fetch"}),
    RuntimeMode.BACKTEST: frozenset({"replay.chunk.direct.fetch"}),
}


def get_mode_policy(mode: RuntimeMode) -> ModePolicy:
    normalized_mode = mode
    if isinstance(mode, str):
        try:
            normalized_mode = RuntimeMode(mode)
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
