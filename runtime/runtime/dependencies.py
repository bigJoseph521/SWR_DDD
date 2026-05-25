from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import UnsupportedDependencyExpansion
from runtime.integration.clock import Clock, SimulatedClock, build_clock
from runtime.integration.manager_gateway import ManagerGateway
from runtime.integration.oms_gateway import OmsGateway
from runtime.integration.replay_gateway import ReplayGateway
from runtime.events.event_envelope import LifecycleEventEnvelope
from runtime.runtime.mode_policy import Capability, get_mode_policy


@dataclass(frozen=True, slots=True)
class RuntimeDependencies:
    clock: Clock
    manager: ManagerGateway
    oms: OmsGateway | None
    replay: ReplayGateway | None


def build_runtime_dependencies(
    mode: RuntimeMode,
    *,
    manager_client: object,
    runtime_identity: object,
    oms_client: object | None = None,
    replay_client: object | None = None,
    on_first_data: Callable[[], None] | None = None,
    on_lifecycle_event: Callable[[LifecycleEventEnvelope], None] | None = None,
    replay_tick_logging_quiet: bool = False,
    heartbeat_log_enabled: bool = False,
    on_oms_order_intent_result: (
        Callable[[str, dict[str, Any], dict[str, Any]], None] | None
    ) = None,
) -> RuntimeDependencies:
    policy = get_mode_policy(mode)

    if oms_client is not None and Capability.OMS_EGRESS not in policy.allowed:
        raise UnsupportedDependencyExpansion(
            mode=policy.mode.value,
            dependency="oms_client",
            capability=Capability.OMS_EGRESS.value,
            reason="dependency_not_allowed_for_mode",
        )

    if replay_client is not None and Capability.REPLAY_INGRESS not in policy.allowed:
        raise UnsupportedDependencyExpansion(
            mode=policy.mode.value,
            dependency="replay_client",
            capability=Capability.REPLAY_INGRESS.value,
            reason="dependency_not_allowed_for_mode",
        )

    clock = build_clock(policy)
    manager = ManagerGateway(
        policy,
        manager_client,
        runtime_identity,
        on_event_emitted=on_lifecycle_event,
        heartbeat_log_enabled=heartbeat_log_enabled,
    )

    oms = (
        OmsGateway(
            policy,
            oms_client,
            on_order_intent_result=on_oms_order_intent_result,
        )
        if oms_client is not None
        else None
    )

    simulated_clock = clock if isinstance(clock, SimulatedClock) else None
    replay = (
        ReplayGateway(
            policy,
            replay_client,
            simulated_clock=simulated_clock,
            on_first_data=on_first_data,
            tick_logging_quiet=replay_tick_logging_quiet,
        )
        if replay_client is not None
        else None
    )

    deps = RuntimeDependencies(clock=clock, manager=manager, oms=oms, replay=replay)
    validate_runtime_dependencies(deps, policy.mode)
    return deps


def validate_runtime_dependencies(deps: RuntimeDependencies, mode: RuntimeMode) -> None:
    if mode is RuntimeMode.BACKTEST:
        return

    if mode in (RuntimeMode.PAPER, RuntimeMode.LIVE):
        if deps.replay is not None:
            raise UnsupportedDependencyExpansion(
                mode=mode.value,
                dependency="replay_gateway",
                capability=Capability.REPLAY_INGRESS.value,
                reason="paper_live_cannot_own_replay_gateway",
            )
