from __future__ import annotations

from typing import Any, Callable

from runtime.application.runtime_dependencies import RuntimeDependencies
from runtime.domain.enums import WorkerMode
from runtime.domain.errors import UnsupportedDependencyExpansion
from runtime.domain.events.event_envelope import LifecycleEventEnvelope
from runtime.domain.policies.mode_policy import Capability, get_mode_policy
from runtime.infrastructure.clock.clock import SimulatedClock, build_clock
from runtime.infrastructure.grpc.replay.replay_gateway import ReplayGateway
from runtime.infrastructure.grpc.risk_order_intent_gateway import RiskOrderIntentGateway
from runtime.infrastructure.http.srm.manager_gateway import ManagerGateway


def build_runtime_dependencies(
    mode: WorkerMode,
    *,
    manager_client: object,
    runtime_identity: object,
    risk_order_intent_client: object | None = None,
    on_first_data: Callable[[], None] | None = None,
    on_lifecycle_event: Callable[[LifecycleEventEnvelope], None] | None = None,
    replay_tick_logging_quiet: bool = False,
    heartbeat_log_enabled: bool = False,
    on_risk_order_intent_result: (
        Callable[[str, dict[str, Any], dict[str, Any]], None] | None
    ) = None,
    # Deprecated parameter names (compatibility).
    oms_client: object | None = None,
    on_oms_order_intent_result: (
        Callable[[str, dict[str, Any], dict[str, Any]], None] | None
    ) = None,
) -> RuntimeDependencies:
    effective_risk_client = (
        risk_order_intent_client if risk_order_intent_client is not None else oms_client
    )
    effective_on_result = (
        on_risk_order_intent_result
        if on_risk_order_intent_result is not None
        else on_oms_order_intent_result
    )

    policy = get_mode_policy(mode)

    if (
        effective_risk_client is not None
        and Capability.RISK_ORDER_INTENT_EGRESS not in policy.allowed
    ):
        raise UnsupportedDependencyExpansion(
            mode=policy.mode.value,
            dependency="risk_order_intent_client",
            capability=Capability.RISK_ORDER_INTENT_EGRESS.value,
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

    risk_order_intent = (
        RiskOrderIntentGateway(
            policy,
            effective_risk_client,
            on_order_intent_result=effective_on_result,
        )
        if effective_risk_client is not None
        else None
    )

    simulated_clock = clock if isinstance(clock, SimulatedClock) else None
    replay = (
        ReplayGateway(
            policy,
            simulated_clock=simulated_clock,
            on_first_data=on_first_data,
            tick_logging_quiet=replay_tick_logging_quiet,
        )
        if mode is WorkerMode.BACKTEST
        else None
    )

    deps = RuntimeDependencies(
        clock=clock,
        manager=manager,
        risk_order_intent=risk_order_intent,
        replay=replay,
    )
    validate_runtime_dependencies(deps, policy.mode)
    return deps


def validate_runtime_dependencies(deps: RuntimeDependencies, mode: WorkerMode) -> None:
    if mode is WorkerMode.BACKTEST:
        return

    if mode in (WorkerMode.PAPER, WorkerMode.LIVE):
        if deps.replay is not None:
            raise UnsupportedDependencyExpansion(
                mode=mode.value,
                dependency="replay_gateway",
                capability=Capability.REPLAY_INGRESS.value,
                reason="paper_live_cannot_own_replay_gateway",
            )
