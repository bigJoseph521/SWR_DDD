from __future__ import annotations

from typing import Any, Callable

from runtime.application.runtime_dependencies import RuntimeDependencies
from runtime.domain.enums import WorkerMode
from runtime.domain.events.event_envelope import LifecycleEventEnvelope
from runtime.domain.policies.mode_policy import Capability, get_mode_policy
from runtime.infrastructure.clock.clock import build_clock
from runtime.infrastructure.grpc.risk_order_intent_gateway import RiskOrderIntentGateway
from runtime.infrastructure.http.srm.manager_gateway import ManagerGateway


def build_runtime_dependencies(
    mode: WorkerMode,
    *,
    manager_client: object,
    runtime_identity: object,
    risk_order_intent_client: object | None = None,
    on_lifecycle_event: Callable[[LifecycleEventEnvelope], None] | None = None,
    heartbeat_log_enabled: bool = False,
    on_risk_order_intent_result: (
        Callable[[str, dict[str, Any], dict[str, Any]], None] | None
    ) = None,
) -> RuntimeDependencies:
    effective_risk_client = risk_order_intent_client
    effective_on_result = on_risk_order_intent_result

    policy = get_mode_policy(mode)

    if (
        effective_risk_client is not None
        and Capability.RISK_ORDER_INTENT_EGRESS not in policy.allowed
    ):
        effective_risk_client = None
        effective_on_result = None

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
        and Capability.RISK_ORDER_INTENT_EGRESS in policy.allowed
        else None
    )

    deps = RuntimeDependencies(
        clock=clock,
        manager=manager,
        risk_order_intent=risk_order_intent,
    )
    return deps
