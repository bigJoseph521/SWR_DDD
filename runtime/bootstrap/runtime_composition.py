from __future__ import annotations

from dataclasses import dataclass

from runtime.bootstrap.dependency_container import (
    DependencyContainer,
    build_dependency_container,
)
from runtime.infrastructure.config.settings import Settings
from runtime.infrastructure.grpc.risk_order_intent_client import (
    build_risk_order_intent_grpc_client,
)
from runtime.infrastructure.http.srm.manager_client import build_manager_client


@dataclass(frozen=True, slots=True)
class RuntimeOutboundClients:
    """Outbound clients wired at bootstrap.

    - ``manager_client``: strategy-runtime-manager over **HTTP** (heartbeats, lifecycle signals)
    - ``risk_order_intent_client``: Risk Service order intents over **gRPC**
    """

    manager_client: object
    risk_order_intent_client: object | None


def build_runtime_outbound_clients(settings: Settings) -> RuntimeOutboundClients:
    """Construct outbound SRM (HTTP) and Risk (gRPC) clients from :class:`Settings`."""
    manager_client = build_manager_client(
        srm_base_url=settings.strategy_runtime_manager_base_url,
        owner_resource_id=settings.deployment_id,
        heartbeat_timeout_seconds=settings.runtime_manager_heartbeat_timeout_seconds,
    )
    risk_order_intent_client = build_risk_order_intent_grpc_client(
        target=settings.risk_grpc_target,
        timeout_seconds=settings.risk_grpc_timeout_seconds,
    )
    return RuntimeOutboundClients(
        manager_client=manager_client,
        risk_order_intent_client=risk_order_intent_client,
    )


def build_runtime_container(
    settings: Settings,
    *,
    manager_client: object | None = None,
    risk_order_intent_client: object | None = None,
) -> DependencyContainer:
    """Wire :class:`DependencyContainer` from settings and optional client overrides."""
    clients = build_runtime_outbound_clients(settings)
    return build_dependency_container(
        settings,
        manager_client=manager_client or clients.manager_client,
        risk_order_intent_client=(
            risk_order_intent_client or clients.risk_order_intent_client
        ),
    )
