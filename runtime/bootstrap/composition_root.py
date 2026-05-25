"""Composition root — re-exports bootstrap wiring helpers."""

from runtime.bootstrap.runtime_composition import (
    RuntimeGrpcClients,
    build_runtime_container,
    build_runtime_grpc_clients,
)

__all__ = [
    "RuntimeGrpcClients",
    "build_runtime_container",
    "build_runtime_grpc_clients",
]
