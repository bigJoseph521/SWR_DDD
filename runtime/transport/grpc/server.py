from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Sequence

import grpc
from runtime.transport.grpc.interceptors import (
    AuthServerInterceptor,
    CorrelationMetadataServerInterceptor,
    ErrorEnvelopeServerInterceptor,
)
from runtime.transport.grpc.manager_service import (
    WorkerControlApplication,
    WorkerControlService,
)
from runtime.transport.grpc.serializers import manager_worker_pb2_grpc

try:
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc
except Exception:  # pragma: no cover - optional dependency
    health = None
    health_pb2 = None
    health_pb2_grpc = None


@dataclass(slots=True)
class GrpcServerRuntime:
    server: grpc.Server
    bind_address: str
    port: int

    def start(self) -> None:
        self.server.start()

    def wait_for_termination(self, timeout: float | None = None) -> bool:
        return self.server.wait_for_termination(timeout=timeout)

    def stop(self, grace_seconds: float = 5.0) -> None:
        self.server.stop(grace_seconds).wait()


def build_grpc_server_first_available(
    *,
    application: WorkerControlApplication,
    bind_candidates: Sequence[str],
    max_workers: int = 8,
    interceptors: Sequence[grpc.ServerInterceptor] | None = None,
) -> GrpcServerRuntime:
    """
    Try each ``host:port`` until ``add_insecure_port`` succeeds.

    gRPC raises :class:`RuntimeError` on bind failure (port in use, Windows excluded
    ranges, etc.) instead of returning 0; this helper builds a fresh server per candidate.
    """
    last_error: RuntimeError | None = None
    for bind_address in bind_candidates:
        addr = bind_address.strip()
        if not addr:
            continue
        try:
            return build_grpc_server(
                application=application,
                bind_address=addr,
                max_workers=max_workers,
                interceptors=interceptors,
            )
        except RuntimeError as exc:
            last_error = exc
            continue
    if last_error is not None:
        tried = ", ".join(c.strip() for c in bind_candidates if c.strip())
        raise RuntimeError(
            f"worker control gRPC: could not bind to any of [{tried}]; last error: {last_error}"
        ) from last_error
    raise RuntimeError("worker control gRPC: no bind addresses provided")


def build_grpc_server(
    *,
    application: WorkerControlApplication,
    bind_address: str = "127.0.0.1:50051",
    max_workers: int = 8,
    interceptors: Sequence[grpc.ServerInterceptor] | None = None,
) -> GrpcServerRuntime:
    configured_interceptors = tuple(
        interceptors
        or (
            ErrorEnvelopeServerInterceptor(),
            CorrelationMetadataServerInterceptor(),
            AuthServerInterceptor(),
        )
    )
    server = grpc.server(
        ThreadPoolExecutor(max_workers=max_workers),
        interceptors=configured_interceptors,
    )
    manager_worker_pb2_grpc.add_WorkerControlServiceServicer_to_server(
        WorkerControlService(application),
        server,
    )
    _register_health_service(server)
    port = server.add_insecure_port(bind_address)
    return GrpcServerRuntime(server=server, bind_address=bind_address, port=port)


def _register_health_service(server: grpc.Server) -> None:
    if health is None or health_pb2_grpc is None or health_pb2 is None:
        return
    servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    servicer.set("", health_pb2.HealthCheckResponse.SERVING)


def run_grpc_server_forever(runtime: GrpcServerRuntime) -> None:
    runtime.start()
    runtime.wait_for_termination()
