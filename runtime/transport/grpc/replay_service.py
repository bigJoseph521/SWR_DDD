from __future__ import annotations

import traceback
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol, cast

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from runtime.transport.grpc.serializers import (
    replay_worker_pb2,
    replay_worker_pb2_grpc,
)


class ReplayIngressApplication(Protocol):
    def push_replay_context(
        self, payload: Mapping[str, object]
    ) -> Mapping[str, object]: ...


def _to_proto_ts(value: object) -> Timestamp:
    ts = Timestamp()
    if isinstance(value, datetime):
        candidate = (
            value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        )
        ts.FromDatetime(candidate.astimezone(timezone.utc))
    return ts


class ReplayIngressService:
    def __init__(self, application: ReplayIngressApplication) -> None:
        self._application = application

    def PushReplayContext(self, request: Any, context: Any) -> Any:  # noqa: N802
        try:
            request_dict = MessageToDict(request, preserving_proto_field_name=True)
            result = self._application.push_replay_context(request_dict)
            return replay_worker_pb2.ReplayAck(
                runtime_id=str(result.get("runtime_id") or ""),
                replay_session_id=str(result.get("replay_session_id") or ""),
                replay_cursor=str(result.get("replay_cursor") or ""),
                consumed_count=int(cast(Any, result.get("consumed_count")) or 0),
                observed_at=_to_proto_ts(result.get("observed_at")),
            )
        except Exception:
            tb = traceback.format_exc()
            traceback.print_exc()
            # Surface the server stack trace to the client (grpc details length is bounded).
            details = tb if len(tb) <= 7000 else f"{tb[:6990]}\n...(truncated)"
            context.abort(grpc.StatusCode.INTERNAL, details)


@dataclass(slots=True)
class ReplayIngressServerRuntime:
    server: grpc.Server
    bind_address: str
    port: int

    def start(self) -> None:
        self.server.start()

    def stop(self, grace_seconds: float = 5.0) -> None:
        self.server.stop(grace_seconds).wait()


def build_replay_ingress_server(
    *,
    application: ReplayIngressApplication,
    bind_address: str = "127.0.0.1:50061",
    max_workers: int = 8,
) -> ReplayIngressServerRuntime:
    server = grpc.server(ThreadPoolExecutor(max_workers=max_workers))
    replay_worker_pb2_grpc.add_ReplayIngressServiceServicer_to_server(
        ReplayIngressService(application),
        server,
    )
    port = server.add_insecure_port(bind_address)
    return ReplayIngressServerRuntime(
        server=server, bind_address=bind_address, port=port
    )


def build_replay_ingress_server_first_available(
    *,
    application: ReplayIngressApplication,
    bind_candidates: Sequence[str],
    max_workers: int = 8,
) -> ReplayIngressServerRuntime:
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
            return build_replay_ingress_server(
                application=application,
                bind_address=addr,
                max_workers=max_workers,
            )
        except RuntimeError as exc:
            last_error = exc
            continue
    if last_error is not None:
        tried = ", ".join(c.strip() for c in bind_candidates if c.strip())
        raise RuntimeError(
            f"replay ingress gRPC: could not bind to any of [{tried}]; last error: {last_error}"
        ) from last_error
    raise RuntimeError("replay ingress gRPC: no bind addresses provided")
