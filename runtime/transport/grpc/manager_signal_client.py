from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import grpc
import google.protobuf.empty_pb2  # noqa: F401 — register empty.proto before generated *_pb2
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
)


def _ensure_generated_proto_path() -> Path:
    generated_dir = Path(__file__).resolve().parents[4] / "protos" / "generated"
    generated_dir_str = str(generated_dir)
    if generated_dir_str not in sys.path:
        sys.path.insert(0, generated_dir_str)
    return generated_dir


_ensure_generated_proto_path()
manager_worker_pb2 = importlib.import_module("manager_worker_pb2")
manager_worker_pb2_grpc = importlib.import_module("manager_worker_pb2_grpc")


def _to_proto_ts(value: Any) -> Timestamp:
    ts = Timestamp()
    if not isinstance(value, datetime):
        return ts
    candidate = (
        value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    )
    ts.FromDatetime(candidate.astimezone(timezone.utc))
    return ts


def _struct_from_mapping(value: Mapping[str, Any] | None) -> Struct:
    struct = Struct()
    if value is None:
        return struct
    struct.update(_normalize_for_struct(dict(value)))
    return struct


def _normalize_for_struct(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, datetime):
        candidate = (
            value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        )
        return candidate.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            normalized[str(key)] = _normalize_for_struct(item)
        return normalized
    if isinstance(value, (list, tuple, set)):
        return [_normalize_for_struct(item) for item in value]
    return str(value)


def _event_fields(
    identity: Mapping[str, Any], envelope: Mapping[str, Any]
) -> dict[str, Any]:
    launch_attempt = int(identity.get("launch_attempt") or 0)
    return {
        "event_id": str(envelope.get("event_id") or ""),
        "event_name": str(envelope.get("event_name") or ""),
        "event_version": int(envelope.get("event_version") or 1),
        "producer": str(envelope.get("producer") or "strategy-worker-runtime"),
        "correlation_id": str(envelope.get("correlation_id") or ""),
        "causation_id": str(envelope.get("causation_id") or ""),
        "tenant_id": str(identity.get("tenant_id") or ""),
        "account_id": str(identity.get("account_id") or ""),
        "runtime_id": str(identity.get("runtime_id") or ""),
        "worker_identity": str(identity.get("worker_identity") or ""),
        "launch_attempt": launch_attempt if launch_attempt > 0 else 0,
        "strategy_version_id": str(identity.get("strategy_version_id") or ""),
    }


class GrpcManagerSignalClient:
    def __init__(self, target: str) -> None:
        self._target = target.strip()
        self._channel = grpc.insecure_channel(self._target)
        self._stub = manager_worker_pb2_grpc.WorkerLifecycleSignalServiceStub(
            self._channel
        )

    def close(self) -> None:
        # Lifecycle rollback can emit manager signals after invoking closeables.
        # Keep channel alive for the process lifetime to avoid dropping failure signals.
        return None

    def emit_signal(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        signal_type = str(envelope.get("signal_type") or "")
        identity = envelope.get("identity")
        payload = envelope.get("payload")
        if not isinstance(identity, Mapping):
            raise ValueError("signal envelope must include identity mapping")
        if not isinstance(payload, Mapping):
            raise ValueError("signal envelope must include payload mapping")

        if signal_type == "heartbeat":
            return {
                "accepted": False,
                "signal_type": signal_type,
                "reason_code": "HEARTBEAT_USE_HTTP",
                "error_code": WorkerErrorCode.INTERNAL_ERROR.value,
                "details": (
                    "SRM heartbeats use HTTP only "
                    "(STRATEGY_RUNTIME_MANAGER_BASE_URL); gRPC is not supported."
                ),
            }

        event_fields = _event_fields(identity, envelope)
        occurred_at = _to_proto_ts(
            envelope.get("occurred_at") or payload.get("occurred_at")
        )

        try:
            if signal_type == "bootstrap_succeeded":
                request = manager_worker_pb2.LaunchSucceeded(
                    **event_fields,
                    occurred_at=occurred_at,
                    payload=_struct_from_mapping(payload),
                )
                self._stub.ReportLaunchSucceeded(request)
                return {"accepted": True, "signal_type": signal_type}

            if signal_type == "bootstrap_failed":
                request = manager_worker_pb2.LaunchFailed(
                    **event_fields,
                    occurred_at=occurred_at,
                    payload=_struct_from_mapping(payload),
                )
                self._stub.ReportLaunchFailed(request)
                return {"accepted": True, "signal_type": signal_type}

            if signal_type == "unhealthy":
                request = manager_worker_pb2.RuntimeDegradedSignal(
                    **event_fields,
                    occurred_at=occurred_at,
                    payload=_struct_from_mapping(payload),
                )
                self._stub.ReportDegraded(request)
                return {"accepted": True, "signal_type": signal_type}

            if signal_type == "terminated":
                request = manager_worker_pb2.TerminationReported(
                    **event_fields,
                    occurred_at=occurred_at,
                    payload=_struct_from_mapping(payload),
                )
                self._stub.ReportTermination(request)
                return {"accepted": True, "signal_type": signal_type}
        except grpc.RpcError as exc:
            return {
                "accepted": False,
                "signal_type": signal_type,
                "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
                "error_code": WorkerErrorCode.DEPENDENCY_UNHEALTHY.value,
                "details": str(exc),
            }

        return {
            "accepted": False,
            "signal_type": signal_type,
            "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
            "error_code": WorkerErrorCode.INTERNAL_ERROR.value,
        }


class _NoopManagerClient:
    def emit_signal(self, payload: dict[str, object]) -> dict[str, object]:
        return {"accepted": True, "signal_type": payload.get("signal_type")}

    def close(self) -> None:
        return None


def build_manager_client(target: str) -> Any:
    """Backward-compatible gRPC-only builder (tests). Prefer ``transport.manager_client``."""
    from runtime.transport.manager_client import build_manager_client as build

    return build(grpc_target=target)
