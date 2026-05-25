from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import google.protobuf.empty_pb2  # noqa: F401 — register empty.proto before generated *_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.validator import LaunchSpecValidator


def _ensure_generated_proto_path() -> Path:
    generated_dir = Path(__file__).resolve().parents[3] / "protos" / "generated"
    generated_dir_str = str(generated_dir)
    if generated_dir_str not in sys.path:
        sys.path.insert(0, generated_dir_str)
    return generated_dir


_ensure_generated_proto_path()
manager_worker_pb2 = importlib.import_module("manager_worker_pb2")
manager_worker_pb2_grpc = importlib.import_module("manager_worker_pb2_grpc")
risk_worker_pb2 = importlib.import_module("risk_worker_pb2")
risk_worker_pb2_grpc = importlib.import_module("risk_worker_pb2_grpc")
replay_worker_pb2 = importlib.import_module("replay_worker_pb2")
replay_worker_pb2_grpc = importlib.import_module("replay_worker_pb2_grpc")


def _proto_ts_from_datetime(value: datetime | None) -> Any:
    ts = Timestamp()
    if value is None:
        return ts
    ts.FromDatetime(value.astimezone(timezone.utc))
    return ts


def _datetime_from_proto_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if not value.ListFields():
        return None
    return value.ToDatetime().astimezone(timezone.utc)


def _runtime_mode_to_domain(mode: int) -> str:
    mapping = {
        manager_worker_pb2.PAPER: "PAPER",
        manager_worker_pb2.LIVE: "LIVE",
        manager_worker_pb2.BACKTEST: "BACKTEST",
    }
    return mapping.get(mode, "PAPER")


def _read(mapping_or_obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(mapping_or_obj, Mapping):
        return mapping_or_obj.get(key, default)
    return getattr(mapping_or_obj, key, default)


def _wire_reason_or_error_code(value: Any) -> str:
    return str(value or "").strip().upper()


@dataclass(frozen=True, slots=True)
class CreateWorkerCommand:
    launch_spec: LaunchSpec
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class StopWorkerCommand:
    runtime_id: str


@dataclass(frozen=True, slots=True)
class HealthCheckCommand:
    runtime_id: str
    metadata: Mapping[str, Any]


class WorkerControlSerializer:
    def __init__(
        self, *, launch_spec_validator: LaunchSpecValidator | None = None
    ) -> None:
        self._launch_spec_validator = launch_spec_validator or LaunchSpecValidator()

    def create_request_to_command(self, request: Any) -> CreateWorkerCommand:
        launch_spec_msg = request.launch_spec
        payload: dict[str, Any] = {
            "runtime_id": launch_spec_msg.runtime_id or None,
            "tenant_id": launch_spec_msg.tenant_id or None,
            "strategy_version_id": launch_spec_msg.strategy_version_id or None,
            "mode": _runtime_mode_to_domain(launch_spec_msg.mode),
            "artifact_digest": launch_spec_msg.artifact_digest,
            "entrypoint": launch_spec_msg.entrypoint,
            "launch_attempt": int(launch_spec_msg.launch_attempt),
        }
        optionals = {
            "trader_id": launch_spec_msg.trader_id or None,
            "account_id": launch_spec_msg.account_id or None,
            "artifact_uri": launch_spec_msg.artifact_uri or None,
        }
        for key, value in optionals.items():
            if value is not None:
                payload[key] = value
        wire_job = str(launch_spec_msg.job_id or "").strip()
        if wire_job:
            payload["job_id"] = wire_job
        launch_spec = self._launch_spec_validator.validate(payload)
        metadata = {
            "runtime_id": launch_spec_msg.runtime_id or None,
            "tenant_id": launch_spec_msg.tenant_id or None,
            "account_id": launch_spec_msg.account_id or None,
            "worker_identity": launch_spec_msg.worker_identity or None,
            "launch_attempt": int(launch_spec_msg.launch_attempt),
            "strategy_version_id": launch_spec_msg.strategy_version_id or None,
            "job_id": launch_spec.job_id,
            "correlation_id": launch_spec_msg.correlation_id or None,
            "causation_id": launch_spec_msg.causation_id or None,
            "deployment_id": launch_spec_msg.deployment_id or None,
        }
        return CreateWorkerCommand(launch_spec=launch_spec, metadata=metadata)

    def stop_request_to_command(self, request: Any) -> StopWorkerCommand:
        return StopWorkerCommand(
            runtime_id=str(getattr(request, "runtime_id", "") or "")
        )

    def health_request_to_command(self, request: Any) -> HealthCheckCommand:
        metadata = {
            "runtime_id": request.runtime_id or None,
            "tenant_id": request.tenant_id or None,
            "account_id": request.account_id or None,
            "worker_identity": request.worker_identity or None,
            "launch_attempt": int(request.launch_attempt),
            "strategy_version_id": request.strategy_version_id or None,
            "job_id": str(getattr(request, "job_id", "") or "").strip() or None,
            "correlation_id": request.correlation_id or None,
            "causation_id": request.causation_id or None,
            "deployment_id": request.deployment_id or None,
        }
        return HealthCheckCommand(
            runtime_id=request.runtime_id or "", metadata=metadata
        )

    def create_result_to_response(self, result: Any) -> Any:
        accepted_at = _read(result, "accepted_at")
        return manager_worker_pb2.WorkerCreateResponse(
            accepted=bool(_read(result, "accepted", False)),
            runtime_id=str(_read(result, "runtime_id", "") or ""),
            worker_identity=str(_read(result, "worker_identity", "") or ""),
            local_state=str(_read(result, "local_state", "") or ""),
            reason_code=_wire_reason_or_error_code(_read(result, "reason_code", "")),
            accepted_at=_proto_ts_from_datetime(accepted_at),
        )

    def stop_result_to_response(self, result: Any) -> Any:
        accepted_at = _read(result, "accepted_at")
        if not isinstance(accepted_at, datetime):
            accepted_at = datetime.now(timezone.utc)
        return manager_worker_pb2.StopWorkerResponse(
            accepted=bool(_read(result, "accepted", False)),
            reason_code=_wire_reason_or_error_code(_read(result, "reason_code", "")),
            local_state=str(_read(result, "local_state", "") or ""),
            message=str(_read(result, "message", "") or ""),
            accepted_at=_proto_ts_from_datetime(accepted_at),
        )

    def health_result_to_response(self, result: Any) -> Any:
        return manager_worker_pb2.HealthCheckResponse(
            state=str(_read(result, "state", "") or ""),
        )


__all__ = [
    "CreateWorkerCommand",
    "HealthCheckCommand",
    "StopWorkerCommand",
    "WorkerControlSerializer",
    "_datetime_from_proto_ts",
    "_proto_ts_from_datetime",
    "manager_worker_pb2",
    "manager_worker_pb2_grpc",
    "risk_worker_pb2",
    "risk_worker_pb2_grpc",
    "replay_worker_pb2",
    "replay_worker_pb2_grpc",
]
