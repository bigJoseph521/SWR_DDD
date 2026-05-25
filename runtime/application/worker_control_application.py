from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from runtime.application.lifecycle_service import LifecycleService
from runtime.application.worker_app import WorkerApp
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
    normalize_runtime_reason_code,
)
from runtime.transport.grpc.serializers import (
    CreateWorkerCommand,
    HealthCheckCommand,
    StopWorkerCommand,
)


def _canonical_worker_identity(launch_spec: LaunchSpec) -> str:
    return (
        f"{launch_spec.runtime_id}:"
        f"{launch_spec.strategy_version_id}:"
        f"{launch_spec.launch_attempt}"
    )


@dataclass(slots=True)
class WorkerControlApplicationImpl:
    """
    Implements WorkerControlService application hooks (manager → worker gRPC).

    StopWorker must target this process's ``runtime_id`` (same as this worker's launch spec).
    """

    launch_spec: LaunchSpec
    lifecycle_service: LifecycleService
    worker_app: WorkerApp

    def create_worker(self, command: Any) -> dict[str, Any]:
        if not isinstance(command, CreateWorkerCommand):
            raise TypeError("command must be CreateWorkerCommand.")
        now = datetime.now(timezone.utc)
        spec = command.launch_spec
        if spec.runtime_id != self.launch_spec.runtime_id:
            return {
                "accepted": False,
                "runtime_id": self.launch_spec.runtime_id,
                "worker_identity": _canonical_worker_identity(self.launch_spec),
                "local_state": self.lifecycle_service.phase.value,
                "reason_code": normalize_runtime_reason_code("runtime_id_mismatch"),
                "error_code": WorkerErrorCode.INVALID_REQUEST.value,
                "accepted_at": now,
                "message": "CreateWorker rejected: runtime_id does not match this process.",
            }
        if spec.launch_attempt != self.launch_spec.launch_attempt:
            return {
                "accepted": False,
                "runtime_id": self.launch_spec.runtime_id,
                "worker_identity": _canonical_worker_identity(self.launch_spec),
                "local_state": self.lifecycle_service.phase.value,
                "reason_code": normalize_runtime_reason_code("launch_attempt_mismatch"),
                "error_code": WorkerErrorCode.INVALID_REQUEST.value,
                "accepted_at": now,
                "message": "CreateWorker rejected: launch_attempt does not match this process.",
            }
        if spec.strategy_version_id != self.launch_spec.strategy_version_id:
            return {
                "accepted": False,
                "runtime_id": self.launch_spec.runtime_id,
                "worker_identity": _canonical_worker_identity(self.launch_spec),
                "local_state": self.lifecycle_service.phase.value,
                "reason_code": normalize_runtime_reason_code(
                    "strategy_version_id_mismatch"
                ),
                "error_code": WorkerErrorCode.INVALID_REQUEST.value,
                "accepted_at": now,
                "message": "CreateWorker rejected: strategy_version_id does not match this process.",
            }
        return {
            "accepted": True,
            "runtime_id": self.launch_spec.runtime_id,
            "worker_identity": _canonical_worker_identity(self.launch_spec),
            "local_state": self.lifecycle_service.phase.value,
            "reason_code": RuntimeWorkerReasonCode.CONTROLLED_SHUTDOWN_INITIATED.value,
            "accepted_at": now,
            "message": "CreateWorker accepted: launch metadata matches this worker.",
        }

    def stop_worker(self, command: Any) -> dict[str, Any]:
        if not isinstance(command, StopWorkerCommand):
            raise TypeError("command must be StopWorkerCommand.")
        now = datetime.now(timezone.utc)
        local = _canonical_worker_identity(self.launch_spec)
        if not self._stop_command_targets_this_worker(command):
            return {
                "accepted": False,
                "runtime_id": self.launch_spec.runtime_id,
                "worker_identity": local,
                "local_state": self.lifecycle_service.phase.value,
                "reason_code": normalize_runtime_reason_code("identity_mismatch"),
                "error_code": WorkerErrorCode.INVALID_REQUEST.value,
                "accepted_at": now,
                "message": "StopWorker rejected: stop command does not target this worker.",
            }
        # STOPPING status is reported via initiate_stop_from_request (HTTP stop API contract).
        # No runtime.terminated signal: RM already receives StopWorkerResponse on this RPC.
        self.lifecycle_service.record_manager_stop_request_checkpoint()
        self.lifecycle_service.initiate_stop_from_request("STOP_REQUESTED")
        self.worker_app.stop(
            emit_termination_signal=False,
            suppress_stopping_phase_stdout=True,
            termination_reason_code=RuntimeWorkerReasonCode.STOP_REQUESTED.value,
        )
        return {
            "accepted": True,
            "runtime_id": self.launch_spec.runtime_id,
            "worker_identity": local,
            "local_state": self.lifecycle_service.phase.value,
            "reason_code": RuntimeWorkerReasonCode.STOP_REQUESTED.value,
            "accepted_at": now,
            "message": "StopWorker accepted; worker shutdown complete.",
        }

    def check_health(self, command: Any) -> dict[str, Any]:
        if not isinstance(command, HealthCheckCommand):
            raise TypeError("command must be HealthCheckCommand.")
        if command.runtime_id != self.launch_spec.runtime_id:
            return {"state": self.lifecycle_service.phase.value}
        phase = self.lifecycle_service.phase
        return {"state": phase.value}

    def _stop_command_targets_this_worker(self, command: StopWorkerCommand) -> bool:
        return command.runtime_id == self.launch_spec.runtime_id
