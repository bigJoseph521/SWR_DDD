from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from runtime.domain.errors import RuntimeStartValidationFailedError
from runtime.transport.grpc.serializers import (
    WorkerControlSerializer,
    manager_worker_pb2,
)


def _launch_spec(*, runtime_id: str = "rt-1", launch_attempt: int = 1) -> Any:
    return manager_worker_pb2.LaunchSpec(
        correlation_id="corr-1",
        causation_id="cause-1",
        tenant_id="tenant-1",
        account_id="acct-1",
        runtime_id=runtime_id,
        worker_identity="worker-1",
        launch_attempt=launch_attempt,
        strategy_version_id="sv-1",
        deployment_id="dep-1",
        mode=manager_worker_pb2.PAPER,
        validated_parameter_identity="vp-1",
        artifact_reference="artifact://strategy",
        artifact_uri="artifact://strategy",
        artifact_digest="sha256:abc",
        entrypoint="strategy.main:run",
        requested_by="runtime-manager",
    )


def test_create_serializer_maps_request_and_is_deterministic() -> None:
    serializer = WorkerControlSerializer()
    request = manager_worker_pb2.WorkerCreateRequest(launch_spec=_launch_spec())

    first = serializer.create_request_to_command(request)
    second = serializer.create_request_to_command(request)

    assert first.launch_spec.runtime_id == "rt-1"
    assert first.launch_spec.launch_attempt == 1
    assert first.launch_spec.to_dict() == second.launch_spec.to_dict()
    assert first.metadata["correlation_id"] == "corr-1"


def test_create_serializer_rejects_payload_broadening_for_same_key() -> None:
    serializer = WorkerControlSerializer()
    first = manager_worker_pb2.WorkerCreateRequest(
        launch_spec=_launch_spec(runtime_id="rt-2")
    )
    serializer.create_request_to_command(first)

    broadened_spec = _launch_spec(runtime_id="rt-2")
    broadened_spec.entrypoint = "strategy.main:run_v2"
    second = manager_worker_pb2.WorkerCreateRequest(launch_spec=broadened_spec)

    with pytest.raises(RuntimeStartValidationFailedError) as exc_info:
        serializer.create_request_to_command(second)

    assert exc_info.value.details["reason"] == "duplicate_key_payload_mismatch"


def test_stop_and_health_serializers_map_commands_and_responses() -> None:
    serializer = WorkerControlSerializer()
    stop_request = manager_worker_pb2.StopWorkerRequest(runtime_id="rt-9")
    stop_command = serializer.stop_request_to_command(stop_request)
    assert stop_command.runtime_id == "rt-9"

    accepted_at = datetime(2026, 3, 29, 11, 30, tzinfo=timezone.utc)
    stop_response = serializer.stop_result_to_response(
        {
            "accepted": True,
            "runtime_id": "rt-9",
            "worker_identity": "w-9",
            "local_state": "STOPPING",
            "reason_code": "STOP_REQUESTED",
            "accepted_at": accepted_at,
            "message": "ok",
        },
    )
    assert stop_response.accepted is True
    assert stop_response.local_state == "STOPPING"
    assert stop_response.reason_code == "STOP_REQUESTED"
    assert stop_response.message == "ok"
    assert stop_response.accepted_at.seconds == int(accepted_at.timestamp())
    assert stop_response.accepted_at.nanos == 0

    health_response = serializer.health_result_to_response({"state": "STARTING"})
    assert health_response.state == "STARTING"
