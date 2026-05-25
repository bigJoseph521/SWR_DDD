from __future__ import annotations

from datetime import datetime, timezone

import grpc
import pytest
from runtime.transport.grpc.serializers import (
    manager_worker_pb2,
    manager_worker_pb2_grpc,
)
from runtime.transport.grpc.server import build_grpc_server


class _CreateFlowApp:
    def __init__(self) -> None:
        self.create_calls: list[object] = []
        self.stop_calls: list[object] = []

    def create_worker(self, command: object) -> dict[str, object]:
        self.create_calls.append(command)
        return {
            "accepted": True,
            "runtime_id": getattr(command, "launch_spec").runtime_id,
            "worker_identity": "worker-created",
            "local_state": "STARTING",
            "reason_code": "",
            "accepted_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
        }

    def stop_worker(self, command: object) -> dict[str, object]:
        self.stop_calls.append(command)
        return {
            "accepted": True,
            "runtime_id": "rt-unused",
            "worker_identity": "worker-unused",
            "local_state": "STOPPING",
            "reason_code": "",
            "accepted_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
        }

    def check_health(self, command: object) -> dict[str, object]:
        return {"state": "RUNNING"}


def test_worker_create_flow_is_thin_delegation_and_enforces_auth() -> None:
    app = _CreateFlowApp()
    runtime = build_grpc_server(application=app, bind_address="127.0.0.1:0")
    runtime.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{runtime.port}")
    stub = manager_worker_pb2_grpc.WorkerControlServiceStub(channel)

    request = manager_worker_pb2.WorkerCreateRequest(
        launch_spec=manager_worker_pb2.LaunchSpec(
            correlation_id="corr-create",
            tenant_id="tenant-1",
            account_id="acct-1",
            runtime_id="rt-create-1",
            launch_attempt=5,
            strategy_version_id="sv-1",
            mode=manager_worker_pb2.PAPER,
            validated_parameter_identity="vp-1",
            artifact_reference="artifact://strategy",
            artifact_uri="artifact://strategy",
            artifact_digest="sha256:abc",
            entrypoint="strategy.main:run",
            requested_by="runtime-manager",
        )
    )

    response = stub.CreateWorker(
        request,
        metadata=[
            ("x-internal-caller", "runtime-manager"),
            ("x-internal-trust-class", "internal:runtimes:substrate"),
            ("correlation-id", "corr-create"),
        ],
    )
    assert response.accepted is True
    assert response.runtime_id == "rt-create-1"
    assert response.local_state == "STARTING"
    assert len(app.create_calls) == 1
    assert len(app.stop_calls) == 0
    command = app.create_calls[0]
    assert getattr(command, "launch_spec").runtime_id == "rt-create-1"
    assert getattr(command, "launch_spec").launch_attempt == 5

    with pytest.raises(grpc.RpcError) as denied:
        stub.CreateWorker(
            request,
            metadata=[
                ("x-internal-caller", "runtime-manager"),
                ("x-internal-trust-class", "internal:runtimes:control"),
            ],
        )
    assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert len(app.create_calls) == 1

    channel.close()
    runtime.stop(0)
