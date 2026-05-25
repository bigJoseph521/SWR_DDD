from __future__ import annotations

from datetime import datetime, timezone

import grpc
import pytest
from runtime.transport.grpc.serializers import (
    manager_worker_pb2,
    manager_worker_pb2_grpc,
)
from runtime.transport.grpc.server import build_grpc_server


class _StopFlowApp:
    def __init__(self) -> None:
        self.stop_calls: list[object] = []
        self.create_calls: list[object] = []

    def create_worker(self, command: object) -> dict[str, object]:
        self.create_calls.append(command)
        return {
            "accepted": True,
            "runtime_id": "rt-unused",
            "worker_identity": "worker-unused",
            "local_state": "STARTING",
            "reason_code": "",
            "accepted_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
        }

    def stop_worker(self, command: object) -> dict[str, object]:
        self.stop_calls.append(command)
        return {
            "accepted": True,
            "runtime_id": getattr(command, "runtime_id"),
            "worker_identity": "worker-stopped",
            "local_state": "STOPPING",
            "reason_code": "STOP_REQUESTED",
            "accepted_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
        }

    def check_health(self, command: object) -> dict[str, object]:
        return {"state": "RUNNING"}


def test_worker_stop_flow_is_thin_delegation_and_enforces_auth() -> None:
    app = _StopFlowApp()
    runtime = build_grpc_server(application=app, bind_address="127.0.0.1:0")
    runtime.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{runtime.port}")
    stub = manager_worker_pb2_grpc.WorkerControlServiceStub(channel)

    request = manager_worker_pb2.StopWorkerRequest(runtime_id="rt-stop-1")
    response = stub.StopWorker(
        request,
        metadata=[
            ("x-internal-caller", "runtime-manager"),
            ("x-internal-trust-class", "internal:runtimes:control"),
            ("correlation-id", "corr-stop"),
        ],
    )
    assert response.accepted is True
    assert response.local_state == "STOPPING"
    assert len(app.stop_calls) == 1
    assert len(app.create_calls) == 0
    command = app.stop_calls[0]
    assert getattr(command, "runtime_id") == "rt-stop-1"

    with pytest.raises(grpc.RpcError) as denied:
        stub.StopWorker(
            request,
            metadata=[
                ("x-internal-caller", "runtime-manager"),
                ("x-internal-trust-class", "internal:runtimes:substrate"),
            ],
        )
    assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert len(app.stop_calls) == 1

    channel.close()
    runtime.stop(0)
