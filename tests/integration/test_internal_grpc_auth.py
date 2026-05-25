from __future__ import annotations

import json
from datetime import datetime, timezone

import grpc
import pytest
from typing import Any
from runtime.transport.grpc.serializers import (
    manager_worker_pb2,
    manager_worker_pb2_grpc,
)
from runtime.transport.grpc.server import build_grpc_server


class _FakeApp:
    def create_worker(self, command: object) -> dict[str, object]:
        return {
            "accepted": True,
            "runtime_id": getattr(command, "launch_spec").runtime_id,
            "worker_identity": "worker-create",
            "local_state": "STARTING",
            "reason_code": "",
            "accepted_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
        }

    def stop_worker(self, command: object) -> dict[str, object]:
        return {
            "accepted": True,
            "runtime_id": getattr(command, "runtime_id"),
            "worker_identity": "worker-stop",
            "local_state": "STOPPING",
            "reason_code": "OPERATOR_REQUESTED",
            "accepted_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
        }

    def check_health(self, command: object) -> dict[str, object]:
        return {"state": "RUNNING"}


@pytest.fixture()
def _stub() -> Any:
    app = _FakeApp()
    runtime = build_grpc_server(application=app, bind_address="127.0.0.1:0")
    runtime.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{runtime.port}")
    stub = manager_worker_pb2_grpc.WorkerControlServiceStub(channel)
    try:
        yield stub
    finally:
        channel.close()
        runtime.stop(0)


def _create_request() -> object:
    return manager_worker_pb2.WorkerCreateRequest(
        launch_spec=manager_worker_pb2.LaunchSpec(
            correlation_id="corr-1",
            tenant_id="tenant-1",
            account_id="acct-1",
            runtime_id="rt-1",
            launch_attempt=1,
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


def _stop_request() -> object:
    return manager_worker_pb2.StopWorkerRequest(runtime_id="rt-1")


def _health_request() -> object:
    return manager_worker_pb2.HealthCheckRequest(
        runtime_id="rt-1",
        launch_attempt=1,
    )


def _meta(caller: str | None, trust_class: str | None) -> list[tuple[str, str]]:
    out = [("correlation-id", "corr-test")]
    if caller is not None:
        out.append(("x-internal-caller", caller))
    if trust_class is not None:
        out.append(("x-internal-trust-class", trust_class))
    return out


def test_create_stop_health_auth_matrix(
    _stub: Any,
) -> None:
    create_ok = _stub.CreateWorker(
        _create_request(),
        metadata=_meta("runtime-manager", "internal:runtimes:substrate"),
    )
    assert create_ok.accepted is True

    with pytest.raises(grpc.RpcError) as create_deny:
        _stub.CreateWorker(
            _create_request(),
            metadata=_meta("runtime-manager", "internal:runtimes:control"),
        )
    assert create_deny.value.code() == grpc.StatusCode.PERMISSION_DENIED

    stop_ok = _stub.StopWorker(
        _stop_request(),
        metadata=_meta("runtime-manager", "internal:runtimes:control"),
    )
    assert stop_ok.accepted is True

    with pytest.raises(grpc.RpcError) as stop_deny:
        _stub.StopWorker(
            _stop_request(),
            metadata=_meta("runtime-manager", "internal:runtimes:substrate"),
        )
    assert stop_deny.value.code() == grpc.StatusCode.PERMISSION_DENIED

    health_ok = _stub.CheckHealth(
        _health_request(),
        metadata=_meta("runtime-manager", "internal:runtimes:substrate"),
    )
    assert health_ok.state == "RUNNING"

    with pytest.raises(grpc.RpcError) as missing:
        _stub.CheckHealth(_health_request(), metadata=_meta(None, None))
    assert missing.value.code() == grpc.StatusCode.PERMISSION_DENIED
    payload = json.loads(missing.value.details())
    assert payload["error"]["code"] == "WORKER_INTERNAL_CALLER_NOT_ALLOWED"
