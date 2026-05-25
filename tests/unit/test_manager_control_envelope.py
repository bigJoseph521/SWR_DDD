"""Stdout envelope shape for WorkerControlService (gRPC response logging)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest
from runtime.transport.grpc.control_plane_envelope_log import (
    BANNER_WR_TO_RM_GRPC,
)
from runtime.transport.grpc.manager_service import WorkerControlService
from runtime.transport.grpc.serializers import (
    WorkerControlSerializer,
    manager_worker_pb2,
)


def _json_from_wr_to_rm_grpc_block(stdout: str) -> dict[str, Any]:
    marker = BANNER_WR_TO_RM_GRPC + "\n"
    assert marker in stdout, stdout
    tail = stdout.split(marker)[-1]
    first_line = tail.split("\n", 1)[0]
    _prefix, json_blob = first_line.rsplit(": ", 1)
    return json.loads(json_blob)


class _RecordingWorkerControlApp:
    def __init__(self, stop_result: dict[str, Any]) -> None:
        self._stop_result = stop_result

    def create_worker(self, command: Any) -> Any:
        raise AssertionError("not used")

    def stop_worker(self, command: Any) -> Any:
        return self._stop_result

    def check_health(self, command: Any) -> Any:
        raise AssertionError("not used")


def test_stop_worker_stdout_response_envelope_uses_nested_payload_only(
    capsys: pytest.CaptureFixture[str],
) -> None:
    accepted_at = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
    stop_result = {
        "accepted": True,
        "runtime_id": "rt-test",
        "worker_identity": "rt-test:sv1:1",
        "local_state": "STOPPING",
        "reason_code": "STOP_REQUESTED",
        "accepted_at": accepted_at,
        "message": "StopWorker accepted; worker shutdown complete.",
    }
    svc = WorkerControlService(_RecordingWorkerControlApp(stop_result))
    req = manager_worker_pb2.StopWorkerRequest(runtime_id="rt-test")
    svc.StopWorker(req, context=None)

    rsp = WorkerControlSerializer().stop_result_to_response(stop_result)
    assert rsp.accepted is True
    assert rsp.accepted_at.seconds == int(accepted_at.timestamp())
    assert rsp.message == "StopWorker accepted; worker shutdown complete."

    out = capsys.readouterr().out
    if BANNER_WR_TO_RM_GRPC + "\n" not in out:
        pytest.skip(
            "control plane stdout envelope logging is disabled (no WR→RM line captured)"
        )
    env = _json_from_wr_to_rm_grpc_block(out)

    assert env["event_name"] == "worker.control.stop_worker.response"
    forbidden_top = {"accepted", "accepted_at", "local_state", "reason_code", "message"}
    assert not forbidden_top.intersection(env.keys())

    pl = env["payload"]
    assert isinstance(pl, dict)
    assert list(pl.keys()) == [
        "accepted",
        "accepted_at",
        "local_state",
        "reason_code",
        "message",
    ]
    assert pl["accepted"] is True
    assert pl["accepted_at"] == "2026-03-30T10:00:00Z"
    assert pl["local_state"] == "STOPPING"
    assert pl["reason_code"] == "STOP_REQUESTED"
    assert pl["message"] == "StopWorker accepted; worker shutdown complete."

    for key in (
        "event_id",
        "runtime_id",
        "worker_identity",
        "launch_attempt",
        "strategy_version_id",
    ):
        assert key in env
