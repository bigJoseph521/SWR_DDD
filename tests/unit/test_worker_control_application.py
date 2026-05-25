from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from runtime.application.worker_control_application import (
    WorkerControlApplicationImpl,
    _canonical_worker_identity,
)
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.domain.enums import WorkerPhase
from runtime.transport.grpc.serializers import (
    CreateWorkerCommand,
    HealthCheckCommand,
    StopWorkerCommand,
)


def _launch_payload() -> dict[str, object]:
    return {
        "runtime_id": "rt-1",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "artifact_uri": "file:///tmp/strategy",
        "artifact_digest": "sha256:abcd",
        "entrypoint": "strategy.main:Strategy",
        "launch_attempt": 1,
    }


@pytest.fixture
def launch_spec() -> LaunchSpec:
    return LaunchSpec.from_payload(_launch_payload())


def test_canonical_worker_identity_matches_runtime_metadata_shape(
    launch_spec: LaunchSpec,
) -> None:
    assert _canonical_worker_identity(launch_spec) == "rt-1:sv-1:1"


def test_stop_worker_accepts_matching_stop_command(launch_spec: LaunchSpec) -> None:
    lifecycle = MagicMock()
    lifecycle.phase = WorkerPhase.READY
    worker_app = MagicMock()
    app = WorkerControlApplicationImpl(
        launch_spec=launch_spec,
        lifecycle_service=lifecycle,
        worker_app=worker_app,
    )
    cmd = StopWorkerCommand(runtime_id="rt-1")
    out = app.stop_worker(cmd)
    assert out["accepted"] is True
    assert out["runtime_id"] == "rt-1"
    assert out["reason_code"] == "STOP_REQUESTED"
    assert "message" in out
    lifecycle.record_manager_stop_request_checkpoint.assert_called_once()
    lifecycle.initiate_stop_from_request.assert_called_once_with("STOP_REQUESTED")
    worker_app.stop.assert_called_once_with(
        emit_termination_signal=False,
        suppress_stopping_phase_stdout=True,
        termination_reason_code="STOP_REQUESTED",
    )


def test_stop_worker_rejects_runtime_id_mismatch(launch_spec: LaunchSpec) -> None:
    lifecycle = MagicMock()
    lifecycle.phase = WorkerPhase.READY
    worker_app = MagicMock()
    app = WorkerControlApplicationImpl(
        launch_spec=launch_spec,
        lifecycle_service=lifecycle,
        worker_app=worker_app,
    )
    cmd = StopWorkerCommand(runtime_id="other")
    out = app.stop_worker(cmd)
    assert out["accepted"] is False
    assert out["reason_code"] == "BOOTSTRAP_METADATA_INCONSISTENT"
    assert "message" in out
    worker_app.stop.assert_not_called()


def test_create_worker_idempotent_when_launch_matches(launch_spec: LaunchSpec) -> None:
    lifecycle = MagicMock()
    lifecycle.phase = WorkerPhase.READY
    worker_app = MagicMock()
    app = WorkerControlApplicationImpl(
        launch_spec=launch_spec,
        lifecycle_service=lifecycle,
        worker_app=worker_app,
    )
    cmd = CreateWorkerCommand(launch_spec=launch_spec, metadata={})
    out = app.create_worker(cmd)
    assert out["accepted"] is True
    assert out["runtime_id"] == "rt-1"


def test_healthcheck_rejects_wrong_runtime(launch_spec: LaunchSpec) -> None:
    lifecycle = MagicMock()
    lifecycle.phase = WorkerPhase.RUNNING
    worker_app = MagicMock()
    app = WorkerControlApplicationImpl(
        launch_spec=launch_spec,
        lifecycle_service=lifecycle,
        worker_app=worker_app,
    )
    cmd = HealthCheckCommand(runtime_id="x", metadata={})
    out = app.check_health(cmd)
    assert out["state"] == "RUNNING"


def test_healthcheck_completed_backtest_worker_not_live(
    launch_spec: LaunchSpec,
) -> None:
    lifecycle = MagicMock()
    lifecycle.phase = WorkerPhase.COMPLETED
    worker_app = MagicMock()
    app = WorkerControlApplicationImpl(
        launch_spec=launch_spec,
        lifecycle_service=lifecycle,
        worker_app=worker_app,
    )
    cmd = HealthCheckCommand(runtime_id="rt-1", metadata={})
    out = app.check_health(cmd)
    assert out["state"] == "COMPLETED"


def test_healthcheck_accepts_matching_runtime_ignores_launch_attempt_in_command(
    launch_spec: LaunchSpec,
) -> None:
    lifecycle = MagicMock()
    lifecycle.phase = WorkerPhase.RUNNING
    worker_app = MagicMock()
    app = WorkerControlApplicationImpl(
        launch_spec=launch_spec,
        lifecycle_service=lifecycle,
        worker_app=worker_app,
    )
    cmd = HealthCheckCommand(
        runtime_id="rt-1",
        metadata={"launch_attempt": 99},
    )
    out = app.check_health(cmd)
    assert out["state"] == "RUNNING"


def test_stop_worker_requires_type() -> None:
    app = WorkerControlApplicationImpl(
        launch_spec=MagicMock(),
        lifecycle_service=MagicMock(),
        worker_app=MagicMock(),
    )
    with pytest.raises(TypeError):
        app.stop_worker({})
