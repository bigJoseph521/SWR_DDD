from __future__ import annotations

import signal
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from runtime.application.cooperative_shutdown import (
    CooperativeShutdownCoordinator,
    install_sigterm_handler,
)
from runtime.bootstrap.minimal_env_validation import HEALTH_STATUS_UNKNOWN
from runtime.bootstrap.srm_env_status_report import (
    KUBERNETES_TERMINATION_REASON,
    build_stopping_status_update_body,
    canonical_reason_from_stop_request,
    initiate_stop_status_update,
    shutdown_swr_reason_code,
)
from runtime.transport.heartbeat import SRM_STATUS_SOURCE_UPDATE
from runtime.transport.http.stop_control_server import (
    StopAlreadyInProgress,
    WorkerAlreadyStopped,
)


def _wait_for_mock_calls(*mocks: MagicMock, timeout: float = 2.0) -> None:
    deadline = threading.Event()
    deadline.wait(timeout=timeout)
    for mock in mocks:
        if mock.call_count < 1:
            raise AssertionError(f"expected call on {mock!r}")


def test_canonical_reason_from_sigterm_aliases() -> None:
    assert (
        canonical_reason_from_stop_request("SIGTERM") == KUBERNETES_TERMINATION_REASON
    )
    assert (
        canonical_reason_from_stop_request("KUBERNETES_TERMINATION")
        == KUBERNETES_TERMINATION_REASON
    )
    assert (
        canonical_reason_from_stop_request("SWR_KUBERNETES_TERMINATION")
        == KUBERNETES_TERMINATION_REASON
    )


def test_shutdown_swr_reason_code_kubernetes_termination() -> None:
    assert shutdown_swr_reason_code(KUBERNETES_TERMINATION_REASON) == (
        "SWR_KUBERNETES_TERMINATION"
    )


def test_kubernetes_stopping_status_payload_shape() -> None:
    body = build_stopping_status_update_body(
        runtime_id="rt-k8s-1",
        mode="PAPER",
        owner_resource_id="dep-k8s-1",
        canonical_reason=KUBERNETES_TERMINATION_REASON,
    )
    assert body["runtime_id"] == "rt-k8s-1"
    assert body["runtime_type"] == "STRATEGY_WORKER_RUNTIME"
    assert body["mode"] == "PAPER"
    assert body["owner_resource_id"] == "dep-k8s-1"
    assert body["runtime_status"] == "STOPPING"
    assert body["health_status"] == HEALTH_STATUS_UNKNOWN
    assert body["source"] == SRM_STATUS_SOURCE_UPDATE
    assert body["metadata"] == {
        "reason_code": "SWR_KUBERNETES_TERMINATION",
        "message": "Worker received Kubernetes SIGTERM.",
        "retryable": False,
    }
    assert "deployment_id" not in body
    assert "failure_reason" not in body
    assert "stage" not in body
    assert "worker_status" not in body
    assert "deployment_id" not in body["metadata"]
    assert "stage" not in body["metadata"]
    assert "worker_status" not in body["metadata"]
    assert "failure_reason" not in body["metadata"]


def test_initiate_stop_status_update_kubernetes_posts_stopping_to_srm() -> None:
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        body, _resp, canonical, _msg = initiate_stop_status_update(
            runtime_id="rt-k8s-2",
            mode="LIVE",
            owner_resource_id="dep-k8s-2",
            srm_base_url="http://127.0.0.1:8080",
            reason=KUBERNETES_TERMINATION_REASON,
        )

    assert canonical == KUBERNETES_TERMINATION_REASON
    assert captured["owner_resource_id"] == "dep-k8s-2"
    assert captured["runtime_status"] == "STOPPING"
    assert captured["health_status"] == HEALTH_STATUS_UNKNOWN
    assert captured["source"] == SRM_STATUS_SOURCE_UPDATE
    assert captured["reason_code"] == "SWR_KUBERNETES_TERMINATION"
    assert captured["message"] == "Worker received Kubernetes SIGTERM."
    assert captured["retryable"] is False
    assert body["metadata"]["reason_code"] == "SWR_KUBERNETES_TERMINATION"


def test_install_sigterm_handler_registers_handler() -> None:
    lifecycle = MagicMock()
    worker_app = MagicMock()
    worker_app.stop.return_value = True
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )
    if not hasattr(signal, "SIGTERM"):
        pytest.skip("SIGTERM not available on this platform")
    previous = signal.getsignal(signal.SIGTERM)
    try:
        assert install_sigterm_handler(coordinator) is True
        assert signal.getsignal(signal.SIGTERM) is not previous
    finally:
        signal.signal(signal.SIGTERM, previous)


def test_sigterm_handler_triggers_cooperative_shutdown() -> None:
    lifecycle = MagicMock()
    lifecycle.initiate_stop_from_request.return_value = {"runtime_status": "STOPPING"}
    worker_app = MagicMock()
    worker_app.stop.return_value = True
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )
    if not hasattr(signal, "SIGTERM"):
        pytest.skip("SIGTERM not available on this platform")
    previous = signal.getsignal(signal.SIGTERM)
    try:
        install_sigterm_handler(coordinator)
        signal.raise_signal(signal.SIGTERM)
    finally:
        signal.signal(signal.SIGTERM, previous)

    _wait_for_mock_calls(lifecycle.initiate_stop_from_request, worker_app.stop)
    lifecycle.initiate_stop_from_request.assert_called_once_with(
        KUBERNETES_TERMINATION_REASON
    )
    worker_app.stop.assert_called_once_with(
        emit_termination_signal=False,
        suppress_stopping_phase_stdout=True,
    )


def test_sigterm_handler_is_idempotent_for_duplicate_signals() -> None:
    lifecycle = MagicMock()
    lifecycle.initiate_stop_from_request.return_value = {"runtime_status": "STOPPING"}
    worker_app = MagicMock()
    worker_app.stop.return_value = True
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )
    if not hasattr(signal, "SIGTERM"):
        pytest.skip("SIGTERM not available on this platform")
    previous = signal.getsignal(signal.SIGTERM)
    try:
        install_sigterm_handler(coordinator)
        signal.raise_signal(signal.SIGTERM)
        signal.raise_signal(signal.SIGTERM)
    finally:
        signal.signal(signal.SIGTERM, previous)

    _wait_for_mock_calls(lifecycle.initiate_stop_from_request, worker_app.stop)
    lifecycle.initiate_stop_from_request.assert_called_once()
    worker_app.stop.assert_called_once()


def test_sigterm_shutdown_skips_when_http_stop_in_progress() -> None:
    lifecycle = MagicMock()
    lifecycle.initiate_stop_from_request.side_effect = StopAlreadyInProgress(
        "Stop is already in progress."
    )
    worker_app = MagicMock()
    worker_app.stop.return_value = True
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )
    coordinator._shutdown_thread_started = True  # noqa: SLF001 — HTTP stop thread already running

    coordinator.request_sigterm_shutdown()

    lifecycle.initiate_stop_from_request.assert_called_once_with(
        KUBERNETES_TERMINATION_REASON
    )
    worker_app.stop.assert_not_called()


def test_sigterm_shutdown_continues_when_srm_initiate_raises() -> None:
    lifecycle = MagicMock()
    lifecycle.initiate_stop_from_request.side_effect = RuntimeError("srm unreachable")
    worker_app = MagicMock()
    worker_app.stop.return_value = True
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )

    coordinator.request_sigterm_shutdown()

    worker_app.stop.assert_called_once()


def test_http_stop_starts_single_shutdown_thread() -> None:
    started = threading.Event()
    lifecycle = MagicMock()
    lifecycle.initiate_stop_from_request.return_value = {"runtime_status": "STOPPING"}

    def _slow_stop(**kwargs: object) -> bool:
        started.set()
        threading.Event().wait(timeout=1.0)
        return True

    worker_app = MagicMock()
    worker_app.stop.side_effect = _slow_stop
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )

    coordinator.request_http_stop("STOP_REQUESTED")
    coordinator.request_sigterm_shutdown()

    assert started.wait(timeout=2.0)
    assert worker_app.stop.call_count == 1


def test_http_stop_propagates_stop_already_in_progress() -> None:
    lifecycle = MagicMock()
    lifecycle.initiate_stop_from_request.side_effect = StopAlreadyInProgress("busy")
    worker_app = MagicMock()
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )

    with pytest.raises(StopAlreadyInProgress):
        coordinator.request_http_stop("STOP_REQUESTED")
    worker_app.stop.assert_not_called()


def test_sigterm_ignored_when_worker_already_stopped() -> None:
    lifecycle = MagicMock()
    lifecycle.initiate_stop_from_request.side_effect = WorkerAlreadyStopped(
        "Worker is already stopped."
    )
    worker_app = MagicMock()
    coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )

    coordinator.request_sigterm_shutdown()

    worker_app.stop.assert_not_called()


def test_lifecycle_initiate_stop_sets_shutdown_in_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from runtime.application.lifecycle_service import LifecycleService
    from runtime.bootstrap.launch_spec import LaunchSpec
    from runtime.bootstrap.validator import LaunchSpecValidator
    from runtime.domain.worker_identity import WorkerIdentity

    payload: dict[str, object] = {
        "runtime_id": "rt-life-1",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "launch_attempt": 1,
        "mode": "PAPER",
        "artifact_uri": "file:///tmp/strategy",
        "artifact_digest": "sha256:abcd",
        "entrypoint": "strategy.main:Strategy",
    }
    spec = LaunchSpec.from_payload(payload)
    settings = SimpleNamespace(
        strategy_runtime_manager_base_url="http://127.0.0.1:8080",
        deployment_id="dep-life-1",
        runtime_manager_heartbeat_timeout_seconds=1.0,
    )
    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=WorkerIdentity(
            runtime_id=spec.runtime_id,
            tenant_id=spec.tenant_id,
            account_id=spec.account_id,
            strategy_version_id=spec.strategy_version_id,
            launch_attempt=spec.launch_attempt,
            mode=spec.mode,
            artifact_uri=spec.artifact_uri,
            artifact_digest=spec.artifact_digest,
            entrypoint=spec.entrypoint,
        ),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=MagicMock(),
        runtime_dependencies_initializer=lambda: None,
        strategy_instance_manager=MagicMock(),
        worker_runtime_settings=settings,
    )
    captured: dict[str, object] = {}

    def _fake_initiate(**kwargs: object) -> tuple[dict[str, object], None, str, str]:
        captured.update(kwargs)
        return (
            {"runtime_status": "STOPPING"},
            None,
            KUBERNETES_TERMINATION_REASON,
            "Worker received Kubernetes SIGTERM.",
        )

    monkeypatch.setattr(
        "runtime.application.lifecycle_service.initiate_stop_status_update",
        _fake_initiate,
    )

    lifecycle.initiate_stop_from_request(KUBERNETES_TERMINATION_REASON)

    assert lifecycle._shutdown_in_progress is True  # noqa: SLF001
    assert captured["owner_resource_id"] == "dep-life-1"
    assert captured["reason"] == KUBERNETES_TERMINATION_REASON
