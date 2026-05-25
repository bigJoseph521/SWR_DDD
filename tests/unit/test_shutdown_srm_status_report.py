from __future__ import annotations

from unittest.mock import patch

from runtime.bootstrap.minimal_env_validation import (
    HEALTH_STATUS_UNHEALTHY,
    HEALTH_STATUS_UNKNOWN,
    RUNTIME_STATUS_STOPPED,
    RUNTIME_STATUS_STOPPING,
)
from runtime.bootstrap.srm_env_status_report import (
    KUBERNETES_TERMINATION_REASON,
    build_stopping_status_update_body,
    canonical_reason_from_stop_request,
    initiate_stop_status_update,
    is_failure_shutdown,
    report_final_shutdown_to_srm,
    report_stopped_to_srm,
    report_stopping_to_srm,
    resolve_shutdown_report,
    shutdown_swr_reason_code,
)
from runtime.infrastructure.http.srm.heartbeat import build_srm_heartbeat_body


def test_shutdown_swr_reason_code_adds_prefix() -> None:
    assert shutdown_swr_reason_code("STOP_REQUESTED") == "SWR_STOP_REQUESTED"
    assert shutdown_swr_reason_code("SWR_STOP_REQUESTED") == "SWR_STOP_REQUESTED"


def test_resolve_shutdown_report_requires_message() -> None:
    reason, message = resolve_shutdown_report(
        canonical_reason="RUNTIME_JOB_COMPLETED",
        message=None,
    )
    assert reason == "SWR_RUNTIME_JOB_COMPLETED"
    assert message == "Runtime job completed."


def test_stopping_status_metadata_has_reason_and_message_without_retryable() -> None:
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        report_stopping_to_srm(
            runtime_id="rt-stop-1",
            mode="LIVE",
            owner_resource_id="dep-1",
            srm_base_url="http://127.0.0.1:8080",
            canonical_reason="STOP_REQUESTED",
            message="Stopping worker.",
        )

    assert captured["runtime_status"] == RUNTIME_STATUS_STOPPING
    assert captured["health_status"] == HEALTH_STATUS_UNKNOWN
    assert captured["reason_code"] == "SWR_STOP_REQUESTED"
    assert captured["message"] == "Stopping worker."
    assert "retryable" not in captured


def test_stopped_status_unhealthy_on_error_detected() -> None:
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        report_stopped_to_srm(
            runtime_id="rt-stop-2",
            mode="PAPER",
            owner_resource_id="dep-2",
            srm_base_url="http://127.0.0.1:8080",
            canonical_reason="ERROR_DETECTED",
            message="Shutdown after failure.",
        )

    assert captured["runtime_status"] == RUNTIME_STATUS_STOPPED
    assert captured["health_status"] == HEALTH_STATUS_UNHEALTHY
    assert captured["reason_code"] == "SWR_ERROR_DETECTED"
    assert "retryable" not in captured


def test_build_body_shutdown_metadata_shape() -> None:
    from datetime import datetime, timezone

    from runtime.infrastructure.http.srm.heartbeat import SRM_STATUS_SOURCE_UPDATE

    body = build_srm_heartbeat_body(
        runtime_id="rt-1",
        mode="LIVE",
        owner_resource_id="dep-1",
        local_state="STOPPING",
        observed_at=datetime(2026, 5, 19, 17, 0, 0, tzinfo=timezone.utc),
        source=SRM_STATUS_SOURCE_UPDATE,
        runtime_status="STOPPING",
        health_status="UNKNOWN",
        reason_code="SWR_STOP_REQUESTED",
        message="Stopping.",
    )
    assert body["metadata"] == {
        "reason_code": "SWR_STOP_REQUESTED",
        "message": "Stopping.",
    }
    assert "retryable" not in body["metadata"]


def test_canonical_reason_from_stop_request_error() -> None:
    assert canonical_reason_from_stop_request("error_detected") == (
        "UNHEALTHY_EXECUTION_DETECTED"
    )
    assert is_failure_shutdown("UNHEALTHY_EXECUTION_DETECTED") is True


def test_report_final_shutdown_failed_on_error() -> None:
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        report_final_shutdown_to_srm(
            runtime_id="rt-fail-stop",
            mode="LIVE",
            owner_resource_id="dep-1",
            srm_base_url="http://127.0.0.1:8080",
            canonical_reason="UNHEALTHY_EXECUTION_DETECTED",
            message="Shutdown after failure.",
        )

    assert captured["runtime_status"] == "FAILED"
    assert captured["health_status"] == "UNHEALTHY"
    assert captured["reason_code"] == "SWR_UNHEALTHY_EXECUTION_DETECTED"
    assert "retryable" not in captured


def test_kubernetes_termination_stopping_includes_retryable_false() -> None:
    body = build_stopping_status_update_body(
        runtime_id="rt-k8s",
        mode="PAPER",
        owner_resource_id="dep-k8s",
        canonical_reason=KUBERNETES_TERMINATION_REASON,
    )
    assert body["metadata"]["reason_code"] == "SWR_KUBERNETES_TERMINATION"
    assert body["metadata"]["retryable"] is False
    assert (
        canonical_reason_from_stop_request("SIGTERM") == KUBERNETES_TERMINATION_REASON
    )


def test_initiate_stop_status_update_returns_stopping_body() -> None:
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        body, _resp, canonical, _msg = initiate_stop_status_update(
            runtime_id="rt-init",
            mode="PAPER",
            owner_resource_id="dep-init",
            srm_base_url="http://127.0.0.1:8080",
            reason="operator",
        )

    assert canonical == "STOP_REQUESTED"
    assert body["runtime_status"] == "STOPPING"
    assert captured["runtime_status"] == "STOPPING"
    assert body["metadata"]["reason_code"] == "SWR_STOP_REQUESTED"
    assert (
        build_stopping_status_update_body(
            runtime_id="rt-init",
            mode="PAPER",
            owner_resource_id="dep-init",
            canonical_reason="STOP_REQUESTED",
        )["runtime_status"]
        == "STOPPING"
    )
