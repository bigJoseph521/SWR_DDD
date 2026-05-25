from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from runtime.bootstrap.minimal_env_validation import (
    HEALTH_STATUS_UNKNOWN,
    HEALTH_STATUS_UNHEALTHY,
    RUNTIME_STATUS_STARTING,
    RUNTIME_STATUS_STARTUP_FAILED,
    SWR_ENV_VALIDATION_FAILED,
    validate_minimal_env_from_environ,
)
from runtime.bootstrap.srm_env_status_report import report_minimal_env_validation_to_srm
from runtime.infrastructure.http.srm.heartbeat import build_srm_heartbeat_body


def test_validate_minimal_env_passes_with_runtime_id_and_srm_base_url(
    monkeypatch,
) -> None:
    monkeypatch.setenv("RUNTIME_ID", "rt-env-1")
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-1")
    monkeypatch.setenv("MODE", "LIVE")

    result = validate_minimal_env_from_environ()
    assert result.valid is True
    assert result.snapshot is not None
    assert result.snapshot.runtime_id == "rt-env-1"
    assert result.snapshot.srm_base_url == "http://127.0.0.1:8080"
    assert result.snapshot.deployment_id == "dep-1"
    assert result.snapshot.mode == "LIVE"


def test_validate_minimal_env_fails_when_runtime_id_missing(monkeypatch) -> None:
    monkeypatch.delenv("RUNTIME_ID", raising=False)
    monkeypatch.delenv("SWR_RUNTIME_ID", raising=False)
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")

    result = validate_minimal_env_from_environ()
    assert result.valid is False
    assert "runtime_id" in result.field_errors
    assert result.snapshot is None


def test_build_env_validation_starting_body() -> None:
    observed = datetime(2026, 5, 19, 14, 29, 55, tzinfo=timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id="rt-1",
        mode="LIVE",
        owner_resource_id="dep-1",
        local_state="STARTING",
        observed_at=observed,
        source="UPDATE",
        runtime_status="STARTING",
        health_status="UNKNOWN",
    )
    assert body["runtime_status"] == RUNTIME_STATUS_STARTING
    assert body["health_status"] == HEALTH_STATUS_UNKNOWN
    assert body["source"] == "UPDATE"
    assert body["owner_resource_id"] == "dep-1"
    assert "deployment_id" not in body
    assert body["metadata"] == {"reason_code": "", "message": ""}


def test_build_env_validation_startup_failed_body() -> None:
    observed = datetime(2026, 5, 19, 14, 30, 0, tzinfo=timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id="rt-1",
        mode="LIVE",
        owner_resource_id="dep-1",
        local_state="STARTUP_FAILED",
        observed_at=observed,
        source="UPDATE",
        runtime_status="STARTUP_FAILED",
        health_status="UNHEALTHY",
        reason_code=SWR_ENV_VALIDATION_FAILED,
        message="Minimal environment validation failed: runtime_id",
        retryable=False,
    )
    assert body["runtime_status"] == RUNTIME_STATUS_STARTUP_FAILED
    assert body["health_status"] == HEALTH_STATUS_UNHEALTHY
    assert body["owner_resource_id"] == "dep-1"
    assert "deployment_id" not in body
    assert body["metadata"] == {
        "reason_code": SWR_ENV_VALIDATION_FAILED,
        "message": "Minimal environment validation failed: runtime_id",
        "retryable": False,
    }


def test_report_minimal_env_validation_valid_posts_starting(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_ID", "rt-1")
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    result = validate_minimal_env_from_environ()
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        response = report_minimal_env_validation_to_srm(result)

    assert response == {"accepted": True}
    assert captured["runtime_status"] == RUNTIME_STATUS_STARTING
    assert captured["health_status"] == HEALTH_STATUS_UNKNOWN
    assert captured["source"] == "UPDATE"
    assert "owner_resource_id" in captured
    assert "deployment_id" not in captured
