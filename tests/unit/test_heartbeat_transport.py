from __future__ import annotations

from datetime import datetime, timezone

from runtime.transport.heartbeat import (
    SRM_STATUS_SOURCE_HEARTBEAT,
    SRM_STATUS_SOURCE_UPDATE,
    build_srm_heartbeat_body,
    build_srm_status_metadata,
    health_status_for_local_state,
    join_heartbeat_url,
    join_runtime_status_url,
    normalize_srm_status_source,
)


def test_join_runtime_status_url() -> None:
    url = join_runtime_status_url(
        "http://127.0.0.1:8080",
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    )
    assert (
        url
        == "http://127.0.0.1:8080/internal/v1/runtimes/a1b2c3d4-e5f6-7890-abcd-ef1234567890/status"
    )
    assert join_heartbeat_url(
        "http://127.0.0.1:8080", "rt-1"
    ) == join_runtime_status_url("http://127.0.0.1:8080", "rt-1")


def test_build_srm_heartbeat_body_healthy_ready() -> None:
    observed = datetime(2026, 5, 19, 14, 29, 55, tzinfo=timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        mode="LIVE",
        owner_resource_id="dep-20260519-001",
        local_state="READY",
        observed_at=observed,
    )
    assert body == {
        "runtime_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "runtime_type": "STRATEGY_WORKER_RUNTIME",
        "mode": "LIVE",
        "owner_resource_id": "dep-20260519-001",
        "runtime_status": "READY",
        "health_status": "HEALTHY",
        "source": "HEARTBEAT",
        "occurred_at": "2026-05-19T14:29:55Z",
        "metadata": {"reason_code": "", "message": ""},
    }
    assert "worker_status" not in body


def test_build_srm_heartbeat_body_failed_update_includes_metadata() -> None:
    observed = datetime(2026, 5, 19, 15, 0, 0, tzinfo=timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id="rt-fail",
        mode="PAPER",
        owner_resource_id="dep-1",
        local_state="FAILED",
        observed_at=observed,
        source=SRM_STATUS_SOURCE_UPDATE,
        reason_code="BOOTSTRAP_METADATA_INCONSISTENT",
        message="Launch spec validation failed.",
    )
    assert body["source"] == "UPDATE"
    assert body["runtime_status"] == "FAILED"
    assert body["health_status"] == "UNHEALTHY"
    assert body["metadata"] == {
        "reason_code": "BOOTSTRAP_METADATA_INCONSISTENT",
        "message": "Launch spec validation failed.",
    }


def test_build_srm_status_metadata_defaults_to_empty_strings() -> None:
    assert build_srm_status_metadata() == {"reason_code": "", "message": ""}


def test_build_srm_heartbeat_body_metadata_empty() -> None:
    observed = datetime(2026, 5, 19, 16, 0, 0, tzinfo=timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id="rt-ok",
        mode="LIVE",
        owner_resource_id="dep-1",
        local_state="RUNNING",
        observed_at=observed,
        source=SRM_STATUS_SOURCE_UPDATE,
        runtime_status="RUNNING",
        health_status="HEALTHY",
        metadata_empty=True,
    )
    assert body["runtime_status"] == "RUNNING"
    assert body["health_status"] == "HEALTHY"
    assert body["metadata"] == {}


def test_health_status_for_failed_is_unhealthy() -> None:
    assert health_status_for_local_state("FAILED") == "UNHEALTHY"
    assert health_status_for_local_state("READY") == "HEALTHY"


def test_normalize_srm_status_source_rejects_unknown() -> None:
    try:
        normalize_srm_status_source("POLL")
    except ValueError as exc:
        assert "HEARTBEAT" in str(exc) and "UPDATE" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_build_srm_heartbeat_body_defaults_source_to_heartbeat() -> None:
    observed = datetime(2026, 5, 19, 14, 29, 55, tzinfo=timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id="rt-1",
        mode="PAPER",
        owner_resource_id="dep-1",
        local_state="RUNNING",
        observed_at=observed,
    )
    assert body["source"] == SRM_STATUS_SOURCE_HEARTBEAT
