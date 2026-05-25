from __future__ import annotations

from unittest.mock import patch

import pytest
from runtime.bootstrap.launch_spec import LaunchSpecValidationError
from runtime.bootstrap.minimal_env_validation import (
    SWR_CONTEXT_FETCH_FAILED,
    SWR_CONTEXT_VALIDATION_FAILED,
    validate_minimal_env_from_environ,
)
from runtime.bootstrap.srm_env_status_report import (
    build_runtime_context_fetch_failure_message,
    is_runtime_context_fetch_failure,
    report_runtime_context_fetch_failure_to_srm,
    runtime_context_failure_status,
)


def test_is_runtime_context_fetch_failure() -> None:
    exc = LaunchSpecValidationError(
        reason="deployment_runtime_context_http_error",
        field_errors={
            "deployment_runtime_context": "HTTP 503",
            "body": '{"error":"unavailable"}',
        },
    )
    assert is_runtime_context_fetch_failure(exc) is True
    assert (
        is_runtime_context_fetch_failure(
            LaunchSpecValidationError(
                reason="launch_identity_source_missing",
                field_errors={"bootstrap": "missing"},
            )
        )
        is False
    )


def test_build_runtime_context_fetch_failure_message() -> None:
    exc = LaunchSpecValidationError(
        reason="deployment_runtime_context_unreachable",
        field_errors={"deployment_runtime_context": "URLError(timed out)"},
    )
    message = build_runtime_context_fetch_failure_message(exc)
    assert "deployment_runtime_context_unreachable" in message
    assert "URLError" in message


def test_report_runtime_context_fetch_failure_posts_startup_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNTIME_ID", "rt-ctx-1")
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-ctx-1")
    monkeypatch.setenv("MODE", "LIVE")
    env = validate_minimal_env_from_environ()
    assert env.valid and env.snapshot is not None

    exc = LaunchSpecValidationError(
        reason="deployment_runtime_context_http_error",
        field_errors={
            "deployment_runtime_context": "HTTP 502",
            "body": "bad gateway",
        },
    )
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        response = report_runtime_context_fetch_failure_to_srm(
            exc,
            snapshot=env.snapshot,
        )

    assert response == {"accepted": True}
    assert captured["runtime_status"] == "STARTUP_FAILED"
    assert captured["health_status"] == "UNHEALTHY"
    assert captured["reason_code"] == SWR_CONTEXT_FETCH_FAILED
    assert captured["retryable"] is True
    assert "HTTP 502" in str(captured["message"])


@pytest.mark.parametrize(
    ("reason", "expected_code", "expected_retryable"),
    [
        ("deployment_runtime_context_empty_body", SWR_CONTEXT_VALIDATION_FAILED, False),
        (
            "deployment_runtime_context_invalid_json",
            SWR_CONTEXT_VALIDATION_FAILED,
            False,
        ),
        (
            "deployment_runtime_context_missing_identity",
            SWR_CONTEXT_VALIDATION_FAILED,
            False,
        ),
    ],
)
def test_runtime_context_validation_failure_status(
    reason: str,
    expected_code: str,
    expected_retryable: bool,
) -> None:
    exc = LaunchSpecValidationError(
        reason=reason,
        field_errors={"deployment_runtime_context": "invalid payload"},
    )
    assert runtime_context_failure_status(exc) == (expected_code, expected_retryable)
    assert is_runtime_context_fetch_failure(exc) is True
    message = build_runtime_context_fetch_failure_message(exc)
    assert "validation failed" in message


def test_report_runtime_context_validation_failure_not_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNTIME_ID", "rt-ctx-2")
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-ctx-2")
    env = validate_minimal_env_from_environ()
    assert env.valid and env.snapshot is not None

    exc = LaunchSpecValidationError(
        reason="deployment_runtime_context_invalid_json",
        field_errors={"deployment_runtime_context": "JSONDecodeError"},
    )
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        report_runtime_context_fetch_failure_to_srm(exc, snapshot=env.snapshot)

    assert captured["reason_code"] == SWR_CONTEXT_VALIDATION_FAILED
    assert captured["retryable"] is False
