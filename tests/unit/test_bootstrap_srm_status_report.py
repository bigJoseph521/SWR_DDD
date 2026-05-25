from __future__ import annotations

from unittest.mock import patch

import pytest
from runtime.domain.bootstrap_failures import (
    ArtifactFetchFailure,
    ArtifactVerificationFailure,
    BootstrapFailure,
    BootstrapStage,
    EntrypointLoadFailure,
    SDKContractFailure,
)
from runtime.bootstrap.minimal_env_validation import (
    HEALTH_STATUS_HEALTHY,
    RUNTIME_STATUS_RUNNING,
    SWR_ARTIFACT_DIGEST_MISMATCH,
    SWR_ARTIFACT_NOT_FOUND,
    SWR_ENTRYPOINT_INVALID,
    SWR_SDK_COMPATIBILITY_FAILED,
)
from runtime.bootstrap.srm_env_status_report import (
    bootstrap_failure_srm_reason_code,
    build_bootstrap_failure_message,
    report_bootstrap_failure_to_srm,
    report_bootstrap_success_to_srm,
)


@pytest.mark.parametrize(
    ("failure", "expected_code"),
    [
        (
            ArtifactFetchFailure(
                reason_code="artifact_not_found",
                retryable=False,
                message="Artifact not found.",
            ),
            SWR_ARTIFACT_NOT_FOUND,
        ),
        (
            ArtifactVerificationFailure(
                reason_code="digest_mismatch",
                retryable=False,
                details={"expected_digest": "sha256:aa", "actual_digest": "sha256:bb"},
            ),
            SWR_ARTIFACT_DIGEST_MISMATCH,
        ),
        (
            EntrypointLoadFailure(
                reason_code="entrypoint_invalid",
                retryable=False,
                details={"entrypoint": "bad:Symbol"},
            ),
            SWR_ENTRYPOINT_INVALID,
        ),
        (
            SDKContractFailure(
                reason_code="sdk_marker_incompatible",
                retryable=False,
            ),
            SWR_SDK_COMPATIBILITY_FAILED,
        ),
    ],
)
def test_bootstrap_failure_srm_reason_code_by_stage(
    failure: BootstrapFailure,
    expected_code: str,
) -> None:
    assert bootstrap_failure_srm_reason_code(failure) == expected_code


def test_build_bootstrap_failure_message_includes_details() -> None:
    failure = EntrypointLoadFailure(
        reason_code="entrypoint_import_failed",
        retryable=False,
        details={"entrypoint": "pkg.main:Strategy"},
        message="Could not import entrypoint.",
    )
    message = build_bootstrap_failure_message(failure)
    assert "Could not import entrypoint." in message
    assert BootstrapStage.ENTRYPOINT_LOAD.value in message
    assert "entrypoint=pkg.main:Strategy" in message


def test_report_bootstrap_failure_posts_startup_failed() -> None:
    failure = ArtifactFetchFailure(
        reason_code="artifact_not_found",
        retryable=False,
        message="missing artifact",
    )
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        response = report_bootstrap_failure_to_srm(
            failure,
            runtime_id="rt-boot-1",
            mode="LIVE",
            owner_resource_id="dep-boot-1",
            srm_base_url="http://127.0.0.1:8080",
        )

    assert response == {"accepted": True}
    assert captured["runtime_status"] == "STARTUP_FAILED"
    assert captured["health_status"] == "UNHEALTHY"
    assert captured["reason_code"] == SWR_ARTIFACT_NOT_FOUND
    assert captured["retryable"] is False
    assert "missing artifact" in str(captured["message"])


def test_report_bootstrap_success_posts_running_healthy_empty_metadata() -> None:
    captured: dict[str, object] = {}

    def _fake_post(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"accepted": True}

    with patch(
        "runtime.bootstrap.srm_env_status_report.post_srm_runtime_status",
        side_effect=_fake_post,
    ):
        response = report_bootstrap_success_to_srm(
            runtime_id="rt-boot-ok",
            mode="PAPER",
            owner_resource_id="dep-boot-ok",
            srm_base_url="http://127.0.0.1:8080",
        )

    assert response == {"accepted": True}
    assert captured["runtime_status"] == RUNTIME_STATUS_RUNNING
    assert captured["health_status"] == HEALTH_STATUS_HEALTHY
    assert captured["metadata_empty"] is True
    assert "reason_code" not in captured or captured.get("reason_code") is None
