from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from runtime.application.dependency_container import (
    build_dependency_container,
)
from runtime.bootstrap.failures import SDKContractFailure
from runtime.bootstrap.minimal_env_validation import SWR_SDK_COMPATIBILITY_FAILED
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from runtime.domain.enums import WorkerPhase
from tests.e2e._helpers import (
    FakeManagerClient,
    build_bundle_dict,
    write_strategy_package,
)


def test_sdk_mismatch_failure_reports_srm_status_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-e2e-sdk")
    source_root = write_strategy_package(
        tmp_path / "source",
        package_name="sdk_mismatch_pkg",
        sdk_marker="2.0",
    )
    manager = FakeManagerClient()
    settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-sdk-mismatch",
            entrypoint="sdk_mismatch_pkg.main:Strategy",
        ),
        base_dir=tmp_path,
    )
    container = build_dependency_container(settings, manager_client=manager)

    with patch(
        "runtime.application.lifecycle.lifecycle_service.report_bootstrap_failure_to_srm"
    ) as report_mock:
        with pytest.raises(SDKContractFailure) as exc_info:
            container.worker_app.start()

    assert exc_info.value.reason_code == "SDK_MARKER_INCOMPATIBLE"
    assert container.lifecycle_service.phase is WorkerPhase.FAILED
    assert not manager.signals
    report_mock.assert_called_once()
    from runtime.bootstrap.srm_env_status_report import (
        bootstrap_failure_srm_reason_code,
    )

    assert (
        bootstrap_failure_srm_reason_code(exc_info.value)
        == SWR_SDK_COMPATIBILITY_FAILED
    )
