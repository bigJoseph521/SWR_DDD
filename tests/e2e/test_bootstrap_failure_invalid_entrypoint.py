from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from runtime.application.dependency_container import (
    build_dependency_container,
)
from runtime.bootstrap.failures import EntrypointLoadFailure
from runtime.bootstrap.minimal_env_validation import SWR_ENTRYPOINT_INVALID
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from runtime.domain.enums import WorkerPhase
from tests.e2e._helpers import (
    FakeManagerClient,
    build_bundle_dict,
    write_strategy_package,
)


def test_bootstrap_failure_invalid_entrypoint_reports_srm_status_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-e2e-entrypoint")
    source_root = write_strategy_package(tmp_path / "source")
    manager = FakeManagerClient()
    settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-invalid-entrypoint",
            entrypoint="bad_entrypoint",
        ),
        base_dir=tmp_path,
    )
    container = build_dependency_container(settings, manager_client=manager)

    with patch(
        "runtime.application.lifecycle.lifecycle_service.report_bootstrap_failure_to_srm"
    ) as report_mock:
        with pytest.raises(EntrypointLoadFailure) as exc_info:
            container.worker_app.start()

    assert exc_info.value.reason_code == "ENTRYPOINT_INVALID"
    assert container.lifecycle_service.phase is WorkerPhase.FAILED
    assert not any(
        signal.get("signal_type") == "bootstrap_failed" for signal in manager.signals
    )
    report_mock.assert_called_once()
    assert report_mock.call_args.args[0] is exc_info.value
    from runtime.bootstrap.srm_env_status_report import (
        bootstrap_failure_srm_reason_code,
    )

    assert bootstrap_failure_srm_reason_code(exc_info.value) == SWR_ENTRYPOINT_INVALID
