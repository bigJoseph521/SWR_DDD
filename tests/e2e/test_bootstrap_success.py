from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from runtime.application.dependency_container import (
    build_dependency_container,
)
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from runtime.domain.enums import WorkerPhase
from tests.e2e._helpers import (
    FakeManagerClient,
    build_bundle_dict,
    write_strategy_package,
)


def test_bootstrap_success_emits_worker_owned_startup_signal_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-e2e-success")
    source_root = write_strategy_package(tmp_path / "source")
    manager = FakeManagerClient()
    settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-bootstrap-success",
        ),
        base_dir=tmp_path,
    )
    container = build_dependency_container(settings, manager_client=manager)

    with patch(
        "runtime.application.lifecycle.lifecycle_service.report_bootstrap_success_to_srm"
    ) as success_report_mock:
        container.worker_app.start()
    assert container.lifecycle_service.phase is WorkerPhase.READY
    assert [signal["signal_type"] for signal in manager.signals] == [
        "bootstrap_succeeded",
        "heartbeat",
    ]
    success_report_mock.assert_called_once()

    container.worker_app.stop()
    assert container.lifecycle_service.phase is WorkerPhase.STOPPED
    assert manager.signals[-1]["signal_type"] == "terminated"
