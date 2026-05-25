from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from runtime.bootstrap.dependency_container import build_dependency_container
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from tests.e2e._helpers import (
    FakeManagerClient,
    build_bundle_dict,
    write_strategy_package,
)


def test_delayed_stale_failure_signal_does_not_regress_current_running_attempt(
    tmp_path: Path,
) -> None:
    source_root = write_strategy_package(tmp_path / "source")
    manager = FakeManagerClient()

    old_settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-non-regression",
            launch_attempt=1,
        ),
        base_dir=tmp_path,
    )
    old_container = build_dependency_container(old_settings, manager_client=manager)
    old_container.worker_app.start()
    old_deps = old_container.runtime_dependencies_initializer()

    new_settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-non-regression",
            launch_attempt=2,
        ),
        base_dir=tmp_path,
    )
    new_container = build_dependency_container(new_settings, manager_client=manager)
    new_container.worker_app.start()
    new_deps = new_container.runtime_dependencies_initializer()
    new_deps.manager.emit_heartbeat(
        local_state="RUNNING",
        observed_at=datetime(2026, 3, 29, 16, 0, 1, tzinfo=timezone.utc),
    )

    stale_unhealthy = old_deps.manager.emit_unhealthy(
        occurred_at=datetime(2026, 3, 29, 16, 0, 2, tzinfo=timezone.utc),
        observed_at=datetime(2026, 3, 29, 16, 0, 3, tzinfo=timezone.utc),
        reason_code="heartbeat_lagging",
        details={"source": "old-attempt"},
    )
    stale_failure = old_deps.manager.emit_bootstrap_failed(
        occurred_at=datetime(2026, 3, 29, 16, 0, 4, tzinfo=timezone.utc),
        reason_code="late_old_failure",
    )

    assert stale_unhealthy["suppressed"] is True
    assert stale_unhealthy["decision"] == "DROP_STALE_ATTEMPT"
    assert stale_failure["suppressed"] is True
    assert stale_failure["decision"] == "DROP_STALE_ATTEMPT"

    runtime_signals = [
        item
        for item in manager.signals
        if item["identity"]["runtime_id"] == "rt-e2e-non-regression"
    ]
    assert all(item["identity"]["launch_attempt"] >= 1 for item in runtime_signals)
    assert any(
        item["signal_type"] == "heartbeat" and item["identity"]["launch_attempt"] == 2
        for item in runtime_signals
    )
    assert all(
        not (
            item["identity"]["launch_attempt"] == 1
            and item["signal_type"] in {"unhealthy", "bootstrap_failed"}
        )
        for item in runtime_signals
    )
