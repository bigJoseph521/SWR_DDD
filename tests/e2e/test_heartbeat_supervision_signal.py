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


def test_heartbeat_supervision_signal_uses_worker_source_contract(
    tmp_path: Path,
) -> None:
    source_root = write_strategy_package(tmp_path / "source")
    manager = FakeManagerClient()
    settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-heartbeat",
        ),
        base_dir=tmp_path,
    )
    container = build_dependency_container(settings, manager_client=manager)
    container.worker_app.start()

    deps = container.runtime_dependencies_initializer()
    deps.manager.emit_unhealthy(
        occurred_at=datetime(2026, 3, 29, 9, 0, tzinfo=timezone.utc),
        observed_at=datetime(2026, 3, 29, 9, 0, 1, tzinfo=timezone.utc),
        reason_code="heartbeat_lagging",
        details={"source": "heartbeat_supervisor"},
    )

    unhealthy = [
        signal for signal in manager.signals if signal["signal_type"] == "unhealthy"
    ]
    assert len(unhealthy) == 1
    assert unhealthy[0]["payload"]["reason_code"] == "UNHEALTHY_EXECUTION_DETECTED"
    assert unhealthy[0]["payload"]["details"]["source"] == "heartbeat_supervisor"
