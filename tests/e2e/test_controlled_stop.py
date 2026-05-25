from __future__ import annotations

from pathlib import Path

from runtime.bootstrap.dependency_container import build_dependency_container
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from tests.e2e._helpers import (
    FakeManagerClient,
    build_bundle_dict,
    write_strategy_package,
)


def test_controlled_stop_is_idempotent_and_emits_single_terminated_signal(
    tmp_path: Path,
) -> None:
    source_root = write_strategy_package(tmp_path / "source")
    manager = FakeManagerClient()
    settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-stop",
        ),
        base_dir=tmp_path,
    )
    container = build_dependency_container(settings, manager_client=manager)

    container.worker_app.start()
    first = container.worker_app.stop()
    second = container.worker_app.stop()

    terminated = [
        signal for signal in manager.signals if signal["signal_type"] == "terminated"
    ]
    assert first is True
    assert second is False
    assert len(terminated) == 1
    p = terminated[0]["payload"]
    assert p["reason_code"] == "STOP_REQUESTED"
    assert p["local_state"] == "STOPPED"
    assert "accepted" not in p
    assert "accepted_at" not in p
    assert p.get("message") == "Worker shutdown complete."
