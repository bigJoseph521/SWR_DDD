from __future__ import annotations

from pathlib import Path

from runtime.application.dependency_container import (
    build_dependency_container,
)
from runtime.config.settings import load_settings_from_bundle_dict
from runtime.config.settings import Settings
from runtime.domain.enums import RuntimeMode


def _settings_bundle(mode: str) -> dict[str, object]:
    return {
        "runtime_id": f"rt-{mode.lower()}",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": mode,
        "artifact_uri": "file:///tmp/strategy",
        "artifact_digest": "sha256:abcd",
        "entrypoint": "strategy.main:Strategy",
        "launch_attempt": 1,
    }


def _load(mode: str, **extra: object) -> Settings:
    d = _settings_bundle(mode)
    d.update(extra)
    return load_settings_from_bundle_dict(
        d, base_dir=Path.cwd(), print_launch_banner=False
    )


def test_build_is_deterministic_and_runtime_deps_are_singleton() -> None:
    settings = _load("PAPER")
    container = build_dependency_container(settings)

    first = container.runtime_dependencies_initializer()
    second = container.runtime_dependencies_initializer()

    assert first is second
    assert container.launch_spec.runtime_id == "rt-paper"
    assert container.mode_policy.mode is RuntimeMode.PAPER


def test_mode_wiring_paper_allows_oms_and_blocks_replay() -> None:
    settings = _load("PAPER")
    container = build_dependency_container(settings)
    deps = container.runtime_dependencies_initializer()

    assert deps.oms is not None
    assert deps.replay is None


def test_mode_wiring_backtest_allows_replay_and_risk_oms_gateway() -> None:
    settings = _load(
        "BACKTEST",
        job_id="job-1",
        ts_start="2026-01-01T00:00:00Z",
        ts_end="2026-01-02T00:00:00Z",
    )
    container = build_dependency_container(settings)
    deps = container.runtime_dependencies_initializer()

    assert deps.oms is not None
    assert deps.replay is not None


def test_same_lifecycle_instance_is_used_by_worker_app() -> None:
    settings = _load("LIVE")
    container = build_dependency_container(settings)

    assert container.worker_app._lifecycle is container.lifecycle_service
