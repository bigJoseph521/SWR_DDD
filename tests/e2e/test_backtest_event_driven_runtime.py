from __future__ import annotations

from pathlib import Path

from runtime.bootstrap.dependency_container import build_dependency_container
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from runtime.domain.enums import ServiceTarget, WorkerMode
from runtime.infrastructure.clock.clock import SimulatedClock
from tests.e2e._helpers import (
    FakeManagerClient,
    FakeRiskOrderIntentClient,
    build_bundle_dict,
    write_strategy_package,
)


def test_backtest_runtime_wires_simulated_clock_and_stdout_order_intent_egress(
    tmp_path: Path,
) -> None:
    source_root = write_strategy_package(tmp_path / "source")
    manager = FakeManagerClient()
    risk_client = FakeRiskOrderIntentClient()

    settings = load_settings_from_bundle_dict(
        build_bundle_dict(
            tmp_path=tmp_path,
            source_root=source_root,
            runtime_id="rt-e2e-backtest",
            mode="BACKTEST",
            job_id="bt-1",
        ),
        base_dir=tmp_path,
    )
    container = build_dependency_container(
        settings,
        manager_client=manager,
        risk_order_intent_client=risk_client,
    )

    deps = container.runtime_dependencies_initializer()
    assert deps.risk_order_intent is None
    assert isinstance(deps.clock, SimulatedClock)
    assert container.mode_policy.route_order_intent() is ServiceTarget.BACKTEST_RUNNER
    assert settings.launch_spec.mode is WorkerMode.BACKTEST
