from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from runtime.application.event_handling.event_dispatcher import EventDispatcher
from runtime.application.lifecycle.lifecycle_service import LifecycleService
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.sdk_contract_validator import BootstrapPipelineSuccess
from runtime.bootstrap.strategy_instance_manager import StrategyInstanceManager
from runtime.bootstrap.validator import LaunchSpecValidator
from runtime.infrastructure.config.settings import Settings
from runtime.domain.enums import WorkerMode, WorkerPhase
from runtime.infrastructure.grpc.replay.replay_gateway import ReplayGateway
from runtime.application.runtime_dependencies import RuntimeDependencies
from runtime.domain.policies.mode_policy import get_mode_policy


def _backtest_launch_payload() -> dict[str, object]:
    return {
        "runtime_id": "rt-bt-1",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": "BACKTEST",
        "launch_attempt": 1,
        "artifact_uri": "file:///tmp/strategy",
        "artifact_digest": "sha256:abcd",
        "entrypoint": "strategy.main:Strategy",
        "job_id": "job-1",
        "parameters": {"symbol": "SPY", "bar_timeframe": "1m"},
        "ts_start": "2020-01-01T00:00:00Z",
        "ts_end": "2020-01-02T00:00:00Z",
    }


def _settings_for_backtest() -> Settings:
    launch = LaunchSpec.from_payload(_backtest_launch_payload())
    return Settings(
        launch_spec=launch,
        launch_payload=_backtest_launch_payload(),
        work_root=__import__("pathlib").Path("/tmp"),
        bundle_resolve_base_dir=__import__("pathlib").Path("/tmp"),
        artifact_local_base_path=None,
        state_journal_enabled=False,
        state_journal_sqlite_path=__import__("pathlib").Path("/tmp/j.sqlite"),
        state_journal_txt_path=__import__("pathlib").Path("/tmp/j.txt"),
        strategy_runtime_manager_base_url="http://127.0.0.1:8080",
        runtime_manager_heartbeat_timeout_seconds=30.0,
        heartbeat_log_enabled=False,
        heartbeat_interval_seconds=10.0,
        deployment_id="",
        worker_control_http_bind="127.0.0.1:0",
        replay_ingress_grpc_bind="127.0.0.1:0",
        replay_ingress_grpc_fallback_ports="",
        replay_ingress_grpc_no_fallback=True,
        oms_grpc_target="",
        oms_grpc_timeout_seconds=3.0,
        risk_grpc_target="127.0.0.1:50054",
        risk_grpc_timeout_seconds=3.0,
        replay_bar_timeframe="1m",
        replay_ingress_trace_payload=False,
        replay_tick_logging_quiet=True,
        order_intent_correlation_id="",
        oms_correlation_id="",
        replay_session_id="",
        disable_order_intent_grpc=True,
    )


class _Strategy:
    def on_bar(self, bar: object) -> None:
        pass


@pytest.fixture
def backtest_lifecycle_with_dispatcher() -> LifecycleService:
    payload = _backtest_launch_payload()
    spec = LaunchSpec.from_payload(payload)
    settings = _settings_for_backtest()
    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=MagicMock(),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=lambda: MagicMock(),
        runtime_dependencies_initializer=lambda: RuntimeDependencies(
            clock=MagicMock(),
            manager=MagicMock(),
            risk_order_intent=None,
            replay=ReplayGateway(
                get_mode_policy(WorkerMode.BACKTEST),
                simulated_clock=MagicMock(),
            ),
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        worker_runtime_settings=settings,
    )
    adapter = MagicMock()
    adapter.bind_and_start.return_value = MagicMock(ok=True)
    lifecycle._strategy_adapter = adapter
    lifecycle._runtime_dependencies = lifecycle._runtime_dependencies_initializer()
    lifecycle._ddd_wiring = MagicMock()
    lifecycle._ddd_wiring.event_dispatcher = MagicMock(spec=EventDispatcher)
    lifecycle._set_phase(WorkerPhase.RUNNING)
    return lifecycle


def test_backtest_replay_ingress_routes_through_event_dispatcher(
    backtest_lifecycle_with_dispatcher: LifecycleService,
) -> None:
    lifecycle = backtest_lifecycle_with_dispatcher
    ingest = lifecycle._runtime_dependencies.replay.ingest_replay_tick
    with patch.object(
        type(lifecycle._runtime_dependencies.replay),
        "ingest_replay_tick",
        wraps=ingest,
    ) as mock_ingest:
        lifecycle.push_replay_context(
            {
                "replay": {"replay_session_id": "s1", "replay_cursor": "c1"},
                "events": [
                    {
                        "event_id": "e1",
                        "event_type": "market.bar",
                        "instrument_id": "SPY",
                        "payload": {
                            "type": "market.bar",
                            "symbol": "SPY",
                            "open": 1.0,
                            "high": 1.0,
                            "low": 1.0,
                            "close": 1.0,
                            "volume": 1.0,
                            "ts_ms": 1710000000000,
                        },
                    }
                ],
                "end_of_stream": False,
            }
        )
    mock_ingest.assert_called_once()
    assert mock_ingest.call_args.kwargs.get("strategy_callback") is None
    lifecycle._ddd_wiring.event_dispatcher.dispatch_raw.assert_called_once()


def test_replay_gateway_does_not_invoke_strategy_adapter_directly() -> None:
    adapter = MagicMock()
    gateway = ReplayGateway(
        get_mode_policy(WorkerMode.BACKTEST),
        simulated_clock=MagicMock(),
    )
    tick = {"type": "market.bar", "symbol": "SPY"}
    gateway.ingest_replay_tick(tick, strategy_callback=None)
    adapter.on_event.assert_not_called()


def test_redis_tick_uses_event_dispatcher_not_direct_adapter() -> None:
    payload = {
        "runtime_id": "rt-paper",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "launch_attempt": 1,
        "artifact_uri": "file:///tmp/strategy",
        "artifact_digest": "sha256:abcd",
        "entrypoint": "strategy.main:Strategy",
        "parameters": {"symbol": "AAPL"},
    }
    spec = LaunchSpec.from_payload(payload)
    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=MagicMock(),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=lambda: MagicMock(),
        runtime_dependencies_initializer=lambda: RuntimeDependencies(
            clock=MagicMock(),
            manager=MagicMock(),
            risk_order_intent=None,
            replay=None,
        ),
        strategy_instance_manager=StrategyInstanceManager(),
    )
    adapter = MagicMock()
    lifecycle._strategy_adapter = adapter
    lifecycle._ddd_wiring = MagicMock()
    lifecycle._ddd_wiring.event_dispatcher = MagicMock(spec=EventDispatcher)
    lifecycle._set_phase(WorkerPhase.RUNNING)
    tick = {
        "type": "market.bar",
        "symbol": "AAPL",
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 1.0,
        "ts_ms": 1710000000000,
        "event_time": datetime(2024, 3, 10, 12, 0, tzinfo=timezone.utc),
    }
    lifecycle._dispatch_paper_live_tick(tick)
    lifecycle._ddd_wiring.event_dispatcher.dispatch_raw.assert_called_once_with(tick)
    adapter.on_event.assert_not_called()


def test_submit_order_intent_uses_risk_submission_port() -> None:
    from runtime.application.order_intents.submit_order_intent import SubmitOrderIntent
    from runtime.infrastructure.grpc.risk_gateway_submission_adapter import (
        RiskGatewaySubmissionAdapter,
    )
    from runtime.bootstrap.launch_spec import LaunchSpec
    from runtime.domain.enums import OrderIntentSide, OrderIntentType
    from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
    from runtime.infrastructure.grpc.risk_order_intent_gateway import RiskOrderIntentGateway
    from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
        OrderSubmissionContext,
    )

    inner = MagicMock()
    inner.submit_order_intent.return_value = {"accepted": True}
    gateway = RiskOrderIntentGateway(
        get_mode_policy(WorkerMode.PAPER),
        inner,
    )
    launch_spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
            "correlation_id": "corr-replay-test",
        }
    )
    port = RiskGatewaySubmissionAdapter(
        gateway,
        submission_context=OrderSubmissionContext(
            launch_spec=launch_spec,
            allocate_order_intent_id=lambda: "oi-1",
        ),
    )
    use_case = SubmitOrderIntent(submission_port=port)
    intent = StrategyOrderIntent(
        instrument_id="AAPL",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.MARKET,
        quantity=Decimal("1"),
    )
    outcome = use_case.execute(intent)
    assert outcome.ok is True
    inner.submit_order_intent.assert_called_once()
