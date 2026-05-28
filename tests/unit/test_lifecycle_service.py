from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from runtime.application.lifecycle.lifecycle_service import LifecycleService
from runtime.infrastructure.strategy_loader.artifact_fetcher import ArtifactFetchResult
from runtime.infrastructure.strategy_loader.artifact_verifier import (
    ArtifactVerificationResult,
)
from runtime.infrastructure.strategy_loader.entrypoint_loader import (
    EntrypointLoadResult,
)
from runtime.domain.bootstrap_failures import (
    ArtifactFetchFailure,
    BootstrapFailure,
    EntrypointLoadFailure,
    SDKContractFailure,
)
from runtime.bootstrap.minimal_env_validation import (
    SWR_ARTIFACT_NOT_FOUND,
    SWR_ENTRYPOINT_INVALID,
    SWR_SDK_COMPATIBILITY_FAILED,
)
from runtime.domain.launch_spec import LaunchSpec
from runtime.bootstrap.sdk_contract_validator import (
    BootstrapPipelineResult,
    BootstrapPipelineSuccess,
    SdkContractValidationResult,
)
from runtime.bootstrap.strategy_instance_manager import (
    StrategyInstanceManager,
)
from runtime.bootstrap.validator import LaunchSpecValidator
from runtime.domain.enums import WorkerPhase
from runtime.domain.worker_identity import WorkerIdentity
from runtime.domain.events.dedupe import LifecycleSignalDedupe
from runtime.infrastructure.clock.clock import SimulatedClock, SystemClock
from runtime.infrastructure.http.srm.manager_gateway import ManagerGateway
from runtime.infrastructure.observability.logger import (
    RuntimeLogContext,
    bind_runtime_context,
)
from runtime.application.runtime_dependencies import RuntimeDependencies
from runtime.domain.policies.mode_policy import get_mode_policy


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class _FakeManagerClient:
    def __init__(self) -> None:
        self.signals: list[dict[str, Any]] = []
        self.close_calls = 0

    def emit_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.signals.append(payload)
        return {"accepted": True}

    def close(self) -> None:
        self.close_calls += 1


class _FailingEmitManagerClient(_FakeManagerClient):
    def emit_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("manager_emit_failed")


@dataclass
class _PipelineStub:
    result: BootstrapPipelineResult

    def run(self, launch_spec: LaunchSpec) -> BootstrapPipelineResult:
        return self.result


class _WorkAcceptor:
    def __init__(self) -> None:
        self.calls = 0

    def stop_accepting_new_work(self) -> None:
        self.calls += 1


class _PeriodicJobs:
    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0

    def start(self) -> None:
        self.start_calls += 1

    def stop(self) -> None:
        self.stop_calls += 1


class _DiagnosticFlusher:
    def __init__(self) -> None:
        self.calls = 0

    def flush(self) -> None:
        self.calls += 1


def _launch_payload(mode: str = "PAPER") -> dict[str, object]:
    payload: dict[str, object] = {
        "runtime_id": "rt-1",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": mode,
        "artifact_uri": "file:///tmp/strategy",
        "artifact_digest": "sha256:abcd",
        "entrypoint": "strategy.main:Strategy",
        "launch_attempt": 1,
    }
    if mode == "BACKTEST":
        payload["job_id"] = "job-1"
        payload["ts_start"] = "2020-01-01T00:00:00Z"
        payload["ts_end"] = "2020-01-02T00:00:00Z"
    return payload


def _identity(spec: LaunchSpec) -> WorkerIdentity:
    return WorkerIdentity(
        runtime_id=spec.runtime_id,
        tenant_id=spec.tenant_id,
        account_id=spec.account_id,
        trader_id=spec.trader_id,
        strategy_version_id=spec.strategy_version_id,
        mode=spec.mode,
        artifact_uri=spec.artifact_uri,
        artifact_digest=spec.artifact_digest,
        entrypoint=spec.entrypoint,
        launch_attempt=spec.launch_attempt,
    )


def _logger_binder(spec: LaunchSpec, handler: _CaptureHandler):
    logger = logging.getLogger(f"test-lifecycle-{id(handler)}")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    context = RuntimeLogContext(
        runtime_id=spec.runtime_id,
        tenant_id=spec.tenant_id,
        worker_identity=f"{spec.runtime_id}:{spec.launch_attempt}",
        launch_attempt=spec.launch_attempt,
        account_id=spec.account_id,
        strategy_version_id=spec.strategy_version_id,
        mode=spec.mode.value,
    )
    return lambda: bind_runtime_context(logger, context)


def _runtime_dependencies(
    spec: LaunchSpec, manager_client: _FakeManagerClient
) -> RuntimeDependencies:
    return RuntimeDependencies(
        clock=SystemClock(),
        manager=ManagerGateway(
            get_mode_policy(spec.mode),
            manager_client,
            _identity(spec),
            dedupe=LifecycleSignalDedupe(),
        ),
        risk_order_intent=None,
    )


def _bootstrap_success(symbol: object) -> BootstrapPipelineResult:
    return BootstrapPipelineResult(
        success=True,
        success_payload=BootstrapPipelineSuccess(
            artifact=ArtifactFetchResult(
                artifact_reference="file:///tmp/strategy",
                materialized_root=Path("."),
                expected_digest="sha256:abcd",
            ),
            verification=ArtifactVerificationResult(
                verified=True,
                algorithm="sha256",
                expected_digest="sha256:abcd",
                actual_digest="sha256:abcd",
            ),
            entrypoint=EntrypointLoadResult(
                entrypoint_spec="strategy.main:Strategy",
                module_name="strategy.main",
                symbol_name="Strategy",
                symbol=symbol,
            ),
            sdk_validation=SdkContractValidationResult(
                is_valid=True,
                sdk_version_marker="1.0",
                validated_type="Strategy",
            ),
        ),
    )


def test_should_stop_for_completed_backtest_job_after_end_of_stream() -> None:
    payload = _launch_payload("BACKTEST")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    clock = SimulatedClock()

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(spec.mode),
                manager_client,
                _identity(spec),
                dedupe=LifecycleSignalDedupe(),
            ),
            risk_order_intent=None,
        )

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=StrategyInstanceManager(),
        periodic_jobs=_PeriodicJobs(),
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
    )
    lifecycle.start()
    lifecycle.run()
    assert lifecycle.should_stop_for_completed_backtest_job() is False
    lifecycle.mark_backtest_market_data_stream_complete()
    assert lifecycle.should_stop_for_completed_backtest_job() is True


def test_should_stop_for_completed_backtest_job_false_for_paper() -> None:
    payload = _launch_payload("PAPER")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        periodic_jobs=_PeriodicJobs(),
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
    )
    lifecycle.start()
    lifecycle.run()
    assert lifecycle.should_stop_for_completed_backtest_job() is False


def test_startup_and_shutdown_run_in_exact_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SWR_MARKET_DATA_REDIS_URL", raising=False)
    monkeypatch.delenv("STRATEGY_DEPLOYMENT_SERVICE_BASE_URL", raising=False)
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    step_events: list[str] = []

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    periodic = _PeriodicJobs()
    settings = SimpleNamespace(
        strategy_runtime_manager_base_url="http://127.0.0.1:8080",
        deployment_id="dep-test",
        runtime_manager_heartbeat_timeout_seconds=30.0,
    )
    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        work_acceptor=_WorkAcceptor(),
        periodic_jobs=periodic,
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
        on_step=step_events.append,
        worker_runtime_settings=settings,
    )

    lifecycle.start()
    with patch.object(lifecycle._host.srm, "report_final_shutdown") as final_mock:
        stopped = lifecycle.stop()
    final_mock.assert_called_once()

    assert periodic.start_calls == 1
    assert periodic.stop_calls == 1

    assert stopped is True
    assert lifecycle.phase is WorkerPhase.STOPPED
    assert lifecycle.startup_steps == (
        "validate_launch_metadata",
        "bind_observability_context",
        "resolve_artifact_and_entrypoint",
        "validate_sdk_contract",
        "initialize_runtime_dependencies",
        "initialize_strategy_instance_coordinator",
        "emit_worker_bootstrap_ready_signal",
        "emit_initial_srm_heartbeat",
        "start_heartbeat_periodic_jobs",
    )
    assert lifecycle.shutdown_steps == (
        "stop_live_market_data_redis_feed",
        "stop_portfolio_update_redis_feed",
        "stop_heartbeat_periodic_jobs",
        "stop_accepting_new_work",
        "stop_strategy_loop",
        "flush_diagnostics",
        "close_downstream_clients",
        "emit_worker_shutdown_signal",
    )
    assert "validate_launch_metadata" in step_events
    assert manager_client.signals[0]["signal_type"] == "bootstrap_succeeded"
    assert manager_client.signals[-1]["signal_type"] == "terminated"
    assert manager_client.signals[-1]["payload"]["reason_code"] == "STOP_REQUESTED"


def test_backtest_runtime_job_completed_stop_sets_phase_completed() -> None:
    payload = _launch_payload("BACKTEST")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    clock = SimulatedClock()

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(spec.mode),
                manager_client,
                _identity(spec),
                dedupe=LifecycleSignalDedupe(),
            ),
            risk_order_intent=None,
        )

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=StrategyInstanceManager(),
        periodic_jobs=_PeriodicJobs(),
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
    )
    lifecycle.start()
    lifecycle.stop(termination_reason_code="RUNTIME_JOB_COMPLETED")

    assert lifecycle.phase is WorkerPhase.COMPLETED
    terminated = [s for s in manager_client.signals if s["signal_type"] == "terminated"]
    assert len(terminated) == 1
    assert terminated[0]["payload"]["local_state"] == "COMPLETED"


def test_runtime_job_completed_on_paper_ends_in_stopped() -> None:
    payload = _launch_payload("PAPER")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        periodic_jobs=_PeriodicJobs(),
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
    )
    lifecycle.start()
    lifecycle.stop(termination_reason_code="RUNTIME_JOB_COMPLETED")

    assert lifecycle.phase is WorkerPhase.STOPPED


def test_stop_emits_error_detected_when_phase_is_failed() -> None:
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        periodic_jobs=_PeriodicJobs(),
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
    )
    lifecycle.start()
    lifecycle._phase = WorkerPhase.FAILED  # noqa: SLF001
    lifecycle.stop()

    assert manager_client.signals[-1]["signal_type"] == "terminated"
    assert (
        manager_client.signals[-1]["payload"]["reason_code"]
        == "UNHEALTHY_EXECUTION_DETECTED"
    )
    assert (
        manager_client.signals[-1]["payload"]["message"]
        == "Shutdown after runtime detected failure."
    )


@pytest.mark.parametrize(
    (
        "failure",
        "expected_srm_reason_code",
        "expected_passed",
        "expected_failed",
        "expected_not_checked",
    ),
    [
        (
            ArtifactFetchFailure(reason_code="artifact_not_found", retryable=False),
            SWR_ARTIFACT_NOT_FOUND,
            [],
            ["artifact_fetch"],
            ["artifact_verify", "entrypoint_load", "sdk_validate"],
        ),
        (
            EntrypointLoadFailure(
                reason_code="entrypoint_import_failed", retryable=False
            ),
            SWR_ENTRYPOINT_INVALID,
            ["artifact_fetch", "artifact_verify"],
            ["entrypoint_load"],
            ["sdk_validate"],
        ),
        (
            SDKContractFailure(reason_code="sdk_marker_incompatible", retryable=False),
            SWR_SDK_COMPATIBILITY_FAILED,
            ["artifact_fetch", "artifact_verify", "entrypoint_load"],
            ["sdk_validate"],
            [],
        ),
    ],
)
def test_startup_aborts_for_bootstrap_failures(
    failure: BootstrapFailure,
    expected_srm_reason_code: str,
    expected_passed: list[str],
    expected_failed: list[str],
    expected_not_checked: list[str],
) -> None:
    _ = (expected_passed, expected_failed, expected_not_checked)
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    pipeline = _PipelineStub(BootstrapPipelineResult(success=False, failure=failure))
    settings = SimpleNamespace(
        strategy_runtime_manager_base_url="http://127.0.0.1:8080",
        deployment_id="dep-test",
        runtime_manager_heartbeat_timeout_seconds=30.0,
    )

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=pipeline,
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        worker_runtime_settings=settings,
    )

    with patch.object(lifecycle._host.srm, "report_bootstrap_failure") as report_mock:
        with pytest.raises(type(failure)):
            lifecycle.start()

    assert lifecycle.phase is WorkerPhase.FAILED
    assert not any(
        signal.get("signal_type") == "bootstrap_failed"
        for signal in manager_client.signals
    )
    report_mock.assert_called_once()
    assert report_mock.call_args.args[0] is failure
    assert report_mock.call_args.kwargs["runtime_id"] == spec.runtime_id
    from runtime.bootstrap.srm_env_status_report import (
        bootstrap_failure_srm_reason_code,
    )

    assert bootstrap_failure_srm_reason_code(failure) == expected_srm_reason_code


def test_partial_startup_failure_rolls_back_safely() -> None:
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _FailingCtor:
        def __init__(self) -> None:
            raise RuntimeError("ctor_failed")

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_FailingCtor)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        closeables=(manager_client,),
    )

    with pytest.raises(RuntimeError, match="ctor_failed"):
        lifecycle.start()

    assert lifecycle.phase is WorkerPhase.FAILED
    assert any(
        signal["signal_type"] == "bootstrap_failed" for signal in manager_client.signals
    )
    assert manager_client.close_calls >= 1


def test_duplicate_stop_is_idempotent() -> None:
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        closeables=(manager_client,),
    )
    lifecycle.start()

    first = lifecycle.stop()
    second = lifecycle.stop()

    terminated_signals = [
        s for s in manager_client.signals if s["signal_type"] == "terminated"
    ]
    assert first is True
    assert second is False
    assert len(terminated_signals) == 1


def test_stop_with_emit_termination_signal_false_skips_runtime_terminated() -> None:
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        closeables=(manager_client,),
    )
    lifecycle.start()
    assert manager_client.signals[0]["signal_type"] == "bootstrap_succeeded"

    lifecycle.stop(emit_termination_signal=False)

    assert lifecycle.phase is WorkerPhase.STOPPED
    terminated = [s for s in manager_client.signals if s["signal_type"] == "terminated"]
    assert terminated == []


@pytest.mark.parametrize(
    ("suppress_stopping", "expected_state_event_lines"),
    [
        (True, 1),
        (False, 2),
    ],
)
def test_stop_internal_state_stdout_respects_suppress_stopping_phase(
    suppress_stopping: bool,
    expected_state_event_lines: int,
) -> None:
    """Manager StopWorker path suppresses STOPPING stdout; default stop logs STOPPING+STOPPED."""
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        closeables=(manager_client,),
    )
    lifecycle.start()

    captured: list[str] = []
    real_write = sys.stdout.write

    def _recording_write(text: str) -> int:
        captured.append(text)
        return real_write(text)

    with patch("sys.stdout.write", side_effect=_recording_write):
        lifecycle.stop(
            emit_termination_signal=False,
            suppress_stopping_phase_stdout=suppress_stopping,
        )

    state_lines = [
        line
        for chunk in captured
        for line in chunk.splitlines()
        if "worker.shutdown.completed" in line or "worker.state.changed" in line
    ]
    assert len(state_lines) == expected_state_event_lines
    assert any("worker.shutdown.completed" in line for line in state_lines)


def test_no_manager_owned_lifecycle_truth_emitted() -> None:
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()

    class _Strategy:
        def run(self, context: object) -> object:
            return context

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_Strategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        closeables=(manager_client,),
    )

    lifecycle.start()
    lifecycle.stop()

    forbidden = {"runtime.started", "runtime.failed", "runtime.degraded"}
    emitted_event_names = [
        str(getattr(record, "event", {}).get("event_name", ""))
        for record in handler.records
    ]
    assert forbidden.isdisjoint(emitted_event_names)


def test_manager_stop_checkpoint_uses_last_handled_market_timestamp_in_backtest() -> (
    None
):
    payload = _launch_payload("BACKTEST")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    clock = SimulatedClock()
    state_journal = MagicMock()

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(spec.mode),
                manager_client,
                _identity(spec),
                dedupe=LifecycleSignalDedupe(),
            ),
            risk_order_intent=None,
        )

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=None,
        state_journal=state_journal,
    )

    market_ts = datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
    with lifecycle._replay_state_lock:
        lifecycle._last_data_event_timestamp = market_ts
    lifecycle.record_manager_stop_request_checkpoint()

    state_journal.record_manager_stop_request.assert_called_once()
    kwargs = state_journal.record_manager_stop_request.call_args.kwargs
    assert kwargs["last_data_event_at"] == market_ts
    assert kwargs["last_replay_cursor"] == ""


def test_manager_stop_checkpoint_records_empty_replay_cursor_without_batch_ingress() -> (
    None
):
    payload = _launch_payload("BACKTEST")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    clock = SimulatedClock()
    state_journal = MagicMock()

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(spec.mode),
                manager_client,
                _identity(spec),
                dedupe=LifecycleSignalDedupe(),
            ),
            risk_order_intent=None,
        )

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=None,
        state_journal=state_journal,
    )
    lifecycle.record_manager_stop_request_checkpoint()

    state_journal.record_manager_stop_request.assert_called_once()
    kwargs = state_journal.record_manager_stop_request.call_args.kwargs
    assert kwargs["last_replay_cursor"] == ""


def test_emit_periodic_heartbeat_emits_manager_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SWR_MARKET_DATA_REDIS_URL", raising=False)
    monkeypatch.delenv("STRATEGY_DEPLOYMENT_SERVICE_BASE_URL", raising=False)
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    state_journal = MagicMock()
    handler = _CaptureHandler()

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=None,
        state_journal=state_journal,
    )
    lifecycle._runtime_dependencies = _runtime_dependencies(spec, manager_client)  # noqa: SLF001
    lifecycle._phase = WorkerPhase.RUNNING  # noqa: SLF001

    lifecycle.emit_periodic_heartbeat()

    heartbeats = [
        s for s in manager_client.signals if s.get("signal_type") == "heartbeat"
    ]
    assert len(heartbeats) == 1
    assert heartbeats[0]["payload"]["local_state"] == WorkerPhase.RUNNING.value
    assert isinstance(heartbeats[0]["payload"]["observed_at"], datetime)


def test_bootstrap_failure_persists_launch_failed_when_manager_unavailable() -> None:
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    handler = _CaptureHandler()
    state_journal = MagicMock()
    failure = ArtifactFetchFailure(reason_code="artifact_not_found", retryable=False)
    pipeline = _PipelineStub(BootstrapPipelineResult(success=False, failure=failure))

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=pipeline,
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: (_ for _ in ()).throw(
            RuntimeError("deps init failed")
        ),
        state_journal=state_journal,
    )

    with pytest.raises(ArtifactFetchFailure):
        lifecycle.start()

    state_journal.record_launch_failed_event.assert_called_once()
    kwargs = state_journal.record_launch_failed_event.call_args.kwargs
    assert kwargs["reason_code"] == "ARTIFACT_REFERENCE_INVALID"


def test_bootstrap_failure_persists_launch_failed_when_manager_emit_fails() -> None:
    payload = _launch_payload()
    spec = LaunchSpec.from_payload(payload)
    handler = _CaptureHandler()
    state_journal = MagicMock()
    manager_client = _FailingEmitManagerClient()
    failure = ArtifactFetchFailure(reason_code="artifact_not_found", retryable=False)
    pipeline = _PipelineStub(BootstrapPipelineResult(success=False, failure=failure))

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=pipeline,
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        state_journal=state_journal,
    )

    with pytest.raises(ArtifactFetchFailure):
        lifecycle.start()

    state_journal.record_launch_failed_event.assert_called_once()
    kwargs = state_journal.record_launch_failed_event.call_args.kwargs
    assert kwargs["reason_code"] == "ARTIFACT_REFERENCE_INVALID"


def test_backtest_lifecycle_bind_and_start_invokes_sdk_hooks() -> None:
    """BACKTEST + simulated clock builds a replay bridge so SdkStrategy can bind context before init."""
    from alphovex_sdk.strategy import Strategy as AlphovexStrategy

    payload = _launch_payload("BACKTEST")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    clock = SimulatedClock()
    recorded: list[str] = []

    class _HookedStrategy(AlphovexStrategy):
        def on_init(self) -> None:
            recorded.append("on_init")

        def on_start(self) -> None:
            recorded.append("on_start")

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(spec.mode),
                manager_client,
                _identity(spec),
                dedupe=LifecycleSignalDedupe(),
            ),
            risk_order_intent=None,
        )

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_HookedStrategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=StrategyInstanceManager(),
        periodic_jobs=_PeriodicJobs(),
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
    )
    from runtime.bootstrap.lifecycle_host_wiring import build_lifecycle_host_ports

    lifecycle.attach_host_ports(build_lifecycle_host_ports(lifecycle))
    lifecycle.start()

    assert recorded == ["on_init", "on_start"]
    assert lifecycle.phase is WorkerPhase.READY
    ready_heartbeats = [
        s
        for s in manager_client.signals
        if s.get("signal_type") == "heartbeat"
        and s.get("payload", {}).get("local_state") == WorkerPhase.READY.value
    ]
    assert len(ready_heartbeats) == 1


def test_paper_lifecycle_bind_and_start_invokes_sdk_hooks() -> None:
    """PAPER builds a replay SDK bridge so SdkStrategy can bind context before init."""
    from alphovex_sdk.strategy import Strategy as AlphovexStrategy

    payload = _launch_payload("PAPER")
    spec = LaunchSpec.from_payload(payload)
    manager_client = _FakeManagerClient()
    handler = _CaptureHandler()
    recorded: list[str] = []

    class _HookedStrategy(AlphovexStrategy):
        def on_init(self) -> None:
            recorded.append("on_init")

        def on_start(self) -> None:
            recorded.append("on_start")

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=_PipelineStub(_bootstrap_success(_HookedStrategy)),
        log_binder=_logger_binder(spec, handler),
        runtime_dependencies_initializer=lambda: _runtime_dependencies(
            spec, manager_client
        ),
        strategy_instance_manager=StrategyInstanceManager(),
        periodic_jobs=_PeriodicJobs(),
        diagnostic_flusher=_DiagnosticFlusher(),
        closeables=(manager_client,),
    )
    from runtime.bootstrap.lifecycle_host_wiring import build_lifecycle_host_ports

    lifecycle.attach_host_ports(build_lifecycle_host_ports(lifecycle))
    lifecycle.start()

    assert recorded == ["on_init", "on_start"]
    assert lifecycle.phase is WorkerPhase.READY
    ready_heartbeats = [
        s
        for s in manager_client.signals
        if s.get("signal_type") == "heartbeat"
        and s.get("payload", {}).get("local_state") == WorkerPhase.READY.value
    ]
    assert len(ready_heartbeats) == 1

    lifecycle.run()
    assert lifecycle.phase is WorkerPhase.RUNNING
    running_heartbeats = [
        s
        for s in manager_client.signals
        if s.get("signal_type") == "heartbeat"
        and s.get("payload", {}).get("local_state") == WorkerPhase.RUNNING.value
    ]
    assert len(running_heartbeats) == 1
