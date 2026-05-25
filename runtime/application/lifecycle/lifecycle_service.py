from __future__ import annotations

import inspect
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol, Sequence

from runtime.application.lifecycle.bootstrap_stages import BOOTSTRAP_STAGE_ORDER
from runtime.application.lifecycle.lifecycle_ddd_wiring import LifecycleDddWiring
from runtime.application.lifecycle.noop_host_ports import noop_lifecycle_host_ports
from runtime.application.lifecycle.stop_errors import (
    StopAlreadyInProgress,
    WorkerAlreadyStopped,
)
from runtime.application.ports.launch_context import LaunchContext
from runtime.application.ports.lifecycle_ports import (
    BootstrapPipelinePort,
    BootstrapSuccessView,
    LaunchMetadataValidatorPort,
    LifecycleHostPorts,
    RuntimeLoggerPort,
    StateJournalPort,
    StrategyInstanceCoordinatorPort,
)
from runtime.application.ports.worker_domain_events import StrategyWorkerDomainEvent
from runtime.application.runtime_dependencies import RuntimeDependencies
from runtime.application.runtime_state.runtime_state import RuntimeState
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.application.strategy_execution.strategy_error_boundary import StrategyCallResult
from runtime.domain.enums import WorkerMode, WorkerPhase
from runtime.domain.errors import (
    RuntimeStartValidationFailedError,
    normalize_runtime_reason_code,
)
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec
from runtime.domain.worker_identity import WorkerIdentity




class WorkAcceptor(Protocol):
    def stop_accepting_new_work(self) -> None: ...


class PeriodicJobController(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...


class DiagnosticFlusher(Protocol):
    def flush(self) -> None: ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _classify_launch_field_errors(
    field_errors: Mapping[str, Any],
) -> tuple[list[str], list[str], list[str], dict[str, Any]]:
    missed: set[str] = set()
    invalid: set[str] = set()
    empty: set[str] = set()
    details: dict[str, Any] = {}

    for field, code_raw in field_errors.items():
        key = str(field)
        code = str(code_raw)
        lower = code.lower()
        if key == "payload" and code.startswith("unknown_fields:"):
            unknown = [x for x in code.split(":", 1)[1].split(",") if x]
            if unknown:
                invalid.update(unknown)
                details["unknown_fields"] = unknown
            continue
        if "required_field_missing" in lower or "required" in lower:
            missed.add(key)
            continue
        if (
            "must_not_be_empty" in lower
            or "must_not_be_blank" in lower
            or "blank" in lower
        ):
            empty.add(key)
            continue
        invalid.add(key)

    return (sorted(missed), sorted(invalid), sorted(empty), details)


class LifecycleService:
    def __init__(
        self,
        *,
        launch_spec: LaunchContext,
        launch_payload: Mapping[str, object],
        worker_identity: WorkerIdentity,
        launch_spec_validator: LaunchMetadataValidatorPort,
        bootstrap_pipeline: BootstrapPipelinePort,
        log_binder: Callable[[], RuntimeLoggerPort],
        runtime_dependencies_initializer: Callable[[], RuntimeDependencies],
        strategy_instance_manager: StrategyInstanceCoordinatorPort | None = None,
        work_acceptor: WorkAcceptor | None = None,
        periodic_jobs: PeriodicJobController | None = None,
        diagnostic_flusher: DiagnosticFlusher | None = None,
        closeables: Sequence[object] = (),
        on_step: Callable[[str], None] | None = None,
        state_journal: StateJournalPort | None = None,
        worker_runtime_settings: Any | None = None,
        ddd_wiring_builder: Callable[..., LifecycleDddWiring] | None = None,
        host_ports: LifecycleHostPorts | None = None,
    ) -> None:
        self._launch_spec = launch_spec
        self._launch_payload = dict(launch_payload)
        self._worker_runtime_settings = worker_runtime_settings
        self._live_md_feed_stop = threading.Event()
        self._live_md_feed_thread: threading.Thread | None = None
        self._live_md_feed_started = False
        self._backtest_stdin_feed: object | None = None
        self._backtest_stdin_feed_started = False
        self._portfolio_update_feed_stop = threading.Event()
        self._portfolio_update_feed_thread: threading.Thread | None = None
        self._portfolio_update_feed_started = False
        self._worker_identity = worker_identity
        self._validator = launch_spec_validator
        self._bootstrap_pipeline = bootstrap_pipeline
        self._log_binder = log_binder
        self._runtime_dependencies_initializer = runtime_dependencies_initializer
        self._strategy_instance_manager = strategy_instance_manager
        self._work_acceptor = work_acceptor
        self._periodic_jobs = periodic_jobs
        self._diagnostic_flusher = diagnostic_flusher
        self._closeables = list(closeables)
        self._on_step = on_step
        self._state_journal = state_journal
        self._ddd_wiring_builder = ddd_wiring_builder
        self._host = host_ports or noop_lifecycle_host_ports()

        self._runtime_logger: RuntimeLoggerPort | None = None
        self._runtime_dependencies: RuntimeDependencies | None = None
        self._strategy_adapter: StrategyAdapter | None = None
        self._phase = WorkerPhase.INITIALIZING
        self._runtime_state = RuntimeState(initial_phase=self._phase)
        self._ddd_wiring: LifecycleDddWiring | None = None
        self._started = False
        self._shutdown_complete = False
        self._shutdown_in_progress = False
        self._stop_initiate_lock = threading.Lock()
        self._stop_request_accepted = False
        self._stop_canonical_reason: str | None = None
        self._stop_resolved_message: str | None = None
        self._startup_steps: list[str] = []
        self._shutdown_steps: list[str] = []
        self._first_data_received = False
        self._backtest_replay_complete = False
        self._last_data_event_timestamp: datetime | None = None
        self._last_replay_cursor: str = ""
        self._replay_state_lock = threading.Lock()
        self._emit_state_message(state=self._phase, level="INFO")
        if self._state_journal is not None:
            self._state_journal.record_phase_change(
                previous_phase=None,
                phase=self._phase,
                level="INFO",
                reason_code=None,
            )

    def attach_host_ports(self, host_ports: LifecycleHostPorts) -> None:
        """Replace noop host ports with bootstrap-wired concrete adapters."""
        self._host = host_ports

    @property
    def phase(self) -> WorkerPhase:
        return self._phase

    @property
    def startup_steps(self) -> tuple[str, ...]:
        return tuple(self._startup_steps)

    @property
    def shutdown_steps(self) -> tuple[str, ...]:
        return tuple(self._shutdown_steps)

    @property
    def first_data_received(self) -> bool:
        return self._runtime_state.first_data_received or self._first_data_received

    @property
    def backtest_replay_complete(self) -> bool:
        return self._runtime_state.backtest_replay_complete or self._backtest_replay_complete

    def should_stop_for_completed_backtest_job(self) -> bool:
        """
        True in BACKTEST when stdin market-data stream ended and the worker should shut down.

        Callers should invoke :meth:`stop` with ``termination_reason_code="RUNTIME_JOB_COMPLETED"``
        so ``runtime.terminated`` is distinct from operator stop (``STOP_REQUESTED``).
        """
        if self._shutdown_complete or self._shutdown_in_progress:
            return False
        if self._launch_spec.mode is not WorkerMode.BACKTEST:
            return False
        if not self.backtest_replay_complete:
            return False
        return self._phase in (WorkerPhase.READY, WorkerPhase.RUNNING)

    @property
    def runtime_metadata(self) -> dict[str, object]:
        return {
            "runtime_id": self._launch_spec.runtime_id,
            "tenant_id": self._launch_spec.tenant_id,
            "account_id": self._launch_spec.account_id or "",
            "strategy_version_id": self._launch_spec.strategy_version_id,
            "launch_attempt": self._launch_spec.launch_attempt,
            "worker_identity": self._worker_identity_value(),
        }

    def mark_first_data_received(self) -> None:
        self._first_data_received = True
        self._runtime_state.mark_first_data_received()
        if self._state_journal is not None:
            self._state_journal.record_first_data()

    def _market_data_stream_log(
        self,
        *,
        level: str,
        event_name: str,
        message: str,
        fields: Mapping[str, Any],
    ) -> None:
        """Structured logs for the Redis market-data stream thread (stdout fallback without logger)."""
        lg = self._runtime_logger
        lvl = getattr(logging, level.upper(), logging.INFO)
        merged = dict(fields)
        if lg is not None:
            try:
                lg.emit(
                    event_name=event_name,
                    message=message,
                    level=lvl,
                    event_extras=merged,
                )
            except Exception:
                print(
                    json.dumps(
                        {"event_name": event_name, "message": message, **merged},
                        default=str,
                        sort_keys=True,
                    ),
                    flush=True,
                )
        else:
            if lvl == logging.DEBUG:
                return
            print(
                json.dumps(
                    {"event_name": event_name, "message": message, **merged},
                    default=str,
                    sort_keys=True,
                ),
                flush=True,
            )

    def emit_periodic_heartbeat(self) -> None:
        """Emit operational heartbeat to strategy-runtime-manager (REST when configured)."""
        if self._shutdown_in_progress:
            return
        if self._runtime_dependencies is None:
            return
        hb = self._ddd_wiring.heartbeat_service if self._ddd_wiring else None
        if hb is not None:
            hb.emit_periodic_heartbeat()
            return
        if self._phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
            return
        now = _utc_now()
        count, sample = self._host.domain_events.next("heartbeat_sent")
        self._emit_domain_event(
            StrategyWorkerDomainEvent.HEARTBEAT_SENT,
            "Worker heartbeat sent to strategy-runtime-manager",
            level=logging.INFO if sample else logging.DEBUG,
            event_extras={
                "phase": self._phase.value,
                "heartbeat_count": count,
                "sampled": sample,
            },
        )
        self._emit_srm_runtime_status(observed_at=now)

    def _emit_srm_runtime_status(
        self,
        *,
        observed_at: datetime | None = None,
        source: str | None = None,
        reason_code: str | None = None,
        message: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        if self._runtime_dependencies is None:
            return
        hb = self._ddd_wiring.heartbeat_service if self._ddd_wiring else None
        if hb is not None:
            hb.emit_runtime_status(
                observed_at=observed_at,
                source=str(
                    source
                    if source is not None
                    else self._host.srm_status_source_heartbeat
                ),
                reason_code=reason_code,
                message=message,
                retryable=retryable,
            )
            return
        when = observed_at if observed_at is not None else _utc_now()
        self._runtime_dependencies.manager.emit_heartbeat(
            local_state=self._phase.value,
            observed_at=when,
            source=str(
                source
                if source is not None
                else self._host.srm_status_source_heartbeat
            ),
            reason_code=reason_code,
            message=message,
            retryable=retryable,
        )

    def _report_bootstrap_success_to_srm(self) -> None:
        wrs = self._worker_runtime_settings
        if wrs is None:
            return
        srm_base_url = str(
            getattr(wrs, "strategy_runtime_manager_base_url", "") or ""
        ).strip()
        if not srm_base_url:
            return
        deployment_id = str(getattr(wrs, "deployment_id", "") or "").strip()
        timeout_seconds = float(
            getattr(wrs, "runtime_manager_heartbeat_timeout_seconds", 30.0) or 30.0
        )
        self._host.srm.report_bootstrap_success(
            runtime_id=self._launch_spec.runtime_id,
            mode=self._launch_spec.mode,
            owner_resource_id=deployment_id,
            srm_base_url=srm_base_url,
            timeout_seconds=timeout_seconds,
        )


    def initiate_stop_from_request(self, reason: str) -> dict[str, object]:
        """
        Handle ``POST /internal/v1/stop``: report STOPPING to SRM and return that status body.

        Caller must invoke :meth:`stop` afterward (typically on a background thread).
        """
        wrs = self._worker_runtime_settings
        if wrs is None:
            raise RuntimeError("worker_runtime_settings required for stop API")
        srm_base_url = str(
            getattr(wrs, "strategy_runtime_manager_base_url", "") or ""
        ).strip()
        if not srm_base_url:
            raise RuntimeError("STRATEGY_RUNTIME_MANAGER_BASE_URL is not configured")
        deployment_id = str(getattr(wrs, "deployment_id", "") or "").strip()
        timeout_seconds = float(
            getattr(wrs, "runtime_manager_heartbeat_timeout_seconds", 30.0) or 30.0
        )
        with self._stop_initiate_lock:
            if self._shutdown_complete:
                raise WorkerAlreadyStopped("Worker is already stopped.")
            if self._stop_request_accepted or self._shutdown_in_progress:
                raise StopAlreadyInProgress("Stop is already in progress.")
            result = self._host.srm.initiate_stop_status_update(
                    runtime_id=self._launch_spec.runtime_id,
                    mode=self._launch_spec.mode.value,
                    owner_resource_id=deployment_id,
                    srm_base_url=srm_base_url,
                    reason=reason,
                    timeout_seconds=timeout_seconds,
                )
            body = result.get("body", result)
            canonical = str(result.get("canonical", ""))
            resolved_message = str(result.get("resolved_message", ""))
            self._stop_request_accepted = True
            self._stop_canonical_reason = canonical
            self._stop_resolved_message = resolved_message
            self._shutdown_in_progress = True
            return dict(body) if isinstance(body, Mapping) else {"status": str(body)}

    def _report_final_shutdown_to_srm(
        self,
        *,
        canonical_reason: str,
        message: str | None,
    ) -> None:
        wrs = self._worker_runtime_settings
        if wrs is None:
            return
        srm_base_url = str(
            getattr(wrs, "strategy_runtime_manager_base_url", "") or ""
        ).strip()
        if not srm_base_url:
            return
        deployment_id = str(getattr(wrs, "deployment_id", "") or "").strip()
        timeout_seconds = float(
            getattr(wrs, "runtime_manager_heartbeat_timeout_seconds", 30.0) or 30.0
        )
        try:
            self._host.srm.report_final_shutdown(
                runtime_id=self._launch_spec.runtime_id,
                mode=self._launch_spec.mode.value,
                owner_resource_id=deployment_id,
                srm_base_url=srm_base_url,
                canonical_reason=canonical_reason,
                message=message,
                timeout_seconds=timeout_seconds,
            )
        except Exception:
            pass

    def mark_backtest_market_data_stream_complete(self) -> None:
        """Mark BACKTEST stdin market-data stream complete (``END_OF_STREAM`` or EOF)."""
        if self._launch_spec.mode is not WorkerMode.BACKTEST:
            return
        self._runtime_state.mark_backtest_replay_complete()
        self._backtest_replay_complete = True

    def _peek_last_data_event_timestamp(self) -> datetime | None:
        """UTC instant of the latest ingested bar/tick/quote (simulated or live feed)."""
        return self._runtime_state.last_data_event_timestamp

    def record_manager_stop_request_checkpoint(self) -> None:
        if self._state_journal is None:
            return
        stop_at = _utc_now()
        last_clock: datetime | None = None
        if self._runtime_dependencies is not None:
            try:
                last_clock = self._runtime_dependencies.clock.now()
            except Exception:
                last_clock = None
        with self._replay_state_lock:
            last_data_event_at = self._last_data_event_timestamp
            last_replay_cursor = self._last_replay_cursor
        self._state_journal.record_manager_stop_request(
            last_data_event_at=last_data_event_at,
            last_clock_at=last_clock,
            last_replay_cursor=last_replay_cursor,
            stop_requested_at=stop_at,
        )

    def run(self) -> None:
        self.start()
        if self._phase is WorkerPhase.READY:
            self._set_phase(WorkerPhase.RUNNING)
            self.emit_periodic_heartbeat()
            self._host.start_market_data_feed()
            self._host.start_portfolio_update_feed()

    def start(self) -> None:
        if self._started:
            return

        bootstrap_success: BootstrapSuccessView | None = None
        try:
            self._record_startup_step("validate_launch_metadata")
            validated = self._validator.validate(self._launch_payload)
            if validated != self._launch_spec:
                raise ValueError(
                    "Validated launch payload does not match approved launch spec."
                )

            self._record_startup_step("bind_observability_context")
            self._runtime_logger = self._log_binder()
            self._emit_domain_event(
                StrategyWorkerDomainEvent.STARTED,
                "Strategy worker runtime startup began",
                event_extras={"phase": WorkerPhase.INITIALIZING.value},
            )

            self._record_startup_step("resolve_artifact_and_entrypoint")
            bootstrap_result = self._bootstrap_pipeline.run(self._launch_spec)
            if not bootstrap_result.success or bootstrap_result.success_payload is None:
                assert bootstrap_result.failure is not None
                raise bootstrap_result.failure
            bootstrap_success = bootstrap_result.success_payload

            self._record_startup_step("validate_sdk_contract")

            self._record_startup_step("initialize_runtime_dependencies")
            self._runtime_dependencies = self._runtime_dependencies_initializer()

            self._record_startup_step("initialize_strategy_instance_coordinator")
            self._strategy_adapter = self._initialize_strategy_coordinator(
                bootstrap_success
            )

            if self._strategy_adapter is not None:
                if self._runtime_dependencies is not None:
                    if self._ddd_wiring_builder is not None:
                        self._ddd_wiring = self._ddd_wiring_builder(
                            launch_mode=self._launch_spec.mode,
                            worker_runtime_settings=self._worker_runtime_settings,
                            strategy_adapter=self._strategy_adapter,
                            runtime_state=self._runtime_state,
                            manager_gateway=self._runtime_dependencies.manager,
                            shutdown_in_progress=lambda: self._shutdown_in_progress,
                            emit_domain_event=self._emit_domain_event,
                            domain_event_sampler=self._host.domain_events,
                            mode=self._launch_spec.mode,
                        )
                bind_result = self._strategy_adapter.bind_and_start()
                if not bind_result.ok:
                    exc = RuntimeError(
                        "Strategy bind_and_start failed during worker startup.",
                    )
                    setattr(exc, "reason_code", "STRATEGY_BIND_AND_START_FAILED")
                    if bind_result.exception is not None:
                        exc.__cause__ = bind_result.exception
                    raise exc
                self._emit_domain_event(
                    StrategyWorkerDomainEvent.STRATEGY_LOADED,
                    "Strategy instance loaded and bound",
                    event_extras={
                        "entrypoint": bootstrap_success.entrypoint.entrypoint_spec,
                        "symbol": (self._launch_spec.symbol or "").strip() or None,
                    },
                )

            self._record_startup_step("emit_worker_bootstrap_ready_signal")
            occurred_at = _utc_now()
            if self._runtime_dependencies is not None:
                self._runtime_dependencies.manager.emit_bootstrap_succeeded(
                    local_state=WorkerPhase.READY.value,
                    occurred_at=occurred_at,
                    details={
                        "entrypoint": bootstrap_success.entrypoint.entrypoint_spec
                    },
                )
            self._report_bootstrap_success_to_srm()

            self._set_phase(WorkerPhase.READY)
            self._emit_domain_event(
                StrategyWorkerDomainEvent.READY,
                "Strategy worker runtime ready",
                event_extras={"phase": WorkerPhase.READY.value},
            )
            self._started = True
            self._shutdown_complete = False
            self._record_startup_step("emit_initial_srm_heartbeat")
            self.emit_periodic_heartbeat()
            self._record_startup_step("start_heartbeat_periodic_jobs")
            if self._periodic_jobs is not None:
                self._periodic_jobs.start()
        except Exception as exc:
            canonical_reason = normalize_runtime_reason_code(
                str(getattr(exc, "reason_code", "bootstrap_start_failed"))
            )
            self._set_phase(
                WorkerPhase.FAILED,
                level="ERROR",
                reason_code=canonical_reason,
            )
            self._rollback_partial_startup(exc)
            raise

    def stop(
        self,
        *,
        emit_termination_signal: bool = True,
        suppress_stopping_phase_stdout: bool = False,
        termination_reason_code: str | None = None,
        termination_message: str | None = None,
    ) -> bool:
        """
        Tear down the worker. When ``emit_termination_signal`` is False, the
        ``runtime.terminated`` lifecycle signal is not sent (used when shutdown
        was requested by the runtime manager via ``StopWorker``, which already
        has the RPC response as the acknowledgement).

        When ``suppress_stopping_phase_stdout`` is True, the STOPPING phase
        transition is still recorded (including state journal) but does not
        print the Internal State block to stdout—so manager-driven shutdown
        emits at most one shutdown-style line (STOPPED / ``worker_shutdown_completed``)
        instead of duplicating semantics with the StopWorker RPC.

        ``termination_reason_code`` / ``termination_message`` control the
        ``runtime.terminated`` payload. Use ``termination_reason_code="RUNTIME_JOB_COMPLETED"``
        for natural job completion (minimal payload: ``local_state`` + ``reason_code`` only).
        In ``BACKTEST`` mode that path ends in ``COMPLETED`` (other modes use ``STOPPED``).

        If ``termination_reason_code`` is omitted: ``ERROR_DETECTED`` when the worker
        is still in ``FAILED`` phase, otherwise ``MANUAL_STOP_REQUESTED``.
        """
        if self._shutdown_complete:
            return False

        self._emit_domain_event(
            StrategyWorkerDomainEvent.SHUTDOWN_REQUESTED,
            "Strategy worker shutdown requested",
            event_extras={
                "phase": self._phase.value,
                "termination_reason_code": termination_reason_code,
            },
        )

        if self._stop_canonical_reason is not None:
            resolved_reason = self._stop_canonical_reason
            resolved_message = (
                termination_message
                if termination_message is not None
                else self._stop_resolved_message
            )
        elif termination_reason_code is None:
            if self._phase is WorkerPhase.FAILED:
                resolved_reason = normalize_runtime_reason_code("ERROR_DETECTED")
                resolved_message = (
                    termination_message
                    if termination_message is not None
                    else "Shutdown after runtime detected failure."
                )
            else:
                resolved_reason = normalize_runtime_reason_code("MANUAL_STOP_REQUESTED")
                resolved_message = (
                    termination_message
                    if termination_message is not None
                    else "Worker shutdown complete."
                )
        elif termination_reason_code == "RUNTIME_JOB_COMPLETED":
            resolved_reason = normalize_runtime_reason_code("RUNTIME_JOB_COMPLETED")
            resolved_message = (
                termination_message
                if termination_message is not None
                else "Runtime job completed."
            )
        else:
            resolved_reason = normalize_runtime_reason_code(termination_reason_code)
            resolved_message = termination_message

        # Block heartbeats before phase moves or the heartbeat thread is joined.
        if not self._shutdown_in_progress:
            self._shutdown_in_progress = True

        self._record_shutdown_step("stop_live_market_data_redis_feed")
        self._host.stop_market_data_feed()

        self._record_shutdown_step("stop_portfolio_update_redis_feed")
        self._host.stop_portfolio_update_feed()

        self._record_shutdown_step("stop_heartbeat_periodic_jobs")
        if self._periodic_jobs is not None:
            self._periodic_jobs.stop()

        self._set_phase(
            WorkerPhase.STOPPING,
            emit_stdout=not suppress_stopping_phase_stdout,
        )
        self._record_shutdown_step("stop_accepting_new_work")
        if self._work_acceptor is not None:
            self._work_acceptor.stop_accepting_new_work()

        self._record_shutdown_step("stop_strategy_loop")
        self._stop_strategy_loop()

        self._record_shutdown_step("flush_diagnostics")
        if self._diagnostic_flusher is not None:
            self._diagnostic_flusher.flush()

        self._record_shutdown_step("close_downstream_clients")
        self._close_downstream_clients()

        final_phase = WorkerPhase.STOPPED
        if (
            termination_reason_code == "RUNTIME_JOB_COMPLETED"
            and self._launch_spec.mode is WorkerMode.BACKTEST
        ):
            final_phase = WorkerPhase.COMPLETED

        self._record_shutdown_step("emit_worker_shutdown_signal")
        occurred_at = _utc_now()
        if emit_termination_signal and self._runtime_dependencies is not None:
            self._runtime_dependencies.manager.emit_terminated(
                occurred_at=occurred_at,
                local_state=final_phase.value,
                reason_code=resolved_reason,
                message=resolved_message,
            )
        # if self._runtime_logger is not None:
        #     self._runtime_logger.emit(
        #         event_name="worker_shutdown_completed",
        #         message="Worker shutdown completed.",
        #         event_extras={"local_phase": WorkerPhase.STOPPED.value},
        #     )

        self._set_phase(final_phase)
        self._report_final_shutdown_to_srm(
            canonical_reason=resolved_reason,
            message=resolved_message,
        )
        self._shutdown_complete = True
        self._started = False
        return True

    def _dispatch_paper_live_tick(self, tick: Mapping[str, Any]) -> None:
        if self._shutdown_in_progress:
            return
        dispatcher = (
            self._ddd_wiring.event_dispatcher if self._ddd_wiring is not None else None
        )
        if dispatcher is None:
            return
        dispatcher.dispatch_raw(dict(tick))
        if self._runtime_state.first_data_received and not self._first_data_received:
            self.mark_first_data_received()
        with self._replay_state_lock:
            self._last_data_event_timestamp = (
                self._runtime_state.last_data_event_timestamp
            )

    def _dispatch_backtest_stdin_tick(self, tick: Mapping[str, Any]) -> None:
        """Route BACKTEST stdin market-data ticks through the shared EventDispatcher."""
        if self._shutdown_in_progress:
            return
        wrs = self._worker_runtime_settings
        bar_tf = (
            str(getattr(wrs, "replay_bar_timeframe", None) or "1m")
            if wrs is not None
            else "1m"
        )
        if self._host.backtest_bar_timeframe_filter.should_skip(
            tick, expected_bar_timeframe=bar_tf
        ):
            return
        self._dispatch_paper_live_tick(tick)

    def _portfolio_update_stream_log(
        self,
        *,
        level: str,
        event_name: str,
        message: str,
        fields: Mapping[str, Any],
    ) -> None:
        payload = {
            "level": level,
            "event": event_name,
            "message": message,
            **fields,
        }
        print(
            f"[portfolio-update-redis] {json.dumps(payload, default=str)}", flush=True
        )

    def _dispatch_portfolio_updated_event(
        self,
        event: object,
    ) -> None:
        """Update runtime account context only; never invoke strategy hooks."""
        if self._shutdown_in_progress:
            return
        if self._phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
            return
        dispatcher = (
            self._ddd_wiring.event_dispatcher if self._ddd_wiring is not None else None
        )
        if dispatcher is not None:
            dispatcher.dispatch_portfolio(event)
            return
        adapter = self._strategy_adapter
        if adapter is None:
            return
        bridge = getattr(adapter, "_backtest_sdk_bridge", None) or getattr(
            adapter, "_replay_sdk_bridge", None
        )
        if bridge is None:
            return
        account = bridge.strategy_context.account
        apply_fn = getattr(account, "apply_portfolio_balance_event", None)
        if not callable(apply_fn):
            apply_fn = getattr(account, "apply_pls_balance_update", None)
        if not callable(apply_fn):
            return
        with self._replay_state_lock:
            if apply_fn is getattr(account, "apply_portfolio_balance_event", None):
                apply_fn(event)
            else:
                apply_fn(
                    {
                        "job_id": event.job_id,
                        "timestamp": event.timestamp.isoformat(),
                        "balance": {
                            "cash_balance": str(event.cash_balance),
                            "buying_power": str(event.buying_power),
                            "equity": str(event.equity),
                        },
                    }
                )

    def _rollback_partial_startup(self, error: Exception) -> None:
        reason_code = normalize_runtime_reason_code(
            str(getattr(error, "reason_code", "bootstrap_start_failed"))
        )
        details = self._bootstrap_failure_details(error)
        failure_at = _utc_now()
        if self._state_journal is not None:
            self._state_journal.record_launch_failed_event(
                occurred_at=failure_at,
                reason_code=str(reason_code),
                details=details,
            )

        self._stop_strategy_loop()
        self._close_downstream_clients()

        if self._runtime_dependencies is None:
            try:
                self._runtime_dependencies = self._runtime_dependencies_initializer()
            except Exception:
                # Best effort: startup failures should still be rolled back deterministically
                # even if manager signaling infrastructure cannot be initialized.
                self._runtime_dependencies = None

        if getattr(error, "stage", None) is not None:
            wrs = self._worker_runtime_settings
            if wrs is not None:
                srm_base_url = str(
                    getattr(wrs, "strategy_runtime_manager_base_url", "") or ""
                ).strip()
                if srm_base_url:
                    deployment_id = str(
                        getattr(wrs, "deployment_id", "") or ""
                    ).strip()
                    timeout_seconds = float(
                        getattr(
                            wrs, "runtime_manager_heartbeat_timeout_seconds", 30.0
                        )
                        or 30.0
                    )
                    try:
                        self._host.srm.report_bootstrap_failure(
                            error,
                            runtime_id=self._launch_spec.runtime_id,
                            mode=self._launch_spec.mode.value,
                            owner_resource_id=deployment_id,
                            srm_base_url=srm_base_url,
                            timeout_seconds=timeout_seconds,
                        )
                    except Exception:
                        pass
        elif self._runtime_dependencies is not None:
            try:
                self._runtime_dependencies.manager.emit_bootstrap_failed(
                    occurred_at=failure_at,
                    reason_code=str(reason_code),
                    details=details,
                )
            except Exception:
                # Best effort: keep original startup failure semantics even when
                # manager transport fails during rollback.
                pass
        if self._runtime_logger is not None:
            validation_diagnostics: dict[str, object] = {}
            passed_v = details.get("passed")
            if isinstance(passed_v, list):
                validation_diagnostics["passed"] = list(passed_v)
            failed_v = details.get("failed")
            if isinstance(failed_v, list):
                validation_diagnostics["failed"] = list(failed_v)
            nc_v = details.get("not_checked")
            if isinstance(nc_v, list):
                validation_diagnostics["not_checked"] = list(nc_v)

        self._started = False
        self._shutdown_complete = False

    def _bootstrap_failure_details(self, error: Exception) -> dict[str, object]:
        details: dict[str, object] = {}

        raw_details = getattr(error, "details", None)
        if isinstance(raw_details, Mapping):
            details.update(dict(raw_details))
        field_errors_raw = details.get("field_errors")
        field_errors = (
            dict(field_errors_raw) if isinstance(field_errors_raw, Mapping) else {}
        )
        missed_fields, invalid_fields, empty_fields, classification_details = (
            _classify_launch_field_errors(field_errors)
        )
        details["field_errors"] = field_errors
        details["missed_fields"] = missed_fields
        details["invalid_fields"] = invalid_fields
        details["empty_fields"] = empty_fields
        if classification_details:
            details["validation_details"] = classification_details

        passed, failed, not_checked = self._validation_outcome(error)
        details["passed"] = passed
        details["failed"] = failed
        details["not_checked"] = not_checked
        return details

    def _validation_outcome(
        self, error: Exception
    ) -> tuple[list[str], list[str], list[str]]:
        return self._host.classify_bootstrap_stages(error, self._startup_steps)


    def _record_order_intent_for_journal(
        self,
        source: str,
        payload: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        if self._state_journal is not None:
            self._state_journal.record_order_intent(source, payload, result)
        if self._launch_spec.mode in (WorkerMode.PAPER, WorkerMode.LIVE):
            count, sample = self._host.domain_events.next("order_intent_emitted")
            extras: dict[str, Any] = {
                "source": source,
                "symbol": str(payload.get("symbol") or ""),
                "instrument_id": str(payload.get("instrument_id") or ""),
                "side": str(payload.get("side") or ""),
                "order_type": str(payload.get("order_type") or ""),
                "accepted": bool(result.get("accepted")),
                "order_intent_count": count,
                "sampled": sample,
            }
            self._emit_domain_event(
                StrategyWorkerDomainEvent.ORDER_INTENT_EMITTED,
                "Order intent submitted from strategy worker",
                level=logging.INFO if sample else logging.DEBUG,
                event_extras=extras,
            )

    def _platform_trace(self) -> PlatformTraceSpec | None:
        return self._host.sdk_bridge.platform_trace(self._worker_runtime_settings)


    def _order_intent_env_correlation(self) -> str:
        wrs = self._worker_runtime_settings
        if wrs is None:
            return ""
        return str(getattr(wrs, "order_intent_correlation_id", None) or "")

    def _simulated_clock_seeded_from_dependencies(self) -> object:
        wall = _utc_now()
        if self._runtime_dependencies is not None:
            now_fn = getattr(self._runtime_dependencies.clock, "now", None)
            if callable(now_fn):
                try:
                    wall = now_fn()
                except Exception:
                    pass
        return self._host.simulated_clock.build_seeded(wall)


    def _make_backtest_sdk_bridge(
        self,
        *,
        strategy_instance: object,
        simulated_clock: object,
    ) -> tuple[object | None, Callable[[Any], dict[str, Any]] | None]:
        assert self._runtime_dependencies is not None
        return self._host.sdk_bridge.build(
            strategy_instance=strategy_instance,
            simulated_clock=simulated_clock,
            launch=self._launch_spec,
            launch_payload=self._launch_payload,
            worker_identity=self._worker_identity,
            runtime_dependencies=self._runtime_dependencies,
            worker_config=self._worker_runtime_settings,
            on_order_intent_result=self._record_order_intent_for_journal,
            latest_market_event_at=self._peek_last_data_event_timestamp,
            allocate_order_intent_id=(
                self._state_journal.allocate_order_intent_id
                if self._state_journal is not None
                else None
            ),
            platform_trace=self._platform_trace(),
        )


    def _emit_backtest_order_submitter_bridge_logs(
        self, submitter: Callable[[Any], dict[str, Any]] | None
    ) -> None:
        log = self._runtime_logger
        if log is None or self._runtime_dependencies is None:
            return
        if submitter is None:
            log.emit(
                event_name="runtime.backtest_order_submitter_missing",
                message=(
                    "Backtest has no order-intent submitter (risk-service gRPC client "
                    "unavailable or misconfigured); order_intent_journal will stay empty until "
                    "this is fixed."
                ),
                level=logging.WARNING,
                event_extras={},
            )
            return
        if self._state_journal is None:
            log.emit(
                event_name="runtime.backtest_state_journal_disabled",
                message=(
                    "State journal is disabled; order intents are not "
                    "persisted to order_intent_journal."
                ),
                level=logging.WARNING,
                event_extras={},
            )
        else:
            log.emit(
                event_name="runtime.backtest_order_intent_journal_ready",
                message=(
                    "Backtest order submitter and state journal are active; "
                    "order intents are written to order_intent_journal when "
                    "the strategy submits orders."
                ),
                level=logging.INFO,
                event_extras={},
            )

    def _initialize_strategy_coordinator(
        self,
        success: BootstrapSuccessView,
    ) -> StrategyAdapter | None:
        if self._strategy_instance_manager is None:
            return None

        from types import SimpleNamespace

        assignment_key = SimpleNamespace(
            runtime_id=self._launch_spec.runtime_id,
            strategy_version_id=self._launch_spec.strategy_version_id,
            tenant_id=self._launch_spec.tenant_id,
            mode=self._launch_spec.mode,
            launch_attempt=self._launch_spec.launch_attempt,
            trader_id=self._launch_spec.trader_id,
            account_id=self._launch_spec.account_id,
        )

        symbol = success.entrypoint.symbol

        def build_adapter() -> StrategyAdapter:
            strategy_instance = symbol() if inspect.isclass(symbol) else symbol
            bridge = None
            if (
                self._launch_spec.mode is WorkerMode.BACKTEST
                and self._runtime_dependencies is not None
                and self._host.simulated_clock.is_simulated(self._runtime_dependencies.clock)
            ):
                bridge, submitter = self._make_backtest_sdk_bridge(
                    strategy_instance=strategy_instance,
                    simulated_clock=self._runtime_dependencies.clock,
                )
                self._emit_backtest_order_submitter_bridge_logs(submitter)
            elif (
                self._runtime_dependencies is not None
                and self._launch_spec.mode in (WorkerMode.PAPER, WorkerMode.LIVE)
                and callable(getattr(strategy_instance, "_bind_context", None))
            ):
                sdk_clock = self._simulated_clock_seeded_from_dependencies()
                bridge, _ = self._make_backtest_sdk_bridge(
                    strategy_instance=strategy_instance,
                    simulated_clock=sdk_clock,
                )
            return StrategyAdapter(
                strategy=strategy_instance,
                backtest_sdk_bridge=bridge,
            )

        return self._strategy_instance_manager.create(assignment_key, build_adapter)

    def _stop_strategy_loop(self) -> None:
        if (
            self._strategy_instance_manager is not None
            and self._strategy_adapter is not None
        ):
            self._strategy_instance_manager.stop(self._strategy_adapter)
            self._strategy_adapter = None

    def _close_downstream_clients(self) -> None:
        for candidate in self._closeables:
            close = getattr(candidate, "close", None)
            if callable(close):
                close()
                continue
            shutdown = getattr(candidate, "shutdown", None)
            if callable(shutdown):
                shutdown()

    def _emit_domain_event(
        self,
        event: StrategyWorkerDomainEvent,
        message: str,
        *,
        level: int = logging.INFO,
        event_extras: Mapping[str, Any] | None = None,
    ) -> None:
        log = self._runtime_logger
        if log is None:
            return
        self._host.domain_events.emit_bound(
            log,
            event=event,
            message=message,
            level=level,
            event_extras=event_extras,
            platform_trace=self._platform_trace(),
            env_correlation_fallback=self._order_intent_env_correlation(),
        )

    def _record_startup_step(self, step: str) -> None:
        self._startup_steps.append(step)
        if self._state_journal is not None:
            self._state_journal.record_startup_step(step)
        if self._on_step is not None:
            self._on_step(step)

    def _record_shutdown_step(self, step: str) -> None:
        self._shutdown_steps.append(step)
        if self._state_journal is not None:
            self._state_journal.record_shutdown_step(step)
        if self._on_step is not None:
            self._on_step(step)

    def _worker_identity_value(self) -> str:
        return (
            f"{self._worker_identity.runtime_id}:"
            f"{self._worker_identity.strategy_version_id}:"
            f"{self._worker_identity.launch_attempt}"
        )

    def _emit_state_message(
        self,
        *,
        state: WorkerPhase,
        level: str,
        reason_code: str | None = None,
        emit_stdout: bool = True,
    ) -> None:
        event_name = "worker_state_changed"
        if state is WorkerPhase.FAILED:
            event_name = "worker_launch_failed"
        elif state in (WorkerPhase.STOPPED, WorkerPhase.COMPLETED):
            event_name = "worker_shutdown_completed"

        trace = self._platform_trace()
        correlation_id = ""
        if trace is not None:
            correlation_id = (
                trace.effective_correlation_id(
                    env_fallback=self._order_intent_env_correlation()
                )
                or ""
            )

        message: dict[str, object] = {
            "event_id": (
                f"{self._launch_spec.runtime_id}:"
                f"{self._launch_spec.launch_attempt}:"
                f"{event_name}:internal_state"
            ),
            "event_name": event_name,
            "event_version": 1,
            "producer": "strategy-worker-runtime",
            "occurred_at": _utc_now().isoformat().replace("+00:00", "Z"),
            "correlation_id": correlation_id,
            "tenant_id": self._launch_spec.tenant_id,
            "account_id": self._launch_spec.account_id or "",
            "runtime_id": self._launch_spec.runtime_id,
            "worker_identity": self._worker_identity_value(),
            "launch_attempt": self._launch_spec.launch_attempt,
            "strategy_version_id": self._launch_spec.strategy_version_id,
            "payload": {
                "level": level,
                "local_phase": state.value,
                "mode": self._launch_spec.mode.value,
            },
        }
        if reason_code is not None:
            payload = message.get("payload")
            if isinstance(payload, dict):
                payload["reason_code"] = reason_code
        if trace is not None and trace.request_id:
            message["request_id"] = trace.request_id
        if emit_stdout:
            print("----------------------------Internal State------------")
            print(json.dumps(message, separators=(",", ":"), ensure_ascii=True))

    def _set_phase(
        self,
        phase: WorkerPhase,
        *,
        level: str = "INFO",
        reason_code: str | None = None,
        emit_stdout: bool = True,
    ) -> None:
        previous = self._phase
        self._phase = phase
        self._runtime_state.set_phase(phase)
        self._emit_state_message(
            state=phase,
            level=level,
            reason_code=reason_code,
            emit_stdout=emit_stdout,
        )
        if phase in (WorkerPhase.STOPPED, WorkerPhase.COMPLETED):
            self._emit_domain_event(
                StrategyWorkerDomainEvent.STOPPED,
                "Strategy worker stopped",
                event_extras={
                    "phase": phase.value,
                    "stage": "shutdown",
                    "state": "stopped",
                },
            )
        elif phase is WorkerPhase.FAILED:
            self._emit_domain_event(
                StrategyWorkerDomainEvent.FAILED,
                "Strategy worker entered FAILED phase",
                level=logging.ERROR,
                event_extras={
                    "phase": phase.value,
                    "reason_code": reason_code,
                    "error_code": reason_code,
                    "retryable": False,
                    "stage": "runtime",
                    "state": "failed",
                },
            )
            if not self._shutdown_in_progress:
                normalized_reason = (
                    normalize_runtime_reason_code(str(reason_code))
                    if reason_code is not None
                    else None
                )
                failure_message = (
                    f"Worker entered FAILED phase ({normalized_reason})."
                    if normalized_reason is not None
                    else "Worker entered FAILED phase."
                )
                try:
                    self._emit_srm_runtime_status(
                        source=self._host.srm_status_source_update,
                        reason_code=normalized_reason,
                        message=failure_message,
                        retryable=False,
                    )
                except Exception:
                    pass
        if self._state_journal is not None:
            self._state_journal.record_phase_change(
                previous_phase=previous,
                phase=phase,
                level=level,
                reason_code=reason_code,
            )
