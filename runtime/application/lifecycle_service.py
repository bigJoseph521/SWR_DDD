from __future__ import annotations

import inspect
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol, Sequence

from runtime.bootstrap.backtest_replay_tick_filter import (
    skip_backtest_replay_tick_for_ingest,
)
from runtime.bootstrap.failures import BootstrapFailure, BootstrapStage
from runtime.bootstrap.srm_env_status_report import (
    initiate_stop_status_update,
    is_failure_shutdown,
    print_bootstrap_failure_outcome,
    print_bootstrap_success_outcome,
    print_shutdown_status_outcome,
    report_bootstrap_failure_to_srm,
    report_bootstrap_success_to_srm,
    report_final_shutdown_to_srm,
    resolve_shutdown_report,
)
from runtime.transport.http.stop_control_server import (
    StopAlreadyInProgress,
    WorkerAlreadyStopped,
)
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.replay_sdk_bridge import (
    ReplaySdkBridge,
    build_replay_sdk_bridge,
)
from runtime.bootstrap.sdk_contract_validator import (
    BootstrapPipeline,
    BootstrapPipelineResult,
    BootstrapPipelineSuccess,
)
from runtime.bootstrap.strategy_adapter import StrategyAdapter
from runtime.bootstrap.strategy_error_boundary import StrategyCallResult
from runtime.bootstrap.strategy_instance_manager import (
    StrategyAssignmentKey,
    StrategyInstanceManager,
)
from runtime.bootstrap.validator import LaunchSpecValidator
from runtime.domain.enums import RuntimeMode, WorkerPhase
from runtime.domain.errors import (
    RuntimeStartValidationFailedError,
    normalize_runtime_reason_code,
)
from runtime.domain.worker_identity import WorkerIdentity
from runtime.integration.clock import SimulatedClock
from runtime.integration.epoch_time import (
    utc_from_epoch_millis,
    utc_from_epoch_seconds,
)
from runtime.observability.domain_events import (
    DomainEventSampler,
    StrategyWorkerDomainEvent,
    emit_bound_domain_event,
)
from runtime.observability.logger import RuntimeBoundLogger
from runtime.persistence.runtime_journal_sink import RuntimeJournalSink
from runtime.runtime.dependencies import RuntimeDependencies
from runtime.transport.heartbeat import (
    SRM_STATUS_SOURCE_HEARTBEAT,
    SRM_STATUS_SOURCE_UPDATE,
)
from runtime.runtime.sdk_order_intent_submission import (
    build_sdk_order_intent_submitter,
)
from runtime.strategy_contract.strategy_params_yaml import (
    load_warmup_bars_default_for_strategy,
)
from runtime.transport.grpc.historical_data_client import (
    HistoricalDataGrpcClient,
    build_historical_bar_replay_tick,
    build_query_historical_request,
    launch_payload_backtest_symbol,
)


class WorkAcceptor(Protocol):
    def stop_accepting_new_work(self) -> None: ...


class PeriodicJobController(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...


class DiagnosticFlusher(Protocol):
    def flush(self) -> None: ...


class Closeable(Protocol):
    def close(self) -> None: ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return (
            parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        )
    return None


def _coerce_timestamp(value: object) -> datetime | None:
    parsed = _parse_datetime(value)
    if parsed is not None:
        return parsed
    if isinstance(value, Mapping):
        sec = value.get("seconds")
        if sec is None:
            return None
        try:
            s = int(sec)
        except (TypeError, ValueError):
            return None
        nanos = int(value.get("nanos") or 0)
        return utc_from_epoch_seconds(float(s) + nanos / 1e9)
    return None


def _extract_market_timestamp(
    event: Mapping[str, object], tick_payload: Mapping[str, object]
) -> datetime | None:
    event_ts = _coerce_timestamp(event.get("event_time"))
    if event_ts is not None:
        return event_ts
    # Replay producers may place OHLC time on payload keys instead of event_time.
    for key in ("event_time", "timestamp", "ts", "time"):
        payload_ts = _coerce_timestamp(tick_payload.get(key))
        if payload_ts is not None:
            return payload_ts
    return None


_REPLAY_DATA_TRACE_MAX_EVENTS = 32


def _summarize_replay_event_for_trace(event: Mapping[str, object]) -> dict[str, object]:
    out: dict[str, object] = {}
    for k in ("event_id", "event_type", "instrument_id", "sequence"):
        v = event.get(k)
        if v is not None and v != "":
            out[k] = v
    et = event.get("event_time")
    if et is not None and et != "":
        out["event_time"] = et
    ep = event.get("payload")
    if isinstance(ep, Mapping):
        for k in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "timestamp",
            "ts",
            "type",
            "event_type",
        ):
            if ep.get(k) is not None:
                out[f"payload.{k}"] = ep[k]
    return out


def _print_replay_ingress_data_trace(
    *,
    replay_session_id: str,
    replay_cursor: str,
    end_of_stream: bool,
    batch_simulated: datetime | None,
    event_list: list[Mapping[str, object]],
) -> None:
    n = len(event_list)
    head = event_list[:_REPLAY_DATA_TRACE_MAX_EVENTS]
    summaries = [_summarize_replay_event_for_trace(e) for e in head]
    line: dict[str, object] = {
        "event_name": "replay.data.received",
        "replay_session_id": replay_session_id,
        "replay_cursor": replay_cursor,
        "end_of_stream": end_of_stream,
        "batch_event_count": n,
        "simulated_time": (
            batch_simulated.isoformat().replace("+00:00", "Z")
            if batch_simulated
            else None
        ),
        "events_sample": summaries,
    }
    if n > _REPLAY_DATA_TRACE_MAX_EVENTS:
        line["events_truncated"] = True
    print("----------------------------Replay data (trace)-------------", flush=True)
    print(
        json.dumps(line, default=str, separators=(",", ":"), ensure_ascii=True),
        flush=True,
    )


def _backtest_window_bounds(
    launch_spec: LaunchSpec,
) -> tuple[datetime, datetime] | None:
    if launch_spec.mode is not RuntimeMode.BACKTEST:
        return None
    ts_start = launch_spec.ts_start
    ts_end = launch_spec.ts_end
    if not ts_start or not ts_end:
        return None
    lo = _parse_datetime(ts_start)
    hi = _parse_datetime(ts_end)
    if lo is None or hi is None:
        return None
    return (lo, hi)


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


_BOOTSTRAP_STAGE_ORDER: tuple[BootstrapStage, ...] = (
    BootstrapStage.ARTIFACT_FETCH,
    BootstrapStage.ARTIFACT_VERIFY,
    BootstrapStage.ENTRYPOINT_LOAD,
    BootstrapStage.SDK_VALIDATE,
)


class BootstrapPipelinePort(Protocol):
    def run(self, launch_spec: LaunchSpec) -> BootstrapPipelineResult: ...


def _env_market_data_suppress_tick_stdout() -> bool:
    """When true, do not print each Redis market tick to stdout (default is to print)."""
    v = os.environ.get("SWR_MARKET_DATA_SUPPRESS_TICK_STDOUT", "").strip().lower()
    return v in ("1", "true", "yes", "on")


class LifecycleService:
    def __init__(
        self,
        *,
        launch_spec: LaunchSpec,
        launch_payload: Mapping[str, object],
        worker_identity: WorkerIdentity,
        launch_spec_validator: LaunchSpecValidator,
        bootstrap_pipeline: BootstrapPipeline | BootstrapPipelinePort,
        log_binder: Callable[[], RuntimeBoundLogger],
        runtime_dependencies_initializer: Callable[[], RuntimeDependencies],
        strategy_instance_manager: StrategyInstanceManager | None = None,
        work_acceptor: WorkAcceptor | None = None,
        periodic_jobs: PeriodicJobController | None = None,
        diagnostic_flusher: DiagnosticFlusher | None = None,
        closeables: Sequence[object] = (),
        on_step: Callable[[str], None] | None = None,
        state_journal: RuntimeJournalSink | None = None,
        worker_runtime_settings: Any | None = None,
        historical_data_client: HistoricalDataGrpcClient | None = None,
    ) -> None:
        self._launch_spec = launch_spec
        self._launch_payload = dict(launch_payload)
        self._worker_runtime_settings = worker_runtime_settings
        self._historical_data_client = historical_data_client
        self._hds_feed_stop = threading.Event()
        self._hds_feed_thread: threading.Thread | None = None
        self._hds_driver_started = False
        self._live_md_feed_stop = threading.Event()
        self._live_md_feed_thread: threading.Thread | None = None
        self._live_md_feed_started = False
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

        self._runtime_logger: RuntimeBoundLogger | None = None
        self._runtime_dependencies: RuntimeDependencies | None = None
        self._strategy_adapter: StrategyAdapter | None = None
        self._phase = WorkerPhase.INITIALIZING
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
        self._domain_event_sampler = DomainEventSampler(sample_every=100)
        self._emit_state_message(state=self._phase, level="INFO")
        if self._state_journal is not None:
            self._state_journal.record_phase_change(
                previous_phase=None,
                phase=self._phase,
                level="INFO",
                reason_code=None,
            )

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
        return self._first_data_received

    @property
    def backtest_replay_complete(self) -> bool:
        return self._backtest_replay_complete

    def should_stop_for_completed_backtest_job(self) -> bool:
        """
        True in BACKTEST when the job is finished and the worker should shut down:

        - Replay ingress sent ``end_of_stream``, or
        - The historical-data driver finished paging bars for ``[ts_start, ts_end]``.

        Callers should invoke :meth:`stop` with ``termination_reason_code="RUNTIME_JOB_COMPLETED"``
        so ``runtime.terminated`` is distinct from operator stop (``STOP_REQUESTED``) and
        manager :class:`StopWorker` (acknowledged via gRPC only, no duplicate signal).
        """
        if self._shutdown_complete or self._shutdown_in_progress:
            return False
        if self._launch_spec.mode is not RuntimeMode.BACKTEST:
            return False
        if not self._backtest_replay_complete:
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
        if self._phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
            return
        now = _utc_now()
        count, sample = self._domain_event_sampler.next("heartbeat_sent")
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
        source: str = SRM_STATUS_SOURCE_HEARTBEAT,
        reason_code: str | None = None,
        message: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        if self._runtime_dependencies is None:
            return
        when = observed_at if observed_at is not None else _utc_now()
        self._runtime_dependencies.manager.emit_heartbeat(
            local_state=self._phase.value,
            observed_at=when,
            source=str(source),
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
        try:
            srm_response = report_bootstrap_success_to_srm(
                runtime_id=self._launch_spec.runtime_id,
                mode=self._launch_spec.mode.value,
                owner_resource_id=deployment_id,
                srm_base_url=srm_base_url,
                timeout_seconds=timeout_seconds,
            )
            print_bootstrap_success_outcome(srm_response=srm_response)
        except Exception:
            pass

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
            body, _srm_response, canonical, resolved_message = (
                initiate_stop_status_update(
                    runtime_id=self._launch_spec.runtime_id,
                    mode=self._launch_spec.mode.value,
                    owner_resource_id=deployment_id,
                    srm_base_url=srm_base_url,
                    reason=reason,
                    timeout_seconds=timeout_seconds,
                )
            )
            self._stop_request_accepted = True
            self._stop_canonical_reason = canonical
            self._stop_resolved_message = resolved_message
            self._shutdown_in_progress = True
            return dict(body)

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
        reason_code, resolved_message = resolve_shutdown_report(
            canonical_reason=canonical_reason,
            message=message,
        )
        runtime_status = (
            "FAILED" if is_failure_shutdown(canonical_reason) else "STOPPED"
        )
        try:
            srm_response = report_final_shutdown_to_srm(
                runtime_id=self._launch_spec.runtime_id,
                mode=self._launch_spec.mode.value,
                owner_resource_id=deployment_id,
                srm_base_url=srm_base_url,
                canonical_reason=canonical_reason,
                message=message,
                timeout_seconds=timeout_seconds,
            )
            print_shutdown_status_outcome(
                runtime_status=runtime_status,
                reason_code=reason_code,
                message=resolved_message,
                srm_response=srm_response,
            )
        except Exception:
            pass

    def push_replay_context(self, payload: Mapping[str, object]) -> dict[str, object]:
        if self._runtime_dependencies is None:
            self._runtime_dependencies = self._runtime_dependencies_initializer()
        replay_gateway = (
            self._runtime_dependencies.replay
            if self._runtime_dependencies is not None
            else None
        )
        if replay_gateway is None:
            return {
                "runtime_id": self._launch_spec.runtime_id,
                "replay_session_id": "",
                "replay_cursor": "",
                "consumed_count": 0,
                "observed_at": _utc_now(),
            }

        replay_meta = payload.get("replay")
        replay_meta_dict = dict(replay_meta) if isinstance(replay_meta, Mapping) else {}
        replay_session_id = str(replay_meta_dict.get("replay_session_id") or "")
        replay_cursor = str(replay_meta_dict.get("replay_cursor") or "")
        end_of_stream = bool(payload.get("end_of_stream", False))
        with self._replay_state_lock:
            self._last_replay_cursor = replay_cursor

        if (
            self._launch_spec.mode is RuntimeMode.BACKTEST
            and self._backtest_replay_complete
        ):
            return {
                "runtime_id": self._launch_spec.runtime_id,
                "replay_session_id": replay_session_id,
                "replay_cursor": replay_cursor,
                "consumed_count": 0,
                "observed_at": _utc_now(),
            }

        events = payload.get("events")
        event_list = (
            [item for item in events if isinstance(item, Mapping)]
            if isinstance(events, list)
            else []
        )
        batch_simulated = _parse_datetime(payload.get("simulated_time"))
        wrs = self._worker_runtime_settings
        if wrs is not None and getattr(wrs, "replay_ingress_trace_payload", False):
            _print_replay_ingress_data_trace(
                replay_session_id=replay_session_id,
                replay_cursor=replay_cursor,
                end_of_stream=end_of_stream,
                batch_simulated=batch_simulated,
                event_list=event_list,
            )
        bounds = _backtest_window_bounds(self._launch_spec)

        consumed = 0
        last_effective_sim: datetime | None = None
        for event in event_list:
            event_payload = event.get("payload")
            tick_payload = (
                dict(event_payload) if isinstance(event_payload, Mapping) else {}
            )
            tick_payload.setdefault("event_id", event.get("event_id"))
            tick_payload.setdefault("event_type", event.get("event_type"))
            tick_payload.setdefault("instrument_id", event.get("instrument_id"))
            if "type" not in tick_payload and isinstance(
                tick_payload.get("event_type"), str
            ):
                tick_payload["type"] = tick_payload["event_type"]
            tick_payload.setdefault("sequence", event.get("sequence"))
            tick_payload.setdefault("event_time", event.get("event_time"))

            event_ts = _extract_market_timestamp(event, tick_payload) or batch_simulated
            if (
                self._launch_spec.mode is RuntimeMode.BACKTEST
                and bounds is not None
                and event_ts is not None
            ):
                lo, hi = bounds
                if event_ts < lo or event_ts > hi:
                    continue

            effective_sim = event_ts if event_ts is not None else batch_simulated

            if self._launch_spec.mode is RuntimeMode.BACKTEST:
                wrs_bt = (
                    getattr(self._worker_runtime_settings, "replay_bar_timeframe", None)
                    or "1m"
                )
                if skip_backtest_replay_tick_for_ingest(
                    tick_payload,
                    expected_bar_timeframe=str(wrs_bt),
                ):
                    continue

            strategy_callback = (
                self._strategy_adapter.on_event
                if self._strategy_adapter is not None
                else None
            )
            replay_gateway.ingest_replay_tick(
                tick_payload,
                simulated_time=effective_sim,
                strategy_callback=strategy_callback,
            )
            consumed += 1
            if effective_sim is not None:
                last_effective_sim = effective_sim

        if last_effective_sim is not None:
            with self._replay_state_lock:
                self._last_data_event_timestamp = last_effective_sim

        if self._launch_spec.mode is RuntimeMode.BACKTEST and end_of_stream:
            self._backtest_replay_complete = True

        return {
            "runtime_id": self._launch_spec.runtime_id,
            "replay_session_id": replay_session_id,
            "replay_cursor": replay_cursor,
            "consumed_count": consumed,
            "observed_at": _utc_now(),
        }

    def _peek_last_data_event_timestamp(self) -> datetime | None:
        """UTC instant of the latest ingested bar/tick/quote (simulated or live feed)."""
        with self._replay_state_lock:
            return self._last_data_event_timestamp

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
            self._maybe_start_historical_backtest_driver()
            self._maybe_start_live_market_data_feed()
            self._maybe_start_portfolio_update_feed()

    def start(self) -> None:
        if self._started:
            return

        bootstrap_success: BootstrapPipelineSuccess | None = None
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
        self._stop_live_market_data_feed_worker()

        self._record_shutdown_step("stop_portfolio_update_redis_feed")
        self._stop_portfolio_update_feed_worker()

        self._record_shutdown_step("stop_historical_backtest_feed")
        self._stop_historical_backtest_feed_worker()

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
            and self._launch_spec.mode is RuntimeMode.BACKTEST
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

    def _maybe_start_historical_backtest_driver(self) -> None:
        if self._hds_driver_started:
            return
        if self._launch_spec.mode is not RuntimeMode.BACKTEST:
            return
        if self._historical_data_client is None:
            return
        wrs = self._worker_runtime_settings
        if wrs is None or not str(wrs.historical_data_grpc_target or "").strip():
            return
        self._hds_driver_started = True
        self._hds_feed_stop.clear()
        thread = threading.Thread(
            target=self._historical_backtest_driver_worker,
            name="swr-historical-backtest-feed",
            daemon=False,
        )
        self._hds_feed_thread = thread
        thread.start()

    def _stop_historical_backtest_feed_worker(self) -> None:
        self._hds_feed_stop.set()
        t = self._hds_feed_thread
        if t is not None and t.is_alive():
            t.join(timeout=60.0)

    def _paper_live_symbol_allowlist(self) -> set[str] | None:
        params = self._launch_payload.get("parameters")
        if not isinstance(params, dict):
            return None
        sym = params.get("symbol")
        if isinstance(sym, str) and sym.strip():
            return {sym.strip().upper()}
            # return {"TSM"}
        return None

    def _paper_live_strategy_symbol(self) -> str | None:
        allow = self._paper_live_symbol_allowlist()
        if not allow:
            return None
        return next(iter(allow))

    def _live_market_data_partition_scope(
        self, *, partition_count: int
    ) -> tuple[str, int, set[str]] | None:
        """
        Resolve strategy symbol and its single Redis stream partition for XREAD.

        Returns ``(symbol_upper, partition, {symbol_upper})`` or ``None`` when
        ``parameters.symbol`` is missing.
        """
        from runtime.integration.market_data_partition import market_data_partition

        sym = self._paper_live_strategy_symbol()
        if not sym:
            print(
                "[market-data-redis] parameters.symbol is required for PAPER/LIVE "
                "market data (single-partition XREAD); not opening all partitions.",
                flush=True,
            )
            return None
        part = market_data_partition(sym, partition_count)
        print(
            "[market-data-redis] "
            f"XREAD BLOCK 0 on partition {part} for strategy symbol {sym!r} "
            f"(partition_count={partition_count})",
            flush=True,
        )
        return sym, part, {sym}

    def _dispatch_paper_live_tick(self, tick: Mapping[str, Any]) -> None:
        if self._shutdown_in_progress:
            return
        if self._phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
            return
        adapter = self._strategy_adapter
        if adapter is None:
            return
        if not self._first_data_received:
            self.mark_first_data_received()
        ts = _parse_datetime(tick.get("event_time"))
        if ts is None:
            raw_ms = tick.get("ts_ms")
            if raw_ms is not None:
                try:
                    ts = utc_from_epoch_millis(int(raw_ms))
                except (TypeError, ValueError, OverflowError):
                    ts = None
        if ts is not None:
            with self._replay_state_lock:
                self._last_data_event_timestamp = ts
        result = adapter.on_event(dict(tick))
        if isinstance(result, StrategyCallResult) and result.ok:
            if self._launch_spec.mode in (RuntimeMode.PAPER, RuntimeMode.LIVE):
                count, sample = self._domain_event_sampler.next("signal_generated")
                self._emit_domain_event(
                    StrategyWorkerDomainEvent.SIGNAL_GENERATED,
                    "Strategy processed a market data event",
                    level=logging.INFO if sample else logging.DEBUG,
                    event_extras={
                        "symbol": str(tick.get("symbol") or ""),
                        "instrument_id": str(tick.get("instrument_id") or ""),
                        "event_type": str(
                            tick.get("event_type") or tick.get("type") or ""
                        ),
                        "signal_count": count,
                        "sampled": sample,
                    },
                )
        elif isinstance(result, StrategyCallResult) and not result.ok:
            self._market_data_stream_log(
                level="WARNING",
                event_name="market_data_stream.strategy_execution_failed",
                message="Strategy adapter rejected or failed processing a market data tick.",
                fields={
                    "symbol": str(tick.get("symbol") or ""),
                    "event_type": str(tick.get("event_type") or tick.get("type") or ""),
                    "error_code": result.error_code,
                    "reason_code": result.reason_code,
                    "diagnostics": dict(result.diagnostics or {}),
                },
            )

    def _maybe_start_live_market_data_feed(self) -> None:
        if self._live_md_feed_started:
            return
        if self._launch_spec.mode not in (RuntimeMode.PAPER, RuntimeMode.LIVE):
            return
        wrs = self._worker_runtime_settings
        redis_url = (
            str(getattr(wrs, "market_data_redis_url", "") or "").strip() if wrs else ""
        )
        if not redis_url:
            print(
                "[market-data-redis] not configured (set market_data_redis_url or "
                "SWR_MARKET_DATA_REDIS_URL); PAPER/LIVE worker will idle until a feed is available.",
                flush=True,
            )
            return
        from runtime.integration.market_data_redis_feed import (
            build_market_data_consumer_group_name,
            expand_market_data_stream_keys,
            market_data_read_command_fields,
            market_data_redis_keys_from_settings,
            redis_stream_key_prefixes_snapshot,
            resolve_stream_names,
            sanitize_redis_url_for_log,
        )

        feeds = tuple(getattr(wrs, "market_data_feeds", ()) or ("bars",))
        bar_tf = (getattr(wrs, "replay_bar_timeframe", None) or "1m").strip() or "1m"
        md_keys = market_data_redis_keys_from_settings(wrs)
        self._market_data_stream_log(
            level="INFO",
            event_name="market_data_stream.step_resolve_stream_prefixes",
            message="Step: stream prefixes from env/bundle (XREAD on md:stream:*; not Pub/Sub md:realtime:*).",
            fields={
                "bar_timeframe": bar_tf,
                "feeds": list(feeds),
                **redis_stream_key_prefixes_snapshot(md_keys),
            },
        )
        stream_bases = resolve_stream_names(
            feeds, bar_timeframe=bar_tf, redis_keys=md_keys
        )
        self._market_data_stream_log(
            level="INFO",
            event_name="market_data_stream.step_resolve_stream_bases",
            message="Step: logical stream base keys for selected feeds (before :partition suffix).",
            fields={
                "stream_bases": list(stream_bases),
            },
        )
        pc_raw = getattr(wrs, "market_data_realtime_partition_count", 128)
        try:
            partition_count = int(pc_raw)
        except (TypeError, ValueError):
            partition_count = 128
        scope = self._live_market_data_partition_scope(partition_count=partition_count)
        if scope is None:
            return
        strategy_symbol, partition, partition_symbols = scope
        stream_names = expand_market_data_stream_keys(
            stream_bases,
            partition_count=partition_count,
            symbol_filter=partition_symbols,
        )
        if not stream_names:
            print(
                "[market-data-redis] market_data_feeds produced no stream keys; "
                "check parameters.data_source / market_data_streams / SWR_MARKET_DATA_STREAMS "
                "and bar_timeframe (Redis bars are 1m / md:stream:am only).",
                flush=True,
            )
            return
        self._market_data_stream_log(
            level="INFO",
            event_name="market_data_stream.step_expand_partitioned_streams",
            message="Step: physical Redis stream keys for XREAD / XREADGROUP (base:partition).",
            fields={
                "partition_count": partition_count,
                "strategy_symbol": strategy_symbol,
                "partition": partition,
                "physical_stream_keys": list(stream_names),
                "physical_stream_key_count": len(stream_names),
            },
        )
        use_cg = bool(getattr(wrs, "market_data_redis_use_consumer_group", False))
        cgp = (
            str(
                getattr(
                    wrs, "market_data_consumer_group_prefix", "strategy-worker-runtime"
                )
                or "strategy-worker-runtime"
            ).strip()
            or "strategy-worker-runtime"
        )
        dep_raw = self._launch_payload.get("deployment_id")
        dep_s = str(dep_raw).strip() if dep_raw is not None else ""
        cg_preview = (
            build_market_data_consumer_group_name(
                group_prefix=cgp,
                deployment_id=dep_s or None,
                runtime_id=self._launch_spec.runtime_id,
            )[:200]
            if use_cg
            else ""
        )
        cmd_preview = market_data_read_command_fields(
            use_consumer_group=use_cg,
            stream_names=list(stream_names),
            stream_start_id=str(
                getattr(wrs, "market_data_stream_start_id", "$") or "$"
            ),
            block_ms=int(getattr(wrs, "market_data_xread_block_ms", 0)),
            count=int(getattr(wrs, "market_data_xread_count", 100)),
            consumer_group_name=cg_preview if use_cg else "",
            consumer_name="(assigned in swr-market-data-redis-feed thread)"
            if use_cg
            else "",
        )
        self._market_data_stream_log(
            level="INFO",
            event_name="market_data_stream.subscription_snapshot",
            message="Resolved Redis stream subscription for market data ingress.",
            fields={
                "redis_url": sanitize_redis_url_for_log(redis_url),
                "feeds": list(feeds),
                "bar_timeframe": bar_tf,
                "strategy_symbol": strategy_symbol,
                "partition": partition,
                "partition_count": partition_count,
                "redis_stream_keys": list(stream_names),
                "use_consumer_group": use_cg,
                "consumer_group": cg_preview or None,
                **redis_stream_key_prefixes_snapshot(md_keys),
                **cmd_preview,
            },
        )
        self._emit_domain_event(
            StrategyWorkerDomainEvent.MARKET_DATA_SUBSCRIPTION_STARTED,
            "Market data Redis subscription started",
            event_extras={
                "stream_count": len(stream_names),
                "feeds": list(feeds),
                "stage": "market_data_subscription",
                "state": "started",
            },
        )
        self._live_md_feed_started = True
        self._live_md_feed_stop.clear()
        thread = threading.Thread(
            target=self._live_market_data_redis_worker,
            name="swr-market-data-redis-feed",
            daemon=False,
        )
        self._live_md_feed_thread = thread
        thread.start()

    def _stop_live_market_data_feed_worker(self) -> None:
        self._live_md_feed_stop.set()
        t = self._live_md_feed_thread
        if t is not None and t.is_alive():
            t.join(timeout=60.0)

    def _live_market_data_redis_worker(self) -> None:
        from runtime.integration.market_data_redis_feed import (
            build_market_data_consumer_group_name,
            expand_market_data_stream_keys,
            market_data_read_command_fields,
            market_data_redis_keys_from_settings,
            redis_stream_key_prefixes_snapshot,
            resolve_stream_names,
            run_market_data_redis_loop,
            sanitize_redis_stream_consumer_token,
            sanitize_redis_url_for_log,
        )

        wrs = self._worker_runtime_settings
        if wrs is None:
            return
        url = str(getattr(wrs, "market_data_redis_url", "") or "").strip()
        if not url:
            return
        feeds = tuple(getattr(wrs, "market_data_feeds", ()) or ("bars",))
        bar_tf = (getattr(wrs, "replay_bar_timeframe", None) or "1m").strip() or "1m"
        md_keys = market_data_redis_keys_from_settings(wrs)
        stream_bases = resolve_stream_names(
            feeds, bar_timeframe=bar_tf, redis_keys=md_keys
        )
        if not stream_bases:
            return
        pc_raw = getattr(wrs, "market_data_realtime_partition_count", 128)
        try:
            partition_count = int(pc_raw)
        except (TypeError, ValueError):
            partition_count = 128
        scope = self._live_market_data_partition_scope(partition_count=partition_count)
        if scope is None:
            return
        strategy_symbol, partition, partition_symbols = scope
        stream_names = expand_market_data_stream_keys(
            stream_bases,
            partition_count=partition_count,
            symbol_filter=partition_symbols,
        )
        if not stream_names:
            return
        use_cg = bool(getattr(wrs, "market_data_redis_use_consumer_group", False))
        cgp = (
            str(
                getattr(
                    wrs, "market_data_consumer_group_prefix", "strategy-worker-runtime"
                )
                or "strategy-worker-runtime"
            ).strip()
            or "strategy-worker-runtime"
        )
        cnp = (
            str(
                getattr(
                    wrs,
                    "market_data_consumer_name_prefix",
                    "strategy-worker-runtime-worker",
                )
                or "strategy-worker-runtime-worker"
            ).strip()
            or "strategy-worker-runtime-worker"
        )
        dep_raw = self._launch_payload.get("deployment_id")
        dep_s = str(dep_raw).strip() if dep_raw is not None else ""
        group = build_market_data_consumer_group_name(
            group_prefix=cgp,
            deployment_id=dep_s or None,
            runtime_id=self._launch_spec.runtime_id,
        )[:200]
        consumer = (
            f"{sanitize_redis_stream_consumer_token(cnp)}-"
            f"{os.getpid()}-{uuid.uuid4().hex[:10]}"
        )
        self._market_data_stream_log(
            level="INFO",
            event_name="market_data_stream.step_worker_thread_ingress",
            message="Step: worker thread starting Redis ingress (same plan as subscription_snapshot).",
            fields={
                "redis_url": sanitize_redis_url_for_log(url),
                "feeds": list(feeds),
                "bar_timeframe": bar_tf,
                "strategy_symbol": strategy_symbol,
                "partition": partition,
                "partition_count": partition_count,
                "physical_stream_keys": list(stream_names),
                "use_consumer_group": use_cg,
                "consumer_group": group if use_cg else None,
                "consumer_name": consumer if use_cg else None,
                **redis_stream_key_prefixes_snapshot(md_keys),
                **market_data_read_command_fields(
                    use_consumer_group=use_cg,
                    stream_names=list(stream_names),
                    stream_start_id=str(
                        getattr(wrs, "market_data_stream_start_id", "$") or "$"
                    ),
                    block_ms=int(getattr(wrs, "market_data_xread_block_ms", 0)),
                    count=int(getattr(wrs, "market_data_xread_count", 100)),
                    consumer_group_name=group if use_cg else "",
                    consumer_name=consumer if use_cg else "",
                ),
            },
        )
        run_market_data_redis_loop(
            redis_url=url,
            stream_names=stream_names,
            stream_start_id=str(
                getattr(wrs, "market_data_stream_start_id", "$") or "$"
            ),
            block_ms=int(getattr(wrs, "market_data_xread_block_ms", 0)),
            count=int(getattr(wrs, "market_data_xread_count", 100)),
            strategy_symbol=strategy_symbol,
            on_tick=self._dispatch_paper_live_tick,
            should_stop=self._live_md_feed_stop,
            bar_timeframe=bar_tf,
            redis_keys=md_keys,
            use_consumer_group=use_cg,
            consumer_group_name=group if use_cg else "",
            consumer_name=consumer if use_cg else "",
            log=lambda **kw: self._market_data_stream_log(
                level=str(kw.get("level") or "INFO"),
                event_name=str(kw.get("event_name") or ""),
                message=str(kw.get("message") or ""),
                fields=dict(kw.get("fields") or {}),
            ),
        )

    def _maybe_start_portfolio_update_feed(self) -> None:
        if self._portfolio_update_feed_started:
            return
        if self._launch_spec.mode not in (RuntimeMode.PAPER, RuntimeMode.LIVE):
            return
        wrs = self._worker_runtime_settings
        if wrs is None or not bool(getattr(wrs, "portfolio_update_enabled", True)):
            return
        job_id = str(self._launch_spec.job_id or "").strip()
        if not job_id:
            print(
                "[portfolio-update-redis] launch job_id is required for portfolio "
                "balance subscription; feed not started.",
                flush=True,
            )
            return
        redis_url = str(getattr(wrs, "portfolio_update_redis_url", "") or "").strip()
        if not redis_url:
            redis_url = str(getattr(wrs, "market_data_redis_url", "") or "").strip()
        if not redis_url:
            print(
                "[portfolio-update-redis] not configured (set portfolio_update_redis_url, "
                "SWR_PORTFOLIO_UPDATE_REDIS_URL, or market_data_redis_url).",
                flush=True,
            )
            return
        pc_raw = getattr(wrs, "market_data_realtime_partition_count", 128)
        try:
            partition_count = int(pc_raw)
        except (TypeError, ValueError):
            partition_count = 128
        from runtime.integration.portfolio_redis_feed import (
            portfolio_update_channel,
            portfolio_update_partition,
        )

        partition = portfolio_update_partition(job_id, partition_count)
        prefix = str(
            getattr(wrs, "portfolio_update_channel_prefix", "portfolio:update")
            or "portfolio:update"
        ).strip()
        channel = portfolio_update_channel(prefix, partition=partition)
        print(
            "[portfolio-update-redis] "
            f"SUBSCRIBE {channel!r} for job_id={job_id!r} "
            f"(partition={partition}, partition_count={partition_count})",
            flush=True,
        )
        self._portfolio_update_feed_started = True
        self._portfolio_update_feed_stop.clear()
        thread = threading.Thread(
            target=self._portfolio_update_redis_worker,
            name="swr-portfolio-update-feed",
            daemon=False,
        )
        self._portfolio_update_feed_thread = thread
        thread.start()

    def _stop_portfolio_update_feed_worker(self) -> None:
        self._portfolio_update_feed_stop.set()
        t = self._portfolio_update_feed_thread
        if t is not None and t.is_alive():
            t.join(timeout=60.0)

    def _portfolio_update_redis_worker(self) -> None:
        from runtime.integration.portfolio_redis_feed import (
            portfolio_update_channel,
            portfolio_update_partition,
            run_portfolio_update_pubsub_loop,
        )

        wrs = self._worker_runtime_settings
        if wrs is None:
            return
        job_id = str(self._launch_spec.job_id or "").strip()
        if not job_id:
            return
        redis_url = str(getattr(wrs, "portfolio_update_redis_url", "") or "").strip()
        if not redis_url:
            redis_url = str(getattr(wrs, "market_data_redis_url", "") or "").strip()
        if not redis_url:
            return
        pc_raw = getattr(wrs, "market_data_realtime_partition_count", 128)
        try:
            partition_count = int(pc_raw)
        except (TypeError, ValueError):
            partition_count = 128
        partition = portfolio_update_partition(job_id, partition_count)
        prefix = str(
            getattr(wrs, "portfolio_update_channel_prefix", "portfolio:update")
            or "portfolio:update"
        ).strip()
        channel = portfolio_update_channel(prefix, partition=partition)
        run_portfolio_update_pubsub_loop(
            redis_url=redis_url,
            channel=channel,
            expected_job_id=job_id,
            on_update=self._apply_portfolio_balance_update,
            should_stop=self._portfolio_update_feed_stop,
            log=lambda **kw: self._portfolio_update_stream_log(
                level=str(kw.get("level") or "INFO"),
                event_name=str(kw.get("event_name") or ""),
                message=str(kw.get("message") or ""),
                fields=dict(kw.get("fields") or {}),
            ),
        )

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

    def _apply_portfolio_balance_update(self, msg: Mapping[str, Any]) -> None:
        """Update in-memory account snapshot only; never invoke strategy hooks."""
        if self._shutdown_in_progress:
            return
        if self._phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
            return
        adapter = self._strategy_adapter
        if adapter is None:
            return
        bridge = getattr(adapter, "_replay_sdk_bridge", None)
        if bridge is None:
            return
        account = bridge.strategy_context.account
        apply_fn = getattr(account, "apply_pls_balance_update", None)
        if not callable(apply_fn):
            return
        with self._replay_state_lock:
            apply_fn(msg)

    def _historical_backtest_driver_worker(self) -> None:
        from runtime.transport.grpc.serializers import (
            _ensure_generated_proto_path,
        )

        _ensure_generated_proto_path()
        import importlib

        historical_data_pb2 = importlib.import_module("historical_data_pb2")

        client = self._historical_data_client
        wrs = self._worker_runtime_settings
        if client is None or wrs is None:
            return

        sym = launch_payload_backtest_symbol(self._launch_payload)
        if not sym:
            sym = str(getattr(wrs, "backtest_symbol", "") or "").strip() or None
        ts_start = self._launch_spec.ts_start
        ts_end = self._launch_spec.ts_end
        if not sym or not ts_start or not ts_end:
            print(
                "[historical] backtest feed skipped: need parameters.symbol and ts_start/ts_end",
                flush=True,
            )
            return

        adapter = self._strategy_adapter
        if adapter is None:
            return
        warmup_total = load_warmup_bars_default_for_strategy(adapter.strategy_object)
        timeframe = (wrs.replay_bar_timeframe or "1m").strip() or "1m"
        page_limit = max(1, int(wrs.historical_bars_page_size))
        bar_replay_interval_ms = max(
            0, int(getattr(wrs, "backtest_bar_replay_interval_ms", 100))
        )

        very_first_page = True
        while not self._hds_feed_stop.is_set():
            if self._shutdown_in_progress:
                break
            if self._phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
                break

            deps = self._runtime_dependencies
            replay_gateway = deps.replay if deps is not None else None
            if replay_gateway is None:
                break

            sa = self._strategy_adapter
            if sa is None:
                break

            cursor: str | None = None
            while not self._hds_feed_stop.is_set():
                if self._shutdown_in_progress:
                    return
                if self._phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
                    return

                warmup_bars = warmup_total if very_first_page and not cursor else 0
                req = build_query_historical_request(
                    symbol=sym,
                    timeframe=timeframe,
                    from_utc=ts_start,
                    to_utc=ts_end,
                    limit=page_limit,
                    cursor=cursor,
                    warmup_bars=warmup_bars,
                    historical_data_pb2=historical_data_pb2,
                )
                try:
                    resp: Any = client.query_historical_data(req)
                except Exception as exc:
                    print(
                        f"[historical] QueryHistoricalData failed: {exc!r}", flush=True
                    )
                    if self._hds_feed_stop.wait(1.0):
                        return
                    continue

                bars = list(resp.bars)
                meta = resp.metadata
                has_more = bool(meta.has_more) if meta is not None else False
                next_cursor = (
                    str(meta.next_cursor or "").strip() if meta is not None else ""
                )

                if not bars:
                    # Until the first bar is replayed, treat empty responses like unprepared
                    # data (same window as WorkerApp "Waiting for data" for ingress).
                    if not self._first_data_received:
                        if self._hds_feed_stop.wait(1.0):
                            return
                        continue
                    if has_more and next_cursor:
                        cursor = next_cursor
                        continue
                    if self._hds_feed_stop.wait(0.25):
                        return
                    break

                for i, bar in enumerate(bars):
                    if self._hds_feed_stop.is_set() or self._shutdown_in_progress:
                        return
                    try:
                        tick = build_historical_bar_replay_tick(
                            bar=bar,
                            symbol=sym,
                            timeframe=timeframe,
                            sequence=i,
                        )
                    except ValueError as exc:
                        print(f"[historical] skip bar: {exc}", flush=True)
                        continue
                    event_ts = _parse_datetime(tick.get("event_time"))
                    replay_gateway.ingest_replay_tick(
                        tick,
                        simulated_time=event_ts,
                        strategy_callback=sa.on_event,
                    )
                    if bar_replay_interval_ms > 0:
                        if self._hds_feed_stop.wait(bar_replay_interval_ms / 1000.0):
                            return

                if not self._first_data_received:
                    if self._hds_feed_stop.wait(1.0):
                        return
                    continue

                if very_first_page:
                    very_first_page = False

                if has_more and next_cursor:
                    cursor = next_cursor
                else:
                    break

            # Inner loop exited without ``return``: no more bars in [ts_start, ts_end] (or empty
            # tail after first data). Align with replay ingress EOS so WorkerApp can stop.
            if self._launch_spec.mode is RuntimeMode.BACKTEST:
                self._backtest_replay_complete = True
            return

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

        if isinstance(error, BootstrapFailure):
            wrs = self._worker_runtime_settings
            if wrs is not None:
                srm_base_url = str(
                    getattr(wrs, "strategy_runtime_manager_base_url", "") or ""
                ).strip()
                if srm_base_url:
                    deployment_id = str(getattr(wrs, "deployment_id", "") or "").strip()
                    timeout_seconds = float(
                        getattr(wrs, "runtime_manager_heartbeat_timeout_seconds", 30.0)
                        or 30.0
                    )
                    try:
                        srm_response = report_bootstrap_failure_to_srm(
                            error,
                            runtime_id=self._launch_spec.runtime_id,
                            mode=self._launch_spec.mode.value,
                            owner_resource_id=deployment_id,
                            srm_base_url=srm_base_url,
                            timeout_seconds=timeout_seconds,
                        )
                        print_bootstrap_failure_outcome(
                            error,
                            srm_response=srm_response,
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
        if isinstance(error, RuntimeStartValidationFailedError):
            return (
                [],
                ["validate_launch_metadata"],
                [stage.value for stage in _BOOTSTRAP_STAGE_ORDER],
            )

        if isinstance(error, BootstrapFailure):
            failed_stage = error.stage
            try:
                failed_index = _BOOTSTRAP_STAGE_ORDER.index(failed_stage)
            except ValueError:
                return ([], [str(failed_stage.value)], [])

            passed = [stage.value for stage in _BOOTSTRAP_STAGE_ORDER[:failed_index]]
            not_checked = [
                stage.value for stage in _BOOTSTRAP_STAGE_ORDER[failed_index + 1 :]
            ]
            return (passed, [failed_stage.value], not_checked)

        if "validate_sdk_contract" in self._startup_steps:
            return ([stage.value for stage in _BOOTSTRAP_STAGE_ORDER], [], [])

        return ([], [], [stage.value for stage in _BOOTSTRAP_STAGE_ORDER])

    def _record_order_intent_for_journal(
        self,
        source: str,
        payload: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        if self._state_journal is not None:
            self._state_journal.record_order_intent(source, payload, result)
        if self._launch_spec.mode in (RuntimeMode.PAPER, RuntimeMode.LIVE):
            count, sample = self._domain_event_sampler.next("order_intent_emitted")
            self._emit_domain_event(
                StrategyWorkerDomainEvent.ORDER_INTENT_EMITTED,
                "Order intent submitted from strategy worker",
                level=logging.INFO if sample else logging.DEBUG,
                event_extras={
                    "source": source,
                    "symbol": str(payload.get("symbol") or ""),
                    "instrument_id": str(payload.get("instrument_id") or ""),
                    "side": str(payload.get("side") or ""),
                    "order_type": str(payload.get("order_type") or ""),
                    "accepted": bool(result.get("accepted")),
                    "order_intent_count": count,
                    "sampled": sample,
                },
            )

    def _simulated_clock_seeded_from_dependencies(self) -> SimulatedClock:
        """Wall- or dependency-seeded simulated clock for SDK context binding (paper/live)."""
        clock = SimulatedClock()
        wall = _utc_now()
        if self._runtime_dependencies is not None:
            now_fn = getattr(self._runtime_dependencies.clock, "now", None)
            if callable(now_fn):
                try:
                    wall = now_fn()
                except Exception:
                    pass
        if wall.tzinfo is None or wall.utcoffset() is None:
            wall = wall.replace(tzinfo=timezone.utc)
        clock.set_time(wall.astimezone(timezone.utc))
        return clock

    def _make_replay_sdk_bridge(
        self,
        *,
        strategy_instance: object,
        simulated_clock: SimulatedClock,
    ) -> tuple[ReplaySdkBridge, Callable[[Any], dict[str, Any]] | None]:
        assert self._runtime_dependencies is not None
        wrs = self._worker_runtime_settings
        submitter = build_sdk_order_intent_submitter(
            dependencies=self._runtime_dependencies,
            launch_spec=self._launch_spec,
            worker_identity=self._worker_identity,
            on_order_intent_result=self._record_order_intent_for_journal,
            oms_correlation_id=wrs.oms_correlation_id if wrs is not None else "",
            disable_order_intent_grpc=(
                wrs.disable_order_intent_grpc if wrs is not None else False
            ),
            latest_market_event_at=self._peek_last_data_event_timestamp,
            allocate_order_intent_id=(
                self._state_journal.allocate_order_intent_id
                if self._state_journal is not None
                else None
            ),
        )
        bridge = build_replay_sdk_bridge(
            strategy=strategy_instance,
            launch_spec=self._launch_spec,
            worker_identity=self._worker_identity,
            simulated_clock=simulated_clock,
            launch_payload=self._launch_payload,
            submit_sdk_order_intent=submitter,
            default_timeframe=wrs.replay_bar_timeframe if wrs is not None else "1m",
        )
        return bridge, submitter

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
                event_extras={
                    "replay_type": type(self._runtime_dependencies.replay).__name__,
                },
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
        success: BootstrapPipelineSuccess,
    ) -> StrategyAdapter | None:
        if self._strategy_instance_manager is None:
            return None

        assignment_key = StrategyAssignmentKey(
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
                self._launch_spec.mode is RuntimeMode.BACKTEST
                and self._runtime_dependencies is not None
                and isinstance(self._runtime_dependencies.clock, SimulatedClock)
            ):
                bridge, submitter = self._make_replay_sdk_bridge(
                    strategy_instance=strategy_instance,
                    simulated_clock=self._runtime_dependencies.clock,
                )
                self._emit_backtest_order_submitter_bridge_logs(submitter)
            elif (
                self._runtime_dependencies is not None
                and self._launch_spec.mode in (RuntimeMode.PAPER, RuntimeMode.LIVE)
                and callable(getattr(strategy_instance, "_bind_context", None))
            ):
                sdk_clock = self._simulated_clock_seeded_from_dependencies()
                bridge, _ = self._make_replay_sdk_bridge(
                    strategy_instance=strategy_instance,
                    simulated_clock=sdk_clock,
                )
            return StrategyAdapter(
                strategy=strategy_instance,
                replay_sdk_bridge=bridge,
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
        try:
            emit_bound_domain_event(
                log,
                event_name=event,
                message=message,
                level=level,
                event_extras=event_extras,
            )
        except Exception:
            return

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
            "correlation_id": "",
            "causation_id": "",
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
                        source=SRM_STATUS_SOURCE_UPDATE,
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
