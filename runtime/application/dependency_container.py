from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from sqlalchemy.engine import Connection
from runtime.application.lifecycle_service import LifecycleService
from runtime.application.worker_app import WorkerApp
from runtime.bootstrap.artifact_fetcher import ArtifactFetcher
from runtime.bootstrap.artifact_verifier import ArtifactVerifier
from runtime.bootstrap.entrypoint_loader import EntrypointLoader
from runtime.bootstrap.failures import BootstrapFailure
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.persistence import (
    BootstrapPersistenceCoordinator,
)
from runtime.bootstrap.sdk_contract_validator import (
    BootstrapPipeline,
    BootstrapPipelineSuccess,
    SdkContractValidator,
)
from runtime.bootstrap.strategy_instance_manager import (
    StrategyInstanceManager,
)
from runtime.bootstrap.validator import LaunchSpecValidator
from runtime.config.logging import (
    build_runtime_log_context,
    configure_logging,
)
from runtime.config.settings import Settings
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
)
from runtime.domain.worker_identity import WorkerIdentity
from runtime.observability.logger import (
    RuntimeBoundLogger,
    bind_runtime_context,
)
from runtime.persistence.db import begin_connection, create_engine
from runtime.persistence.migrations import apply_migrations
from runtime.persistence.repositories import (
    SQLiteDiagnosticRepository,
    SQLiteLaunchAttemptRepository,
    SQLiteWorkerEventRepository,
    SQLiteWorkerInstanceRepository,
)
from runtime.persistence.runtime_journal_sink import RuntimeJournalSink
from runtime.runtime.dependencies import (
    RuntimeDependencies,
    build_runtime_dependencies,
)
from runtime.runtime.heartbeat_periodic import HeartbeatPeriodicJobs
from runtime.runtime.mode_policy import ModePolicy, get_mode_policy
from runtime.transport.grpc.historical_data_client import HistoricalDataGrpcClient


class _NoopManagerClient:
    def emit_signal(self, payload: dict[str, object]) -> dict[str, object]:
        return {"accepted": True, "signal_type": payload.get("signal_type")}


class _NoopOmsClient:
    def submit_order_intent(self, payload: dict[str, object]) -> dict[str, object]:
        return {
            "accepted": False,
            "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
            "error_code": WorkerErrorCode.DEPENDENCY_UNHEALTHY.value,
        }

    def submit_cancel_order_intent(
        self, payload: dict[str, object]
    ) -> dict[str, object]:
        return {
            "accepted": False,
            "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
            "error_code": WorkerErrorCode.DEPENDENCY_UNHEALTHY.value,
        }

    def submit_replace_order_intent(
        self, payload: dict[str, object]
    ) -> dict[str, object]:
        return {
            "accepted": False,
            "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
            "error_code": WorkerErrorCode.DEPENDENCY_UNHEALTHY.value,
        }


class _NoopReplayClient:
    def ingest_replay_tick(self, payload: dict[str, object]) -> dict[str, object]:
        return {"accepted": True, "replay": payload}

    def submit_backtest_order_intent(
        self, payload: dict[str, object]
    ) -> dict[str, object]:
        return {
            "accepted": False,
            "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
            "error_code": WorkerErrorCode.DEPENDENCY_UNHEALTHY.value,
            "payload_echo": dict(payload),
        }


class _SQLiteBootstrapPersistence:
    """Persist bootstrap lifecycle transitions into worker tables."""

    def __init__(self, *, db_path: Path) -> None:
        self._engine = create_engine(db_path)
        self._lock = threading.Lock()
        with begin_connection(self._engine) as connection:
            apply_migrations(connection)

    def _coordinator(self, connection: Connection) -> BootstrapPersistenceCoordinator:
        return BootstrapPersistenceCoordinator(
            instances=SQLiteWorkerInstanceRepository(connection),
            attempts=SQLiteLaunchAttemptRepository(connection),
            events=SQLiteWorkerEventRepository(connection),
            diagnostics=SQLiteDiagnosticRepository(connection),
        )

    def on_bootstrap_start(
        self, launch_spec: LaunchSpec, *, observed_at: datetime
    ) -> None:
        with self._lock:
            with begin_connection(self._engine) as connection:
                self._coordinator(connection).on_bootstrap_start(
                    launch_spec, observed_at=observed_at
                )

    def on_bootstrap_failure(
        self,
        launch_spec: LaunchSpec,
        failure: BootstrapFailure,
        *,
        occurred_at: datetime,
        observed_at: datetime,
    ) -> None:
        with self._lock:
            with begin_connection(self._engine) as connection:
                self._coordinator(connection).on_bootstrap_failure(
                    launch_spec,
                    failure,
                    occurred_at=occurred_at,
                    observed_at=observed_at,
                )

    def on_bootstrap_success(
        self,
        launch_spec: LaunchSpec,
        success: BootstrapPipelineSuccess,
        *,
        occurred_at: datetime,
        observed_at: datetime,
    ) -> None:
        with self._lock:
            with begin_connection(self._engine) as connection:
                self._coordinator(connection).on_bootstrap_success(
                    launch_spec,
                    success,
                    occurred_at=occurred_at,
                    observed_at=observed_at,
                )


@dataclass(frozen=True, slots=True)
class DependencyContainer:
    settings: Settings
    launch_spec: LaunchSpec
    worker_identity: WorkerIdentity
    mode_policy: ModePolicy
    launch_spec_validator: LaunchSpecValidator
    bootstrap_pipeline: BootstrapPipeline
    runtime_dependencies_initializer: Callable[[], RuntimeDependencies]
    lifecycle_service: LifecycleService
    worker_app: WorkerApp


def _build_worker_identity(launch_spec: LaunchSpec) -> WorkerIdentity:
    return WorkerIdentity(
        runtime_id=launch_spec.runtime_id,
        tenant_id=launch_spec.tenant_id,
        strategy_version_id=launch_spec.strategy_version_id,
        mode=launch_spec.mode,
        trader_id=launch_spec.trader_id,
        account_id=launch_spec.account_id,
        artifact_uri=launch_spec.artifact_uri,
        artifact_digest=launch_spec.artifact_digest,
        entrypoint=launch_spec.entrypoint,
        launch_attempt=launch_spec.launch_attempt,
    )


def build_dependency_container(
    settings: Settings,
    *,
    manager_client: object | None = None,
    oms_client: object | None = None,
    replay_client: object | None = None,
    historical_data_client: HistoricalDataGrpcClient | None = None,
    strategy_instance_manager: StrategyInstanceManager | None = None,
    bootstrap_pipeline: BootstrapPipeline | None = None,
) -> DependencyContainer:
    launch_spec = settings.launch_spec
    mode_policy = get_mode_policy(launch_spec.mode)
    worker_identity = _build_worker_identity(launch_spec)

    launch_spec_validator = LaunchSpecValidator()
    bootstrap_persistence = (
        _SQLiteBootstrapPersistence(db_path=settings.state_journal_sqlite_path)
        if settings.state_journal_enabled
        else None
    )

    if bootstrap_pipeline is None:
        artifact_fetcher = ArtifactFetcher(
            work_root=Path(settings.work_root),
            strategy_bundle_base=settings.bundle_resolve_base_dir,
            artifact_local_base=settings.artifact_local_base_path,
        )
        artifact_verifier = ArtifactVerifier()
        entrypoint_loader = EntrypointLoader()
        sdk_validator = SdkContractValidator()
        bootstrap_pipeline = BootstrapPipeline(
            fetcher=artifact_fetcher,
            verifier=artifact_verifier,
            entrypoint_loader=entrypoint_loader,
            sdk_validator=sdk_validator,
            persistence=bootstrap_persistence,
        )

    base_logger = configure_logging()
    runtime_log_context = build_runtime_log_context(
        settings=settings, worker_identity=worker_identity
    )

    def bind_logger() -> RuntimeBoundLogger:
        return bind_runtime_context(base_logger, runtime_log_context)

    effective_manager_client = manager_client or _NoopManagerClient()
    if launch_spec.mode.value == "BACKTEST":
        effective_oms_client = oms_client or _NoopOmsClient()
        effective_replay_client = replay_client or _NoopReplayClient()
    else:
        effective_oms_client = oms_client or _NoopOmsClient()
        effective_replay_client = None

    runtime_dependencies_cache: RuntimeDependencies | None = None
    lifecycle_service: LifecycleService | None = None
    lifecycle_ref: list[LifecycleService | None] = [None]

    def heartbeat_tick() -> None:
        svc = lifecycle_ref[0]
        if svc is not None:
            svc.emit_periodic_heartbeat()

    heartbeat_periodic = HeartbeatPeriodicJobs(
        on_tick=heartbeat_tick,
        interval_seconds=settings.heartbeat_interval_seconds,
    )

    state_journal: RuntimeJournalSink | None = None
    if settings.state_journal_enabled:
        state_journal = RuntimeJournalSink(
            db_path=settings.state_journal_sqlite_path,
            txt_path=settings.state_journal_txt_path,
            runtime_id=launch_spec.runtime_id,
            launch_attempt=launch_spec.launch_attempt,
            launch_job_id=launch_spec.job_id,
        )

    def initialize_runtime_dependencies() -> RuntimeDependencies:
        nonlocal runtime_dependencies_cache
        if runtime_dependencies_cache is None:

            def _on_oms_order_intent_result(
                source: str,
                payload: dict[str, object],
                result: dict[str, object],
            ) -> None:
                if state_journal is not None:
                    state_journal.record_order_intent(source, payload, result)

            runtime_dependencies_cache = build_runtime_dependencies(
                launch_spec.mode,
                manager_client=effective_manager_client,
                runtime_identity=worker_identity,
                oms_client=effective_oms_client,
                replay_client=effective_replay_client,
                on_first_data=(
                    lifecycle_service.mark_first_data_received
                    if lifecycle_service is not None
                    else None
                ),
                on_lifecycle_event=(
                    state_journal.record_lifecycle_event
                    if state_journal is not None
                    else None
                ),
                replay_tick_logging_quiet=settings.replay_tick_logging_quiet,
                heartbeat_log_enabled=settings.heartbeat_log_enabled,
                on_oms_order_intent_result=(
                    _on_oms_order_intent_result if state_journal is not None else None
                ),
            )
        return runtime_dependencies_cache

    closeables_list: list[object] = [
        effective_manager_client,
        effective_oms_client,
        effective_replay_client,
    ]
    if historical_data_client is not None:
        closeables_list.append(historical_data_client)

    lifecycle_service = LifecycleService(
        launch_spec=launch_spec,
        launch_payload=settings.launch_payload,
        worker_identity=worker_identity,
        launch_spec_validator=launch_spec_validator,
        bootstrap_pipeline=bootstrap_pipeline,
        log_binder=bind_logger,
        runtime_dependencies_initializer=initialize_runtime_dependencies,
        strategy_instance_manager=strategy_instance_manager
        or StrategyInstanceManager(),
        periodic_jobs=heartbeat_periodic,
        closeables=tuple(closeables_list),
        state_journal=state_journal,
        worker_runtime_settings=settings,
        historical_data_client=historical_data_client,
    )
    lifecycle_ref[0] = lifecycle_service
    worker_app = WorkerApp(lifecycle_service)

    return DependencyContainer(
        settings=settings,
        launch_spec=launch_spec,
        worker_identity=worker_identity,
        mode_policy=mode_policy,
        launch_spec_validator=launch_spec_validator,
        bootstrap_pipeline=bootstrap_pipeline,
        runtime_dependencies_initializer=initialize_runtime_dependencies,
        lifecycle_service=lifecycle_service,
        worker_app=worker_app,
    )
