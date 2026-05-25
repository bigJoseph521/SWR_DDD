from __future__ import annotations

from datetime import datetime

from sqlalchemy.exc import IntegrityError
from runtime.bootstrap.failures import BootstrapFailure
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.sdk_contract_validator import (
    BootstrapPipelineSuccess,
)
from runtime.domain.enums import WorkerEventName
from runtime.persistence.repositories import (
    DiagnosticRecord,
    LaunchAttemptRecord,
    SQLiteDiagnosticRepository,
    SQLiteLaunchAttemptRepository,
    SQLiteWorkerEventRepository,
    SQLiteWorkerInstanceRepository,
    WorkerEventRecord,
    WorkerInstanceRecord,
)


class BootstrapPersistenceCoordinator:
    def __init__(
        self,
        *,
        instances: SQLiteWorkerInstanceRepository,
        attempts: SQLiteLaunchAttemptRepository,
        events: SQLiteWorkerEventRepository,
        diagnostics: SQLiteDiagnosticRepository,
    ) -> None:
        self._instances = instances
        self._attempts = attempts
        self._events = events
        self._diagnostics = diagnostics

    def on_bootstrap_start(
        self, launch_spec: LaunchSpec, *, observed_at: datetime
    ) -> None:
        worker_identity = self._canonical_worker_identity(launch_spec)
        self._instances.upsert_instance(
            WorkerInstanceRecord(
                runtime_id=launch_spec.runtime_id,
                worker_identity=worker_identity,
                tenant_id=launch_spec.tenant_id,
                trader_id=launch_spec.trader_id,
                account_id=launch_spec.account_id,
                strategy_version_id=launch_spec.strategy_version_id,
                launch_attempt=launch_spec.launch_attempt,
                state="INITIALIZING",
                reason_code=None,
                occurred_at=observed_at,
                observed_at=observed_at,
                correlation_id=None,
                causation_id=None,
            )
        )
        try:
            self._attempts.append_attempt(
                LaunchAttemptRecord(
                    runtime_id=launch_spec.runtime_id,
                    launch_attempt=launch_spec.launch_attempt,
                    state="STARTING",
                    reason_code=None,
                    occurred_at=observed_at,
                    observed_at=observed_at,
                    correlation_id=None,
                    causation_id=None,
                    details={"stage": "bootstrap_start"},
                )
            )
        except IntegrityError:
            # Duplicate launch attempt rows are idempotent for replayed start signals.
            pass

    def on_bootstrap_failure(
        self,
        launch_spec: LaunchSpec,
        failure: BootstrapFailure,
        *,
        occurred_at: datetime,
        observed_at: datetime,
    ) -> None:
        self._instances.update_state(
            launch_spec.runtime_id,
            state="FAILED",
            reason_code=failure.reason_code,
            launch_attempt=launch_spec.launch_attempt,
            occurred_at=occurred_at,
            observed_at=observed_at,
            correlation_id=None,
            causation_id=None,
        )
        self._attempts.mark_outcome(
            launch_spec.runtime_id,
            launch_spec.launch_attempt,
            state="FAILED",
            reason_code=failure.reason_code,
            occurred_at=occurred_at,
            observed_at=observed_at,
            details={
                "stage": failure.stage.value,
                "retryable": failure.retryable,
                "details": dict(failure.details),
            },
        )
        self._diagnostics.append_diagnostic(
            DiagnosticRecord(
                runtime_id=launch_spec.runtime_id,
                launch_attempt=launch_spec.launch_attempt,
                diagnostic_type="bootstrap_failure",
                stage=failure.stage.value,
                reason_code=failure.reason_code,
                occurred_at=occurred_at,
                observed_at=observed_at,
                correlation_id=None,
                causation_id=None,
                details=dict(failure.details),
            )
        )
        self._events.append_event(
            WorkerEventRecord(
                event_id=f"{launch_spec.runtime_id}:{launch_spec.launch_attempt}:runtime.launch_failed",
                runtime_id=launch_spec.runtime_id,
                worker_identity=self._canonical_worker_identity(launch_spec),
                launch_attempt=launch_spec.launch_attempt,
                event_family=WorkerEventName.LAUNCH_FAILED.value,
                state="FAILED",
                reason_code=failure.reason_code,
                occurred_at=occurred_at,
                observed_at=observed_at,
                correlation_id=None,
                causation_id=None,
                payload={
                    "stage": failure.stage.value,
                    "details": dict(failure.details),
                    "retryable": failure.retryable,
                },
            )
        )

    def on_bootstrap_success(
        self,
        launch_spec: LaunchSpec,
        success: BootstrapPipelineSuccess,
        *,
        occurred_at: datetime,
        observed_at: datetime,
    ) -> None:
        self._instances.update_state(
            launch_spec.runtime_id,
            state="READY",
            reason_code=None,
            launch_attempt=launch_spec.launch_attempt,
            occurred_at=occurred_at,
            observed_at=observed_at,
            correlation_id=None,
            causation_id=None,
        )
        self._attempts.mark_outcome(
            launch_spec.runtime_id,
            launch_spec.launch_attempt,
            state="SUCCEEDED",
            reason_code=None,
            occurred_at=occurred_at,
            observed_at=observed_at,
            details={
                "entrypoint": success.entrypoint.entrypoint_spec,
                "sdk_validation": dict(success.sdk_validation.details),
            },
        )
        self._events.append_event(
            WorkerEventRecord(
                event_id=f"{launch_spec.runtime_id}:{launch_spec.launch_attempt}:runtime.launch_succeeded",
                runtime_id=launch_spec.runtime_id,
                worker_identity=self._canonical_worker_identity(launch_spec),
                launch_attempt=launch_spec.launch_attempt,
                event_family=WorkerEventName.LAUNCH_SUCCEEDED.value,
                state="READY",
                reason_code=None,
                occurred_at=occurred_at,
                observed_at=observed_at,
                correlation_id=None,
                causation_id=None,
                payload={"entrypoint": success.entrypoint.entrypoint_spec},
            )
        )

    def _canonical_worker_identity(self, launch_spec: LaunchSpec) -> str:
        return (
            f"{launch_spec.runtime_id}:"
            f"{launch_spec.strategy_version_id}:"
            f"{launch_spec.launch_attempt}"
        )
