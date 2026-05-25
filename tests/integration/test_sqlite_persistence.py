from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from runtime.persistence.db import begin_connection, create_engine
from runtime.persistence.migrations import apply_migrations
from runtime.persistence.repositories import (
    DiagnosticRecord,
    HeartbeatObservationRecord,
    LaunchAttemptRecord,
    SQLiteDiagnosticRepository,
    SQLiteHeartbeatRepository,
    SQLiteLaunchAttemptRepository,
    SQLiteWorkerEventRepository,
    SQLiteWorkerInstanceRepository,
    WorkerEventRecord,
    WorkerInstanceRecord,
)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def test_sqlite_persistence_crud_and_idempotency(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        instances = SQLiteWorkerInstanceRepository(connection)
        attempts = SQLiteLaunchAttemptRepository(connection)
        heartbeats = SQLiteHeartbeatRepository(connection)
        events = SQLiteWorkerEventRepository(connection)
        diagnostics = SQLiteDiagnosticRepository(connection)

        now = _now()
        instances.upsert_instance(
            WorkerInstanceRecord(
                runtime_id="rt-1",
                worker_identity='{"runtime_id":"rt-1"}',
                tenant_id="tenant-1",
                account_id="acct-1",
                trader_id=None,
                strategy_version_id="sv-1",
                launch_attempt=1,
                state="INITIALIZING",
                reason_code=None,
                occurred_at=now,
                observed_at=now,
            )
        )
        attempts.append_attempt(
            LaunchAttemptRecord(
                runtime_id="rt-1",
                launch_attempt=1,
                state="STARTING",
                reason_code=None,
                occurred_at=now,
                observed_at=now + timedelta(seconds=1),
            )
        )
        heartbeats.append_observation(
            HeartbeatObservationRecord(
                runtime_id="rt-1",
                launch_attempt=1,
                occurred_at=now + timedelta(seconds=2),
                observed_at=now + timedelta(seconds=3),
                details={"policy": "accept_all_observations"},
            )
        )
        diagnostics.append_diagnostic(
            DiagnosticRecord(
                runtime_id="rt-1",
                launch_attempt=1,
                diagnostic_type="bootstrap_failure",
                stage="artifact_verify",
                reason_code="DIGEST_MISMATCH",
                occurred_at=now + timedelta(seconds=2),
                observed_at=now + timedelta(seconds=4),
                details={"actual": "sha256:bad"},
            )
        )
        first_inserted = events.append_event(
            WorkerEventRecord(
                event_id="evt-1",
                runtime_id="rt-1",
                worker_identity='{"runtime_id":"rt-1"}',
                launch_attempt=1,
                event_family="runtime.launch_failed",
                state="FAILED",
                reason_code="DIGEST_MISMATCH",
                occurred_at=now + timedelta(seconds=2),
                observed_at=now + timedelta(seconds=5),
                payload={"stage": "artifact_verify"},
            )
        )
        second_inserted = events.append_event(
            WorkerEventRecord(
                event_id="evt-1",
                runtime_id="rt-1",
                worker_identity='{"runtime_id":"rt-1"}',
                launch_attempt=1,
                event_family="runtime.launch_failed",
                state="FAILED",
                reason_code="DIGEST_MISMATCH",
                occurred_at=now + timedelta(seconds=2),
                observed_at=now + timedelta(seconds=5),
                payload={"stage": "artifact_verify"},
            )
        )
        third_inserted = events.append_event(
            WorkerEventRecord(
                event_id="evt-2",
                runtime_id="rt-1",
                worker_identity='{"runtime_id":"rt-1"}',
                launch_attempt=2,
                event_family="runtime.launch_succeeded",
                state="READY",
                reason_code=None,
                occurred_at=now + timedelta(seconds=6),
                observed_at=now + timedelta(seconds=7),
                payload={"stage": "bootstrap_complete"},
            )
        )

        assert first_inserted is True
        assert second_inserted is False
        assert third_inserted is True
        assert attempts.get_attempt("rt-1", 1) is not None
        first_event = events.get_by_event_id("evt-1")
        second_event = events.get_by_event_id("evt-2")
        assert first_event is not None
        assert second_event is not None
        assert second_event.launch_attempt == 2
        assert heartbeats.latest_for_runtime("rt-1") is not None
        assert len(diagnostics.list_for_runtime("rt-1")) == 1
