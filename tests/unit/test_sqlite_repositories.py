from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError
from runtime.infrastructure.persistence.db import begin_connection, create_engine
from runtime.infrastructure.persistence.migrations import apply_migrations
from runtime.infrastructure.persistence.repositories import (
    HeartbeatObservationRecord,
    LaunchAttemptRecord,
    SQLiteHeartbeatRepository,
    SQLiteLaunchAttemptRepository,
    SQLiteWorkerEventRepository,
    SQLiteWorkerInstanceRepository,
    WorkerEventRecord,
    WorkerInstanceRecord,
)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _upsert_seed_instance(
    instances: SQLiteWorkerInstanceRepository, runtime_id: str = "rt-1"
) -> None:
    now = _now()
    instances.upsert_instance(
        WorkerInstanceRecord(
            runtime_id=runtime_id,
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
            correlation_id="corr-1",
        )
    )


def test_upsert_read_update_and_strategy_filter(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        instances = SQLiteWorkerInstanceRepository(connection)
        _upsert_seed_instance(instances)

        row = instances.get_by_runtime_id("rt-1")
        assert row is not None
        assert row.state == "INITIALIZING"

        now = _now()
        instances.update_state(
            "rt-1",
            state="FAILED",
            reason_code="DIGEST_MISMATCH",
            launch_attempt=1,
            occurred_at=now,
            observed_at=now,
            correlation_id="corr-2",
        )

        updated = instances.get_by_runtime_id("rt-1")
        assert updated is not None
        assert updated.state == "FAILED"
        assert updated.reason_code == "DIGEST_MISMATCH"
        assert len(instances.list_by_strategy_version("sv-1")) == 1


def test_duplicate_launch_attempt_raises(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        instances = SQLiteWorkerInstanceRepository(connection)
        attempts = SQLiteLaunchAttemptRepository(connection)
        _upsert_seed_instance(instances)

        now = _now()
        attempt = LaunchAttemptRecord(
            runtime_id="rt-1",
            launch_attempt=1,
            state="STARTING",
            reason_code=None,
            occurred_at=now,
            observed_at=now,
            details={"stage": "start"},
        )
        attempts.append_attempt(attempt)
        with pytest.raises(IntegrityError):
            attempts.append_attempt(attempt)


def test_event_id_idempotency(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        instances = SQLiteWorkerInstanceRepository(connection)
        events = SQLiteWorkerEventRepository(connection)
        _upsert_seed_instance(instances)

        now = _now()
        event = WorkerEventRecord(
            event_id="evt-1",
            runtime_id="rt-1",
            worker_identity='{"runtime_id":"rt-1"}',
            launch_attempt=1,
            event_family="runtime.launch_succeeded",
            state="READY",
            reason_code=None,
            occurred_at=now,
            observed_at=now,
            payload={"key": "value"},
        )
        assert events.append_event(event) is True
        assert events.append_event(event) is False


def test_occurred_vs_observed_semantics_are_preserved(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        instances = SQLiteWorkerInstanceRepository(connection)
        attempts = SQLiteLaunchAttemptRepository(connection)
        _upsert_seed_instance(instances)

        occurred = _now()
        observed = occurred + timedelta(seconds=7)
        attempts.append_attempt(
            LaunchAttemptRecord(
                runtime_id="rt-1",
                launch_attempt=2,
                state="STARTING",
                reason_code=None,
                occurred_at=occurred,
                observed_at=observed,
                details={"order": "preserved"},
            )
        )
        saved = attempts.get_attempt("rt-1", 2)
        assert saved is not None
        assert saved.occurred_at == occurred
        assert saved.observed_at == observed


def test_heartbeat_updates_last_heartbeat_at_with_latest_policy(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        instances = SQLiteWorkerInstanceRepository(connection)
        heartbeats = SQLiteHeartbeatRepository(connection)
        _upsert_seed_instance(instances)

        base = _now()
        heartbeats.append_observation(
            HeartbeatObservationRecord(
                runtime_id="rt-1",
                launch_attempt=1,
                occurred_at=base,
                observed_at=base,
                details={"seq": 1},
            )
        )
        instances.record_heartbeat("rt-1", heartbeat_at=base, observed_at=base)

        newer = base + timedelta(seconds=5)
        heartbeats.append_observation(
            HeartbeatObservationRecord(
                runtime_id="rt-1",
                launch_attempt=1,
                occurred_at=newer,
                observed_at=newer,
                details={"seq": 2},
            )
        )
        instances.record_heartbeat("rt-1", heartbeat_at=newer, observed_at=newer)

        older = base - timedelta(seconds=5)
        instances.record_heartbeat("rt-1", heartbeat_at=older, observed_at=newer)

        latest = instances.get_by_runtime_id("rt-1")
        assert latest is not None
        assert latest.last_heartbeat_at == newer
