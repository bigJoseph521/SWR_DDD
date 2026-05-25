from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from runtime.observability.diagnostic_recorder import (
    DiagnosticEntry,
    SQLiteDiagnosticRecorder,
)
from runtime.persistence.db import begin_connection, create_engine
from runtime.persistence.migrations import apply_migrations
from runtime.persistence.repositories import (
    SQLiteWorkerInstanceRepository,
    WorkerInstanceRecord,
)


def _ts(seconds: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def test_persists_bootstrap_failures_and_queries_by_runtime_and_attempt(
    tmp_path: Path,
) -> None:
    engine = create_engine(tmp_path / "diag.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        SQLiteWorkerInstanceRepository(connection).upsert_instance(
            WorkerInstanceRecord(
                runtime_id="rt-1",
                worker_identity='{"runtime_id":"rt-1"}',
                tenant_id="tenant-1",
                strategy_version_id="sv-1",
                launch_attempt=1,
                state="INITIALIZING",
                occurred_at=_ts(1),
                observed_at=_ts(1),
                account_id="acct-1",
            )
        )
        recorder = SQLiteDiagnosticRecorder(connection)

        first_attempt = DiagnosticEntry(
            runtime_id="rt-1",
            worker_identity="worker:rt-1:1",
            launch_attempt=1,
            diagnostic_type="bootstrap_failure",
            observed_at=_ts(10),
            reason_code="ARTIFACT_NOT_FOUND",
            details={
                "stage": "artifact_fetch",
                "artifact_reference": "artifact://missing",
            },
            correlation_id="corr-1",
        )
        second_attempt = DiagnosticEntry(
            runtime_id="rt-1",
            worker_identity="worker:rt-1:2",
            launch_attempt=2,
            diagnostic_type="bootstrap_failure",
            observed_at=_ts(20),
            reason_code="ENTRYPOINT_IMPORT_FAILED",
            details={"stage": "entrypoint_load", "module": "broken_strategy"},
            correlation_id="corr-2",
        )

        recorder.record(first_attempt)
        recorder.record(second_attempt)

        by_runtime = recorder.list_for_runtime("rt-1")
        assert len(by_runtime) == 2
        assert [item.launch_attempt for item in by_runtime] == [1, 2]
        assert by_runtime[0].worker_identity == "worker:rt-1:1"
        assert by_runtime[1].worker_identity == "worker:rt-1:2"

        by_attempt_1 = recorder.list_for_runtime_attempt("rt-1", 1)
        assert len(by_attempt_1) == 1
        assert by_attempt_1[0].launch_attempt == 1
        assert by_attempt_1[0].reason_code == "ARTIFACT_NOT_FOUND"
        assert by_attempt_1[0].details == {
            "stage": "artifact_fetch",
            "artifact_reference": "artifact://missing",
        }
        assert by_attempt_1[0].observed_at == _ts(10)

        by_attempt_2 = recorder.list_for_runtime_attempt("rt-1", 2)
        assert len(by_attempt_2) == 1
        assert by_attempt_2[0].launch_attempt == 2
        assert by_attempt_2[0].reason_code == "ENTRYPOINT_IMPORT_FAILED"
        assert by_attempt_2[0].details == {
            "stage": "entrypoint_load",
            "module": "broken_strategy",
        }
