from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from runtime.domain.bootstrap_failures import BootstrapFailure, BootstrapStage
from runtime.domain.launch_spec import LaunchSpec
from runtime.bootstrap.persistence import (
    BootstrapPersistenceCoordinator,
)
from runtime.domain.enums import WorkerMode
from runtime.infrastructure.persistence.db import begin_connection, create_engine
from runtime.infrastructure.persistence.migrations import apply_migrations
from runtime.infrastructure.persistence.repositories import (
    SQLiteDiagnosticRepository,
    SQLiteLaunchAttemptRepository,
    SQLiteWorkerEventRepository,
    SQLiteWorkerInstanceRepository,
)


def _launch_spec(launch_attempt: int = 1) -> LaunchSpec:
    return LaunchSpec(
        runtime_id="rt-bootstrap",
        tenant_id="tenant-1",
        strategy_version_id="sv-1",
        mode=WorkerMode.BACKTEST,
        launch_attempt=launch_attempt,
        artifact_uri="file:///tmp/strategy",
        entrypoint="strategy.main:Strategy",
        account_id="acct-1",
        trader_id=None,
        artifact_digest="sha256:abc",
        ts_start="2026-01-01T10:00:00Z",
        ts_end="2026-01-07T10:00:00Z",
    )


def test_bootstrap_persistence_uses_canonical_worker_identity(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)
        coordinator = BootstrapPersistenceCoordinator(
            instances=SQLiteWorkerInstanceRepository(connection),
            attempts=SQLiteLaunchAttemptRepository(connection),
            events=SQLiteWorkerEventRepository(connection),
            diagnostics=SQLiteDiagnosticRepository(connection),
        )
        observed = datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
        spec = _launch_spec(launch_attempt=1)
        coordinator.on_bootstrap_start(spec, observed_at=observed)
        failure = BootstrapFailure(
            stage=BootstrapStage.ARTIFACT_VERIFY,
            reason_code="DIGEST_MISMATCH",
            retryable=False,
            details={},
        )
        coordinator.on_bootstrap_failure(
            spec,
            failure,
            occurred_at=observed,
            observed_at=observed,
        )

        instance = SQLiteWorkerInstanceRepository(connection).get_by_runtime_id(
            "rt-bootstrap"
        )
        failed_event = SQLiteWorkerEventRepository(connection).get_by_event_id(
            "rt-bootstrap:1:runtime.launch_failed"
        )

    assert instance is not None
    assert instance.worker_identity == "rt-bootstrap:sv-1:1"
    assert failed_event is not None
    assert failed_event.worker_identity == "rt-bootstrap:sv-1:1"
