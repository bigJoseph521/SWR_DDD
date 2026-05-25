from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
import runtime.persistence.migrations as migrations_module
from runtime.persistence.db import begin_connection, create_engine
from runtime.persistence.migrations import (
    MIGRATION_FILENAMES,
    _ensure_order_intent_journal_symbol_column,
    apply_migrations,
)


def _now_wire() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def test_migrate_empty_db_through_latest_and_smoke_queries(tmp_path: Path) -> None:
    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        apply_migrations(connection)

        tables = {
            row[0]
            for row in connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
        }
        assert "worker_instances" in tables
        assert "worker_launch_attempts" in tables
        assert "worker_heartbeat_observations" in tables
        assert "worker_events" in tables
        assert "worker_diagnostics" in tables
        assert "runtime_state_journal" in tables
        assert "order_intent_journal" in tables

        oij_cols = {
            row[1]
            for row in connection.execute(
                text("PRAGMA table_info(order_intent_journal)")
            ).fetchall()
        }
        assert "job_id" in oij_cols
        assert "idempotency_key" in oij_cols
        assert "order_intent_id" in oij_cols
        assert "symbol" in oij_cols
        assert "requested_at" in oij_cols

        index_names = {
            row[0]
            for row in connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='index'")
            ).fetchall()
        }
        assert "ix_worker_instances_strategy_version_id" in index_names
        assert "ix_worker_events_runtime_launch_occurred" in index_names
        assert "ix_worker_diagnostics_runtime_launch_type_observed" in index_names
        assert "ix_order_intent_journal_created_at" in index_names
        assert "ix_order_intent_journal_idempotency_key" in index_names
        assert "ix_order_intent_journal_order_intent_id" in index_names
        assert "ix_order_intent_journal_symbol" in index_names
        assert "ix_order_intent_journal_runtime_launch_requested" in index_names

        wire_now = _now_wire()
        connection.execute(
            text(
                """
                INSERT INTO worker_instances(
                    runtime_id, worker_identity, tenant_id, trader_id, account_id,
                    strategy_version_id, launch_attempt, state, reason_code,
                    occurred_at, observed_at, last_heartbeat_at, correlation_id, causation_id
                ) VALUES (
                    :runtime_id, :worker_identity, :tenant_id, :trader_id, :account_id,
                    :strategy_version_id, :launch_attempt, :state, :reason_code,
                    :occurred_at, :observed_at, :last_heartbeat_at, :correlation_id, :causation_id
                )
                """
            ),
            {
                "runtime_id": "rt-1",
                "worker_identity": '{"runtime_id":"rt-1"}',
                "tenant_id": "tenant-1",
                "trader_id": None,
                "account_id": "acct-1",
                "strategy_version_id": "sv-1",
                "launch_attempt": 1,
                "state": "INITIALIZING",
                "reason_code": None,
                "occurred_at": wire_now,
                "observed_at": wire_now,
                "last_heartbeat_at": None,
                "correlation_id": "corr-1",
                "causation_id": "cause-1",
            },
        )
        count = connection.execute(
            text("SELECT COUNT(*) FROM worker_instances WHERE runtime_id='rt-1'")
        ).scalar_one()
        assert count == 1

        applied = {
            row[0]
            for row in connection.execute(
                text("SELECT version FROM schema_migrations")
            ).fetchall()
        }
        assert set(MIGRATION_FILENAMES).issubset(applied)


def test_ensure_order_intent_journal_symbol_repairs_missing_column(
    tmp_path: Path,
) -> None:
    engine = create_engine(tmp_path / "partial_oij.db")
    with begin_connection(engine) as connection:
        connection.execute(
            text(
                """
                CREATE TABLE order_intent_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    runtime_id TEXT NOT NULL,
                    launch_attempt INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    result_json TEXT,
                    occurred_at TEXT NOT NULL,
                    correlation_id TEXT
                )
                """
            )
        )
        _ensure_order_intent_journal_symbol_column(connection)
        cols = {
            row[1]
            for row in connection.execute(
                text("PRAGMA table_info(order_intent_journal)")
            ).fetchall()
        }
        assert "symbol" in cols


def test_apply_migrations_raises_when_migration_file_missing(
    tmp_path: Path, monkeypatch
) -> None:
    migrations_dir = tmp_path / "missing-migrations"
    migration_name = "001_create_worker_instances.sql"

    monkeypatch.setattr(migrations_module, "_migrations_dir", lambda: migrations_dir)
    monkeypatch.setattr(migrations_module, "MIGRATION_FILENAMES", (migration_name,))

    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        try:
            apply_migrations(connection)
        except FileNotFoundError as exc:
            assert migration_name in str(exc)
        else:
            raise AssertionError("expected FileNotFoundError")


def test_apply_migrations_repairs_schema_history_without_tables(
    tmp_path: Path, monkeypatch
) -> None:
    """Simulates a DB written by an image that applied empty migration files."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    repo_migrations = (
        Path(migrations_module.__file__).resolve().parents[2] / "migrations"
    )
    for name in migrations_module.MIGRATION_FILENAMES:
        (migrations_dir / name).write_text(
            (repo_migrations / name).read_text(encoding="utf-8"), encoding="utf-8"
        )

    monkeypatch.setattr(migrations_module, "_migrations_dir", lambda: migrations_dir)

    engine = create_engine(tmp_path / "worker.db")
    with begin_connection(engine) as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
        )
        for name in migrations_module.MIGRATION_FILENAMES:
            connection.execute(
                text(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (:version, 'x')"
                ),
                {"version": name},
            )

    with begin_connection(engine) as connection:
        apply_migrations(connection)
        tables = {
            row[0]
            for row in connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
        }
        assert "worker_instances" in tables
