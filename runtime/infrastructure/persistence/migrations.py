from __future__ import annotations

from pathlib import Path
from typing import Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection

MIGRATION_FILENAMES: Sequence[str] = (
    "001_create_worker_instances.sql",
    "002_create_worker_launch_attempts.sql",
    "003_create_worker_heartbeat_observations.sql",
    "004_create_worker_events.sql",
    "005_create_worker_diagnostics.sql",
    "006_add_runtime_indexes.sql",
    "007_create_runtime_state_journal.sql",
    "008_create_order_intent_journal.sql",
    "009_add_order_intent_journal_created_at.sql",
    "010_add_order_intent_journal_job_id.sql",
    "011_order_intent_journal_created_at_index.sql",
    "012_add_order_intent_journal_idempotency_key.sql",
    "013_add_order_intent_journal_order_intent_id.sql",
    "014_strip_replay_session_id_from_order_intent_journal.sql",
    "015_add_order_intent_journal_symbol.sql",
    "016_rename_order_intent_journal_occurred_at_to_requested_at.sql",
    "017_strip_strategy_context_from_order_intent_journal.sql",
    "018_order_intent_journal_order_intent_id_text.sql",
    "019_drop_causation_id_columns.sql",
)


def _migrations_dir() -> Path:
    # runtime/infrastructure/persistence/migrations.py -> repo root is parents[3]
    return Path(__file__).resolve().parents[3] / "migrations"


def apply_migrations(connection: Connection) -> None:
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

    # Repair: a DB can have schema_migrations rows but no real tables if migration
    # SQL files were missing at apply time (e.g. image without /app/pythonpath/migrations).
    wi = connection.execute(
        text(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_instances'"
        )
    ).first()
    if wi is None:
        n_applied = connection.execute(
            text("SELECT COUNT(*) FROM schema_migrations")
        ).scalar_one()
        if int(n_applied or 0) > 0:
            connection.execute(text("DELETE FROM schema_migrations"))

    migrations_dir = _migrations_dir()
    for name in MIGRATION_FILENAMES:
        already_applied = connection.execute(
            text("SELECT 1 FROM schema_migrations WHERE version = :version"),
            {"version": name},
        ).first()
        if already_applied:
            continue

        sql_path = migrations_dir / name
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        if not sql_path.is_file():
            raise FileNotFoundError(
                f"Migration SQL not found: {sql_path}. "
                "Ensure migrations/ is deployed next to the runtime package (see Dockerfile)."
            )
        sql_text = sql_path.read_text(encoding="utf-8")
        statements = [
            segment.strip() for segment in sql_text.split(";") if segment.strip()
        ]
        if not statements:
            raise ValueError(
                f"Migration {name!r} contains no SQL statements (empty file: {sql_path})"
            )
        for statement in statements:
            connection.execute(text(statement))

        connection.execute(
            text(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (:version, CURRENT_TIMESTAMP)"
            ),
            {"version": name},
        )

    _ensure_order_intent_journal_symbol_column(connection)
    _ensure_order_intent_journal_requested_at_column(connection)


def _ensure_order_intent_journal_requested_at_column(connection: Connection) -> None:
    """Rename ``occurred_at`` → ``requested_at`` when migration 016 was skipped (partial deploy)."""
    if (
        connection.execute(
            text(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='order_intent_journal'"
            )
        ).first()
        is None
    ):
        return
    cols = {
        row[1]
        for row in connection.execute(
            text("PRAGMA table_info(order_intent_journal)")
        ).fetchall()
    }
    if "requested_at" in cols:
        return
    if "occurred_at" not in cols:
        return
    connection.execute(
        text(
            "ALTER TABLE order_intent_journal RENAME COLUMN occurred_at TO requested_at"
        )
    )
    connection.execute(
        text("DROP INDEX IF EXISTS ix_order_intent_journal_runtime_launch_occurred")
    )
    connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_order_intent_journal_runtime_launch_requested "
            "ON order_intent_journal(runtime_id, launch_attempt, requested_at)"
        )
    )


def _ensure_order_intent_journal_symbol_column(connection: Connection) -> None:
    """Idempotent repair if ``order_intent_journal`` exists without ``symbol`` (e.g. partial deploy)."""
    if (
        connection.execute(
            text(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='order_intent_journal'"
            )
        ).first()
        is None
    ):
        return
    cols = {
        row[1]
        for row in connection.execute(
            text("PRAGMA table_info(order_intent_journal)")
        ).fetchall()
    }
    if "symbol" in cols:
        return
    connection.execute(text("ALTER TABLE order_intent_journal ADD COLUMN symbol TEXT"))
    connection.execute(
        text(
            """
            UPDATE order_intent_journal
            SET symbol = trim(CAST(json_extract(payload_json, '$.symbol') AS TEXT))
            WHERE symbol IS NULL
              AND json_valid(payload_json)
              AND json_type(payload_json) = 'object'
              AND json_extract(payload_json, '$.symbol') IS NOT NULL
              AND trim(CAST(json_extract(payload_json, '$.symbol') AS TEXT)) != ''
            """
        )
    )
    connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_order_intent_journal_symbol "
            "ON order_intent_journal(symbol)"
        )
    )
