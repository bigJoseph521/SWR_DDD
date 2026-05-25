from __future__ import annotations

from sqlalchemy import (
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
)

metadata = MetaData()

worker_instances = Table(
    "worker_instances",
    metadata,
    Column("runtime_id", String(128), nullable=False),
    Column("worker_identity", Text, nullable=False),
    Column("tenant_id", String(128), nullable=False),
    Column("trader_id", String(128), nullable=True),
    Column("account_id", String(128), nullable=True),
    Column("strategy_version_id", String(128), nullable=False),
    Column("launch_attempt", Integer, nullable=False),
    Column("state", String(64), nullable=False),
    Column("reason_code", String(128), nullable=True),
    Column("occurred_at", String(32), nullable=False),
    Column("observed_at", String(32), nullable=False),
    Column("last_heartbeat_at", String(32), nullable=True),
    Column("correlation_id", String(128), nullable=True),
    PrimaryKeyConstraint("runtime_id", name="pk_worker_instances_runtime_id"),
)

worker_launch_attempts = Table(
    "worker_launch_attempts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "runtime_id",
        String(128),
        ForeignKey("worker_instances.runtime_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("launch_attempt", Integer, nullable=False),
    Column("state", String(64), nullable=False),
    Column("reason_code", String(128), nullable=True),
    Column("occurred_at", String(32), nullable=False),
    Column("observed_at", String(32), nullable=False),
    Column("correlation_id", String(128), nullable=True),
    Column("details", Text, nullable=True),
    UniqueConstraint(
        "runtime_id", "launch_attempt", name="uq_worker_launch_attempt_runtime_attempt"
    ),
)

worker_heartbeat_observations = Table(
    "worker_heartbeat_observations",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "runtime_id",
        String(128),
        ForeignKey("worker_instances.runtime_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("launch_attempt", Integer, nullable=False),
    Column("occurred_at", String(32), nullable=False),
    Column("observed_at", String(32), nullable=False),
    Column("correlation_id", String(128), nullable=True),
    Column("details", Text, nullable=True),
)

worker_events = Table(
    "worker_events",
    metadata,
    Column("event_id", String(128), nullable=False),
    Column(
        "runtime_id",
        String(128),
        ForeignKey("worker_instances.runtime_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("worker_identity", Text, nullable=False),
    Column("launch_attempt", Integer, nullable=False),
    Column("event_family", String(128), nullable=False),
    Column("state", String(64), nullable=True),
    Column("reason_code", String(128), nullable=True),
    Column("occurred_at", String(32), nullable=False),
    Column("observed_at", String(32), nullable=False),
    Column("correlation_id", String(128), nullable=True),
    Column("payload", Text, nullable=True),
    PrimaryKeyConstraint("event_id", name="pk_worker_events_event_id"),
)

worker_diagnostics = Table(
    "worker_diagnostics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "runtime_id",
        String(128),
        ForeignKey("worker_instances.runtime_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("launch_attempt", Integer, nullable=False),
    Column("diagnostic_type", String(128), nullable=False),
    Column("stage", String(64), nullable=True),
    Column("reason_code", String(128), nullable=True),
    Column("occurred_at", String(32), nullable=False),
    Column("observed_at", String(32), nullable=False),
    Column("correlation_id", String(128), nullable=True),
    Column("details", Text, nullable=True),
)

Index("ix_worker_instances_strategy_version_id", worker_instances.c.strategy_version_id)
Index("ix_worker_instances_tenant_id", worker_instances.c.tenant_id)
Index("ix_worker_instances_account_id", worker_instances.c.account_id)
Index("ix_worker_instances_trader_id", worker_instances.c.trader_id)
Index("ix_worker_instances_state", worker_instances.c.state)
Index("ix_worker_instances_reason_code", worker_instances.c.reason_code)
Index("ix_worker_instances_last_heartbeat_at", worker_instances.c.last_heartbeat_at)
Index(
    "ix_worker_events_runtime_launch_occurred",
    worker_events.c.runtime_id,
    worker_events.c.launch_attempt,
    worker_events.c.occurred_at,
)
Index(
    "ix_worker_diagnostics_runtime_launch_type_observed",
    worker_diagnostics.c.runtime_id,
    worker_diagnostics.c.launch_attempt,
    worker_diagnostics.c.diagnostic_type,
    worker_diagnostics.c.observed_at,
)

runtime_state_journal = Table(
    "runtime_state_journal",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("runtime_id", String(128), nullable=False),
    Column("launch_attempt", Integer, nullable=False),
    Column("kind", String(64), nullable=False),
    Column("phase", String(64), nullable=True),
    Column("previous_phase", String(64), nullable=True),
    Column("step_name", String(256), nullable=True),
    Column("level", String(32), nullable=True),
    Column("reason_code", String(256), nullable=True),
    Column("observed_at", String(32), nullable=False),
    Column("details_json", Text, nullable=True),
)

# DDL source of truth: migrations/008_create_order_intent_journal.sql,
# migrations/009_add_order_intent_journal_created_at.sql,
# migrations/010_add_order_intent_journal_job_id.sql,
# migrations/011_order_intent_journal_created_at_index.sql,
# migrations/012_add_order_intent_journal_idempotency_key.sql,
# migrations/013_add_order_intent_journal_order_intent_id.sql,
# migrations/014_strip_replay_session_id_from_order_intent_journal.sql,
# migrations/015_add_order_intent_journal_symbol.sql,
# migrations/016_rename_order_intent_journal_occurred_at_to_requested_at.sql,
# migrations/017_strip_strategy_context_from_order_intent_journal.sql,
# migrations/018_order_intent_journal_order_intent_id_text.sql
# Rows are inserted by SQLiteOrderIntentJournalRepository.append (runtime_journal_sink).
# Columns: id, runtime_id, launch_attempt, source, payload_json, result_json, requested_at,
# correlation_id, created_at, job_id, idempotency_key, order_intent_id, symbol (015).
# ``job_id`` is stored as its own column (and typically mirrored in ``payload_json``).
# ``created_at`` (column and payload field): UTC wire time of the latest bar/tick/quote
# at intent creation (simulated replay clock or live feed event time), not worker wall clock.
order_intent_journal = Table(
    "order_intent_journal",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("runtime_id", String(128), nullable=False),
    Column("launch_attempt", Integer, nullable=False),
    Column("source", String(32), nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("result_json", Text, nullable=True),
    Column("requested_at", String(32), nullable=False),
    Column("created_at", String(32), nullable=True),
    Column("correlation_id", String(128), nullable=True),
    Column("job_id", String(256), nullable=True),
    Column("idempotency_key", String(256), nullable=True),
    Column("order_intent_id", String(36), nullable=True),
    Column("symbol", String(64), nullable=True),
)

Index(
    "ix_runtime_state_journal_runtime_launch_observed",
    runtime_state_journal.c.runtime_id,
    runtime_state_journal.c.launch_attempt,
    runtime_state_journal.c.observed_at,
)
Index(
    "ix_order_intent_journal_runtime_launch_requested",
    order_intent_journal.c.runtime_id,
    order_intent_journal.c.launch_attempt,
    order_intent_journal.c.requested_at,
)
Index(
    "ix_order_intent_journal_created_at",
    order_intent_journal.c.created_at,
)
Index(
    "ix_order_intent_journal_idempotency_key",
    order_intent_journal.c.idempotency_key,
)
Index(
    "ix_order_intent_journal_order_intent_id",
    order_intent_journal.c.order_intent_id,
)
Index(
    "ix_order_intent_journal_symbol",
    order_intent_journal.c.symbol,
)
