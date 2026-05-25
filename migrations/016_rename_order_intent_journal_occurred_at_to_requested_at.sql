-- Wall-clock instant when the worker submitted the intent (OMS / backtest egress).
ALTER TABLE order_intent_journal RENAME COLUMN occurred_at TO requested_at;

DROP INDEX IF EXISTS ix_order_intent_journal_runtime_launch_occurred;

CREATE INDEX IF NOT EXISTS ix_order_intent_journal_runtime_launch_requested
    ON order_intent_journal(runtime_id, launch_attempt, requested_at);
