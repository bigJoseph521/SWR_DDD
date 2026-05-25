CREATE TABLE IF NOT EXISTS order_intent_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    runtime_id TEXT NOT NULL,
    launch_attempt INTEGER NOT NULL,
    source TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    result_json TEXT,
    occurred_at TEXT NOT NULL,
    correlation_id TEXT
);

CREATE INDEX IF NOT EXISTS ix_order_intent_journal_runtime_launch_occurred
    ON order_intent_journal(runtime_id, launch_attempt, occurred_at);
