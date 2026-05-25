CREATE TABLE IF NOT EXISTS worker_instances (
    runtime_id TEXT PRIMARY KEY,
    worker_identity TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    trader_id TEXT NULL,
    account_id TEXT NULL,
    strategy_version_id TEXT NOT NULL,
    launch_attempt INTEGER NOT NULL,
    state TEXT NOT NULL,
    reason_code TEXT NULL,
    occurred_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    last_heartbeat_at TEXT NULL,
    correlation_id TEXT NULL,
    causation_id TEXT NULL
);
