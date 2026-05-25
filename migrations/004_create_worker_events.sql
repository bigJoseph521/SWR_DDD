CREATE TABLE IF NOT EXISTS worker_events (
    event_id TEXT PRIMARY KEY,
    runtime_id TEXT NOT NULL,
    worker_identity TEXT NOT NULL,
    launch_attempt INTEGER NOT NULL,
    event_family TEXT NOT NULL,
    state TEXT NULL,
    reason_code TEXT NULL,
    occurred_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    correlation_id TEXT NULL,
    causation_id TEXT NULL,
    payload TEXT NULL,
    CONSTRAINT fk_worker_events_runtime_id
        FOREIGN KEY(runtime_id) REFERENCES worker_instances(runtime_id)
        ON DELETE CASCADE
);
