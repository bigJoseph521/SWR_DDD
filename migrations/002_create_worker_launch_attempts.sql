CREATE TABLE IF NOT EXISTS worker_launch_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    runtime_id TEXT NOT NULL,
    launch_attempt INTEGER NOT NULL,
    state TEXT NOT NULL,
    reason_code TEXT NULL,
    occurred_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    correlation_id TEXT NULL,
    causation_id TEXT NULL,
    details TEXT NULL,
    CONSTRAINT fk_worker_launch_attempts_runtime_id
        FOREIGN KEY(runtime_id) REFERENCES worker_instances(runtime_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_worker_launch_attempt_runtime_attempt
        UNIQUE(runtime_id, launch_attempt)
);
