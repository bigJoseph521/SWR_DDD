CREATE TABLE IF NOT EXISTS worker_diagnostics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    runtime_id TEXT NOT NULL,
    launch_attempt INTEGER NOT NULL,
    diagnostic_type TEXT NOT NULL,
    stage TEXT NULL,
    reason_code TEXT NULL,
    occurred_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    correlation_id TEXT NULL,
    causation_id TEXT NULL,
    details TEXT NULL,
    CONSTRAINT fk_worker_diagnostics_runtime_id
        FOREIGN KEY(runtime_id) REFERENCES worker_instances(runtime_id)
        ON DELETE CASCADE
);
