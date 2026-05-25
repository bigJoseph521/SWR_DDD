CREATE TABLE IF NOT EXISTS runtime_state_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    runtime_id TEXT NOT NULL,
    launch_attempt INTEGER NOT NULL,
    kind TEXT NOT NULL,
    phase TEXT,
    previous_phase TEXT,
    step_name TEXT,
    level TEXT,
    reason_code TEXT,
    observed_at TEXT NOT NULL,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS ix_runtime_state_journal_runtime_launch_observed
    ON runtime_state_journal(runtime_id, launch_attempt, observed_at);
