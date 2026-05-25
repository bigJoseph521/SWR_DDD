CREATE INDEX IF NOT EXISTS ix_worker_instances_strategy_version_id
    ON worker_instances(strategy_version_id);
CREATE INDEX IF NOT EXISTS ix_worker_instances_tenant_id
    ON worker_instances(tenant_id);
CREATE INDEX IF NOT EXISTS ix_worker_instances_account_id
    ON worker_instances(account_id);
CREATE INDEX IF NOT EXISTS ix_worker_instances_trader_id
    ON worker_instances(trader_id);
CREATE INDEX IF NOT EXISTS ix_worker_instances_state
    ON worker_instances(state);
CREATE INDEX IF NOT EXISTS ix_worker_instances_reason_code
    ON worker_instances(reason_code);
CREATE INDEX IF NOT EXISTS ix_worker_instances_last_heartbeat_at
    ON worker_instances(last_heartbeat_at);
CREATE INDEX IF NOT EXISTS ix_worker_events_runtime_launch_occurred
    ON worker_events(runtime_id, launch_attempt, occurred_at);
CREATE INDEX IF NOT EXISTS ix_worker_diagnostics_runtime_launch_type_observed
    ON worker_diagnostics(runtime_id, launch_attempt, diagnostic_type, observed_at);
