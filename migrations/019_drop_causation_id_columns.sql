-- causation_id is unused — drop from worker state tables
ALTER TABLE worker_instances DROP COLUMN causation_id;
ALTER TABLE worker_launch_attempts DROP COLUMN causation_id;
ALTER TABLE worker_heartbeat_observations DROP COLUMN causation_id;
ALTER TABLE worker_events DROP COLUMN causation_id;
ALTER TABLE worker_diagnostics DROP COLUMN causation_id;
