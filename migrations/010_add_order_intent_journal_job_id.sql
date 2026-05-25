-- Denormalized job identifier for queries and stable schema (also inside payload_json).
ALTER TABLE order_intent_journal ADD COLUMN job_id TEXT;
