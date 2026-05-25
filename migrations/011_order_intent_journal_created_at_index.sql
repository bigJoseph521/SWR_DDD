-- Semantics: order_intent_journal.created_at is the UTC instant of the latest bar/tick/quote
-- when the strategy submitted the intent (replay/sim clock or live feed), not worker wall time.
-- occurred_at remains worker-local submission time for the journal row.
CREATE INDEX IF NOT EXISTS ix_order_intent_journal_created_at ON order_intent_journal(created_at);
