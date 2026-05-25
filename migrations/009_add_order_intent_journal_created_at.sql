-- UTC instant of the latest bar/tick/quote when the intent was formed (payload + column).
-- occurred_at remains worker-local wall time for the journal row / correlation.
ALTER TABLE order_intent_journal ADD COLUMN created_at TEXT;
