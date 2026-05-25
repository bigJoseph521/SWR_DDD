ALTER TABLE order_intent_journal ADD COLUMN order_intent_id INTEGER;

UPDATE order_intent_journal
SET order_intent_id = CAST(json_extract(payload_json, '$.order_intent_id') AS INTEGER)
WHERE order_intent_id IS NULL
  AND json_extract(payload_json, '$.order_intent_id') IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_order_intent_journal_order_intent_id
    ON order_intent_journal(order_intent_id);
