ALTER TABLE order_intent_journal ADD COLUMN symbol TEXT;

UPDATE order_intent_journal
SET symbol = trim(CAST(json_extract(payload_json, '$.symbol') AS TEXT))
WHERE symbol IS NULL
  AND json_valid(payload_json)
  AND json_type(payload_json) = 'object'
  AND json_extract(payload_json, '$.symbol') IS NOT NULL
  AND trim(CAST(json_extract(payload_json, '$.symbol') AS TEXT)) != '';

CREATE INDEX IF NOT EXISTS ix_order_intent_journal_symbol
    ON order_intent_journal(symbol);
