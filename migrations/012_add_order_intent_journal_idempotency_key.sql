-- Dedicated idempotency key for order intent journal rows (also present in payload_json).
ALTER TABLE order_intent_journal ADD COLUMN idempotency_key TEXT;

-- Prefer payload idempotency_key, else legacy client_intent_id from stored JSON.
UPDATE order_intent_journal
SET idempotency_key = CASE
    WHEN trim(COALESCE(json_extract(payload_json, '$.idempotency_key'), '')) != ''
        THEN trim(json_extract(payload_json, '$.idempotency_key'))
    WHEN trim(COALESCE(json_extract(payload_json, '$.client_intent_id'), '')) != ''
        THEN trim(json_extract(payload_json, '$.client_intent_id'))
    ELSE NULL
END
WHERE idempotency_key IS NULL;

CREATE INDEX IF NOT EXISTS ix_order_intent_journal_idempotency_key
    ON order_intent_journal(idempotency_key);
