-- Remove deprecated ``strategy_context`` from persisted order-intent payloads (OMS no longer carries it).
UPDATE order_intent_journal
SET payload_json = json_remove(payload_json, '$.strategy_context')
WHERE json_valid(payload_json)
  AND json_type(payload_json) = 'object'
  AND json_extract(payload_json, '$.strategy_context') IS NOT NULL;
