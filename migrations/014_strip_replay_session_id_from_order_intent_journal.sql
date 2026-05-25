-- Remove replay_session_id from persisted JSON (not part of journal contract)
UPDATE order_intent_journal
SET payload_json = json_remove(payload_json, '$.replay_session_id')
WHERE json_valid(payload_json)
  AND json_type(payload_json) = 'object'
  AND json_extract(payload_json, '$.replay_session_id') IS NOT NULL;
