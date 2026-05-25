-- Normalize legacy INTEGER order_intent_id values to TEXT for UUID strings.
UPDATE order_intent_journal
SET order_intent_id = CAST(order_intent_id AS TEXT)
WHERE order_intent_id IS NOT NULL;
