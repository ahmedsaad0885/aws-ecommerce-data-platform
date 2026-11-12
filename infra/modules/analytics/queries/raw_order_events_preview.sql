-- Raw order events. One JSON object per line, directly under raw/order-events/.
-- An empty prefix returns zero rows. That is the expected result until data is landed.
-- total_amount is text such as 24.00, matching the JSON Schema. It is not a float.
SELECT
  event_id,
  event_type,
  occurred_at,
  order_id,
  customer_id,
  currency,
  total_amount,
  items
FROM raw_order_events
LIMIT 20
