-- One row per order for the sample placement date.
-- Files belong at gold/orders/order_date=2026-01-15/ and are newline-delimited JSON.
-- order_date is the directory name. The date filter keeps Athena on that one day.
-- Zero rows means the prefix is empty, which is expected before any gold write.
-- item_count is the number of lines, not the sum of quantities.
-- total_amount is stored as text so the cents stay exact. The cast is DECIMAL(12, 2).
SELECT
  order_id,
  customer_id,
  status,
  currency,
  CAST(total_amount AS DECIMAL(12, 2)) AS total_amount,
  placed_at,
  item_count,
  order_date
FROM gold_orders
WHERE order_date = '2026-01-15'
LIMIT 20
