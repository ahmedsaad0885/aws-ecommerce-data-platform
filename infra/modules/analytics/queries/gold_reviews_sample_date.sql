-- Review mart for the sample review's UTC date.
-- Files belong at gold/reviews/created_date=2026-01-16/.
-- Nothing writes this prefix yet, so the result is zero rows.
-- summary is null until a later write. This query does not call a model.
SELECT
  review_id,
  product_id,
  customer_id,
  rating,
  body,
  created_at,
  summary,
  created_date
FROM gold_reviews
WHERE created_date = '2026-01-16'
LIMIT 20
