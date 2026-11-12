-- Raw reviews under raw/reviews/. summary is not a column on this table.
-- An empty prefix returns zero rows until a file is landed.
SELECT
  review_id,
  product_id,
  customer_id,
  rating,
  body,
  created_at
FROM raw_reviews
LIMIT 20
