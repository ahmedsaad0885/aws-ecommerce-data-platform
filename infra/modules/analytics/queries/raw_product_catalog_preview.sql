-- Catalog file drop under raw/product-catalog/. Not the DynamoDB product table.
-- An empty prefix returns zero rows until a file is landed.
-- price is text. The cast shows the two-place decimal the contract describes.
SELECT
  product_id,
  sku,
  name,
  category,
  status,
  CAST(price AS DECIMAL(12, 2)) AS price,
  updated_at
FROM raw_product_catalog
LIMIT 20
