-- Gold marts registered in the Glue Data Catalog by infra/modules/analytics.
-- LAKE_BUCKET stands for the lake bucket. Terraform fills in the real name.
-- Do not run this file by hand and do not point a crawler at it.
--
-- Storage for this registration is newline-delimited JSON: one JSON object per
-- line, read with the Hive JSON SerDe org.apache.hive.hcatalog.data.JsonSerDe.
-- A later Parquet rewrite can keep these column names and partition keys.
-- It is not what this first catalog registration stores.
--
-- total_amount is STRING because the file stores a decimal string ("24.00"),
-- not a JSON number. The logical mart type is still DECIMAL(12, 2). Cast it
-- in the query: CAST(total_amount AS DECIMAL(12, 2)). Athena engine version 3
-- does not reliably read that string as a decimal column, so the catalog
-- matches the bytes.
--
-- placed_at and created_at are TIMESTAMP. The SerDe property timestamp.formats
-- accepts 2026-01-15T14:03:00Z and the same shape with exactly three
-- fractional digits (milliseconds).
--
-- order_date is the UTC date of placed_at. created_date is the UTC date of
-- created_at. Both are partition columns: directory names such as
-- order_date=2026-01-15, not fields the SerDe has to read from the JSON line.
-- Partition projection covers 2020-01-01 through the current day, so there is
-- no MSCK REPAIR TABLE and no crawler. An empty directory returns no rows.

CREATE EXTERNAL TABLE gold_orders (
  order_id STRING,
  customer_id STRING,
  status STRING,
  currency STRING,
  total_amount STRING,
  placed_at TIMESTAMP,
  item_count INT
)
PARTITIONED BY (order_date STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://LAKE_BUCKET/gold/orders/';

CREATE EXTERNAL TABLE gold_reviews (
  review_id STRING,
  product_id STRING,
  customer_id STRING,
  rating INT,
  body STRING,
  created_at TIMESTAMP,
  summary STRING
)
PARTITIONED BY (created_date STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://LAKE_BUCKET/gold/reviews/';
