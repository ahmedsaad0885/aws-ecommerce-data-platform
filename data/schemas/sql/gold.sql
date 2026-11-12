-- Gold marts for Athena.
-- Contract stub. CI does not register these tables.
-- Replace the bucket location before any future registration.
-- order_date and created_date are the UTC date of the source timestamp,
-- which can differ from the Firehose arrival prefix under raw/.
-- For gold_orders the source timestamp is placed_at: the OrderPlaced time.
-- A later paid, shipped, or cancelled event does not move placed_at.

CREATE EXTERNAL TABLE gold_orders (
  order_id STRING,
  customer_id STRING,
  status STRING,
  currency STRING,
  total_amount DECIMAL(12, 2),
  placed_at TIMESTAMP, -- earliest OrderPlaced occurred_at, in UTC
  item_count INT -- number of lines on the winning event, not the sum of quantities or the event count
)
PARTITIONED BY (order_date STRING)
STORED AS PARQUET
LOCATION 's3://REPLACE_ME_LAKE_BUCKET/gold/orders/';

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
STORED AS PARQUET
LOCATION 's3://REPLACE_ME_LAKE_BUCKET/gold/reviews/';
