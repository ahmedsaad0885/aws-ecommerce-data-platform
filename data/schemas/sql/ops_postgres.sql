-- Serving schema for the operational PostgreSQL database.
-- Contract stub. CI does not apply it.
-- Money is NUMERIC. Interchange files carry decimal strings; ELT converts them.
-- reviews.summary stays null until the Bedrock milestone writes it.

CREATE TABLE customers (
  customer_id TEXT PRIMARY KEY,
  email TEXT NOT NULL,
  full_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE products (
  product_id TEXT PRIMARY KEY,
  sku TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'discontinued')),
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL REFERENCES customers (customer_id),
  status TEXT NOT NULL CHECK (status IN ('placed', 'paid', 'shipped', 'cancelled')),
  currency CHAR(3) NOT NULL,
  total_amount NUMERIC(12, 2) NOT NULL,
  placed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX orders_customer_placed_idx ON orders (customer_id, placed_at DESC);

CREATE TABLE order_items (
  order_id TEXT NOT NULL REFERENCES orders (order_id),
  line_number INTEGER NOT NULL,
  product_id TEXT NOT NULL REFERENCES products (product_id),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price NUMERIC(12, 2) NOT NULL,
  PRIMARY KEY (order_id, line_number)
);

CREATE TABLE reviews (
  review_id TEXT PRIMARY KEY,
  product_id TEXT NOT NULL REFERENCES products (product_id),
  customer_id TEXT NOT NULL REFERENCES customers (customer_id),
  rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
  body TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  summary TEXT
);

CREATE INDEX reviews_product_idx ON reviews (product_id, created_at DESC);
