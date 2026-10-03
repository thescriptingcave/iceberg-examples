-- Spark Initialization Script
-- Creates the `tutorial` namespace used by every lab, plus two small sample
-- tables, in the `lakehouse` catalog (Polaris).
--
-- See "Initialize the tutorial namespace" in lab0-setup/README.md for how to
-- run it. The runner there splits this file on semicolons, so never put a
-- semicolon inside a comment.

-- Create the namespace the labs use
CREATE NAMESPACE IF NOT EXISTS lakehouse.tutorial;

-- Set default namespace
USE lakehouse.tutorial;

-- Create example tables for testing
CREATE TABLE IF NOT EXISTS lakehouse.tutorial.customers (
  customer_id INT,
  name STRING,
  email STRING,
  created_at TIMESTAMP
) USING ICEBERG
PARTITIONED BY (customer_id);

CREATE TABLE IF NOT EXISTS lakehouse.tutorial.orders (
  order_id INT,
  customer_id INT,
  product STRING,
  amount DECIMAL(10,2),
  order_date DATE
) USING ICEBERG
PARTITIONED BY (order_date);

-- Load sample data. INSERT OVERWRITE replaces the table contents, so running
-- this script twice does not duplicate rows.
INSERT OVERWRITE lakehouse.tutorial.customers VALUES
  (1, 'Alice Smith', 'alice@example.com', TIMESTAMP '2024-01-01 10:00:00'),
  (2, 'Bob Johnson', 'bob@example.com', TIMESTAMP '2024-01-02 11:00:00'),
  (3, 'Charlie Brown', 'charlie@example.com', TIMESTAMP '2024-01-03 12:00:00');

INSERT OVERWRITE lakehouse.tutorial.orders VALUES
  (101, 1, 'Widget A', 25.50, DATE '2024-01-15'),
  (102, 1, 'Widget B', 15.00, DATE '2024-01-16'),
  (103, 2, 'Widget A', 25.50, DATE '2024-01-17'),
  (104, 3, 'Widget C', 35.75, DATE '2024-01-18');

-- Verify tables
SHOW TABLES IN lakehouse.tutorial;

SELECT * FROM lakehouse.tutorial.customers ORDER BY customer_id;
SELECT * FROM lakehouse.tutorial.orders ORDER BY order_id;
