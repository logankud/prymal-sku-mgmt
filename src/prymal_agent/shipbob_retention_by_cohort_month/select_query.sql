-- Full recompute of 30/60/90 day retention by cohort month over all orders
-- since 2023-01-01. Customer emails are used only inside this query to find
-- first-order dates; nothing at customer grain leaves it.

WITH orders AS (

  -- Orders loaded from the historic ShipBob export that the daily extract
  -- has not also captured
  SELECT h.customer_email, h.purchase_date
  FROM ${DATABASE}.shipbob_historic_order_data_fmtd h
  WHERE NOT EXISTS (
    SELECT 1
    FROM ${DATABASE}.shipbob_order_details d
    WHERE d.order_number = h.order_number
      AND d.order_date >= DATE '2023-01-01'
  )

  UNION ALL

  SELECT customer_email, purchase_date
  FROM ${DATABASE}.shipbob_order_details
  WHERE order_date >= DATE '2023-01-01'

),

-- Step 1: first order date per customer
first_orders AS (
  SELECT
    customer_email AS email,
    MIN(CAST(CAST(purchase_date AS TIMESTAMP) AS DATE)) AS first_order_date
  FROM orders
  WHERE purchase_date IS NOT NULL
    AND purchase_date >= DATE '2023-01-01'
    AND customer_email != ''
  GROUP BY customer_email
),

-- Step 2: days between each order and the customer's first order
customer_orders AS (
  SELECT
    o.customer_email AS email,
    f.first_order_date,
    DATE_DIFF('day', f.first_order_date,
              CAST(CAST(o.purchase_date AS TIMESTAMP) AS DATE)) AS days_since_first
  FROM orders o
  LEFT JOIN first_orders f ON o.customer_email = f.email
  WHERE o.purchase_date IS NOT NULL
    AND o.customer_email != ''
),

-- Step 3: whether the customer came back within each window
retention_flags AS (
  SELECT
    email,
    first_order_date,
    MAX(CASE WHEN days_since_first BETWEEN 1 AND 30 THEN 1 ELSE 0 END) AS retained_30d,
    MAX(CASE WHEN days_since_first BETWEEN 1 AND 60 THEN 1 ELSE 0 END) AS retained_60d,
    MAX(CASE WHEN days_since_first BETWEEN 1 AND 90 THEN 1 ELSE 0 END) AS retained_90d
  FROM customer_orders
  GROUP BY email, first_order_date
)

-- Step 4: aggregate to cohort month. Types are cast explicitly to match the
-- table declared in config.yml.
SELECT
  DATE_TRUNC('month', first_order_date)                             AS cohort_month,
  CAST(COUNT(*) AS bigint)                                          AS total_customers,
  CAST(SUM(retained_30d) AS bigint)                                 AS retained_30d_count,
  CAST(ROUND(100.0 * SUM(retained_30d) / COUNT(*), 2) AS double)   AS retained_30d_pct,
  CAST(SUM(retained_60d) AS bigint)                                 AS retained_60d_count,
  CAST(ROUND(100.0 * SUM(retained_60d) / COUNT(*), 2) AS double)   AS retained_60d_pct,
  CAST(SUM(retained_90d) AS bigint)                                 AS retained_90d_count,
  CAST(ROUND(100.0 * SUM(retained_90d) / COUNT(*), 2) AS double)   AS retained_90d_pct
FROM retention_flags
GROUP BY 1
