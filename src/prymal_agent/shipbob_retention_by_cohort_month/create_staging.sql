CREATE TABLE prymal_agent.tmp_shipbob_retention_by_cohort_stage
WITH (
  format = 'PARQUET',
  parquet_compression = 'GZIP',
  external_location = 's3://${S3_BUCKET}/staging/shipbob/retention_by_cohort/report_date=${RUN_DATE}/run_id=${RUN_ID}/'
  
) AS

-- ====== your logic wrapped & normalized ======
WITH orders AS (
  -- Historic not in recurring
  SELECT h.*
  FROM prymal.shipbob_historic_order_data_fmtd h
  WHERE NOT EXISTS (
    SELECT 1
    FROM prymal.shipbob_order_details d
    WHERE d.order_number = h.order_number
  )
  UNION ALL
  -- Recurring table
  SELECT *
  FROM prymal.shipbob_order_details
),

first_orders AS (
  SELECT
    customer_email                                      AS email,
    MIN(CAST(CAST(purchase_date AS TIMESTAMP) AS DATE)) AS first_order_date
  FROM orders
  WHERE purchase_date IS NOT NULL
    AND customer_email <> ''
  GROUP BY customer_email
),

customer_orders AS (
  SELECT
    o.customer_email                                     AS email,
    CAST(CAST(o.purchase_date AS TIMESTAMP) AS DATE)     AS order_date,
    f.first_order_date                                   AS first_order_date,
    DATE_DIFF('day', f.first_order_date,
              CAST(CAST(o.purchase_date AS TIMESTAMP) AS DATE)) AS days_since_first
  FROM orders o
  LEFT JOIN first_orders f ON o.customer_email = f.email
  WHERE o.purchase_date IS NOT NULL
    AND o.customer_email <> ''
),

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

SELECT
  CAST(DATE_TRUNC('month', first_order_date) AS DATE)        AS cohort_month,
  CAST(COUNT(*) AS BIGINT)                                   AS total_customers,
  CAST(SUM(retained_30d) AS BIGINT)                          AS retained_30d_count,
  CAST(ROUND(100.0 * SUM(retained_30d) / NULLIF(COUNT(*),0), 2) AS DOUBLE) AS retained_30d_pct,
  CAST(SUM(retained_60d) AS BIGINT)                          AS retained_60d_count,
  CAST(ROUND(100.0 * SUM(retained_60d) / NULLIF(COUNT(*),0), 2) AS DOUBLE) AS retained_60d_pct,
  CAST(SUM(retained_90d) AS BIGINT)                          AS retained_90d_count,
  CAST(ROUND(100.0 * SUM(retained_90d) / NULLIF(COUNT(*),0), 2) AS DOUBLE) AS retained_90d_pct
FROM retention_flags
GROUP BY 1
ORDER BY 1;
