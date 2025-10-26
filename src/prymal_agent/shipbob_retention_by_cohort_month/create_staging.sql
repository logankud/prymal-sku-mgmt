CREATE TABLE prymal_agent.tmp_shipbob_retention_by_cohort_stage
WITH (
  format = 'PARQUET',
  parquet_compression = 'GZIP',
  external_location = '${STAGING_LOCATION'
  
) AS

-- =========== SELECT Query  ===========
  
  -- Step 0: Merge historic + recurring order tables
  WITH orders AS (

  (

  SELECT h.*
  FROM prymal.shipbob_historic_order_data_fmtd h
  WHERE NOT EXISTS (    -- filter out orders already captured in the order details table (populated via recurring ETL job)
    SELECT 1
    FROM prymal.shipbob_order_details d
    WHERE d.order_number = h.order_number
    AND order_date >= DATE('2023-01-01')
  )

  )
  UNION ALL 
  (

  SELECT * 
  FROM shipbob_order_details
  WHERE order_date >= DATE('2023-01-01')

  )

  )

  , 

  -- Step 1: Get the first order date for each customer
  first_orders AS (
    SELECT 
      customer_email as email,
      MIN(CAST(CAST(purchase_date AS TIMESTAMP) AS DATE)) AS first_order_date
    FROM orders
      WHERE purchase_date IS NOT NULL
      AND purchase_date >= DATE('2023-01-01')
      AND customer_email != ''
    GROUP BY customer_email
  )


  ,


  -- Step 2: Join back to all orders and calculate days since first purchase
  customer_orders AS (
    SELECT 
      o.customer_email as email,
      CAST(CAST(purchase_date AS TIMESTAMP) AS DATE) AS order_date,
      f.first_order_date,
      DATE_DIFF('day', f.first_order_date, CAST(CAST(purchase_date AS TIMESTAMP) AS DATE)) AS days_since_first
    FROM orders o
    LEFT JOIN first_orders f ON o.customer_email = f.email
    WHERE o.purchase_date IS NOT NULL
      AND o.customer_email != ''
  )

  ,



  -- Step 3: Flag whether customer was retained in 30, 60, or 90 day window
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



  -- Step 4: Aggregate retention by cohort month
  SELECT 
    DATE_TRUNC('month', first_order_date) AS cohort_month,
    COUNT(*) AS total_customers,
    SUM(retained_30d) AS retained_30d_count,
    ROUND(100.0 * SUM(retained_30d) / COUNT(*), 2) AS retained_30d_pct,
    SUM(retained_60d) AS retained_60d_count,
    ROUND(100.0 * SUM(retained_60d) / COUNT(*), 2) AS retained_60d_pct,
    SUM(retained_90d) AS retained_90d_count,
    ROUND(100.0 * SUM(retained_90d) / COUNT(*), 2) AS retained_90d_pct
  FROM retention_flags
  GROUP BY 1
  ORDER BY 1;
