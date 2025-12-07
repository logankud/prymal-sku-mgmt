CREATE TABLE IF NOT EXISTS prymal_agent.tmp_shipbob_daily_stage
WITH (
  format = 'PARQUET',
  parquet_compression = 'GZIP',
  external_location = 's3://prymal-ops/staging/prymal_agent/shipbob/daily_order_cnt_by_channel/run_date=${RUN_DATE}/'
) AS
SELECT
  CAST(channel_id   AS VARCHAR)            AS channel_id,
    CAST(channel_name AS VARCHAR)            AS channel_name,
    COUNT(DISTINCT(order_number))            AS order_cnt,
    CAST(order_date   AS DATE)               AS order_date
  FROM prymal.shipbob_order_details
  WHERE DATE(order_date) = DATE '${RUN_DATE}'
  GROUP BY
    CAST(channel_id   AS VARCHAR),
    CAST(channel_name AS VARCHAR),
    CAST(order_date   AS DATE);
