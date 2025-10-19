CREATE TABLE "prymal-agent".tmp_shipbob_daily_stage
WITH (
  format = 'PARQUET',
  parquet_compression = 'GZIP',
  external_location = 's3://prymal-ops/prymal_agent/staging/shipbob/daily_order_cnt_by_channel/run_date=${RUN_DATE}/'
) AS
SELECT
  channel_id
  , channel_name
  , CAST(COUNT(*) AS INTEGER) AS order_cnt
  , DATE '${RUN_DATE}'        AS order_date
FROM source_db.shipbob_orders 
WHERE DATE(o.order_created_at) = DATE '${RUN_DATE}'
GROUP BY channel_id
  , channel_name
  , DATE '${RUN_DATE}'
