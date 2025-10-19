CREATE TABLE IF NOT EXISTS "prymal-agent".shipbob_daily_order_cnt_by_channel
(
    channel_id         VARCHAR COMMENT 'ID of the sales channel (in ShipBob)',
    channel_name      VARCHAR COMMENT 'Name of the sales channel (in ShipBob)',
    order_cnt  INT COMMENT 'Total count of orders placed'
)
COMMENT = 'Total orders placed per day across all sales channels, extracted from ShipBob (the 3PL and order management solution used by Prymal).  Paritioned by order_date (the date the order was placed)'
WITH (
  format='PARQUET',
  external_location='s3://prymal-ops/prymal_agent/shipbob/daily_order_cnt_by_channel',
  partitioned_by = ARRAY['order_date']
);
