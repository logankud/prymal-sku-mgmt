CREATE EXTERNAL TABLE IF NOT EXISTS prymal_agent.shipbob_daily_order_cnt_by_channel (
    channel_id   STRING,
    channel_name STRING,
    order_cnt    INT
)
PARTITIONED BY (
    order_date   DATE
)
STORED AS PARQUET
LOCATION 's3://prymal-ops/prymal_agent/shipbob/daily_order_cnt_by_channel/'
TBLPROPERTIES (
    'comment'='Total orders placed per day across all sales channels, extracted from ShipBob (the 3PL and order management solution used by Prymal). Partitioned by order_date (the date the order was placed).'
);
