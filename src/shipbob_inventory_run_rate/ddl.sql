CREATE EXTERNAL TABLE IF NOT EXISTS shipbob_inventory_run_rate (
    inventory_id int,
    run_rate double,
    name string,
    total_fulfillable_quantity int,
    est_stock_days_on_hand double,
    estimated_stockout_date date,
    restock_point int
)
PARTITIONED BY (
    partition_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/shipbob/inventory_run_rate/'
TBLPROPERTIES ('skip.header.line.count'='1');