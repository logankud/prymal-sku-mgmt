CREATE EXTERNAL TABLE IF NOT EXISTS katana_raw_material_run_rate (
    katana_ingredient_sku STRING,
    katana_ingredient_name STRING,
    katana_unit_of_measure STRING,
    daily_run_rate DOUBLE,
    inventory_on_hand DOUBLE,
    inventory_as_of DATE,
    days_on_hand DOUBLE,
    reorder_point DOUBLE
)
PARTITIONED BY (
    partition_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/katana/raw_material_run_rate/'
TBLPROPERTIES ('skip.header.line.count'='1');