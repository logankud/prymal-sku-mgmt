CREATE EXTERNAL TABLE IF NOT EXISTS katana_inventory (
    name STRING,
    variant_code_sku STRING,
    category STRING,
    default_supplier STRING,
    units_of_measure STRING,
    average_cost DOUBLE,
    value_in_stock DOUBLE,
    in_stock DOUBLE,
    expected DOUBLE,
    committed DOUBLE,
    safety_stock DOUBLE,
    calculated_stock DOUBLE,
    location STRING
)
PARTITIONED BY (
  partition_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/katana/inventory/'
TBLPROPERTIES ('skip.header.line.count'='1');
