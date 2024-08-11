CREATE EXTERNAL TABLE shopify_active_variant_sku_details (
    product_id STRING,
    product_title STRING,
    variant_id STRING,
    variant_title STRING,
    variant_sku STRING,
    inventory_quantity FLOAT,
    published_at DATE
)
PARTITIONED BY (partition_date DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/shopify/active_variant_sku_details/'
TBLPROPERTIES ('skip.header.line.count'='1');