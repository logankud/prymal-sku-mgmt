CREATE EXTERNAL TABLE IF NOT EXISTS shipbob_order_details_json (
  
  created_date TIMESTAMP,
  purchase_date TIMESTAMP,
  shipbob_order_id BIGINT,
  order_number STRING,
  order_status STRING,
  order_type STRING,
  channel_id BIGINT,
  channel_name STRING,
  product_id BIGINT,
  sku STRING,
  shipping_method STRING,
  customer_name STRING,
  customer_email STRING,
  customer_address_city STRING,
  customer_address_state STRING,
  customer_address_country STRING,
  sku_name STRING,
  inventory_id BIGINT,
  inventory_name STRING,
  inventory_qty BIGINT
)
PARTITIONED BY (order_date DATE)
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY ','
-- STORED AS TEXTFILE
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

LOCATION 's3://S3_BUCKET_NAME/shipbob/order_details_json/'
-- TBLPROPERTIES ('skip.header.line.count'='1');