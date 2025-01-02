CREATE EXTERNAL TABLE katana_open_manufacturing_orders (
  mo string,
  created_date date,
  done_date date,
  production_status string,
  product_variant_code_sku double,
  product_variant string,
  planned_quantity_of_product double,
  actual_quantity_of_product double,
  unit_of_measure string,
  ingredient_variant_code_sku string,
  ingredient_variant string,
  ingredient_notes string,
  planned_quantity_of_ingredient double,
  actual_quantity_of_ingredient double,
  ingredient_unit_of_measure string,
  ingredient_cost double,
  ingredient_status string
)
PARTITIONED BY (
  partition_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/katana/open_manufacturing_orders/'
TBLPROPERTIES ('skip.header.line.count'='1');
