CREATE EXTERNAL TABLE katana_formulas (
  product_variant_code double,
  product_variant_name string,
  product_supplier_item_code string,
  product_internal_barcode string,
  product_registered_barcode string,
  ingredient_variant_code_sku_required string,
  ingredient_variant_name string,
  ingredient_supplier_item_code string,
  ingredient_internal_barcode string,
  ingredient_registered_barcode string,
  notes string,
  quantity_required double,
  unit_of_measure string,
  current_stock_price double
)
PARTITIONED BY (
  partition_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/katana/formulas/'
TBLPROPERTIES ('skip.header.line.count'='1');
