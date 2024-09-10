CREATE EXTERNAL TABLE katana_raw_material_status (
  name string,
  units_of_measure string,
  in_stock double,
  in_stock_as_of date,
  planned_qty double,
  planned_qty_as_of date,
  inventory_remaining double,
  in_stock_percentage double,
  needs_replenished boolean
)
PARTITIONED BY (
  partition_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/katana/raw_material_status/'
TBLPROPERTIES ('skip.header.line.count'='1');
