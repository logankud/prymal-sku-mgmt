CREATE TABLE shipbob_inventory_metadata
(
  id INT COMMENT 'ShipBob inventory ID - a unique identifier for the product',
  product_name STRING COMMENT 'Name of the product',
  size_variant STRING COMMENT 'Size variant of the product (e.g., "Large Bag", "Bulk Bag", "Single Serve")',
  sku_variant STRING COMMENT 'Flavor of the product',
  product_type_variant STRING COMMENT 'The product type (e.g., "Creamer","Coffee", "Merch")',
  limited_edition_flag BOOLEAN COMMENT 'Whether or not the product is limited edition (limited and not likely to return)',
  seasonal_flag BOOLEAN COMMENT 'Whether or not the product is seasonal (limited but recurring annually)'
)
LOCATION 's3://S3_BUCKET_NAME/shipbob/inventory_metadata/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_target_data_file_size_bytes' = '134217728'
);