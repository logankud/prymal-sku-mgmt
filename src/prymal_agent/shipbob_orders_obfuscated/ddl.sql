CREATE EXTERNAL TABLE IF NOT EXISTS prymal_agent.shipbob_all_line_item_obfuscated (
  customer_hash                string COMMENT 'sha256(to_utf8(lower(customer_email))) – unique but not PII',
  shipbob_order_id_hash        string COMMENT 'sha256(to_utf8(cast(shipbob_order_id as varchar))) – unique but not PII',
  customer_email_masked        string COMMENT '***@domain – preserves domain signal only',
  customer_name_masked         string COMMENT 'masked name – first initial only',
  customer_address_state       string COMMENT 'US state / region – retained for geo segmentation',
  customer_address_country     string COMMENT 'country – retained for geo segmentation',

  order_status                 string COMMENT 'ShipBob order status at snapshot time',
  order_type                   string COMMENT 'order type – e.g. DTC, FBA, wholesale',
  channel_id                   string COMMENT 'ID of the sales channel for this order',
  channel_name                 string COMMENT 'name of the sales channel for this order',
  product_id                   string COMMENT 'the sales channel-specific product ID',
  sku                          string COMMENT 'the sales channel-specific SKU of the product',
  shipping_method              string COMMENT 'selected shipping method for this line item',
  sku_name                     string COMMENT 'the sales channel-specific name of the product',
  inventory_id                 string COMMENT 'inventory id at ShipBob - this is the gold source product ID, the central mapping of shared physical products sold across multiple channels',
  inventory_name               string COMMENT 'name of the product in ShipBob - this is the gold source product name, the central mapping of shared physical products sold across multiple channels',
  inventory_qty                string COMMENT 'quantity for this line item (string in raw extract)',

  order_month                  date   COMMENT 'coarse bucket – order date truncated to month (for seasonality)',
  snapshot_date                date   COMMENT 'date of the snapshot',
  created_date                 date   COMMENT 'date that the order was created'
)
STORED AS PARQUET
LOCATION 's3://prymal-ops/prymal_agent/shipbob/all_line_item_obfuscated/run_date=${RUN_DATE}/'
TBLPROPERTIES (
  'comment'='One record per line item per order for all Shipbob orders as of created_date (the date the order was placed). Partitioned by purchase_month. Obfuscated to protect customer data. Full snapshot taken once per day, so all historic data is contained within the the entirety of this table.  The latest snapshot date contains data through ${RUN_DATE}'
);