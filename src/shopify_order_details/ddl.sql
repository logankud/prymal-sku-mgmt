-- Schema for the two tables written by src/shopify_order_details/main.py, and
-- the single source of truth for their column set and order.
--
-- main.py parses this file on every run and appends any column the live Glue
-- table is missing, so a schema change means editing this file and the
-- matching Pydantic model in src/models.py. There is no separate migration.
--
-- A partition is one calendar day in the Shopify store's timezone, which is
-- the day the Shopify admin UI shows. created_at and order_date are in that
-- same zone, so grouping by partition and grouping by order_date agree.
--
-- Column order is load-bearing: the CSV is written in Pydantic field order and
-- read back by field position, so a column added anywhere but the end shifts
-- every later value. tests/test_shopify_extract.py asserts the orders agree.

CREATE EXTERNAL TABLE IF NOT EXISTS shopify_orders (
    order_id                BIGINT    COMMENT 'Shopify order_number, the human-facing number. This is what ShipBob records as order_number and what the two systems join on',
    email                   STRING    COMMENT 'Customer email. Invalid or blank addresses are written as unknown@unknown.com by the ETL',
    created_at              TIMESTAMP COMMENT 'When the order was placed, in the Shopify store timezone',
    shipping_address        STRING    COMMENT 'Street address line 1',
    shipping_city           STRING    COMMENT 'Shipping city',
    shipping_province       STRING    COMMENT 'Shipping state or province',
    shipping_country        STRING    COMMENT 'Shipping country',
    subtotal_price          DOUBLE    COMMENT 'Merchandise total after discounts, before tax and shipping',
    total_line_items_price  DOUBLE    COMMENT 'Merchandise total before discounts. Equals SUM(price * quantity) over shopify_line_items',
    total_tax               DOUBLE    COMMENT 'Tax charged',
    total_discounts         DOUBLE    COMMENT 'Total discount applied to the order',
    total_shipping_fee      DOUBLE    COMMENT 'Shipping charged',
    total_price             DOUBLE    COMMENT 'Amount the customer paid, including tax and shipping',
    order_date              TIMESTAMP COMMENT 'Calendar day of created_at in the store timezone, at midnight. Matches the year/month/day partition. NULL in partitions written before 2026-09',
    shopify_order_id        BIGINT    COMMENT 'Shopify internal order id. Distinct from order_id above, which is order_number',
    customer_id             BIGINT    COMMENT 'Shopify customer id. Durable identity that survives an email change. Null for guest checkout',
    is_test                 BOOLEAN   COMMENT 'Shopify test-order flag. Exclude these from any reported figure',
    financial_status        STRING    COMMENT 'paid, pending, refunded, partially_refunded, voided',
    fulfillment_status      STRING    COMMENT 'fulfilled, partial, restocked, or null when unfulfilled',
    cancelled_at            TIMESTAMP COMMENT 'When the order was cancelled, null if it was not',
    tags                    STRING    COMMENT 'Comma-separated Shopify order tags'
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/shopify/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS shopify_line_items (
    order_id        BIGINT COMMENT 'Shopify order_number, joins to shopify_orders.order_id',
    email           STRING COMMENT 'Customer email on the parent order',
    created_at      STRING COMMENT 'When the parent order was placed, in the Shopify store timezone',
    order_date      STRING COMMENT 'Calendar day of created_at in the store timezone. Matches the year/month/day partition',
    price           DOUBLE COMMENT 'Unit price before discount. Varies within a SKU as prices change',
    quantity        BIGINT COMMENT 'Units on this line',
    sku             STRING COMMENT 'Merchant-assigned SKU. Editable and reusable, and written as the literal string No SKU when absent. Prefer variant_id for identity',
    title           STRING COMMENT 'Product title at time of sale. Changes when a product is renamed and may carry marketing text. Not an identifier',
    variant_title   STRING COMMENT 'Variant title at time of sale, typically the pack format such as Large Bag',
    line_item_name  STRING COMMENT 'Full display name of the line item',
    variant_id      BIGINT COMMENT 'Shopify variant id. The durable product identity: unchanged by renames, repricing and title edits. Null for lines with no variant, such as gift cards',
    product_id      BIGINT COMMENT 'Shopify product id that the variant belongs to',
    line_discount   DOUBLE COMMENT 'Discount allocated to this line item'
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/shopify/line_items/'
TBLPROPERTIES ('skip.header.line.count'='1');
