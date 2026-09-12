-- Schema for the two tables written by src/shopify_order_details/main.py.
--
-- These tables were created by hand and had no DDL in the repo, which is how
-- the column set drifted from what the extract actually needs.
--
-- This file is the single source of truth for the column set and its order.
-- main.py parses it on every run and appends any column the live Glue table
-- is missing, so adding a column here and to the matching Pydantic model is
-- all that is needed to ship a schema change. There is no separate migration
-- to remember.
--
-- Column order is load-bearing. The job writes CSV with columns in Pydantic
-- field-definition order (models.ShopifyOrder / models.ShopifyLineItem), and
-- Athena maps CSV position to column position. Adding a column anywhere but
-- the end shifts every later value into the wrong column, silently.
-- tests/test_shopify_extract.py asserts the two orders agree.

CREATE EXTERNAL TABLE IF NOT EXISTS shopify_orders (
    order_id                BIGINT    COMMENT 'Shopify order_number, the human-facing number. This is what ShipBob records as order_number and what the two systems join on',
    email                   STRING    COMMENT 'Customer email. Invalid or blank addresses are written as unknown@unknown.com by the ETL',
    created_at              TIMESTAMP COMMENT 'When the order was placed. This is the reliable order timestamp',
    shipping_address        STRING    COMMENT 'Street address line 1',
    shipping_city           STRING    COMMENT 'Shipping city',
    shipping_province       STRING    COMMENT 'Shipping state or province',
    shipping_country        STRING    COMMENT 'Shipping country',
    subtotal_price          DOUBLE    COMMENT 'Merchandise total after discounts, before tax and shipping',
    total_line_items_price  DOUBLE    COMMENT 'Merchandise total before discounts. Reconciles exactly to SUM(price * quantity) over shopify_line_items',
    total_tax               DOUBLE    COMMENT 'Tax charged',
    total_discounts         DOUBLE    COMMENT 'Total discount applied to the order',
    total_shipping_fee      DOUBLE    COMMENT 'Shipping charged',
    total_price             DOUBLE    COMMENT 'Amount the customer paid, including tax and shipping',
    order_date              TIMESTAMP COMMENT 'KNOWN BROKEN: null in every row written before 2026-09 despite the ETL populating it. Cause not yet identified. Use created_at instead',
    -- appended 2026-09
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
    created_at      STRING COMMENT 'When the parent order was placed',
    order_date      STRING COMMENT 'Order date as YYYY-MM-DD',
    price           DOUBLE COMMENT 'Unit price before discount. One SKU has been sold at up to 7 prices, so this varies within a SKU',
    quantity        BIGINT COMMENT 'Units on this line',
    sku             STRING COMMENT 'Merchant-assigned SKU. Editable and reusable, and written as the literal string No SKU when absent. Prefer variant_id for identity',
    title           STRING COMMENT 'Product title at time of sale. Carries marketing text such as PRE-ORDER and Auto renew, and changes when a product is renamed. Not an identifier',
    variant_title   STRING COMMENT 'Variant title at time of sale, typically the pack format such as Large Bag',
    line_item_name  STRING COMMENT 'Full display name of the line item',
    -- appended 2026-09
    variant_id      BIGINT COMMENT 'Shopify variant id. THE durable product identity: it survives renames, repricing and marketing text, none of which sku or title do. Null for line items with no variant, such as gift cards',
    product_id      BIGINT COMMENT 'Shopify product id that the variant belongs to',
    line_discount   DOUBLE COMMENT 'Discount allocated to this line, so line-level discounting no longer has to be apportioned from the order total'
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET_NAME/shopify/line_items/'
TBLPROPERTIES ('skip.header.line.count'='1');
