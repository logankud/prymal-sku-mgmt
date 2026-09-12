-- Run once against the live Glue tables, before the first extract that writes
-- the new columns.
--
-- ADD COLUMNS appends to the end of the column list, which is exactly where
-- the extract appends them in the CSV. Partitions written before this
-- migration have fewer fields per line; the SerDe returns NULL for the
-- missing trailing columns rather than failing, so history stays queryable
-- until the backfill rewrites it.
--
-- Nothing here drops or rewrites data.

ALTER TABLE shopify_orders ADD COLUMNS (
    shopify_order_id   BIGINT    COMMENT 'Shopify internal order id. Distinct from order_id, which is order_number',
    customer_id        BIGINT    COMMENT 'Shopify customer id. Durable identity that survives an email change. Null for guest checkout',
    is_test            BOOLEAN   COMMENT 'Shopify test-order flag. Exclude these from any reported figure',
    financial_status   STRING    COMMENT 'paid, pending, refunded, partially_refunded, voided',
    fulfillment_status STRING    COMMENT 'fulfilled, partial, restocked, or null when unfulfilled',
    cancelled_at       TIMESTAMP COMMENT 'When the order was cancelled, null if it was not',
    tags               STRING    COMMENT 'Comma-separated Shopify order tags'
);

ALTER TABLE shopify_line_items ADD COLUMNS (
    variant_id    BIGINT COMMENT 'Shopify variant id. THE durable product identity: it survives renames, repricing and marketing text, none of which sku or title do',
    product_id    BIGINT COMMENT 'Shopify product id that the variant belongs to',
    line_discount DOUBLE COMMENT 'Discount allocated to this line item'
);
