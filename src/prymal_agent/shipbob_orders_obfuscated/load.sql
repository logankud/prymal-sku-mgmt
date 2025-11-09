INSERT INTO prymal_agent.shipbob_all_line_item_obfuscated
WITH all_data AS (
    (
        SELECT h.*
        FROM prymal.shipbob_historic_order_data_fmtd h
        WHERE NOT EXISTS (
            SELECT 1
            FROM prymal.shipbob_order_details d
            WHERE d.order_number = h.order_number
        )
    )
    UNION ALL
    (
        SELECT d.*
        FROM prymal.shipbob_order_details d
    )
)
SELECT
  -- deterministic hashes
  to_hex(sha256(to_utf8(lower(coalesce(customer_email, '')))))         AS customer_hash,
  to_hex(sha256(to_utf8(cast(shipbob_order_id AS varchar))))           AS shipbob_order_id_hash,

  -- masking
  CASE
    WHEN customer_email IS NULL THEN NULL
    WHEN strpos(customer_email, '@') > 0
      THEN concat('***@', split_part(customer_email, '@', 2))
    ELSE '***'
  END                                                                  AS customer_email_masked,
  CASE
    WHEN customer_name IS NULL THEN NULL
    ELSE concat(substr(customer_name, 1, 1), '***')
  END                                                                  AS customer_name_masked,

  customer_address_state,
  customer_address_country,

  order_status,
  order_type,

  -- cast numerics to varchar to match table
  CAST(channel_id   AS varchar)                                        AS channel_id,
  channel_name,
  CAST(product_id   AS varchar)                                        AS product_id,
  sku,
  shipping_method,
  sku_name,
  CAST(inventory_id AS varchar)                                        AS inventory_id,
  inventory_name,
  CAST(inventory_qty AS varchar)                                       AS inventory_qty,

  -- dates
  date_trunc('month', try_cast(order_date AS date))                    AS order_month,
  current_date                                                         AS snapshot_date,  
  try_cast(created_date AS date)                                       AS created_date    
FROM all_data;



