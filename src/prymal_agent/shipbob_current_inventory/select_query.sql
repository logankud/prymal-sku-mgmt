SELECT
CAST(channel_id   AS VARCHAR)            AS channel_id,
  CAST(channel_name AS VARCHAR)            AS channel_name,
  COUNT(DISTINCT(order_number))            AS order_cnt,
  CAST(order_date   AS DATE)               AS order_date
FROM prymal.shipbob_order_details
WHERE DATE(order_date) = DATE '${RUN_DATE}'
GROUP BY
  CAST(channel_id   AS VARCHAR),
  CAST(channel_name AS VARCHAR),
  CAST(order_date   AS DATE);
