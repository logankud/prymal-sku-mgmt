-- Distinct orders per sales channel for one day. order_date is the partition
-- column of both the source and the target, so this reads a single partition.
SELECT
  CAST(channel_id AS varchar)                 AS channel_id,
  CAST(channel_name AS varchar)               AS channel_name,
  CAST(COUNT(DISTINCT order_number) AS integer) AS order_cnt
FROM ${DATABASE}.shipbob_order_details
WHERE order_date = DATE '${RUN_DATE}'
GROUP BY 1, 2
