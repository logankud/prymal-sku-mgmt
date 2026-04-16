SELECT
  inventory_id,
  name,
  total_fulfillable_quantity,
  CAST(run_rate AS double) AS est_daily_run_rate,
  CAST(est_stock_days_on_hand AS double) AS est_stock_days_on_hand,
  CAST(estimated_stockout_date AS date) AS estimated_stockout_date,
  CAST(restock_point AS bigint) AS restock_point        
FROM "prymal"."shipbob_inventory_run_rate" 
WHERE partition_date = (
    SELECT MAX(partition_date)
    FROM "prymal"."shipbob_inventory_run_rate" 
    )