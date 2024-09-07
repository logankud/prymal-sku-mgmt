with active_sku_run_rate AS (

  SELECT * 
  , run_rate * 30 as restock_amount
  FROM "prymal"."shipbob_inventory_run_rate"
  WHERE partition_date = (SELECT MAX(partition_date) FROM "prymal"."shipbob_inventory_run_rate")

  )

  , 

  active_sku_raw_materials_line_item AS (

  SELECT rr.* 
  , formulas.product_variant_name
  , formulas.ingredient_variant_code_sku_required as katana_ingredient_sku
  , formulas.ingredient_variant_name as katana_ingredient_name
  , formulas.quantity_required as katana_ingredient_qty_per_unit
  , formulas.unit_of_measure as katana_unit_of_measure
  , rr.run_rate * formulas.quantity_required as total_katana_ingredient_qty
  FROM active_sku_run_rate rr 
  LEFT JOIN "prymal"."katana_formulas" formulas
  ON rr.inventory_id = formulas.product_variant_code
  WHERE formulas.partition_date = (SELECT MAX(partition_date) FROM "prymal"."katana_formulas")   -- latest partition
  -- AND formulas.product_variant_code = 3640649.0        

  )

  ,

  ingredient_daily_run_rate AS (

  SELECT katana_ingredient_sku
  , katana_ingredient_name
  , katana_unit_of_measure
  , SUM(total_katana_ingredient_qty) as daily_run_rate
  FROM active_sku_raw_materials_line_item
  GROUP BY katana_ingredient_sku
  , katana_ingredient_name
  , katana_unit_of_measure

  )

  SELECT rr.*
  , inv.in_stock as inventory_on_hand
  , inv.partition_date as inventory_as_of
  , inv.in_stock / rr.daily_run_rate AS days_on_hand
  , (rr.daily_run_rate * 30) +   CASE WHEN safety_stock = 0 THEN 30 * rr.daily_run_rate      -- default lead time = 30 days
                                      ELSE safety_stock END as reorder_point            -- default lead time = 30 days
  FROM ingredient_daily_run_rate rr 
  LEFT JOIN "prymal"."katana_inventory" inv
  ON rr.katana_ingredient_sku = inv.variant_code_sku
  WHERE inv.partition_date = (SELECT MAX(partition_date) FROM "prymal"."katana_inventory")