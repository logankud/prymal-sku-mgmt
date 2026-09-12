-- Copy the inventory snapshot for the run date, so a backfilled partition
-- holds that day's inventory rather than whatever is newest at backfill time.
SELECT
  id,
  name,
  is_digital,
  is_case_pick,
  is_lot,
  total_fulfillable_quantity,
  total_onhand_quantity,
  total_committed_quantity,
  total_sellable_quantity,
  total_awaiting_quantity,
  total_exception_quantity,
  total_internal_transfer_quantity,
  total_backordered_quantity,
  is_active
FROM ${DATABASE}.shipbob_inventory_details
WHERE partition_date = DATE '${RUN_DATE}'
