CREATE EXTERNAL TABLE IF NOT EXISTS shipbob_inventory_details (
    id int,
    name string,
    is_digital boolean,
    is_case_pick boolean,
    is_lot boolean,
    total_fulfillable_quantity int,
    total_onhand_quantity int,
    total_committed_quantity int,
    total_sellable_quantity int,
    total_awaiting_quantity int,
    total_exception_quantity int,
    total_internal_transfer_quantity int,
    total_backordered_quantity int,
    is_active boolean
)
