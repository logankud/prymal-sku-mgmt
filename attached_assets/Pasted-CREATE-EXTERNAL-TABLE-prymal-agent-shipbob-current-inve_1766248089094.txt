CREATE EXTERNAL TABLE prymal_agent.shipbob_current_inventory (
    id                                  bigint COMMENT 'shipbob inventory ID (unique identifier for a physical product across all sales channels.  Use this to map store listings to inventory items)',
    name                                string COMMENT 'name of the product',
    is_digital                          boolean COMMENT 'whether the product is a digital item',
    is_case_pick                        boolean COMMENT 'whether the product is case picked',
    is_lot                              boolean COMMENT 'whether the product is part of a lot',
    total_fulfillable_quantity          bigint COMMENT 'total inventory qty available to be fulfilled (after accounting for existing unfilled orders)',
    total_onhand_quantity               bigint COMMENT 'total inventory qty available (not accounting for unfilled orders)',
    total_committed_quantity            bigint COMMENT 'total inventory qty commited to orders to be fulfilled',
    total_sellable_quantity             bigint COMMENT 'total inventory qty available to be sold',
    total_awaiting_quantity             bigint COMMENT 'total inventory awaiting (in route to fulfilment center)',
    total_exception_quantity            bigint COMMENT 'total inventory qty commited to orders above and beyond available inventory',
    total_internal_transfer_quantity    bigint COMMENT 'total inventory qty being transferred to/from other fultillment center locations',
    total_backordered_quantity          bigint COMMENT 'total units on back order ',
    is_active                           boolean COMMENT 'whether or not the product is active'
)
PARTITIONED BY (
    partition_date date    
)
STORED AS PARQUET
LOCATION 's3://prymal-ops/prymal_agent/shipbob/current_inventory/run_date=${RUN_DATE}/'
TBLPROPERTIES (
  'comment'='One record per inventory item in ShipBob.  ShipBob is where product inventory is maintained, and shows the inventory level of products that are sold across multiple sales channel.  The "id" column is the distinct identifier for a product and maps to the inventory_id in ShipBob order data. The latest snapshot date contains the current inventory levels as of ${RUN_DATE}'
);