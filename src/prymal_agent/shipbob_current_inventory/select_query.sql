SELECT * 
FROM prymal.shipbob_inventory_details
WHERE partition_date = (SELECT MAX(partition_date) FROM prymal.shipbob_inventory_details)
