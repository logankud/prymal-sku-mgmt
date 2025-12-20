INSERT INTO prymal_agent.shipbob_current_inventory
SELECT * 
FROM shipbob_inventory_details
WHERE partition_date = (SELECT MAX(partition_date) FROM shipbob_inventory_details)