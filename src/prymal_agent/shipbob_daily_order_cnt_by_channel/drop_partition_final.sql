ALTER TABLE prymal_agent.shipbob_daily_order_cnt_by_channel
DROP PARTITION (order_date = DATE '${RUN_DATE}');