ALTER TABLE prymal_agent.shipbob_daily_order_cnt_by_channel
ADD PARTITION (order_date = DATE '${RUN_DATE}')
LOCATION 's3://prymal-ops/staging/prymal_agent/shipbob/daily_order_cnt_by_channel/run_date=${RUN_DATE}/';