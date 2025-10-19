ALTER TABLE "prymal-agent".shipbob_daily_order_cnt_by_channel
DROP IF EXISTS PARTITION (order_date = DATE '${RUN_DATE}');

ALTER TABLE "prymal-agent".shipbob_daily_order_cnt_by_channel
ADD PARTITION (order_date = DATE '${RUN_DATE}')
LOCATION 's3://prymal-ops/prymal_agent/staging/shipbob/daily_order_cnt_by_channel/run_date=${RUN_DATE}/';

DROP TABLE IF EXISTS "prymal-agent".tmp_shipbob_daily_stage;
