ALTER TABLE "prymal-agent".shipbob_daily_order_cnt_by_channel
DROP IF EXISTS PARTITION (order_date = DATE '${RUN_DATE}');
