ALTER TABLE prymal_agent.shipbob_retention_by_cohort
DROP PARTITION (report_date = DATE '${RUN_DATE}');
