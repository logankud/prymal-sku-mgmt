ALTER TABLE prymal_agent.shipbob_retention_by_cohort
ADD PARTITION (report_date = DATE '${RUN_DATE}')
LOCATION 's3://${S3_BUCKET}/staging/prymal_agent/shipbob/retention_by_cohort/report_date=${RUN_DATE}/';
