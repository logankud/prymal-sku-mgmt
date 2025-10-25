CREATE EXTERNAL TABLE IF NOT EXISTS prymal_agent.shipbob_retention_by_cohort (
  cohort_month        DATE,
  total_customers     BIGINT,
  retained_30d_count  BIGINT,
  retained_30d_pct    DOUBLE,
  retained_60d_count  BIGINT,
  retained_60d_pct    DOUBLE,
  retained_90d_count  BIGINT,
  retained_90d_pct    DOUBLE
)
PARTITIONED BY (report_date DATE)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/prymal_agent/shipbob/retention_by_cohort/'
TBLPROPERTIES (
    'comment'='30/60/90 day retention metrics by cohort month, extracted from ShipBob (the 3PL and order management solution used by Prymal). Partitioned by report_date (the date the report was generated).'
);
