
CREATE TABLE IF NOT EXISTS ${DATABASE}.tmp_${TABLE_NAME}_stage
WITH (
  format = 'PARQUET',
  parquet_compression = 'GZIP',
  external_location = 's3://${S3_BUCKET}/staging/prymal_agent/${TABLE_NAME}/run_date=${RUN_DATE}/'
) AS
${SELECT_QUERY}
