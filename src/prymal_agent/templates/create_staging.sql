
CREATE TABLE IF NOT EXISTS ${AGENT_DATABASE}.tmp_${TABLE_NAME}_stage
WITH (
  format = 'PARQUET',
  parquet_compression = 'GZIP',
  external_location = 's3://${S3_BUCKET}/staging/prymal_agent/${TABLE_NAME}/${PARTITION_COLUMN}=${RUN_DATE}/'
) AS
${SELECT_QUERY}
