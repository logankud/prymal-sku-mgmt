import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# Add parent directory to path to import utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
from src.utils import run_athena_query_no_results
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Get environment variables
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    database = os.getenv("GLUE_DATABASE_NAME")
    region = "us-east-1"

    if not s3_bucket or not database:
        raise ValueError("Required environment variables not set")

    # Define date range for backfill
    # MODIFY THESE DATES AS NEEDED
    start_date = "2023-01-01"  # Change this to your start date
    end_date = "2025-12-31"  # Change this to your end date

    logger.info(f"Starting backfill from {start_date} to {end_date}")

    # Generate list of all dates in range
    current_date = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    date_conditions = []
    while current_date <= end_dt:
        run_date = current_date.strftime('%Y-%m-%d')
        date_conditions.append(f"DATE '{run_date}'")
        current_date += timedelta(days=1)

    # Join all dates with OR
    date_filter = " OR ".join(
        [f"DATE(order_date) = {d}" for d in date_conditions])

    # Create unique run ID for this backfill
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")

    # 1. Create staging table with ALL dates
    logger.info("Creating staging table with all backfill dates...")

    create_staging_query = f"""
    CREATE TABLE prymal_agent.tmp_shipbob_backfill_{run_id}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'GZIP',
      external_location = 's3://{s3_bucket}/staging/prymal_agent/shipbob/daily_order_cnt_by_channel/backfill_{run_id}/'
    ) AS
    SELECT
      CAST(channel_id AS VARCHAR) AS channel_id,
      CAST(channel_name AS VARCHAR) AS channel_name,
      COUNT(DISTINCT(order_number)) AS order_cnt,
      CAST(order_date AS DATE) AS order_date
    FROM prymal.shipbob_order_details
    WHERE {date_filter}
    GROUP BY
      CAST(channel_id AS VARCHAR),
      CAST(channel_name AS VARCHAR),
      CAST(order_date AS DATE)
    """

    run_athena_query_no_results(bucket=s3_bucket,
                                query=create_staging_query,
                                database=database,
                                region=region)
    logger.info(f"Created staging table with all dates")

    # 2. For each date, drop partition and add it pointing to the staging location
    current_date = pd.to_datetime(start_date)
    while current_date <= end_dt:
        run_date = current_date.strftime('%Y-%m-%d')

        try:
            # Drop existing partition if exists
            drop_partition_query = f"""
            ALTER TABLE prymal_agent.shipbob_daily_order_cnt_by_channel
            DROP IF EXISTS PARTITION (order_date = DATE '{run_date}')
            """

            run_athena_query_no_results(bucket=s3_bucket,
                                        query=drop_partition_query,
                                        database=database,
                                        region=region)

            # Add partition pointing to staging data
            add_partition_query = f"""
            ALTER TABLE prymal_agent.shipbob_daily_order_cnt_by_channel
            ADD PARTITION (order_date = DATE '{run_date}')
            LOCATION 's3://{s3_bucket}/staging/prymal_agent/shipbob/daily_order_cnt_by_channel/backfill_{run_id}/'
            """

            run_athena_query_no_results(bucket=s3_bucket,
                                        query=add_partition_query,
                                        database=database,
                                        region=region)
            logger.info(f"Added partition for {run_date}")

        except Exception as e:
            logger.error(
                f"Error processing partition for {run_date}: {str(e)}")

        # Move to next day
        current_date += timedelta(days=1)

    # 3. Drop staging table
    drop_staging_query = f"""
    DROP TABLE IF EXISTS prymal_agent.tmp_shipbob_backfill_{run_id}
    """

    run_athena_query_no_results(bucket=s3_bucket,
                                query=drop_staging_query,
                                database=database,
                                region=region)
    logger.info(f"Dropped staging table")

    logger.info("Backfill complete!")


if __name__ == "__main__":
    main()
