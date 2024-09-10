from loguru import logger
import argparse
import pytz
from datetime import datetime, timedelta

import sys
sys.path.append('src/')  # updating path back to root for importing modules

from utils import *
from models import *


def main():
    logger.info('Running main()')

    # -----------------
    # Get params from environment variables
    # -----------------

    # Set aws region
    region = 'us-east-1'

    # Get s3 bucket
    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    if not s3_bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")

    # Get Glue database
    glue_database = os.getenv('GLUE_DATABASE_NAME')
    if not glue_database:
        raise ValueError("S3_BUCKET environment variable is not set")

    # Get s3 bucket
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY')
    if not s3_bucket:
        raise ValueError("AWS_ACCESS_KEY environment variable is not set")

    # Get s3 bucket
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_ACCESS_SECRET')
    if not s3_bucket:
        raise ValueError("AWS_ACCESS_SECRET environment variable is not set")

    # ------ CONVERT PRODUCT RUN RATE TO RAW MATERIAL RUN RATE ------

    # Read the SQL file
    with open('src/raw_material_run_rate/query.sql', 'r') as file:
        query = file.read()

    logger.info(f'SQL query: {query}')

    # Run Athena query
    raw_material_run_rate_df = run_athena_query(query, glue_database, region,
        s3_bucket)

    logger.info(raw_material_run_rate_df.head())

    if len(raw_material_run_rate_df) == 0:
        logger.info(f'0 Records returned from ShipBob API')

    else:

        # Instantiate s3 client
        s3_client = boto3.client('s3',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # Validate data w/ Pydantic
        valid_data, invalid_data = validate_dataframe(raw_material_run_rate_df,
                                                      RawMaterialRunRate)

        logger.info(f'Total records in df: {len(raw_material_run_rate_df)}')
        logger.info(f'Total records in valid_data: {len(valid_data)}')
        logger.info(f'Total records in invalid_data: {len(invalid_data)}')

        if len(invalid_data) > 0:
            for invalid in invalid_data:
                logger.error(f'Invalid data: {invalid}')

                raise ValueError(f'Invalid data!')

        if len(valid_data) > 0:

            logger.info(valid_data)
            logger.info(pd.DataFrame(valid_data))

            # define path to write to
            today = pd.to_datetime(
                pd.to_datetime('today') -
                timedelta(hours=4)).strftime('%Y-%m-%d')
            s3_prefix = f"katana/raw_material_run_rate/partition_date={today}/katana_raw_material_run_rate_{today.replace('-','_')}.csv"

            try:
                # Write to s3
                write_df_to_s3(bucket=s3_bucket,
                               key=s3_prefix,
                               df=pd.DataFrame(valid_data),
                               s3_client=s3_client)

            except Exception as e:
                logger.error(f'Error writing to s3: {str(e)}')
                raise ValueError(f'Error writing data! {str(e)}')

        logger.info(f'Finished extracting inventory')

        # -----------------
        # Run Athena query to update partitions
        # -----------------
        logger.info('Running MKSCK REPAIR TABLE to update partitions')

        # Define SQL query
        sql_query = """MSCK REPAIR TABLE katana_raw_material_run_rate"""

        logger.info(f'SQL query: {sql_query}')

        run_athena_query_no_results(query=sql_query,
                                    bucket=s3_bucket,
                                    database=glue_database,
                                    region=region)


if __name__ == "__main__":

    main()