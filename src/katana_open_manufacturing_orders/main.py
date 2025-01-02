from datetime import datetime
import pandas as pd
import argparse
from loguru import logger
import os
import sys

sys.path.append('src/')  # updating path back to root for importing modules

from utils import *
from models import *

def main():
    
    logger.info('Running main()')

    parser = argparse.ArgumentParser(
        description=
        'Format Katana product recipe & ingredient inventory from raw export to a single table in data lake'
    )

    parser.add_argument(
        '--path',
        type=str,
        required=False,
        default='src/katana_open_manufacturing_orders/future_mo.csv',
        help=
        'Relative path to the manufacturing order (MO) export file from Katana'
    )

    # Parse input args
    args = parser.parse_args()
    logger.info(f'Args: {args}')

    path_to_csv = args.path
    
    # ------------------- CONFIGURE ENV VARIABLES -------------------
    
    # Configure Athena / Glue
    region = 'us-east-1'
    glue_database = os.getenv('GLUE_DATABASE_NAME')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    
    # ------------------- FORMAT CSV - katana_open_manufacturing_orders -------------------

    mo_df = pd.read_csv(path_to_csv)

    logger.info('Formatting columns')

    # Format column names
    mo_df.columns = [clean_column_name(col) for col in mo_df.columns]

    logger.info(mo_df.columns)
    logger.info(mo_df.head())
    

    mo_df.head(10).to_csv('mo_df.csv',index=False)
    

    # ------------------- VALIDATE DATA - katana_open_manufacturing_orders -------------------

    # Validate data w/ Pydantic
    valid_data, invalid_data = validate_dataframe(mo_df,
                                                 ManufacturingOrder)

    logger.info(f'Total records in formulas_df: {len(mo_df)}')
    logger.info(f'Total records in valid_data: {len(valid_data)}')
    logger.info(f'Total records in invalid_data: {len(invalid_data)}')

    if len(invalid_data) > 0:
        for invalid in invalid_data:
            logger.error(f'Invalid data: {invalid}')

            raise ValueError(f'Invalid data!')

    if len(valid_data) > 0:

        logger.info(valid_data)
        logger.info(pd.DataFrame(valid_data))


        # ------------------- WRITE TO S3 - katana_open_manufacturing_orders -------------------

        # Instantiate s3 client
        s3_client = boto3.client('s3',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        # define path to write to
        today = pd.to_datetime(
            pd.to_datetime('today') -
            timedelta(hours=4)).strftime('%Y-%m-%d')
        s3_prefix = f"katana/open_manufacturing_orders/partition_date={today}/katana_open_manufacturing_orders_{today.replace('-','_')}.csv"

        try:
            # Write to s3
            write_df_to_s3(bucket=s3_bucket,
                           key=s3_prefix,
                           df=pd.DataFrame(valid_data),
                           s3_client=s3_client)

        except Exception as e:
            logger.error(f'Error writing to s3: {str(e)}')
            raise ValueError(f'Error writing data! {str(e)}')

    logger.info(f'Finished validating data & writing to s3!')

    # -----------------
    # Run Athena query to update partitions - katana_open_manufacturing_orders
    # -----------------
    logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # Define SQL query
    sql_query = """MSCK REPAIR TABLE katana_open_manufacturing_orders"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=region)

if __name__ == "__main__":

    main()


    