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
    parser = argparse.ArgumentParser(
        description='ETL for Shipbob Data (order details, inventory, etc.)')

    parser.add_argument(
        '--start_date',
        type=str,
        required=False,
        default=pd.to_datetime(datetime.now(pytz.utc) -
                               timedelta(days=1)).replace(
                                   hour=0, minute=0, second=0,
                                   microsecond=0).strftime('%Y-%m-%d'),
        help=
        'Start date to use to extract records.  Records will be extracted from the shipbob_order_details table                                 from 00:00:00 (UTC) on the start_date through 00:00:00 (UTC) on the end_date - end_date is not inclusive'
    )

    parser.add_argument(
        '--end_date',
        type=str,
        required=False,
        default=pd.to_datetime(datetime.now(
            pytz.utc)).replace(hour=23, minute=59, second=59,
                               microsecond=59).strftime('%Y-%m-%d'),
        help=
        'End date to use to extract records.  Records will be extracted from the shipbob_order_details table                                 from 00:00:00 (UTC) on the start_date through 23:59:59 (UTC) on the end_date'
    )
    # Parse input args
    args = parser.parse_args()
    logger.info(f'Args: {args}')

    start_date = args.start_date
    end_date = args.end_date

    # Log date range of job
    logger.info(
        f'Job date range: {(pd.to_datetime(end_date) - (pd.to_datetime(start_date))).days}'
    )

    # -----------------
    # Get params from environment variables
    # -----------------

    # Set aws region
    region = 'us-east-1'

    # Get Shipbob API secret
    shipbob_api_secret = os.getenv('SHIPBOB_API_SECRET')
    if not shipbob_api_secret:
        raise ValueError("shipbob_api_secret environment variable is not set")

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


    # ------ GET ORDERS ------

    # List all orders in shipbob for a date range
    shipbob_orders_df = get_shipbob_orders_by_date(
        api_secret=shipbob_api_secret,
        start_date=start_date,
        end_date=end_date)

    logger.info(
        f"Total order count: {shipbob_orders_df['order_number'].nunique()}")

    if len(shipbob_orders_df) == 0:
        logger.info(f'0 Records returned from ShipBob API')

    else:

        logger.info(
            shipbob_orders_df.groupby('purchase_date')
            ['purchase_date'].count())

        # Instantiate s3 client
        s3_client = boto3.client('s3',
                                 region_name=region,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        while pd.to_datetime(start_date) <= pd.to_datetime(end_date):
            logger.info(f'start_date {start_date} - end_date {end_date}')

            # Subset shipbob_orders_df to the current start_date (to avoid overlap)
            df = shipbob_orders_df[pd.to_datetime(
                shipbob_orders_df['purchase_date']).dt.strftime(
                    '%Y-%m-%d') == pd.to_datetime(start_date).strftime(
                        '%Y-%m-%d')]

            logger.info(f'Number of records for {start_date}: {len(df)}')


           
            # Validate data w/ Pydantic
            valid_data, invalid_data = validate_dataframe(
                df, ShipbobOrderDetails)

            logger.info(f'Total records in df: {len(df)}')
            logger.info(f'Total records in valid_data: {len(valid_data)}')
            logger.info(f'Total records in invalid_data: {len(invalid_data)}')

            if len(invalid_data) > 0:
                for invalid in invalid_data:
                    logger.error(f'Invalid data: {invalid}')

                    raise ValueError(f'Invalid data!')

            if len(valid_data) > 0:

                # define path to write to
                s3_prefix = f"shipbob/order_details/order_date={start_date}/shipbob_order_details_{start_date.replace('-','_')}.csv"

                logger.info(f"Total nutella records: {df.loc[df['inventory_id']==14423625,'inventory_qty'].sum()}")

                try:
                    # Write to s3
                    write_df_to_s3(bucket=s3_bucket,
                                   key=s3_prefix,
                                   df=pd.DataFrame(valid_data),
                                   s3_client=s3_client)

                    
                    # with open('test.json', 'w', encoding='utf-8') as json_file:
                    #     json.dump(valid_data, json_file, indent=2, ensure_ascii=False)
                        
                    #     write_list_of_dicts_to_s3(bucket=s3_bucket,
                    #    key=s3_prefix,
                    #    list_of_dicts=valid_data,
                    #    s3_client=s3_client)

                except Exception as e:
                    logger.error(f'Error writing to s3: {str(e)}')
                    raise ValueError(f'Error writing data! {str(e)}')

            # Increment start_date by 1 day
            start_date = pd.to_datetime(
                pd.to_datetime(start_date) +
                pd.DateOffset(days=1)).strftime('%Y-%m-%d')

        logger.info(
            f'Finished extracting data for date range: {start_date} - {end_date}'
        )

        # -----------------
        # Run Athena query to update partitions
        # -----------------
        logger.info('Running MKSCK REPAIR TABLE to update partitions')

        # Define SQL query
        sql_query = """MSCK REPAIR TABLE shipbob_order_details"""

        logger.info(f'SQL query: {sql_query}')

        run_athena_query_no_results(query=sql_query,
                                    bucket=s3_bucket,
                                    database=glue_database,
                                    region=region)


if __name__ == "__main__":

    main()
