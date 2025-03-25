from loguru import logger
import argparse
import pytz
from pytz import timezone
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
        'End date to use to extract records.  Records will be extracted from the shopify /orders API                                 from 00:00:00 (UTC) on the start_date through 23:59:59 (UTC) on the end_date'
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

    # Shopify API version 
    API_VERSION = '2021-07'

    # Set aws region
    REGION = 'us-east-1'

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

    # Get shopify api key 
    SHOPIFY_API_KEY = os.environ.get('SHOPIFY_API_KEY')
    if not s3_bucket:
        raise ValueError("SHOPIFY_API_KEY environment variable is not set")

    # Get shopify api pw 
    SHOPIFY_API_PW = os.environ.get('SHOPIFY_API_PW')
    if not s3_bucket:
        raise ValueError("SHOPIFY_API_PASSWORD environment variable is not set")


    # Iterate through all dates in the date range
    while pd.to_datetime(start_date) <= pd.to_datetime(end_date):
    
        # ------ GET Shopify Data
    
        # List all orders in shipbob for a date range
        shopify_orders_df, shopify_line_item_df = get_shopify_orders_by_date(shopify_api_key=SHOPIFY_API_KEY, shopify_api_pw=SHOPIFY_API_PW,start_date=start_date, end_date=start_date)
        
        if len(shopify_orders_df) == 0:
            logger.info(f'0 Records returned from Shopify API')
    
        else:

            logger.info(
                f"Total order count (shopify_orders_df): {shopify_orders_df['order_id'].nunique()}")

            logger.info(
                f"Total order count (shopify_line_item_df): {shopify_line_item_df['order_id'].nunique()}")

    
            #  --------- SHOPIFY ORDERS -----------

            
            logger.info(f'Processing shopify_orders date: {start_date}')

            # rename dataframe for validation 
            df = shopify_orders_df.copy()
    
            # Validate data w/ Pydantic
            valid_data, invalid_data = validate_dataframe(
                df, ShopifyOrder)

            logger.info(f'Total records in df: {len(df)}')
            logger.info(f'Total Distinct Order ID: {len(df["order_id"].unique())}')
            logger.info(f'Total records in valid_data: {len(valid_data)}')
            logger.info(f'Total records in invalid_data: {len(invalid_data)}')

            if len(invalid_data) > 0:
                for invalid in invalid_data:
                    logger.error(f'Invalid data: {invalid}')

                    raise ValueError(f'Invalid data!')

            if len(valid_data) > 0:

                logger.info(pd.DataFrame(valid_data).head())

                # define path to write to
                year = pd.to_datetime(start_date).strftime('%Y')
                month = pd.to_datetime(start_date).strftime('%m')
                day = pd.to_datetime(start_date).strftime('%d')

                s3_prefix = f"shopify/orders/year={year}/month={month}/day={day}/shopify_orders_{start_date.replace('-','_')}.csv"

                try:

                    # instantiate s3 client
                    s3_client = boto3.client(
                        's3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=REGION)
                    
                    # Write to s3
                    write_df_to_s3(bucket=s3_bucket,
                                   key=s3_prefix,
                                   df=pd.DataFrame(valid_data),
                                   s3_client=s3_client)



                except Exception as e:
                    logger.error(f'Error writing to s3: {str(e)}')
                    raise ValueError(f'Error writing data! {str(e)}')



            #  --------- SHOPIFY LINE ITEMS -----------


            logger.info(f'Processing shopify_line_items date: {start_date}')

            # subset dataframe
            df = shopify_line_item_df.copy()
            # df = shopify_line_item_df.loc[shopify_line_item_df['order_date'] == start_date].copy()

            df.to_csv('line_item_df.csv', index=False)

            # Validate data w/ Pydantic
            valid_data, invalid_data = validate_dataframe(
                df, ShopifyLineItem)

            logger.info(f'Total records in df: {len(df)}')
            logger.info(f'Total records in valid_data: {len(valid_data)}')
            logger.info(f'Total records in invalid_data: {len(invalid_data)}')

            if len(invalid_data) > 0:
                for invalid in invalid_data:
                    logger.error(f'Invalid data: {invalid}')

                    raise ValueError(f'Invalid data!')

            if len(valid_data) > 0:

                logger.info(pd.DataFrame(valid_data).head())

                # define path to write to
                year = pd.to_datetime(start_date).strftime('%Y')
                month = pd.to_datetime(start_date).strftime('%m')
                day = pd.to_datetime(start_date).strftime('%d')
                
                s3_prefix = f"shopify/line_items/year={year}/month={month}/day={day}/shopify_line_items_{start_date.replace('-','_')}.csv"

                try:
                    # Write to s3
                    write_df_to_s3(bucket=s3_bucket,
                                   key=s3_prefix,
                                   df=pd.DataFrame(valid_data),
                                   s3_client=s3_client)



                except Exception as e:
                    logger.error(f'Error writing to s3: {str(e)}')
                    raise ValueError(f'Error writing data! {str(e)}')

            




        # ---------- INCREMENT DATE ------------

        # Increment start_date by 1 day
        start_date = pd.to_datetime(
            pd.to_datetime(start_date) +
            pd.DateOffset(days=1)).strftime('%Y-%m-%d')


    # -----------------
    # Run Athena query to update partitions
    # -----------------
    
    #  --------- ORDERS -----------
    
    logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # Define SQL query
    sql_query = """MSCK REPAIR TABLE shopify_orders"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=REGION)
    #  --------- LINE ITEMS -----------

    logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # Define SQL query
    sql_query = """MSCK REPAIR TABLE shopify_line_items"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=REGION)

        
if __name__ == "__main__":

    main()
