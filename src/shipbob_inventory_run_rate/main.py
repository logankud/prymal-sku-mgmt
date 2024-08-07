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
        description=
        'Calculate current recent run rate (exponentially weighted mean) & current inventory on hand for all products sold in the past 90 days'
    )

    parser.add_argument(
        '--start_date',
        type=str,
        required=False,
        default=pd.to_datetime(datetime.now(pytz.utc) -
                               timedelta(days=1)).replace(
                                   hour=0, minute=0, second=0,
                                   microsecond=0).strftime('%Y-%m-%d'),
        help=
        'Start date to use to for generating run rate report.  Report will be run for each day in from start_date to end_date (inclusive)'
    )

    parser.add_argument(
        '--end_date',
        type=str,
        required=False,
        default=pd.to_datetime(datetime.now(
            pytz.utc)).replace(hour=23, minute=59, second=59,
                               microsecond=59).strftime('%Y-%m-%d'),
        help=
        'End date to use to for generating run rate report.  Report will be run for each day in from start_date to end_date (inclusive)'
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

    # ====================== QUERY DATA =============================================

    # Configure Athena / Glue
    database = os.getenv('GLUE_DATABASE_NAME')
    s3_bucket = os.getenv('S3_BUCKET_NAME')

    # Athena Query to pull historic order details
    from_date = pd.to_datetime(f'{start_date}') - timedelta(90)

    query = f"""
    SELECT * 
    FROM shipbob_order_details 
    WHERE order_date >= DATE('{pd.to_datetime(from_date).strftime('%Y-%m-%d')}')
    
    """

    logger.info(query)

    shipbob_order_details_df = run_athena_query(query, database, region,
                                                s3_bucket)

    # Update data types
    shipbob_order_details_df['order_date'] = pd.to_datetime(
        shipbob_order_details_df['order_date']).dt.strftime('%Y-%m-%d')
    shipbob_order_details_df['inventory_qty'] = shipbob_order_details_df[
        'inventory_qty'].fillna(0).astype(int)

    # Athena Query to pull historic order details
    query = f"""
    SELECT * 
    FROM shipbob_inventory_details 
    WHERE partition_date = DATE('{pd.to_datetime(pd.to_datetime(f"{start_date}") - timedelta(1)).strftime('%Y-%m-%d')}')
    
    """

    shipbob_inventory_details_df = run_athena_query(query, database, region,
                                                    s3_bucket)

    # Update data types
    shipbob_inventory_details_df['partition_date'] = pd.to_datetime(
        shipbob_inventory_details_df['partition_date']).dt.strftime('%Y-%m-%d')
    shipbob_inventory_details_df[
        'inventory_id'] = shipbob_inventory_details_df['id']
    shipbob_inventory_details_df[
        'total_fulfillable_quantity'] = shipbob_inventory_details_df[
            'total_fulfillable_quantity'].fillna(0).astype(int)

    # ====================== METRICS =============================================

    # Group by order_date & inventory_id , sum inventory_qty (qty sold)
    daily_qty_sold_df = shipbob_order_details_df.groupby(
        ['order_date', 'inventory_id'], as_index=False)['inventory_qty'].sum()

    # Rename columns
    daily_qty_sold_df.columns = ['order_date', 'inventory_id', 'qty_sold']

    # Iterate through each product & calculate weighted mean
    current_run_rate_df = pd.DataFrame(columns=['inventory_id', 'run_rate'])
    for product in daily_qty_sold_df['inventory_id'].unique():
        logger.info(f'Calculating weighted median for: {product}')

        # Filter for the current product and generate exponentially weighted mean
        ewm_series = daily_qty_sold_df.loc[daily_qty_sold_df['inventory_id'] ==
                                           product,
                                           'qty_sold'].ewm(alpha=0.5).mean()

        logger.info(f'EWMA: {ewm_series.iloc[-1]}')

        # append to current_run_rate_df
        current_run_rate_df = pd.concat([
            current_run_rate_df,
            pd.DataFrame({
                'inventory_id': [product],
                'run_rate': [ewm_series.iloc[-1]]
            })
        ])

    # Join run rate data with inventory data
    inventory_cols = ['inventory_id', 'name', 'total_fulfillable_quantity'
                      ]  # column subset to merge with run rate data
    daily_metrics_df = current_run_rate_df.merge(
        shipbob_inventory_details_df[inventory_cols],
        how='left',
        on='inventory_id')

    # Calculate estimated days of stock onhand & estimated stockout date
    daily_metrics_df['est_stock_days_on_hand'] = daily_metrics_df[
        'total_fulfillable_quantity'] / daily_metrics_df['run_rate']
    daily_metrics_df['estimated_stockout_date'] = daily_metrics_df[
        'est_stock_days_on_hand'].apply(
            lambda x: pd.to_datetime('today') + timedelta(x))

    # Calculate restock point
    production_lead_time = 7  # number of days for a new production run
    safety_stock_days = 7  # number of days of stock to keep as a safety stock
    daily_metrics_df['restock_point'] = daily_metrics_df['run_rate'].apply(
        lambda x: (x * production_lead_time) + (x * safety_stock_days)).astype(
            int)

    # ====================== VALIDATE DATA & WRITE TO S3 ============================================

    # Validate data w/ Pydantic
    valid_data, invalid_data = validate_dataframe(daily_metrics_df,
                                                  DailyRunRate)

    logger.info(f'Total records in df: {len(daily_metrics_df)}')
    logger.info(f'Total records in valid_data: {len(valid_data)}')
    logger.info(f'Total records in invalid_data: {len(invalid_data)}')

    if len(invalid_data) > 0:
        for invalid in invalid_data:
            logger.error(f'Invalid data: {invalid}')

            raise ValueError('Invalid data!')

    if len(valid_data) > 0:

        logger.info(f'Attempting to write valid data to s3..')

        # define path to write to
        today = pd.to_datetime(pd.to_datetime('today') -
                               timedelta(hours=4)).strftime('%Y-%m-%d')

        s3_prefix = f"shipbob/inventory_run_rate/partition_date={today}/shipbob_inventory_run_rate_{today.replace('-','_')}.csv"

        try:

            # Instantiate s3 client
            s3_client = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

            # Write to s3
            write_df_to_s3(bucket=s3_bucket,
                           key=s3_prefix,
                           df=pd.DataFrame(valid_data),
                           s3_client=s3_client)

        except Exception as e:
            logger.error(f'Error writing to s3: {str(e)}')
            raise ValueError(f'Error writing data! {str(e)}')

    logger.info(f'Finished writing daily run rate data to s3!')

    # -----------------
    # Run Athena query to update partitions
    # -----------------
    logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # Define SQL query
    sql_query = """MSCK REPAIR TABLE shipbob_inventory_run_rate"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=region)


if __name__ == "__main__":

    main()
