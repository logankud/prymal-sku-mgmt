from loguru import logger
import pytz
from datetime import datetime, timedelta
import argparse

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

    while pd.to_datetime(start_date) < pd.to_datetime(end_date):
        logger.info(f'{start_date} -  {end_date}')

        # ====================== QUERY DATA =============================================

        # Configure Athena / Glue
        database = os.getenv('GLUE_DATABASE_NAME')
        s3_bucket = os.getenv('S3_BUCKET_NAME')

        # ------
        #  Shipbob Order Data
        # ------

        # Athena Query to pull historic order details
        from_date = pd.to_datetime(f'{start_date}') - timedelta(90)

        query = f"""

        with active_fl AS (
            SELECT inventory_id
            , MAX(CASE WHEN sku IN (SELECT DISTINCT(variant_sku)
                                FROM shopify_active_variant_sku_details
                                WHERE partition_date = DATE('{pd.to_datetime(pd.to_datetime(f"{start_date}")).strftime('%Y-%m-%d')}'))
                    THEN 1 ELSE 0 END) AS active_sku_fl
            FROM shipbob_order_details 
            WHERE order_date >= DATE('{pd.to_datetime(from_date).strftime('%Y-%m-%d')}')
            GROUP BY inventory_id
        )

        SELECT orders.*
        , CASE WHEN active_fl.active_sku_fl IS NULL THEN 0
            ELSE active_fl.active_sku_fl END AS active_sku_fl
        FROM shipbob_order_details orders
        LEFT JOIN active_fl 
        ON orders.inventory_id = active_fl.inventory_id
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
        shipbob_order_details_df['active_sku_fl'] = shipbob_order_details_df[
            'active_sku_fl'].astype(int)

        # ------
        #  Shipbob Inventory Data
        # ------

        # Athena Query to pull latest inventory data
        query = f"""
        SELECT * 
        FROM shipbob_inventory_details 
        WHERE partition_date = DATE('{pd.to_datetime(pd.to_datetime(f"{start_date}") - timedelta(1)).strftime('%Y-%m-%d')}')

        """

        shipbob_inventory_details_df = run_athena_query(
            query, database, region, s3_bucket)

        if len(shipbob_inventory_details_df) == 0:

            logger.info(
                f'No records in shipbob_inventory_details_df for {start_date}')

        else:
            # Update data types
            shipbob_inventory_details_df['partition_date'] = pd.to_datetime(
                shipbob_inventory_details_df['partition_date']).dt.strftime(
                    '%Y-%m-%d')
            shipbob_inventory_details_df[
                'inventory_id'] = shipbob_inventory_details_df['id']
            shipbob_inventory_details_df[
                'total_fulfillable_quantity'] = shipbob_inventory_details_df[
                    'total_fulfillable_quantity'].fillna(0).astype(int)

        # # ------
        # #  Shopify Active SKU Data
        # # ------

        # # Athena Query to pull latest inventory data
        # query = f"""
        # SELECT *
        # FROM shopify_active_variant_sku_details
        # WHERE partition_date = DATE('{pd.to_datetime(pd.to_datetime(f"{start_date}")).strftime('%Y-%m-%d')}')

        # """

        # shopify_active_variant_sku_details_df = run_athena_query(
        #     query, database, region, s3_bucket)

        # if len(shopify_active_variant_sku_details_df) == 0:

        #     logger.info(
        #         f'No records in shopify_active_variant_sku_details_df for {start_date}')

        # ====================== METRICS =============================================

        shipbob_order_details_df.to_csv('shipbob_order_details_df.csv',
                                        index=False)
        # Group by order_date & inventory_id , sum inventory_qty (qty sold)
        daily_qty_sold_df = shipbob_order_details_df.groupby(
            ['order_date', 'active_sku_fl', 'inventory_id'],
            as_index=False)['inventory_qty'].sum()

        # Rename columns
        daily_qty_sold_df.columns = [
            'order_date', 'active_sku_fl', 'inventory_id', 'qty_sold'
        ]

        # Fill in dates w/ no sales with qty_sold=0
        # ------------------------------------------------

        # daily_qty_sold_df['order_date'] = pd.to_datetime(daily_qty_sold_df['order_date'])
        logger.info(daily_qty_sold_df['order_date'].unique())
        min_date = daily_qty_sold_df['order_date'].min()
        max_date = daily_qty_sold_df['order_date'].max()
        # Create a date range
        all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
        logger.info(all_dates)
        # Find all unique inventory IDs
        inventory_ids = daily_qty_sold_df['inventory_id'].unique()
        # Create a new DataFrame from all combinations of dates and inventory IDs
        all_combinations = pd.MultiIndex.from_product(
            [all_dates, inventory_ids], names=['order_date', 'inventory_id'])
        expanded_df = pd.DataFrame(index=all_combinations).reset_index()
        # Set datetime back to YYYY-MM-DD string
        expanded_df['order_date'] = pd.to_datetime(
            expanded_df['order_date']).dt.strftime('%Y-%m-%d')
        # Merge the expanded DataFrame with the original sales data
        daily_qty_sold_df = expanded_df.merge(
            daily_qty_sold_df, on=['order_date', 'inventory_id'], how='left')
        # Fill missing qty_sold values with 0
        daily_qty_sold_df['qty_sold'].fillna(0, inplace=True)
        # Set datetime back to YYYY-MM-DD string
        daily_qty_sold_df['order_date'] = pd.to_datetime(
            daily_qty_sold_df['order_date']).dt.strftime('%Y-%m-%d')

        daily_qty_sold_df.to_csv(f'daily_qty_sold_df.csv', index=False)

        # Iterate through each product & calculate weighted mean
        current_run_rate_df = pd.DataFrame(
            columns=['inventory_id', 'run_rate'])
        for product in daily_qty_sold_df['inventory_id'].unique():
            logger.info(f'Calculating weighted median for: {product}')

            # Filter for the current product and generate exponentially weighted mean
            ewm_series = daily_qty_sold_df.loc[
                daily_qty_sold_df['inventory_id'] == product,
                'qty_sold'].ewm(alpha=0.5).mean()

            # Filter for the current product and caluculate skew / kurtosis to determine limited edition vs classic product
            kurtosis = daily_qty_sold_df.loc[
                daily_qty_sold_df['inventory_id'] == product,
                'qty_sold'].kurtosis()

            skew = daily_qty_sold_df.loc[daily_qty_sold_df['inventory_id'] ==
                                         product, 'qty_sold'].skew()

            # Carry forward flag for whether sku is active in Shopify as of the start_date
            if daily_qty_sold_df.loc[daily_qty_sold_df['inventory_id'] ==
                                     product, 'active_sku_fl'].nunique() > 1:
                # Raise exception due to sku being both active & inactive
                raise ValueError("Sku is both active & inactive")
            else:
                active_sku_fl = daily_qty_sold_df.loc[
                    daily_qty_sold_df['inventory_id'] == product,
                    'active_sku_fl'].max()

            logger.info(
                f'EWMA: {ewm_series.iloc[-1]}, active_sku_fl: {active_sku_fl}, kurtosis: {kurtosis}, skew: {skew}'
            )

            # append to current_run_rate_df
            current_run_rate_df = pd.concat([
                current_run_rate_df,
                pd.DataFrame([{
                    'inventory_id': product,
                    'active_fl': active_sku_fl,
                    'run_rate': ewm_series.iloc[-1],
                    'kurtosis': kurtosis,
                    'skew': skew
                }])
            ])

        # Join run rate data with inventory data
        inventory_cols = [
            'inventory_id', 'name', 'total_fulfillable_quantity'
        ]  # column subset to merge with run rate data
        daily_metrics_df = current_run_rate_df.merge(
            shipbob_inventory_details_df[inventory_cols],
            how='left',
            on='inventory_id')

        # Replace null values
        daily_metrics_df['total_fulfillable_quantity'] = daily_metrics_df[
            'total_fulfillable_quantity'].fillna(0)
        daily_metrics_df['name'] = daily_metrics_df['name'].fillna(
            'INVENTORY_ID_NOT_IN_INVENTORY_DETAILS')

        # Calculate estimated days of stock onhand & estimated stockout date
        daily_metrics_df['est_stock_days_on_hand'] = (
            daily_metrics_df['total_fulfillable_quantity'] /
            daily_metrics_df['run_rate']).replace([np.inf, -np.inf],
                                                  np.nan).fillna(0)

        # Set cap of est. days of stock onhand to 365 days (anything greater than that cast to 365)
        daily_metrics_df['est_stock_days_on_hand'] = daily_metrics_df[
            'est_stock_days_on_hand'].apply(lambda x: min(x, 365))

        # Calculate estimated stockout date
        daily_metrics_df['estimated_stockout_date'] = daily_metrics_df[
            'est_stock_days_on_hand'].apply(
                lambda x: pd.to_datetime('today') + timedelta(int(x))
                if not pd.isna(x) else pd.to_datetime('today')  # Handle NaN
            ).fillna(pd.to_datetime('today'))  # Fill NaN with today's date

        # Calculate restock point
        raw_material_max_lead_time = 70  # number of days for the longest raw material lead time (10 weeks as of 10/1/24)
        safety_stock_days = 7  # number of days of stock to keep as a safety stock
        daily_metrics_df['restock_point'] = daily_metrics_df['run_rate'].apply(
            lambda x: (x * raw_material_max_lead_time) +
            (x * safety_stock_days)).astype(int)

        # fig = px.line(daily_qty_sold_df,
        #              x='order_date',
        #              y='qty_sold',
        #               color='inventory_id',
        #              title=f'Daily Qty Sold for {start_date}')

        # # Order the legend alphabetically
        # fig.update_layout(
        #     legend_traceorder='reversed',  # Reverse order to get alphabetical from top to bottom

        # )

        # fig.show()

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
            partition_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')

            s3_prefix = f"shipbob/inventory_run_rate/partition_date={partition_date}/shipbob_inventory_run_rate_{partition_date.replace('-','_')}.csv"

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

        logger.info(
            f'Finished writing daily run rate data to s3 for {start_date}!')

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

        # Increment start date
        start_date = pd.to_datetime(start_date) + timedelta(days=1)


if __name__ == "__main__":

    main()