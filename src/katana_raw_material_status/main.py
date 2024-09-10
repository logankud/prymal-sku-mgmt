from datetime import datetime
import pandas as pd
from loguru import logger
import os
import sys

sys.path.append('src/')  # updating path back to root for importing modules

from utils import *
from models import *


def main():

    logger.info('Running main()')

    # ------------------- CONFIGURE ENV VARIABLES -------------------

    # Configure Athena / Glue
    region = 'us-east-1'
    glue_database = os.getenv('GLUE_DATABASE_NAME')
    s3_bucket = os.getenv('S3_BUCKET_NAME')

    # ====================== QUERY DATA =============================================

    # Configure Athena / Glue
    database = os.getenv('GLUE_DATABASE_NAME')
    s3_bucket = os.getenv('S3_BUCKET_NAME')

    # ------
    #  Katana Raw Material Inventory
    # ------

    # Athena Query to pull latest partition of Katana inventory data
    query = """

    SELECT * 
    , partition_date as in_stock_as_of
    FROM "prymal"."katana_inventory"
    WHERE partition_date = (SELECT MAX(partition_date) FROM "prymal"."katana_inventory")
    
    """

    logger.info(query)

    katana_inventory_df = run_athena_query(query, database, region, s3_bucket)

    # Update data types
    katana_inventory_df['in_stock'] = katana_inventory_df['in_stock'].fillna(0.0).astype(float)

    # ------
    #  Katana Raw Material Inventory
    # ------

    # Athena Query to pull latest partition of Katana inventory data
    query = """
    
    SELECT * 
    , partition_date as planned_qty_as_of
    FROM "prymal"."katana_open_manufacturing_orders"
    WHERE partition_date = (SELECT MAX(partition_date) FROM "prymal"."katana_open_manufacturing_orders")
    
    """

    logger.info(query)

    open_mo_df = run_athena_query(query, database, region, s3_bucket)

    # Update data types
    open_mo_df['planned_quantity_of_ingredient'] = open_mo_df[
        'planned_quantity_of_ingredient'].fillna(0.0).astype(float)

    # # ------------------- CALCULATE TOTAL UPCOMING CONSUMPTION OF RAW MATERIALS -------------------

    rm_qty_planned_by_sku = open_mo_df.groupby(
        [
            'planned_qty_as_of','ingredient_variant_code_sku', 'ingredient_variant',
            'unit_of_measure'
        ],
        as_index=False)['planned_quantity_of_ingredient'].sum()

    # rename columns
    rm_qty_planned_by_sku.columns = [
        'planned_qty_as_of','variant_code_sku', 'variant', 'uom', 'planned_qty'
    ]

    # # ------------------- JOIN RAW MATERIAL PLANNED CONSUMPTION TO KATANA INVENTORY -------------------

    # Join raw material planned consumption to katana inventory
    katana_inventory_df = katana_inventory_df.merge(rm_qty_planned_by_sku,
                                                    on='variant_code_sku',
                                                    how='left')

    # Exclude raw materials not forecasted for consumption (ie. planned qty = 0))
    katana_inventory_df['planned_qty'].fillna(0.0, inplace=True)
    katana_inventory_df = katana_inventory_df.loc[katana_inventory_df['planned_qty']>0].copy()
    

    logger.info(katana_inventory_df.columns)

    # # ------------------- CHECK IF ANY RAW MATERIALS NEED REPLENISHED -------------------

    # 

    # Calculate inventory remaining after planned consumption via open MO's
    katana_inventory_df['inventory_remaining'] = katana_inventory_df[
        'in_stock'] - katana_inventory_df['planned_qty']

    # Calcualte in_stock_percentage 
    katana_inventory_df['in_stock_percentage'] = katana_inventory_df[
        'in_stock'].fillna(0.0) / katana_inventory_df['planned_qty']
    

    # Select relevant columns
    katana_raw_material_status_df = katana_inventory_df[[
        'name', 'units_of_measure', 'in_stock', 'in_stock_as_of','planned_qty',
        'planned_qty_as_of', 'inventory_remaining','in_stock_percentage']].copy()

    # Flag raw material records that need replenished
    katana_raw_material_status_df['needs_replenished'] = False
    katana_raw_material_status_df.loc[
        katana_raw_material_status_df['inventory_remaining'] < 0,
        'needs_replenished'] = True

    # save to csv locally
    katana_raw_material_status_df.to_csv('katana_raw_material_status.csv',index=False)

    # ------------------- VALIDATE DATA - katana_raw_material_status -------------------

    # Validate data w/ Pydantic
    valid_data, invalid_data = validate_dataframe(katana_raw_material_status_df,
                                                 RawMaterialStatus)

    logger.info(f'Total records in formulas_df: {len(katana_raw_material_status_df)}')
    logger.info(f'Total records in valid_data: {len(valid_data)}')
    logger.info(f'Total records in invalid_data: {len(invalid_data)}')

    if len(invalid_data) > 0:
        for invalid in invalid_data:
            logger.error(f'Invalid data: {invalid}')

            raise ValueError(f'Invalid data!')

    if len(valid_data) > 0:

        logger.info(valid_data)
        logger.info(pd.DataFrame(valid_data))



        # ------------------- WRITE TO S3 - katana_raw_material_status -------------------

        # Instantiate s3 client
        s3_client = boto3.client('s3',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # define path to write to
        today = pd.to_datetime(
            pd.to_datetime('today') -
            timedelta(hours=4)).strftime('%Y-%m-%d')
        s3_prefix = f"katana/raw_material_status/partition_date={today}/katana_raw_material_status_{today.replace('-','_')}.csv"

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
    sql_query = """MSCK REPAIR TABLE katana_raw_material_status"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=region)


if __name__ == "__main__":

    main()
