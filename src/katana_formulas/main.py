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
        '--path_recipes',
        type=str,
        required=False,
        default='src/katana_formulas/data/recipes.csv',
        help=
        'Relative path to the recipes export file from Katana'
    )

    parser.add_argument(
        '--path_inventory',
        type=str,
        required=False,
        default='src/katana_formulas/data/inventory.csv',
        help=
        'Relative path to the inventory export file from Katana'
        
    )

    # Parse input args
    args = parser.parse_args()
    logger.info(f'Args: {args}')

    path_recipes = args.path_recipes
    path_inventory = args.path_inventory
    
    # ------------------- CONFIGURE ENV VARIABLES -------------------
    
    # Configure Athena / Glue
    region = 'us-east-1'
    glue_database = os.getenv('GLUE_DATABASE_NAME')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    
    # ------------------- FORMAT CSV - katana_formulas -------------------

    logger.info(os.getcwd())
    formulas_df = pd.read_csv(path_recipes)

    # Apply function to clean all column names
    formulas_df.columns = ['product_variant_code', 'product_variant_name',
                              'product_supplier_item_code', 'product_internal_barcode',
                              'product_registered_barcode', 'ingredient_variant_code_sku_required',
                              'ingredient_variant_name', 'ingredient_supplier_item_code',
                              'ingredient_internal_barcode', 'ingredient_registered_barcode', 'notes',
                              'quantity_required', 'unit_of_measure','current_stock_price']

    logger.info(formulas_df.columns)
    logger.info(formulas_df.head())
    

    formulas_df.head(10).to_csv('formulas_df.csv',index=False)
    

    # ------------------- VALIDATE DATA - katana_formulas -------------------

    # Validate data w/ Pydantic
    valid_data, invalid_data = validate_dataframe(formulas_df,
                                                 KatanaRecipeIngredient)

    logger.info(f'Total records in formulas_df: {len(formulas_df)}')
    logger.info(f'Total records in valid_data: {len(valid_data)}')
    logger.info(f'Total records in invalid_data: {len(invalid_data)}')

    if len(invalid_data) > 0:
        for invalid in invalid_data:
            logger.error(f'Invalid data: {invalid}')

            raise ValueError(f'Invalid data!')

    if len(valid_data) > 0:

        logger.info(valid_data)
        logger.info(pd.DataFrame(valid_data))


        # ------------------- WRITE TO S3 - katana_formulas -------------------

        # Instantiate s3 client
        s3_client = boto3.client('s3',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        # define path to write to
        today = pd.to_datetime(
            pd.to_datetime('today') -
            timedelta(hours=4)).strftime('%Y-%m-%d')
        s3_prefix = f"katana/formulas/partition_date={today}/katana_formulas_{today.replace('-','_')}.csv"

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
    # Run Athena query to update partitions - katana_formulas
    # -----------------
    logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # Define SQL query
    sql_query = """MSCK REPAIR TABLE katana_formulas"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=region)


    # ------------------- FORMAT CSV - katana_inventory -------------------

    logger.info(os.getcwd())
    inventory_df = pd.read_csv(path_inventory)

    # Apply function to clean all column names
    # inventory_df.columns = ['product_variant_code', 'product_variant_name',
    #                           'product_supplier_item_code', 'product_internal_barcode',
    #                           'product_registered_barcode', 'ingredient_variant_code_sku_required',
    #                           'ingredient_variant_name', 'ingredient_supplier_item_code',
    #                           'ingredient_internal_barcode', 'ingredient_registered_barcode', 'notes',
    #                           'quantity_required', 'unit_of_measure','current_stock_price']

    # function to clean column names
    inventory_df.columns = [clean_column_name(col) for col in inventory_df.columns]

    logger.info(inventory_df.columns)
    logger.info(inventory_df.dtypes)
    logger.info(inventory_df.head())


    inventory_df.head(10).to_csv('inventory_df.csv',index=False)


    # ------------------- VALIDATE DATA - katana_inventory -------------------

    # Validate data w/ Pydantic
    valid_data, invalid_data = validate_dataframe(inventory_df,
                                                 KatanaInventory)

    logger.info(f'Total records in inventory_df: {len(inventory_df)}')
    logger.info(f'Total records in valid_data: {len(valid_data)}')
    logger.info(f'Total records in invalid_data: {len(invalid_data)}')

    if len(invalid_data) > 0:
        for invalid in invalid_data:
            logger.error(f'Invalid data: {invalid}')

            raise ValueError(f'Invalid data!')

    if len(valid_data) > 0:

        logger.info(valid_data)
        logger.info(pd.DataFrame(valid_data))


        # ------------------- WRITE TO S3 - katana_formulas -------------------

        # Instantiate s3 client
        s3_client = boto3.client('s3',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # define path to write to
        today = pd.to_datetime(
            pd.to_datetime('today') -
            timedelta(hours=4)).strftime('%Y-%m-%d')
        s3_prefix = f"katana/inventory/partition_date={today}/katana_inventory_{today.replace('-','_')}.csv"

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
    # Run Athena query to update partitions - katana_inventory
    # -----------------
    logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # Define SQL query
    sql_query = """MSCK REPAIR TABLE katana_inventory"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=region)


if __name__ == "__main__":

    main()


    