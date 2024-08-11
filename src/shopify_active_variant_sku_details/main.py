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
    
    # Shopify API credentials
    shopify_api_key = os.getenv('SHOPIFY_API_KEY')
    shopify_api_pw = os.getenv('SHOPIFY_API_PW')
    shopify_store_url = os.getenv('SHOPIFY_STORE_URL')
    
    # ------------------- LIST ACTIVE SKUS -------------------
    
    active_variant_sku_dict = list_active_shopify_variant_skus(shopify_api_key, shopify_api_pw,shopify_store_url)

    # Convert to pd dataframe
    active_variant_sku_df = pd.DataFrame(active_variant_sku_dict)

    active_variant_sku_df.to_csv('products_with_active_variants.csv',
                                  index=False,
                                  quotechar='"',
                                  quoting=csv.QUOTE_NONNUMERIC,
                                  sep=',', 
                                  encoding='utf-8')

    # ------------------- VALIDATE DATA -------------------


    # Validate data w/ Pydantic
    valid_data, invalid_data = validate_dataframe(active_variant_sku_df,
                                                 ShopifyProductVariantDetails)

    logger.info(f'Total records in df: {len(active_variant_sku_df)}')
    logger.info(f'Total records in valid_data: {len(valid_data)}')
    logger.info(f'Total records in invalid_data: {len(invalid_data)}')

    if len(invalid_data) > 0:
        for invalid in invalid_data:
            logger.error(f'Invalid data: {invalid}')

            raise ValueError(f'Invalid data!')

    if len(valid_data) > 0:

        logger.info(valid_data)
        logger.info(pd.DataFrame(valid_data))


        # ------------------- WRITE TO S3 -------------------

        # Instantiate s3 client
        s3_client = boto3.client('s3',
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        # define path to write to
        today = pd.to_datetime(
            pd.to_datetime('today') -
            timedelta(hours=4)).strftime('%Y-%m-%d')
        s3_prefix = f"shopify/active_variant_sku_details/partition_date={today}/shopify_active_variant_sku_details_{today.replace('-','_')}.csv"

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
    # Run Athena query to update partitions
    # -----------------
    logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # Define SQL query
    sql_query = """MSCK REPAIR TABLE shopify_active_variant_sku_details"""

    logger.info(f'SQL query: {sql_query}')

    run_athena_query_no_results(query=sql_query,
                                bucket=s3_bucket,
                                database=glue_database,
                                region=region)


if __name__ == "__main__":

    main()


    