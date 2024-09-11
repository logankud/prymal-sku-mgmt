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
    alert_topic_arn = os.getenv('ALERT_TOPIC_ARN')

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
    FROM "prymal"."katana_raw_material_status"
    WHERE partition_date = (SELECT MAX(partition_date) FROM "prymal"."katana_raw_material_status")
    
    """

    logger.info(query)

    katana_raw_material_status_df = run_athena_query(query, database, region, s3_bucket)

    # Update data types
    katana_raw_material_status_df['planned_qty_as_of'] = pd.to_datetime(katana_raw_material_status_df['planned_qty_as_of']).dt.strftime('%Y-%m-%d')
    katana_raw_material_status_df['needs_replenished'] = katana_raw_material_status_df['needs_replenished'].map({'true': True, 'false': False})

    logger.info(katana_raw_material_status_df.columns)
    logger.info(katana_raw_material_status_df.head())

    

    # # ------
    # #  Katana Raw Material Inventory
    # # ------

    # # Athena Query to pull latest partition of Katana inventory data
    # query = """
    
    # SELECT * 
    # , partition_date as planned_qty_as_of
    # FROM "prymal"."katana_open_manufacturing_orders"
    # WHERE partition_date = (SELECT MAX(partition_date) FROM "prymal"."katana_open_manufacturing_orders")
    
    # """

    # logger.info(query)

    # open_mo_df = run_athena_query(query, database, region, s3_bucket)

    # # Update data types
    # open_mo_df['planned_quantity_of_ingredient'] = open_mo_df[
    #     'planned_quantity_of_ingredient'].fillna(0.0).astype(float)

    
    # # ------------------- CHECK IF ANY RAW MATERIALS NEED REPLENISHED -------------------

    # Filter to only include raw materials that need replenished 
    raw_materials_needing_replinished = katana_raw_material_status_df.loc[katana_raw_material_status_df['needs_replenished'] == True]

    # Make a list of products to replenish
    products_to_replenish = raw_materials_needing_replinished['name'].unique().tolist()

    # Format for alerting to SNS topic w/ SNS client
    alert_message = f"""[Katana Raw Material Status Alert] \n 
    
    The following products needs replenished to fulfill upcoming open MO's as of {raw_materials_needing_replinished['planned_qty_as_of'].max()} \n\n
    
    {products_to_replenish} 
    
    """

    logger.info(alert_message)

    # ------------------- SEND SNS ALERT -------------------

    alert_subject = 'Katana Raw Material Status Alert'
    
    send_sns_alert(message=alert_message, 
                   topic_arn=alert_topic_arn, 
                   subject=alert_subject, 
                  region=region)
    
            

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



    #     # ------------------- WRITE TO S3 - katana_raw_material_status -------------------

    #     # Instantiate s3 client
    #     s3_client = boto3.client('s3',
    #                              region_name=region,
    #                              aws_access_key_id=AWS_ACCESS_KEY_ID,
    #                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    #     # define path to write to
    #     today = pd.to_datetime(
    #         pd.to_datetime('today') -
    #         timedelta(hours=4)).strftime('%Y-%m-%d')
    #     s3_prefix = f"katana/raw_material_status/partition_date={today}/katana_raw_material_status_{today.replace('-','_')}.csv"

    #     try:
    #         # Write to s3
    #         write_df_to_s3(bucket=s3_bucket,
    #                        key=s3_prefix,
    #                        df=pd.DataFrame(valid_data),
    #                        s3_client=s3_client)

    #     except Exception as e:
    #         logger.error(f'Error writing to s3: {str(e)}')
    #         raise ValueError(f'Error writing data! {str(e)}')

    # logger.info(f'Finished validating data & writing to s3!')

    # # -----------------
    # # Run Athena query to update partitions - katana_open_manufacturing_orders
    # # -----------------
    # logger.info('Running MKSCK REPAIR TABLE to update partitions')

    # # Define SQL query
    # sql_query = """MSCK REPAIR TABLE katana_raw_material_status"""

    # logger.info(f'SQL query: {sql_query}')

    # run_athena_query_no_results(query=sql_query,
    #                             bucket=s3_bucket,
    #                             database=glue_database,
    #                             region=region)


if __name__ == "__main__":

    main()
