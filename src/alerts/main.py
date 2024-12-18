from datetime import datetime
import pandas as pd
from loguru import logger
import os
import sys
import yaml

sys.path.append('src/')  # updating path back to root for importing modules

from utils import *
from models import *


def main():

    logger.info('Running main()')

    logger.info(os.getcwd())
    logger.info(os.listdir())

    # ------------------- CONFIGURE ENV VARIABLES -------------------

    # Configure Athena / Glue
    region = 'us-east-1'
    glue_database = os.getenv('GLUE_DATABASE_NAME')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    alert_topic_arn = os.getenv('ALERT_TOPIC_ARN')

    # read in config yaml & parse variable values
    with open('src/alerts/config.yml', 'r') as file:
        config = yaml.safe_load(file)

    restock_point_in_days_finished_goods = config[
        'restock_point_in_days_finished_goods']  # no. of days to replenish for
    alert_point_in_days_finished_goods = config[
        'alert_point_in_days_finished_goods']  # no. of days of inventory remaining before alerting

    # ====================== QUERY DATA - Katana Raw Material Status =============================================

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

    katana_raw_material_status_df = run_athena_query(query, database, region,
                                                     s3_bucket)

    # Update data types
    katana_raw_material_status_df['planned_qty_as_of'] = pd.to_datetime(
        katana_raw_material_status_df['planned_qty_as_of']).dt.strftime(
            '%Y-%m-%d')
    katana_raw_material_status_df[
        'needs_replenished'] = katana_raw_material_status_df[
            'needs_replenished'].map({
                'true': True,
                'false': False
            })

    logger.info(katana_raw_material_status_df.columns)
    logger.info(katana_raw_material_status_df.head())

    # # ------------------- CHECK IF ANY RAW MATERIALS NEED REPLENISHED -------------------

    # Filter to only include raw materials that need replenished
    raw_materials_needing_replinished = katana_raw_material_status_df.loc[
        katana_raw_material_status_df['needs_replenished'] == True]

    # Make a list of products to replenish
    products_to_replenish = raw_materials_needing_replinished['name'].unique(
    ).tolist()

    # Format for alerting to SNS topic w/ SNS client
    alert_message = f"""[Katana Raw Material Status Alert] \n 
    
    The following products needs replenished to fulfill upcoming open MO's as of {raw_materials_needing_replinished['planned_qty_as_of'].max()} \n\n
    
    
    """

    # Iterate through records where
    for _, row in raw_materials_needing_replinished.iterrows():
        alert_message += f"• {row['name']}:\n"
        alert_message += f"  - Current stock: {row['in_stock']} {row['units_of_measure']}\n"
        alert_message += f"  - Minimum Quantity Required to Meet Upcoming MO's as of {pd.to_datetime(row['in_stock_as_of']).strftime('%Y-%m-%d')}: {-int(round(float(row['inventory_remaining']),0))} {row['units_of_measure']}\n\n"

    logger.info(alert_message)

    logger.info(alert_message)

    # ------------------- SEND SNS ALERT -------------------

    alert_subject = 'Katana Raw Material Status Alert'

    send_sns_alert(message=alert_message,
                   topic_arn=alert_topic_arn,
                   subject=alert_subject,
                   region=region)

    # ====================== QUERY DATA - Shipbob Inventory Run Rate =============================================

    # ------
    #  Shipbop Inventory Run Rate
    # ------

    # Athena Query to pull latest partition of Katana inventory data
    query = f"""
    
    SELECT *  
    FROM "prymal"."shipbob_inventory_run_rate"
    WHERE partition_date = (SELECT MAX(partition_date) FROM "prymal"."shipbob_inventory_run_rate")   -- latest partition
    AND est_stock_days_on_hand < {alert_point_in_days_finished_goods}   -- only include products with < X days of stock on hand
    
    """

    logger.info(query)

    shipbob_inventory_run_rate_df = run_athena_query(query, database, region,
                                                     s3_bucket)

    # Update data types
    shipbob_inventory_run_rate_df['partition_date'] = pd.to_datetime(
        shipbob_inventory_run_rate_df['partition_date']).dt.strftime(
            '%Y-%m-%d')
    shipbob_inventory_run_rate_df['run_rate'] = shipbob_inventory_run_rate_df[
        'run_rate'].astype(float)
    shipbob_inventory_run_rate_df[
        'est_stock_days_on_hand'] = shipbob_inventory_run_rate_df[
            'est_stock_days_on_hand'].astype(float)
    shipbob_inventory_run_rate_df[
        'total_fulfillable_quantity'] = shipbob_inventory_run_rate_df[
            'total_fulfillable_quantity'].astype(int)
    shipbob_inventory_run_rate_df[
        'restock_point'] = shipbob_inventory_run_rate_df[
            'restock_point'].astype(int)

    logger.info(shipbob_inventory_run_rate_df.columns)
    logger.info(shipbob_inventory_run_rate_df.head())

    # # ------------------- CHECK FOR FINISHED GOODS NEEDING REPLENISHED -------------------

    # Calcualte restock_amount (to replenish XX days of inventory based on run rate & stock on hand)
    shipbob_inventory_run_rate_df['restock_amount'] = (
        shipbob_inventory_run_rate_df['run_rate'] *
        shipbob_inventory_run_rate_df['est_stock_days_on_hand'].apply(
            lambda x: int(restock_point_in_days_finished_goods - x))
    ).astype(int)

    # Filter to only include finished goods that need replenished
    products_needing_restocked = shipbob_inventory_run_rate_df.loc[
        shipbob_inventory_run_rate_df['restock_amount'] > 0].copy()

    # If there are products needing replenished, format for alerting to SNS topic w/ SNS client
    if len(products_needing_restocked) > 0:

        # # ------------------- SEND SNS ALERT -------------------

        # Format for alerting to SNS topic w/ SNS client
        alert_message = "URGENT: Product Replenishment Required\n\n"
        alert_message += "The following products need to be replenished:\n\n"

        # Iterate through records where
        for _, row in products_needing_restocked.iterrows():
            alert_message += f"• {row['name']}:\n"
            alert_message += f"  - Current stock: {row['total_fulfillable_quantity']}\n"
            alert_message += f"  - Estimated stockout date: {row['estimated_stockout_date']}\n"
            alert_message += f"  - Quantity to restock: {int(row['run_rate'] * (restock_point_in_days_finished_goods - row['est_stock_days_on_hand']))}\n\n"

        logger.info(alert_message)

        # ------------------- SEND SNS ALERT -------------------

        alert_subject = 'Finished Products Needing Replenished'

        send_sns_alert(message=alert_message,
                       topic_arn=alert_topic_arn,
                       subject=alert_subject,
                       region=region)


if __name__ == "__main__":

    main()
