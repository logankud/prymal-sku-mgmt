from utils import *
from models import *

from loguru import logger

import boto3
import os
from botocore.exceptions import ClientError


def main():

  # -----------------
  # Get AWS params from environment variables
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

  # Set default athena query results location within s3 bucket to store query results
  today_y = pd.to_datetime('today').strftime('%Y')
  today_m = pd.to_datetime('today').strftime('%m')
  today_d = pd.to_datetime('today').strftime('%d')
  s3_output = f's3://{s3_bucket}/athena_query_results/year={today_y}/month={today_m}/day={today_d}/'  # Output location for query results (default to today's date as a partition)

  # Read the SQL file
  sql_file_path = 'src/ddl.sql'
  with open(sql_file_path, 'r') as file:
    sql_query = file.read()

  logger.info(f'SQL query: {sql_query}')

  # Inject S3 bucket into SQL
  sql_query = sql_query.replace('S3_BUCKET_NAME', s3_bucket)

  # -----------------
  # Run the Athena query
  # -----------------

  run_athena_query_no_results(query=sql_query,
                              bucket=s3_bucket,
                              database=glue_database,
                              region=region)

  # # -----------------
  # # Create Glue table
  # # -----------------

  # # Convert Pydantic model to Glue schema
  # glue_schema = pydantic_to_glue_schema(ShipbobOrderDetails)

  # logger.info(f'Glue schema: {glue_schema}')

  # # Define your Glue table name and S3 location
  # table_name = 'shipbob_order_details'
  # s3_location = f"s3://{s3_bucket}/shipbob/order_details"

  # # Create the Glue table
  # create_glue_table(region, glue_database, table_name, glue_schema, s3_location)


if __name__ == "__main__":
  main()
