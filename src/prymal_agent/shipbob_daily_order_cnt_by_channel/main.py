from datetime import date, timedelta
from utils import run_athena_query_no_results
import os

run_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


# -----------------------------------------------
# STAGE data for loading into final table

# Read staging query
with open("stage.sql") as f:
    QUERY = f.read().replace("${RUN_DATE}", run_date)

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"), 
                            query=QUERY, 
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')


# -----------------------------------------------
# LOAD data from staging -> final table

# Read insert query
with open("load.sql") as f:
    QUERY = f.read().replace("${RUN_DATE}", run_date)

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"), 
                            query=QUERY, 
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')
