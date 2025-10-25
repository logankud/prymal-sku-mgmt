from datetime import date, timedelta
import os
import sys
from loguru import logger
from datetime import datetime

# Add the src/ directory to path using absolute path
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
sys.path.append(os.path.join(workspace_root, 'src'))

from utils import run_athena_query_no_results


current_ts = datetime.now()
run_date = (current_ts - timedelta(hours=24) - timedelta(days=1)).strftime("%Y-%m-%d")

logger.info(f'Run date: {run_date}')

# -----------------------------------------------
# Create final table

# Read DDL query
with open("ddl.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date)

logger.info('Creating final table (DDL)')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Create staging table

# Read staging query
with open("create_staging.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date).replace("${RUN_ID}", current_ts.strftime("%Y%m%d%H%M%S"))

logger.info('Creating staging table')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Drop partition if exists in the final table

# Read insert query
with open("drop_partition_final.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date)

logger.info('Drop partition if exists in the final table')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Add partition to the final table (pointing to the staging table)

# Read insert query
with open("add_partition_final.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date).replace("${RUN_ID}", current_ts.strftime("%Y%m%d%H%M%S"))

logger.info('Add partition to the final table (pointing to the staging table)')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Drop staging table

# Read insert query
with open("drop_table_staging.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date)

logger.info('Drop staging table')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')
