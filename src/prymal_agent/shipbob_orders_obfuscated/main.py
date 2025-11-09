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
run_date = (current_ts - timedelta(hours=24) -
            timedelta(days=1)).strftime("%Y-%m-%d")

logger.info(f'Run date: {run_date}')

# -----------------------------------------------
# Drop final table

# Read DDL query
with open("drop.sql") as f:
  QUERY = f.read()

logger.info('Dropping table to recreate')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Create final table

# Read DDL query
with open("ddl.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", current_ts.strftime("%Y-%m-%d"))

logger.info('Creating final table (DDL)')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Load table

# Read staging query
with open("load.sql") as f:
  QUERY = f.read()

logger.info('Loading data into table')

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')
