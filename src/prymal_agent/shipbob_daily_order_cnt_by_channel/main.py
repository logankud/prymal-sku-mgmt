from datetime import date, timedelta
import os
import sys

# Add the src/ directory to path using absolute path
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
sys.path.append(os.path.join(workspace_root, 'src'))

from utils import run_athena_query_no_results

run_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# -----------------------------------------------
# Create staging table

# Read staging query
with open("create_staging.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date)

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Drop partition if exists in the final table

# Read insert query
with open("drop_partition_final.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date)

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Add partition to the final table (pointing to the staging table)

# Read insert query
with open("add_partition_final.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date)

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')

# -----------------------------------------------
# Drop staging table

# Read insert query
with open("drop_table_staging.sql") as f:
  QUERY = f.read().replace("${RUN_DATE}", run_date)

run_athena_query_no_results(bucket=os.getenv("S3_BUCKET_NAME"),
                            query=QUERY,
                            database=os.getenv("GLUE_DATABASE_NAME"),
                            region='us-east-1')
