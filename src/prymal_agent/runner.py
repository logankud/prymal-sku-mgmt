#!/usr/bin/env python3
"""
Standardized Table Manager for Prymal Agent
Handles all common logic that was previously in individual main.py files
"""

import os
import sys
import yaml
from datetime import datetime, timedelta
from loguru import logger
import pytz
from pydantic import BaseModel

# Add the src/ directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.dirname(os.path.dirname(script_dir))
sys.path.append(os.path.join(workspace_root, 'src'))

from utils import run_athena_query_no_results, delete_s3_data

class JobConfig(BaseModel):
    table_description: str
    

class JobRunner:
    """Manages job for populating Prymal Agent tables (prymal_agent database) once daily using standardized workflows for pulling data out of other databases (prymal)"""

    def __init__(self, config_path='src/prymal_agent/config.yml',
partition_date=None
 
):
        self.config_path = config_path
        self.config = self._load_config()
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.database = os.getenv("GLUE_DATABASE_NAME")
        self.region = 'us-east-1'
        self.run_date = self._load_run_date(partition_date)

    def _load_config(self):
        """Load and validate configuration from YAML file"""
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Attempt to validate using the JobConfig model
        try:
            validated_config = JobConfig(**config_data)
            return validated_config
        except Exception as e:
            raise ValueError("Configuration validation error: " + str(e))


    def _load_run_date(self, partition_date):
        """Load run_date based on provided partition_date"""
    
        if partition_date:
            run_date = (partition_date).strftime("%Y-%m-%d")
        else:
            current_ts = datetime.now(pytz.utc)
            run_date = (current_ts - timedelta(hours=24)).strftime("%Y-%m-%d")
            
        return run_date

    def _prepare_colummns(self):
        """Format columns from config to use in Athena DDL"""
        columns = self.config.columns
        athena_columns = []
        for column in columns:
            column_name = column['name']
            column_type = column['type']
            column_comment = column.get('comment', '')
            athena_col = f"{column_name} {column_type} COMMENT '{column_comment}'"
            athena_columns.append(athena_col)
        return ',\n'.join(athena_columns)
    

    def _populate_sql_template(self, sql_file_path):
        """
        Read a SQL template from the given path, replace the variables (${var} format), return the populated query as a string.
        """

        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"SQL template not found: {sql_file_path}")

        # Read the template file
        with open(sql_file_path, 'r') as f:
            query_template = f.read()

        # Prepare variable replacements
        replacements = {
            "${S3_BUCKET}": self.s3_bucket,
            "${DATABASE}": self.database,
            "${COLUMNS}": self._prepare_colummns(),
            "${TABLE_NAME}": self.config.table_name,
            "${TABLE_DESCRIPTION}": self.config.table_description,
            "${RUN_DATE}": self.run_date
        }

        # Replace variables in the query
        for var, replacement in replacements.items():
            query_template = query_template.replace(var, replacement)

        return query_template


    def _execute_query(self, query):
        """Execute a SQL query (str)"""
        
        logger.info(f"Executing {query}")

        run_athena_query_no_results(
            bucket=self.s3_bucket,
            query=query,
            database=self.database,
            region=self.region
        )
        return True

    def run_job(self, table_name, partition_date=None):
        """
        Run a standardized job workflow for a table
        This will generate the DDL and execute the query
        """
        config = self._get_table_config(table_name)

        # Get the SQL query for the table
        ddl = self._populate_sql_template('path/to/ddl.sql')

        # Execute the generated SQL query
        self._execute_query(sql_query)

        logger.info(f"Job completed: {table_name}")

    def list_tables(self):
        """List all configured tables"""
        return list(self.config['tables'].keys())