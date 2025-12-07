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
 
):
        self.config_path = config_path
        self.config = self._load_config()
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.database = os.getenv("GLUE_DATABASE_NAME")
        self.region = 'us-east-1'

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


    def _get_run_date(self, date_offset_days=1):
        """Generate run_date with configurable offset"""
        current_ts = datetime.now(pytz.utc)
        run_date = (current_ts - timedelta(days=date_offset_days)).strftime("%Y-%m-%d")
        return run_date, current_ts

    def _execute_sql_file(self, job_dir, sql_file, replacements):
        """Execute a SQL file with variable replacements"""
        sql_path = os.path.join(job_dir, sql_file)

        if not os.path.exists(sql_path):
            logger.warning(f"SQL file not found: {sql_path}, skipping")
            return False

        with open(sql_path, 'r') as f:
            query = f.read()

        # Apply replacements
        for key, value in replacements.items():
            query = query.replace(key, str(value))

        logger.info(f"Executing {sql_file}")
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
        Executes SQL files in a standard order
        """
        config = self._get_table_config(table_name)
        job_dir = config['job_directory']

        # Get dates
        if partition_date:
            run_date = partition_date
            current_ts = datetime.now(pytz.utc)
        else:
            run_date, current_ts = self._get_run_date(
                date_offset_days=config.get('date_offset_days', 1)
            )

        run_id = current_ts.strftime("%Y%m%d%H%M%S")

        logger.info(f"Running job: {table_name}")
        logger.info(f"Run date: {run_date}")
        logger.info(f"Run ID: {run_id}")

        # Standard replacements available to all SQL files
        replacements = {
            "${RUN_DATE}": run_date,
            "${RUN_ID}": run_id,
            "${S3_BUCKET}": self.s3_bucket,
            "${DATABASE}": self.database,
        }

        # Add any custom replacements from config
        if 'custom_replacements' in config:
            replacements.update(config['custom_replacements'])

        # Get workflow steps from config or use defaults
        workflow = config.get('workflow', self._get_default_workflow(config))

        # Execute workflow steps
        for step in workflow:
            step_name = step['name']
            logger.info(f"Step: {step_name}")

            if step['type'] == 'delete_s3':
                # Delete S3 data before creating staging table
                prefix = step['prefix'].format(
                    run_date=run_date,
                    run_id=run_id
                )
                logger.info(f"Deleting S3 data: s3://{self.s3_bucket}/{prefix}")
                delete_s3_data(bucket=self.s3_bucket, prefix=prefix)

            elif step['type'] == 'sql':
                # Execute SQL file
                sql_file = step['file']
                self._execute_sql_file(job_dir, sql_file, replacements)

        logger.info(f"Job completed: {table_name}")

    def _get_default_workflow(self, config):
        """Generate default workflow based on table type"""
        workflow_type = config.get('workflow_type', 'partitioned_staging')

        if workflow_type == 'partitioned_staging':
            # Standard workflow for partitioned tables with staging
            return [
                {'name': 'Create final table', 'type': 'sql', 'file': 'ddl.sql'},
                {'name': 'Delete staging S3 data', 'type': 'delete_s3',
                 'prefix': config.get('staging_prefix', 'staging/{table_name}/partition_date={run_date}/')},
                {'name': 'Create staging table', 'type': 'sql', 'file': 'create_staging.sql'},
                {'name': 'Drop partition if exists', 'type': 'sql', 'file': 'drop_partition_final.sql'},
                {'name': 'Add partition to final table', 'type': 'sql', 'file': 'add_partition_final.sql'},
                {'name': 'Drop staging table', 'type': 'sql', 'file': 'drop_table_staging.sql'},
            ]

        elif workflow_type == 'drop_recreate':
            # Workflow for tables that get dropped and recreated
            return [
                {'name': 'Drop final table', 'type': 'sql', 'file': 'drop.sql'},
                {'name': 'Create final table', 'type': 'sql', 'file': 'ddl.sql'},
                {'name': 'Load data', 'type': 'sql', 'file': 'load.sql'},
            ]

        else:
            raise ValueError(f"Unknown workflow_type: {workflow_type}")

    def list_tables(self):
        """List all configured tables"""
        return list(self.config['tables'].keys())