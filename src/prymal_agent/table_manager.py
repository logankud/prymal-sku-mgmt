
import os
import logging
import boto3
import yaml
from typing import Dict, List, Optional
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AthenaTableManager:
    """Manages Athena table creation, loading, and partitioning"""
    
    def __init__(self, config_path: str = "src/prymal_agent/config.yml"):
        self.config_path = config_path
        self.config = self._load_config()
        
        # AWS configuration
        self.region = 'us-east-1'
        self.glue_database = os.getenv('GLUE_DATABASE_NAME')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        
        if not self.glue_database or not self.s3_bucket:
            raise ValueError("GLUE_DATABASE_NAME and S3_BUCKET_NAME environment variables must be set")
        
        self.athena_client = boto3.client('athena', region_name=self.region)
        self.s3_output_location = f"s3://{self.s3_bucket}/athena-results/"
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _execute_athena_query(self, query: str, wait: bool = True) -> Optional[str]:
        """Execute an Athena query and optionally wait for completion"""
        logger.info(f"Executing query:\n{query}")
        
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.glue_database},
            ResultConfiguration={'OutputLocation': self.s3_output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        if wait:
            self._wait_for_query_completion(query_execution_id)
        
        return query_execution_id
    
    def _wait_for_query_completion(self, query_execution_id: str, max_wait: int = 300):
        """Wait for query to complete"""
        start_time = time.time()
        
        while True:
            if time.time() - start_time > max_wait:
                raise TimeoutError(f"Query {query_execution_id} exceeded max wait time")
            
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                logger.info(f"Query {query_execution_id} succeeded")
                return
            elif status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise RuntimeError(f"Query {query_execution_id} failed: {reason}")
            
            time.sleep(2)
    
    def _generate_ddl(self, table_name: str, table_config: Dict) -> str:
        """Generate CREATE TABLE DDL from configuration"""
        columns = table_config['columns']
        partition_column = table_config.get('partition_column')
        partition_type = table_config.get('partition_type', 'date')
        s3_path = table_config['s3_path']
        
        # Build column definitions
        col_definitions = []
        for col in columns:
            col_definitions.append(f"    {col['name']} {col['type']}")
        
        ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS {self.glue_database}.{table_name} (\n"
        ddl += ",\n".join(col_definitions)
        ddl += "\n)"
        
        # Add partition if configured
        if table_config.get('is_partitioned', False) and partition_column:
            ddl += f"\nPARTITIONED BY (\n    {partition_column} {partition_type}\n)"
        
        ddl += "\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY ','"
        ddl += f"\nSTORED AS TEXTFILE\nLOCATION 's3://{self.s3_bucket}/{s3_path}/'"
        ddl += "\nTBLPROPERTIES ('skip.header.line.count'='1');"
        
        return ddl
    
    def _generate_staging_ddl(self, table_name: str, table_config: Dict) -> str:
        """Generate staging table DDL (temporary, non-partitioned version)"""
        staging_config = table_config.copy()
        staging_config['is_partitioned'] = False
        staging_config['s3_path'] = f"{table_config['s3_path']}_staging"
        
        return self._generate_ddl(f"{table_name}_staging", staging_config)
    
    def create_table(self, table_name: str) -> None:
        """Create a table based on configuration"""
        if table_name not in self.config['tables']:
            raise ValueError(f"Table {table_name} not found in configuration")
        
        table_config = self.config['tables'][table_name]
        logger.info(f"Creating table: {table_name}")
        
        # Create main table
        ddl = self._generate_ddl(table_name, table_config)
        self._execute_athena_query(ddl)
        
        # Create staging table if configured
        if table_config.get('use_staging', False):
            logger.info(f"Creating staging table: {table_name}_staging")
            staging_ddl = self._generate_staging_ddl(table_name, table_config)
            self._execute_athena_query(staging_ddl)
    
    def drop_table(self, table_name: str, drop_staging: bool = True) -> None:
        """Drop a table and optionally its staging table"""
        logger.info(f"Dropping table: {table_name}")
        drop_query = f"DROP TABLE IF EXISTS {self.glue_database}.{table_name}"
        self._execute_athena_query(drop_query)
        
        if drop_staging:
            logger.info(f"Dropping staging table: {table_name}_staging")
            drop_staging_query = f"DROP TABLE IF EXISTS {self.glue_database}.{table_name}_staging"
            self._execute_athena_query(drop_staging_query)
    
    def load_partition(self, table_name: str, partition_date: str) -> None:
        """Load data into a partition using staging table pattern"""
        if table_name not in self.config['tables']:
            raise ValueError(f"Table {table_name} not found in configuration")
        
        table_config = self.config['tables'][table_name]
        
        if not table_config.get('use_staging', False):
            raise ValueError(f"Table {table_name} does not use staging pattern")
        
        if not table_config.get('source_query'):
            raise ValueError(f"Table {table_name} does not have a source_query defined")
        
        logger.info(f"Loading partition {partition_date} for table {table_name}")
        
        # Step 1: Drop staging table
        drop_staging = f"DROP TABLE IF EXISTS {self.glue_database}.{table_name}_staging"
        self._execute_athena_query(drop_staging)
        
        # Step 2: Recreate staging table
        staging_ddl = self._generate_staging_ddl(table_name, table_config)
        self._execute_athena_query(staging_ddl)
        
        # Step 3: Load data into staging
        source_query = table_config['source_query']
        insert_query = f"""
        INSERT INTO {self.glue_database}.{table_name}_staging
        {source_query}
        """
        self._execute_athena_query(insert_query)
        
        # Step 4: Drop existing partition in final table
        partition_column = table_config.get('partition_column', 'partition_date')
        drop_partition = f"""
        ALTER TABLE {self.glue_database}.{table_name}
        DROP IF EXISTS PARTITION ({partition_column}='{partition_date}')
        """
        self._execute_athena_query(drop_partition)
        
        # Step 5: Add partition from staging data
        s3_staging_path = f"s3://{self.s3_bucket}/{table_config['s3_path']}_staging/"
        add_partition = f"""
        ALTER TABLE {self.glue_database}.{table_name}
        ADD PARTITION ({partition_column}='{partition_date}')
        LOCATION '{s3_staging_path}'
        """
        self._execute_athena_query(add_partition)
        
        logger.info(f"Successfully loaded partition {partition_date} for {table_name}")
    
    def list_tables(self) -> List[str]:
        """List all configured tables"""
        return list(self.config['tables'].keys())
