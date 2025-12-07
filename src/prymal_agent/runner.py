
#!/usr/bin/env python3
"""
Runner for Prymal Agent table management jobs
Executes table creation and loading based on configuration
"""

import argparse
import logging
from datetime import datetime, timedelta
import pytz
from table_manager import AthenaTableManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Manage Prymal Agent Athena tables'
    )
    
    parser.add_argument(
        '--action',
        type=str,
        required=True,
        choices=['create', 'drop', 'load', 'list'],
        help='Action to perform: create, drop, load, or list tables'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        required=False,
        help='Name of the table to operate on'
    )
    
    parser.add_argument(
        '--partition_date',
        type=str,
        required=False,
        help='Partition date in YYYY-MM-DD format (for load action)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='src/prymal_agent/config.yml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    # Initialize table manager
    manager = AthenaTableManager(config_path=args.config)
    
    # Execute action
    if args.action == 'list':
        tables = manager.list_tables()
        logger.info("Configured tables:")
        for table in tables:
            logger.info(f"  - {table}")
    
    elif args.action == 'create':
        if not args.table:
            logger.error("--table argument is required for create action")
            return
        manager.create_table(args.table)
        logger.info(f"Successfully created table: {args.table}")
    
    elif args.action == 'drop':
        if not args.table:
            logger.error("--table argument is required for drop action")
            return
        manager.drop_table(args.table)
        logger.info(f"Successfully dropped table: {args.table}")
    
    elif args.action == 'load':
        if not args.table:
            logger.error("--table argument is required for load action")
            return
        
        # Use provided partition date or default to yesterday
        if args.partition_date:
            partition_date = args.partition_date
        else:
            yesterday = datetime.now(pytz.utc) - timedelta(days=1)
            partition_date = yesterday.strftime('%Y-%m-%d')
        
        logger.info(f"Loading partition: {partition_date}")
        manager.load_partition(args.table, partition_date)
        logger.info(f"Successfully loaded partition {partition_date} for table: {args.table}")


if __name__ == '__main__':
    main()
