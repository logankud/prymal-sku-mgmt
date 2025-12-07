#!/usr/bin/env python3
"""
Runner for Prymal Agent table management jobs
Executes standardized workflows based on SQL files
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
        description='Manage Prymal Agent Athena tables with standardized workflows'
    )

    parser.add_argument(
        '--table',
        type=str,
        required=True,
        help='Name of the table to run'
    )

    parser.add_argument(
        '--partition_date',
        type=str,
        required=False,
        help='Optional: partition date in YYYY-MM-DD format (defaults based on table config)'
    )

    parser.add_argument(
        '--config',
        type=str,
        default='src/prymal_agent/config.yml',
        help='Path to configuration file'
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List all configured tables'
    )

    args = parser.parse_args()

    # Initialize table manager
    manager = AthenaTableManager(config_path=args.config)

    # List tables if requested
    if args.list:
        tables = manager.list_tables()
        logger.info("Configured tables:")
        for table in tables:
            logger.info(f"  - {table}")
        return

    # Run the job
    manager.run_job(args.table, partition_date=args.partition_date)
    logger.info(f"Successfully completed job: {args.table}")


if __name__ == '__main__':
    main()