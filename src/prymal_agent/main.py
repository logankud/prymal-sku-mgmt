#!/usr/bin/env python3
"""
Runner for Prymal Agent table management jobs
Executes standardized workflows based on SQL files
"""

import argparse
import logging
from datetime import datetime, timedelta
import pytz
import os
from runner import JobRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description=
        'Manage Prymal Agent Athena tables with standardized workflows')

    parser.add_argument(
        '--job_dir',
        type=str,
        required=True,
        help='Path to job directory containing config.yml (use this OR --table)'
    )

    parser.add_argument(
        '--partition_date',
        type=str,
        required=False,
        help=
        'Optional: partition date in YYYY-MM-DD format (defaults yesterday)')

    args = parser.parse_args()

    logger.info(f"Starting job with args: {args}")

    # Initialize job runner (pass partition_date so self.run_date is set correctly)
    runner = JobRunner(job_dir=args.job_dir, partition_date=args.partition_date)

    # Run the job
    runner.run_job(partition_date=args.partition_date)


if __name__ == '__main__':
    main()
