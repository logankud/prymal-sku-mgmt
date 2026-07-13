#!/usr/bin/env python3
"""
Detects missing partition dates for an Athena table within a lookback window.
Prints missing dates to stdout (one per line) for shell scripts to consume.

Usage:
  python3 src/gap_detector.py --table shipbob_order_details --date_col order_date
  python3 src/gap_detector.py --table shipbob_inventory_run_rate --lookback_days 14
  python3 src/gap_detector.py --table shipbob_current_inventory --database prymal_agent
"""
import argparse
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.append('src/')
from utils import run_athena_query


def get_missing_dates(table, date_col, database, region, bucket, lookback_days):
    yesterday = date.today() - timedelta(days=1)
    cutoff = yesterday - timedelta(days=lookback_days - 1)

    if table == 'shopify_orders':
        date_expr = "DATE(CONCAT(year, '-', month, '-', day))"
        query = f"""
        SELECT CAST({date_expr} AS VARCHAR) as dt
        FROM {table}
        WHERE {date_expr} >= DATE('{cutoff}')
          AND {date_expr} <= DATE('{yesterday}')
        GROUP BY {date_expr}
        ORDER BY {date_expr}
        """
    else:
        query = f"""
        SELECT CAST({date_col} AS VARCHAR) as dt
        FROM {table}
        WHERE {date_col} >= DATE('{cutoff}')
          AND {date_col} <= DATE('{yesterday}')
        GROUP BY {date_col}
        ORDER BY {date_col}
        """

    df = run_athena_query(query, database, region, bucket)
    present = set(df['dt'].tolist()) if len(df) > 0 else set()

    missing = []
    d = cutoff
    while d <= yesterday:
        if str(d) not in present:
            missing.append(str(d))
        d += timedelta(days=1)

    return missing


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Detect missing Athena partition dates')
    parser.add_argument('--table', required=True, help='Athena table name')
    parser.add_argument('--date_col', default='partition_date',
                        help='Date column name in the table (default: partition_date)')
    parser.add_argument('--database', default=None,
                        help='Glue database name (default: $GLUE_DATABASE_NAME env var)')
    parser.add_argument('--lookback_days', type=int, default=7,
                        help='Number of days back to check for gaps (default: 7)')
    args = parser.parse_args()

    region = 'us-east-1'
    database = args.database or os.getenv('GLUE_DATABASE_NAME')
    bucket = os.getenv('S3_BUCKET_NAME')

    if not database:
        print('ERROR: --database or GLUE_DATABASE_NAME env var required', file=sys.stderr)
        sys.exit(1)
    if not bucket:
        print('ERROR: S3_BUCKET_NAME env var required', file=sys.stderr)
        sys.exit(1)

    missing = get_missing_dates(args.table, args.date_col, database, region, bucket,
                                args.lookback_days)
    for d in missing:
        print(d)
