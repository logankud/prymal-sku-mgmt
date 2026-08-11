"""The two daily ShipBob jobs, and the date defaults their crons rely on."""
import os
from typing import Any, Callable, Optional, Tuple

import boto3
import pandas as pd
from loguru import logger

from shipbob.client import API_VERSION, ShipBobClient
from shipbob.extract import fetch_inventory, fetch_orders
from shipbob.load import INVENTORY_DETAILS, ORDER_DETAILS, publish, split_by_date

# Inventory snapshots are stamped with the operating day, not the UTC day.
BUSINESS_TIMEZONE = 'America/New_York'
AWS_REGION = 'us-east-1'


# --------------------------------------------------------------------------
# Date defaults
# --------------------------------------------------------------------------

def _utc_now(now=None) -> pd.Timestamp:
    stamp = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz='UTC')
    return stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')


def default_partition_date(now=None) -> str:
    """Today in the business timezone. The 05:00 UTC cron runs during the
    previous US evening for part of the year, and the DST offset is not always
    four hours."""
    return _utc_now(now).tz_convert(BUSINESS_TIMEZONE).strftime('%Y-%m-%d')


def default_order_window(now=None) -> Tuple[str, str]:
    """Yesterday through today, UTC - matching ShipBob's own date filtering."""
    stamp = _utc_now(now)
    return ((stamp - pd.Timedelta(days=1)).strftime('%Y-%m-%d'),
            stamp.strftime('%Y-%m-%d'))


# --------------------------------------------------------------------------
# Jobs
# --------------------------------------------------------------------------

def run_inventory(client: ShipBobClient, *, s3_client: Any, bucket: str,
                  athena: Callable[[str], None],
                  partition_date: Optional[str] = None) -> int:
    """Snapshot current inventory into one dated partition."""
    partition_date = partition_date or default_partition_date()
    logger.info(f'shipbob_inventory_details: snapshot for {partition_date}')
    df = fetch_inventory(client)
    return publish({partition_date: df}, INVENTORY_DETAILS,
                   s3_client=s3_client, bucket=bucket, athena=athena)


def run_orders(client: ShipBobClient, *, s3_client: Any, bucket: str,
               athena: Callable[[str], None], start_date: Optional[str] = None,
               end_date: Optional[str] = None) -> int:
    """Extract orders for a window and write one partition per purchase day."""
    if start_date is None or end_date is None:
        start_date, end_date = default_order_window()
    logger.info(f'shipbob_order_details: {start_date}..{end_date}')

    df = fetch_orders(client, start_date, end_date)
    partitions = split_by_date(df, 'purchase_date', window=(start_date, end_date))
    return publish(partitions, ORDER_DETAILS,
                   s3_client=s3_client, bucket=bucket, athena=athena)


# --------------------------------------------------------------------------
# Wiring from the environment (used by the two main.py entry points)
# --------------------------------------------------------------------------

def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f'{name} environment variable is not set')
    return value


class _LocalS3:
    """Stands in for the boto3 S3 client and writes objects under a local dir,
    preserving the key layout so a dry run is diffable against production."""

    def __init__(self, root: str):
        self.root = root

    def put_object(self, *, Body: str, Bucket: str, Key: str, ContentType: str) -> dict:
        path = os.path.join(self.root, Key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as handle:
            handle.write(Body)
        logger.info(f'[dry-run] wrote {path}')
        return {}


def local_context(output_dir: str = 'dryrun') -> dict:
    """Everything a job needs to run against the live API without touching AWS.

    Only SHIPBOB_API_SECRET is required; CSVs land under output_dir and the
    table repair is logged rather than executed.
    """
    client = ShipBobClient(_require('SHIPBOB_API_SECRET'),
                           version=os.getenv('SHIPBOB_API_VERSION', API_VERSION))

    def athena(query: str) -> None:
        logger.info(f'[dry-run] would run: {query}')

    return {'client': client, 's3_client': _LocalS3(output_dir),
            'bucket': output_dir, 'athena': athena}


def build_context() -> dict:
    """Read every required secret up front so a misconfigured workflow fails
    before it makes an API call."""
    bucket = _require('S3_BUCKET_NAME')
    database = _require('GLUE_DATABASE_NAME')
    # SHIPBOB_API_VERSION lets a future version bump be a workflow env change.
    client = ShipBobClient(_require('SHIPBOB_API_SECRET'),
                           version=os.getenv('SHIPBOB_API_VERSION', API_VERSION))

    s3_client = boto3.client(
        's3', region_name=AWS_REGION,
        aws_access_key_id=_require('AWS_ACCESS_KEY'),
        aws_secret_access_key=_require('AWS_ACCESS_SECRET'))

    def athena(query: str) -> None:
        from utils import run_athena_query_no_results
        logger.info(f'Athena: {query}')
        run_athena_query_no_results(query=query, bucket=bucket,
                                    database=database, region=AWS_REGION)

    return {'client': client, 's3_client': s3_client, 'bucket': bucket,
            'athena': athena}
