"""Publish a partitioned DataFrame to S3 and register it with Athena.

Both ShipBob jobs did this inline: validate with Pydantic, write one CSV per
date partition, skip empty partitions, then MSCK REPAIR. This is that sequence
once, ordered so nothing is written until every partition validates, and
nothing is registered until every write succeeds.
"""
from dataclasses import dataclass
from io import StringIO
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Type

import pandas as pd
from loguru import logger
from pydantic import BaseModel

from models import ShipbobInventory, ShipbobOrderDetails
from utils import format_df_for_s3, validate_dataframe


class ValidationFailed(ValueError):
    """A partition contained rows that do not match the table's schema."""


@dataclass(frozen=True)
class Dataset:
    """A Glue table and where its partitions live in S3."""
    table: str
    s3_prefix: str
    partition_key: str
    model: Type[BaseModel]


INVENTORY_DETAILS = Dataset(
    table='shipbob_inventory_details',
    s3_prefix='shipbob/inventory_details',
    partition_key='partition_date',
    model=ShipbobInventory,
)

ORDER_DETAILS = Dataset(
    table='shipbob_order_details',
    s3_prefix='shipbob/order_details',
    partition_key='order_date',
    model=ShipbobOrderDetails,
)


def s3_key(dataset: Dataset, partition_date: str) -> str:
    """Deterministic key, so re-running a date replaces its partition."""
    return (f'{dataset.s3_prefix}/{dataset.partition_key}={partition_date}/'
            f"{dataset.table}_{partition_date.replace('-', '_')}.csv")


def split_by_date(df: pd.DataFrame, column: str,
                  window: Optional[Tuple[str, str]] = None) -> Dict[str, pd.DataFrame]:
    """Group rows into one partition per calendar day of `column`.

    ShipBob filters orders by *created* date, so a response can contain an order
    purchased weeks earlier. Writing that row would replace a complete old
    partition with a one-row file, so rows outside `window` are dropped - the
    same net effect as the old day-walking loop, but counted and logged.
    """
    if len(df) == 0:
        return {}

    days = pd.to_datetime(df[column], utc=True).dt.strftime('%Y-%m-%d')
    keep = days.between(*window) if window else pd.Series(True, index=df.index)

    dropped = int((~keep).sum())
    if dropped:
        logger.warning(f'Dropping {dropped} row(s) whose {column} falls outside '
                       f'{window[0]}..{window[1]}')

    return {day: group for day, group in df[keep].groupby(days[keep], sort=True)}


def publish(partitions: Mapping[str, pd.DataFrame], dataset: Dataset, *,
            s3_client: Any, bucket: str, athena: Callable[[str], None]) -> int:
    """Validate, write, then register. Returns the number of rows written.

    Empty partitions are skipped. If nothing is written the table is not
    repaired, since there is no new partition to discover.
    """
    validated = {}
    for partition_date in sorted(partitions):
        frame = partitions[partition_date]
        if len(frame) == 0:
            logger.info(f'{dataset.table} {partition_date}: no rows, skipping')
            continue
        validated[partition_date] = _validate(frame, dataset, partition_date)

    if not validated:
        logger.info(f'{dataset.table}: nothing to publish')
        return 0

    for partition_date, frame in validated.items():
        key = s3_key(dataset, partition_date)
        logger.info(f'Writing {len(frame)} row(s) to s3://{bucket}/{key}')
        _put_csv(s3_client, bucket, key, frame)

    athena(f'MSCK REPAIR TABLE {dataset.table}')
    return sum(len(frame) for frame in validated.values())


def _validate(df: pd.DataFrame, dataset: Dataset, partition_date: str) -> pd.DataFrame:
    valid, invalid = validate_dataframe(df, dataset.model)
    if invalid:
        _record, error = invalid[0]
        raise ValidationFailed(
            f'{dataset.table} {partition_date}: {len(invalid)} of {len(df)} row(s) '
            f'failed validation. First error: {error}')
    return pd.DataFrame(valid, columns=list(dataset.model.model_fields))


def _put_csv(s3_client: Any, bucket: str, key: str, df: pd.DataFrame) -> None:
    """Write a DataFrame as CSV. Unlike utils.write_df_to_s3, S3 errors raise -
    a swallowed PutObject means a green run that published nothing."""
    buffer = StringIO()
    format_df_for_s3(df).to_csv(buffer, index=False, encoding='utf-8')
    s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket, Key=key,
                         ContentType='text/csv')
