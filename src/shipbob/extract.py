"""Turn ShipBob API JSON into the DataFrames the Glue tables already expect.

The column lists below are the contract with `shipbob_inventory_details` and
`shipbob_order_details`, and through them with the run-rate, alerting and
prymal_agent jobs. Adding or renaming a column here is a schema migration.
"""
from typing import Iterator, List

import pandas as pd
from loguru import logger

from shipbob.client import PAGE_SIZE, ShipBobClient

# shipbob_inventory_details/ddl.sql
INVENTORY_COLUMNS: List[str] = [
    'id', 'name', 'is_digital', 'is_case_pick', 'is_lot',
    'total_fulfillable_quantity', 'total_onhand_quantity',
    'total_committed_quantity', 'total_sellable_quantity',
    'total_awaiting_quantity', 'total_exception_quantity',
    'total_internal_transfer_quantity', 'total_backordered_quantity',
    'is_active',
]

# models.ShipbobOrderDetails
ORDER_COLUMNS: List[str] = [
    'created_date', 'purchase_date', 'shipbob_order_id', 'order_number',
    'order_status', 'order_type', 'shipping_method', 'channel_id',
    'channel_name', 'customer_name', 'customer_email',
    'customer_address_city', 'customer_address_state',
    'customer_address_country', 'product_id', 'sku', 'sku_name',
    'inventory_id', 'inventory_qty', 'inventory_name',
]

_FLAG_COLUMNS = ['is_digital', 'is_case_pick', 'is_lot', 'is_active']
_COUNT_COLUMNS = [c for c in INVENTORY_COLUMNS if c == 'id' or c.startswith('total_')]

# 1.0 returned these on one payload; 2026-07 splits them across two endpoints.
_LEVEL_RENAMES = {'inventory_id': 'id', 'total_on_hand_quantity': 'total_onhand_quantity'}


# --------------------------------------------------------------------------
# Inventory
# --------------------------------------------------------------------------

def fetch_inventory(client: ShipBobClient) -> pd.DataFrame:
    """Current inventory snapshot: quantities joined to item attributes.

    GET /inventory-level carries the quantities, GET /inventory the flags.
    Quantities are the operational signal, so the level feed drives the row set:
    an item with quantities but no attribute record is kept with its flags
    defaulted to False rather than dropped or left as NaN.
    """
    levels = pd.DataFrame(list(client.paginate_cursor('inventory-level')))
    if levels.empty:
        logger.info('ShipBob returned no inventory levels')
        return pd.DataFrame(columns=INVENTORY_COLUMNS)

    attributes = pd.DataFrame(
        [_inventory_attributes(item) for item in client.paginate_cursor('inventory')],
        columns=['id'] + _FLAG_COLUMNS,
    )

    df = levels.rename(columns=_LEVEL_RENAMES).merge(attributes, on='id', how='left')

    orphans = int(df['is_active'].isna().sum())
    if orphans:
        logger.warning(f'{orphans} inventory item(s) have levels but no attribute '
                       f'record; defaulting their flags to False')

    return _coerce_inventory(df)


def _inventory_attributes(item: dict) -> dict:
    variant = item.get('variant') or {}
    return {
        'id': item.get('inventory_id'),
        'is_digital': variant.get('is_digital'),
        'is_case_pick': item.get('is_case'),   # renamed in 2026-07
        'is_lot': item.get('is_lot'),
        'is_active': variant.get('is_active'),
    }


def _coerce_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """Pin the column set and dtypes so the CSV matches the Glue schema.

    Left-joined nulls would otherwise upcast the integer columns to float and
    write '100.0' into columns Athena reads as int.
    """
    df = df.reindex(columns=INVENTORY_COLUMNS)
    for column in _FLAG_COLUMNS:
        df[column] = df[column].astype('boolean').fillna(False).astype(bool)
    for column in _COUNT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype('int64')
    df['name'] = df['name'].fillna('')
    return df


# --------------------------------------------------------------------------
# Orders
# --------------------------------------------------------------------------

def fetch_orders(client: ShipBobClient, start_date: str, end_date: str,
                 page_size: int = PAGE_SIZE) -> pd.DataFrame:
    """Orders created in [start_date, end_date], flattened to line-item grain.

    One row per order x shipment x product x inventory item. Orders that have
    not shipped yet contribute no rows.
    """
    orders = client.paginate_pages(
        'order', params={'StartDate': start_date, 'EndDate': end_date}, limit=page_size)

    rows = [row for record in orders for row in _flatten_order(record)]
    df = pd.DataFrame(rows, columns=ORDER_COLUMNS)
    for column in ('created_date', 'purchase_date'):
        df[column] = pd.to_datetime(df[column], format='ISO8601', utc=True)

    logger.info(f'{start_date}..{end_date}: {df["shipbob_order_id"].nunique()} shipped '
                f'order(s), {len(df)} line item(s)')
    return df


def _flatten_order(record: dict) -> Iterator[dict]:
    channel = record.get('channel') or {}
    recipient = record.get('recipient') or {}
    address = recipient.get('address') or {}

    order = {
        'created_date': record.get('created_date'),
        'purchase_date': record.get('purchase_date'),
        'shipbob_order_id': record.get('id'),
        'order_number': record.get('order_number'),
        'order_status': record.get('status'),
        'order_type': record.get('type'),
        'shipping_method': record.get('shipping_method'),
        'channel_id': channel.get('id'),
        'channel_name': channel.get('name'),
        'customer_name': recipient.get('name'),
        'customer_email': recipient.get('email'),
        'customer_address_city': address.get('city'),
        'customer_address_state': address.get('state'),
        'customer_address_country': address.get('country'),
    }

    for shipment in record.get('shipments') or []:
        for product in shipment.get('products') or []:
            for item in product.get('inventory_items') or []:
                yield {
                    **order,
                    'product_id': product.get('id'),
                    'sku': product.get('sku'),
                    'sku_name': product.get('name'),
                    'inventory_id': item.get('id'),
                    'inventory_qty': item.get('quantity'),
                    'inventory_name': item.get('name'),
                }
