"""Order extraction.

The 2026-07 order payload keeps the 1.0 nesting the flattener walks
(shipments[].products[].inventory_items[]), so the grain is unchanged: one row
per order x shipment x product x inventory item.
"""
import pandas as pd
import pytest
import responses

from conftest import API, inventory_line, order, shipment, shipment_product

from models import ShipbobOrderDetails
from shipbob.extract import ORDER_COLUMNS, fetch_orders
from utils import validate_dataframe

WINDOW = ('2026-08-07', '2026-08-08')


def stub_orders(*pages):
    for page in pages:
        responses.get(f'{API}/order', json=page)
    responses.get(f'{API}/order', json=[])


# --------------------------------------------------------------------------
# Schema + grain
# --------------------------------------------------------------------------

def test_column_contract_matches_the_glue_table_exactly():
    assert ORDER_COLUMNS == [
        'created_date', 'purchase_date', 'shipbob_order_id', 'order_number',
        'order_status', 'order_type', 'shipping_method', 'channel_id',
        'channel_name', 'customer_name', 'customer_email',
        'customer_address_city', 'customer_address_state',
        'customer_address_country', 'product_id', 'sku', 'sku_name',
        'inventory_id', 'inventory_qty', 'inventory_name',
    ]


@responses.activate
def test_returns_exactly_the_legacy_columns_in_order(client):
    stub_orders([order()])

    assert list(fetch_orders(client, *WINDOW).columns) == ORDER_COLUMNS


@responses.activate
def test_grain_is_one_row_per_order_shipment_product_inventory_item(client):
    """2 shipments x 2 products x 2 inventory items = 8 rows for one order."""
    products = [
        shipment_product(product_id=1, sku='A',
                         inventory_items=[inventory_line(1), inventory_line(2)]),
        shipment_product(product_id=2, sku='B',
                         inventory_items=[inventory_line(3), inventory_line(4)]),
    ]
    stub_orders([order(shipments=[shipment(9001, products), shipment(9002, products)])])

    df = fetch_orders(client, *WINDOW)

    assert len(df) == 8
    assert df['shipbob_order_id'].nunique() == 1


@responses.activate
def test_flattens_nested_fields_onto_every_row(client):
    stub_orders([order(order_id=100001, order_number='PRY-1001')])

    row = fetch_orders(client, *WINDOW).iloc[0]

    assert row['shipbob_order_id'] == 100001
    assert row['order_number'] == 'PRY-1001'
    assert row['order_status'] == 'Fulfilled'
    assert row['order_type'] == 'DTC'
    assert row['shipping_method'] == 'Standard'
    assert row['channel_id'] == 100
    assert row['channel_name'] == 'Shopify'
    assert row['customer_name'] == 'Jane Doe'
    assert row['customer_email'] == 'jane@example.com'
    assert row['customer_address_city'] == 'Chicago'
    assert row['customer_address_state'] == 'IL'
    assert row['customer_address_country'] == 'US'
    assert row['product_id'] == 55
    assert row['sku'] == 'PRY-NUT-LG'
    assert row['sku_name'] == 'Prod A' or row['sku_name'] == 'Nutella Creamer - Large'
    assert row['inventory_id'] == 777
    assert row['inventory_qty'] == 2
    assert row['inventory_name'] == 'Nutella Creamer 12oz'


@responses.activate
def test_timestamps_are_parsed_not_left_as_strings(client):
    """purchase_date drives partitioning, so it has to be a real timestamp."""
    stub_orders([order(purchase_date='2026-08-07T09:15:00Z',
                       created_date='2026-08-07T10:00:00Z')])

    df = fetch_orders(client, *WINDOW)

    assert pd.api.types.is_datetime64_any_dtype(df['purchase_date'])
    assert pd.api.types.is_datetime64_any_dtype(df['created_date'])


# --------------------------------------------------------------------------
# Request shaping
# --------------------------------------------------------------------------

@responses.activate
def test_sends_the_requested_date_window(client):
    stub_orders([order()])

    fetch_orders(client, '2026-08-07', '2026-08-08')

    assert responses.calls[0].request.params['StartDate'] == '2026-08-07'
    assert responses.calls[0].request.params['EndDate'] == '2026-08-08'


@responses.activate
def test_collects_orders_across_all_pages(client):
    stub_orders([order(order_id=1)], [order(order_id=2)], [order(order_id=3)])

    df = fetch_orders(client, *WINDOW, page_size=1)

    assert sorted(df['shipbob_order_id'].unique().tolist()) == [1, 2, 3]


# --------------------------------------------------------------------------
# Edges that break the current implementation
# --------------------------------------------------------------------------

@responses.activate
def test_order_with_no_shipments_is_skipped_without_failing_the_run(client):
    """Today an unfulfilled order contributes no rows, which trips the
    `assert count == unique order ids` check and kills the whole day."""
    stub_orders([order(order_id=1), order(order_id=2, shipments=[])])

    df = fetch_orders(client, *WINDOW)

    assert df['shipbob_order_id'].unique().tolist() == [1]


@responses.activate
def test_shipment_with_no_products_is_skipped(client):
    stub_orders([order(order_id=1, shipments=[shipment(9001, products=[])])])

    assert len(fetch_orders(client, *WINDOW)) == 0


@responses.activate
def test_product_with_no_inventory_items_is_skipped(client):
    stub_orders([order(shipments=[shipment(
        9001, products=[shipment_product(inventory_items=[])])])])

    assert len(fetch_orders(client, *WINDOW)) == 0


@responses.activate
def test_missing_channel_yields_nulls_rather_than_raising(client):
    stub_orders([order(channel=False)])

    row = fetch_orders(client, *WINDOW).iloc[0]

    assert pd.isna(row['channel_id'])
    assert pd.isna(row['channel_name'])


@responses.activate
def test_missing_recipient_email_yields_null(client):
    stub_orders([order(email=None)])

    assert pd.isna(fetch_orders(client, *WINDOW).iloc[0]['customer_email'])


@responses.activate
def test_empty_window_returns_empty_frame_with_columns(client):
    responses.get(f'{API}/order', json=[])

    df = fetch_orders(client, *WINDOW)

    assert len(df) == 0
    assert list(df.columns) == ORDER_COLUMNS


# --------------------------------------------------------------------------
# Downstream contract
# --------------------------------------------------------------------------

@responses.activate
def test_output_passes_pydantic_validation(client):
    stub_orders([order(order_id=1), order(order_id=2)])

    df = fetch_orders(client, *WINDOW)
    valid, invalid = validate_dataframe(df, ShipbobOrderDetails)

    assert invalid == []
    assert len(valid) == 2


@responses.activate
def test_commas_in_text_fields_survive_validation(client):
    """Customer names and SKU names routinely contain commas; the CSV layer
    depends on them being neutralised before write."""
    stub_orders([order(recipient_name='Doe, Jane')])

    df = fetch_orders(client, *WINDOW)
    valid, invalid = validate_dataframe(df, ShipbobOrderDetails)

    assert invalid == []
    assert ',' not in valid[0]['customer_name']
