"""Shopify order and line-item extraction.

The failure these tests exist to prevent is silent. The job writes headerless
CSV and Athena maps file position to column position, so a field added in the
middle of a Pydantic model shifts every later value one column to the left in
the table while the DDL stays put. Nothing errors; the data is just wrong.
"""
import re
from pathlib import Path

import pandas as pd
import pytest

from models import ShopifyLineItem, ShopifyOrder
from utils import shopify_line_item_records, shopify_order_record, validate_dataframe

DDL = Path(__file__).resolve().parent.parent / 'src' / 'shopify_order_details' / 'ddl.sql'


def ddl_columns(table: str):
    """Column names of one CREATE EXTERNAL TABLE block, in declaration order."""
    body = re.search(
        rf'CREATE EXTERNAL TABLE IF NOT EXISTS {table} \((.*?)\n\)\s*\nPARTITIONED BY',
        DDL.read_text(), re.S).group(1)
    cols = []
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith('--'):
            continue
        cols.append(line.split()[0])
    return cols


# --------------------------------------------------------------------------
# A realistic order. Synthetic addresses and ids, shaped like the live payload.
# --------------------------------------------------------------------------

ORDER = {
    'id': 5544332211,
    'order_number': 266585,
    'email': 'Buyer@Example.test',
    'created_at': '2026-08-15T10:23:45-04:00',
    'cancelled_at': None,
    'test': False,
    'financial_status': 'paid',
    'fulfillment_status': 'fulfilled',
    'tags': 'vip, wholesale',
    'subtotal_price': '44.98',
    'total_line_items_price': '50.97',
    'total_tax': '0.00',
    'total_discounts': '5.99',
    'total_shipping_price_set': {'shop_money': {'amount': '5.99'}},
    'total_price': '50.97',
    'customer': {'id': 998877},
    'shipping_address': {'address1': '1 Test St', 'city': 'Austin',
                         'province': 'Texas', 'country': 'United States'},
    'line_items': [
        {'variant_id': 111, 'product_id': 222, 'sku': '16',
         'title': 'Salted Caramel', 'variant_title': 'Large Bag',
         'name': 'Salted Caramel - Large Bag', 'price': '25.99',
         'quantity': 1, 'total_discount': '5.99'},
        {'variant_id': None, 'product_id': None, 'sku': None,
         'title': 'Gift Card', 'variant_title': '$10.00 USD',
         'name': 'Gift Card - $10.00 USD', 'price': '10.00',
         'quantity': 1, 'total_discount': '0.00'},
    ],
}


# --------------------------------------------------------------------------
# Column order is the contract with Athena
# --------------------------------------------------------------------------

def test_order_model_matches_ddl_column_order():
    assert list(ShopifyOrder.model_fields) == ddl_columns('shopify_orders')


def test_line_item_model_matches_ddl_column_order():
    assert list(ShopifyLineItem.model_fields) == ddl_columns('shopify_line_items')


def test_extracted_order_keys_match_model_order():
    """pd.DataFrame preserves dict insertion order, so the extract's key order
    has to agree with the model's too."""
    assert list(shopify_order_record(ORDER)) == list(ShopifyOrder.model_fields)


def test_extracted_line_item_keys_match_model_order():
    assert list(shopify_line_item_records(ORDER)[0]) == list(ShopifyLineItem.model_fields)


def test_new_columns_are_appended_not_inserted():
    """History written before this change has fewer trailing fields. That is
    only safe while the new columns sit at the end."""
    for model, original_last in ((ShopifyOrder, 'order_date'),
                                 (ShopifyLineItem, 'line_item_name')):
        fields = list(model.model_fields)
        added = fields[fields.index(original_last) + 1:]
        assert added, f'{model.__name__} gained no new fields'
        assert fields[:fields.index(original_last) + 1] == fields[:len(fields) - len(added)]


def test_validated_frame_writes_columns_in_ddl_order():
    """End to end: extract, validate, build the frame that gets written."""
    valid, invalid = validate_dataframe(pd.DataFrame([shopify_order_record(ORDER)]),
                                        ShopifyOrder)
    assert not invalid
    assert list(pd.DataFrame(valid).columns) == ddl_columns('shopify_orders')


# --------------------------------------------------------------------------
# The identifiers themselves
# --------------------------------------------------------------------------

def test_variant_and_product_ids_are_captured():
    line = shopify_line_item_records(ORDER)[0]
    assert line['variant_id'] == 111
    assert line['product_id'] == 222


def test_order_id_stays_the_order_number_that_joins_to_shipbob():
    """ShipBob records order_number, so order_id must remain order_number.
    Shopify's internal id is carried separately."""
    record = shopify_order_record(ORDER)
    assert record['order_id'] == 266585
    assert record['shopify_order_id'] == 5544332211


def test_line_level_discount_is_captured():
    assert shopify_line_item_records(ORDER)[0]['line_discount'] == '5.99'


def test_customer_id_and_test_flag_are_captured():
    record = shopify_order_record(ORDER)
    assert record['customer_id'] == 998877
    assert record['is_test'] is False


def test_a_line_item_without_a_variant_is_kept():
    """Gift cards have no variant. They must still produce a row, with a null
    identity rather than a dropped line."""
    gift_card = shopify_line_item_records(ORDER)[1]
    assert gift_card['variant_id'] is None
    validated = ShopifyLineItem(**gift_card)
    assert validated.variant_id is None


# --------------------------------------------------------------------------
# Shapes the live data actually contains
# --------------------------------------------------------------------------

def test_guest_checkout_has_no_customer_id():
    order = {**ORDER, 'customer': None}
    assert shopify_order_record(order)['customer_id'] is None


def test_missing_shipping_address_does_not_raise():
    order = {**ORDER, 'shipping_address': None}
    assert shopify_order_record(order)['shipping_city'] is None


def test_test_orders_are_flagged():
    record = shopify_order_record({**ORDER, 'test': True})
    assert ShopifyOrder(**record).is_test is True


def test_cancelled_order_parses_its_timestamp():
    record = shopify_order_record({**ORDER, 'cancelled_at': '2026-08-16T09:00:00-04:00'})
    assert ShopifyOrder(**record).cancelled_at is not None


def test_uncancelled_order_has_null_cancelled_at():
    assert ShopifyOrder(**shopify_order_record(ORDER)).cancelled_at is None


def test_absent_discount_defaults_to_zero():
    """Older payloads omit total_discount entirely."""
    line = {**shopify_line_item_records(ORDER)[0]}
    del line['line_discount']
    assert ShopifyLineItem(**line, line_discount=None).line_discount == 0.0


def test_every_line_item_of_an_order_is_returned():
    assert len(shopify_line_item_records(ORDER)) == 2


def test_an_order_with_no_line_items_returns_nothing():
    assert shopify_line_item_records({**ORDER, 'line_items': []}) == []
