"""The export is checked against Shopify's own numbers, not against a fixture.

Every other test here compares the extract to a payload written by hand, which
only proves the code does what its author intended. These use
`total_line_items_price` as an independent witness: Shopify computes it, the
extract never reads it into a line, and the admin UI reports from the same
field. If the exported per-line price and quantity sum to it, the line
extraction agrees with what the UI shows.

Each test breaks the export in one specific way and asserts it is caught.
"""
import pandas as pd
import pytest

from utils import (reconcile_shopify_export, shopify_line_item_records,
                   shopify_order_record)

PACIFIC = 'America/Los_Angeles'


def api_order(order_number=266585, lines=((25.99, 1), (10.00, 2)), gross=None):
    """An order shaped like Shopify's, with totals it computed itself."""
    line_items = [
        {'variant_id': 100 + i, 'product_id': 200 + i, 'sku': str(i),
         'title': f'Product {i}', 'variant_title': 'Large Bag',
         'name': f'Product {i} - Large Bag',
         'price': f'{price:.2f}', 'quantity': quantity, 'total_discount': '0.00'}
        for i, (price, quantity) in enumerate(lines)
    ]
    computed = sum(price * quantity for price, quantity in lines)
    return {
        'id': 999, 'order_number': order_number, 'email': 'a@b.com',
        'created_at': '2026-09-10T10:00:00-07:00', 'cancelled_at': None,
        'test': False, 'financial_status': 'paid', 'fulfillment_status': None,
        'tags': None, 'customer': {'id': 1}, 'shipping_address': None,
        'subtotal_price': f'{computed:.2f}',
        'total_line_items_price': f'{gross if gross is not None else computed:.2f}',
        'total_tax': '0.00', 'total_discounts': '0.00',
        'total_shipping_price_set': {'shop_money': {'amount': '0.00'}},
        'total_price': f'{computed:.2f}',
        'line_items': line_items,
    }


def export(orders):
    """Run the real extract over a payload, as the job does."""
    order_rows = [shopify_order_record(o, PACIFIC) for o in orders]
    line_rows = [r for o in orders for r in shopify_line_item_records(o, PACIFIC)]
    return pd.DataFrame(order_rows), pd.DataFrame(line_rows)


# --------------------------------------------------------------------------
# A faithful export reconciles
# --------------------------------------------------------------------------

def test_a_correct_export_reports_no_problems():
    orders = [api_order()]
    assert reconcile_shopify_export(orders, *export(orders)) == []


def test_many_orders_reconcile():
    orders = [api_order(order_number=n, lines=((n / 100, 2),)) for n in range(1, 26)]
    assert reconcile_shopify_export(orders, *export(orders)) == []


def test_an_order_with_one_line_reconciles():
    orders = [api_order(lines=((19.99, 3),))]
    assert reconcile_shopify_export(orders, *export(orders)) == []


def test_a_zero_priced_line_reconciles():
    """Free gifts appear at price 0 and must not be mistaken for a discrepancy."""
    orders = [api_order(lines=((25.99, 1), (0.00, 1)))]
    assert reconcile_shopify_export(orders, *export(orders)) == []


# --------------------------------------------------------------------------
# Each way the export can be wrong is caught
# --------------------------------------------------------------------------

def test_a_dropped_order_is_caught():
    orders = [api_order(order_number=1), api_order(order_number=2)]
    orders_df, lines_df = export(orders)
    orders_df = orders_df[orders_df['order_id'] != 2]

    problems = reconcile_shopify_export(orders, orders_df, lines_df)
    assert any('not exported' in p for p in problems)


def test_a_duplicated_order_is_caught():
    orders = [api_order()]
    orders_df, lines_df = export(orders)
    orders_df = pd.concat([orders_df, orders_df], ignore_index=True)

    problems = reconcile_shopify_export(orders, orders_df, lines_df)
    assert any('duplicate' in p for p in problems)


def test_an_order_that_was_never_returned_is_caught():
    orders = [api_order(order_number=1)]
    orders_df, lines_df = export([api_order(order_number=1), api_order(order_number=7)])

    problems = reconcile_shopify_export(orders, orders_df, lines_df)
    assert any('not in the API response' in p for p in problems)


def test_a_dropped_line_item_is_caught():
    orders = [api_order()]
    orders_df, lines_df = export(orders)
    lines_df = lines_df.iloc[:-1]

    problems = reconcile_shopify_export(orders, orders_df, lines_df)
    assert any('line(s)' in p for p in problems)


def test_a_wrong_quantity_is_caught():
    """The failure a hand-written fixture cannot catch: the mapping looks
    plausible but reads the wrong field."""
    orders = [api_order()]
    orders_df, lines_df = export(orders)
    lines_df.loc[0, 'quantity'] = 99

    problems = reconcile_shopify_export(orders, orders_df, lines_df)
    assert any('total_line_items_price' in p for p in problems)


def test_a_wrong_price_is_caught():
    orders = [api_order()]
    orders_df, lines_df = export(orders)
    lines_df.loc[0, 'price'] = '1.00'

    problems = reconcile_shopify_export(orders, orders_df, lines_df)
    assert any('total_line_items_price' in p for p in problems)


def test_a_penny_of_float_noise_is_tolerated():
    orders = [api_order(lines=((10.005, 1),))]
    assert reconcile_shopify_export(orders, *export(orders)) == []


def test_every_order_is_checked_not_just_the_first():
    orders = [api_order(order_number=n) for n in (1, 2, 3)]
    orders_df, lines_df = export(orders)
    lines_df.loc[lines_df['order_id'] == 3, 'price'] = '0.01'

    problems = reconcile_shopify_export(orders, orders_df, lines_df)
    assert any('order 3' in p for p in problems)


def test_an_empty_response_reconciles():
    assert reconcile_shopify_export([], pd.DataFrame(), pd.DataFrame()) == []
