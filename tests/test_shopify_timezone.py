"""A partition is one calendar day in the Shopify store's timezone.

The API filters on an absolute instant, so a UTC day and a store-local day are
not the same set of orders. These tests pin the conversion, the boundary, and
the rule that decides what a partition contains.
"""
import pandas as pd
import pytest

import utils
from utils import (DEFAULT_STORE_TIMEZONE, shopify_local_date,
                   shopify_order_record, shopify_store_timezone)

PACIFIC = 'America/Los_Angeles'


class Resp:
    status_code = 200
    links = {}
    headers = {}

    def __init__(self, payload, ok=True):
        self._payload = payload
        self._ok = ok

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError('boom')


# --------------------------------------------------------------------------
# Reading the zone from Shopify
# --------------------------------------------------------------------------

def test_timezone_is_read_from_the_shop_endpoint(monkeypatch):
    seen = {}

    def fake_get(url, **kwargs):
        seen['url'] = url
        return Resp({'shop': {'iana_timezone': PACIFIC}})

    monkeypatch.setattr(utils.requests, 'get', fake_get)
    assert shopify_store_timezone('k', 'p') == PACIFIC
    assert '/shop.json' in seen['url']


def test_unreachable_shop_endpoint_falls_back(monkeypatch):
    """A timezone lookup failure must not take the whole extract down."""
    monkeypatch.setattr(utils.requests, 'get',
                        lambda url, **kw: Resp({}, ok=False))
    assert shopify_store_timezone('k', 'p') == DEFAULT_STORE_TIMEZONE


# --------------------------------------------------------------------------
# Local date of an instant
# --------------------------------------------------------------------------

@pytest.mark.parametrize('created_at,expected', [
    # Late evening Pacific is already the next day in UTC. The store's day wins.
    ('2026-09-09T20:30:00-07:00', '2026-09-09'),
    ('2026-09-09T23:59:59-07:00', '2026-09-09'),
    ('2026-09-10T00:00:00-07:00', '2026-09-10'),
    # Same instants expressed as UTC resolve to the same local day.
    ('2026-09-10T03:30:00Z', '2026-09-09'),
    ('2026-09-10T06:59:59Z', '2026-09-09'),
    ('2026-09-10T07:00:00Z', '2026-09-10'),
])
def test_local_date_uses_the_store_zone(created_at, expected):
    assert shopify_local_date(created_at, PACIFIC) == expected


def test_local_date_differs_from_the_utc_date_at_the_boundary():
    instant = '2026-09-10T03:30:00Z'
    assert shopify_local_date(instant, PACIFIC) == '2026-09-09'
    assert shopify_local_date(instant, 'UTC') == '2026-09-10'


def test_naive_timestamps_are_read_as_store_local():
    assert shopify_local_date('2026-09-09 20:30:00', PACIFIC) == '2026-09-09'


def test_daylight_saving_shift_is_handled():
    """Pacific is UTC-7 in September and UTC-8 in December, so a fixed offset
    would put one of these on the wrong day."""
    assert shopify_local_date('2026-09-10T06:30:00Z', PACIFIC) == '2026-09-09'
    assert shopify_local_date('2026-12-10T07:30:00Z', PACIFIC) == '2026-12-09'


# --------------------------------------------------------------------------
# order_date follows the same rule
# --------------------------------------------------------------------------

def test_order_date_is_the_store_local_day():
    record = shopify_order_record({'created_at': '2026-09-10T03:30:00Z'}, PACIFIC)
    assert record['order_date'] == '2026-09-09'


def test_order_date_without_a_zone_keeps_the_offset_in_the_string():
    record = shopify_order_record({'created_at': '2026-09-09T20:30:00-07:00'})
    assert record['order_date'] == '2026-09-09'


def test_missing_created_at_gives_no_order_date():
    assert shopify_order_record({'created_at': None}, PACIFIC)['order_date'] is None


# --------------------------------------------------------------------------
# created_at is stored in the same zone as order_date and the partition
# --------------------------------------------------------------------------

@pytest.mark.parametrize('created_at,expected', [
    # Shopify's own format, an offset.
    ('2026-09-10T00:00:00-07:00', '2026-09-10 00:00:00'),
    ('2026-09-09T23:59:59-07:00', '2026-09-09 23:59:59'),
    # A UTC instant converts rather than being stored as UTC wall time.
    ('2026-09-11T06:59:59Z', '2026-09-10 23:59:59'),
    ('2026-09-10T07:00:00Z', '2026-09-10 00:00:00'),
])
def test_created_at_is_converted_to_store_local_wall_time(created_at, expected):
    assert shopify_order_record({'created_at': created_at}, PACIFIC)['created_at'] == expected


def test_created_at_and_order_date_always_agree():
    """The two are read from the same instant, so they cannot disagree about
    which day an order belongs to."""
    for created_at in ('2026-09-10T07:00:00Z', '2026-09-11T06:59:59Z',
                       '2026-09-10T00:00:00-07:00', '2026-09-10T23:59:59-07:00'):
        record = shopify_order_record({'created_at': created_at}, PACIFIC)
        assert record['created_at'].startswith(record['order_date'])


def test_line_items_carry_the_same_converted_timestamp():
    order = {'created_at': '2026-09-11T06:59:59Z', 'order_number': 1,
             'line_items': [{'variant_id': 1}]}
    line = utils.shopify_line_item_records(order, PACIFIC)[0]
    assert line['created_at'] == '2026-09-10 23:59:59'
    assert line['order_date'] == '2026-09-10'


def test_utc_suffix_does_not_break_validation():
    """datetime.fromisoformat rejects a 'Z' suffix on Python 3.10, so the
    conversion has to happen before the model sees the value."""
    from models import ShopifyOrder
    record = shopify_order_record({
        'created_at': '2026-09-11T06:59:59Z', 'order_number': 1,
        'email': 'a@b.com', 'subtotal_price': '1', 'total_line_items_price': '1',
        'total_tax': '0', 'total_discounts': '0', 'total_price': '1',
        'total_shipping_price_set': {'shop_money': {'amount': '0'}},
    }, PACIFIC)
    assert ShopifyOrder(**record).created_at is not None


# --------------------------------------------------------------------------
# The window sent to the API
# --------------------------------------------------------------------------

def utc_window(start, end, tz=PACIFIC):
    """Reproduces the conversion get_shopify_orders_by_date performs."""
    start_local = pd.Timestamp(f'{start} 00:00:00', tz=tz)
    end_local = pd.Timestamp(f'{end} 23:59:59', tz=tz)
    return (start_local.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_local.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ'))


def test_local_day_is_sent_to_the_api_as_utc_instants():
    """A Pacific day in September runs 07:00 UTC to 06:59 UTC the next day."""
    assert utc_window('2026-09-10', '2026-09-10') == (
        '2026-09-10T07:00:00Z', '2026-09-11T06:59:59Z')


def test_window_shifts_with_daylight_saving():
    assert utc_window('2026-12-10', '2026-12-10') == (
        '2026-12-10T08:00:00Z', '2026-12-11T07:59:59Z')


def test_window_is_not_the_naive_utc_day():
    """The previous behaviour asked for 00:00Z..23:59Z, which is why roughly
    seven hours of each partition belonged to the previous local day."""
    assert utc_window('2026-09-10', '2026-09-10')[0] != '2026-09-10T00:00:00Z'


# --------------------------------------------------------------------------
# What a partition ends up containing
# --------------------------------------------------------------------------

def keep(created_at, start, end, tz=PACIFIC):
    """The filter applied to each order after the window is fetched."""
    return start <= shopify_local_date(created_at, tz) <= end


@pytest.mark.parametrize('created_at,kept', [
    ('2026-09-10T07:00:00Z', True),    # local midnight, first of the day
    ('2026-09-11T06:59:59Z', True),    # local 23:59:59, last of the day
    ('2026-09-10T06:59:59Z', False),   # local 23:59:59 the day before
    ('2026-09-11T07:00:00Z', False),   # local midnight the day after
])
def test_only_the_requested_local_day_is_kept(created_at, kept):
    assert keep(created_at, '2026-09-10', '2026-09-10') is kept


def test_a_multi_day_range_keeps_every_day_in_it():
    assert keep('2026-09-10T12:00:00Z', '2026-09-09', '2026-09-11')
    assert not keep('2026-09-12T12:00:00Z', '2026-09-09', '2026-09-11')
