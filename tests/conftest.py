"""Shared fixtures for the ShipBob pipeline tests.

Payload builders mirror the shapes documented for ShipBob API version 2026-07:
  - GET /2026-07/inventory        -> {"items": [...], "next": ...}  (cursor)
  - GET /2026-07/inventory-level  -> {"items": [...], "next": ...}  (cursor)
  - GET /2026-07/order            -> [ ... ]                        (Page/Limit)
"""
import os

import pytest

# utils.py reads AWS credentials at import time; make importing it safe under test.
os.environ.setdefault('AWS_ACCESS_KEY', 'testing')
os.environ.setdefault('AWS_ACCESS_SECRET', 'testing')

API = 'https://api.shipbob.com/2026-07'


# --------------------------------------------------------------------------
# Payload builders
# --------------------------------------------------------------------------

def inventory_level(inventory_id=14423625, name='Nutella Creamer', sku='NUT-LG', **overrides):
    """One item from GET /inventory-level (the quantity half of the old 1.0 payload)."""
    item = {
        'inventory_id': inventory_id,
        'name': name,
        'sku': sku,
        'total_fulfillable_quantity': 100,
        'total_on_hand_quantity': 120,
        'total_committed_quantity': 20,
        'total_sellable_quantity': 130,
        'total_awaiting_quantity': 30,
        'total_exception_quantity': 0,
        'total_internal_transfer_quantity': 5,
        'total_backordered_quantity': 2,
    }
    item.update(overrides)
    return item


def inventory_item(inventory_id=14423625, name='Nutella Creamer', sku='NUT-LG',
                   is_case=False, is_lot=True, is_active=True, is_digital=False):
    """One item from GET /inventory (the attribute half of the old 1.0 payload)."""
    return {
        'inventory_id': inventory_id,
        'name': name,
        'sku': sku,
        'barcode': '00012345',
        'is_case': is_case,
        'is_lot': is_lot,
        'variant': {
            'is_active': is_active,
            'is_digital': is_digital,
            'is_bundle': False,
            'hazmat': {'is_hazmat': False, 'validated': True},
        },
        'weight': {'unit': 'pounds', 'value': 1.5},
    }


def cursor_page(items, next_url=None):
    """The cursor-pagination envelope returned by the inventory endpoints."""
    return {
        'first': f'{API}/inventory-level',
        'prev': None,
        'next': next_url,
        'last': None,
        'items': items,
    }


def inventory_line(inventory_id=777, name='Nutella Creamer 12oz', quantity=2):
    return {'id': inventory_id, 'name': name, 'quantity': quantity,
            'quantity_committed': quantity, 'lot': None, 'serial_numbers': []}


def shipment_product(product_id=55, sku='PRY-NUT-LG', name='Nutella Creamer - Large',
                     inventory_items=None):
    return {
        'id': product_id,
        'sku': sku,
        'name': name,
        'reference_id': sku,
        'inventory_items': [inventory_line()] if inventory_items is None else inventory_items,
    }


def shipment(shipment_id=9001, products=None):
    return {
        'id': shipment_id,
        'status': 'Completed',
        'location': {'id': 1, 'name': 'Cicero (IL)'},
        'products': [shipment_product()] if products is None else products,
    }


def order(order_id=100001, order_number='PRY-1001', purchase_date='2026-08-07T09:15:00Z',
          created_date='2026-08-07T10:00:00Z', status='Fulfilled', order_type='DTC',
          channel=True, shipments=None, recipient_name='Jane Doe',
          email='jane@example.com'):
    """One order from GET /order. Response is a bare JSON array of these."""
    rec = {
        'id': order_id,
        'order_number': order_number,
        'status': status,
        'type': order_type,
        'shipping_method': 'Standard',
        'created_date': created_date,
        'purchase_date': purchase_date,
        'reference_id': f'ref-{order_id}',
        'financials': {'total_price': 42.5},
        'channel': {'id': 100, 'name': 'Shopify'} if channel else None,
        'recipient': {
            'name': recipient_name,
            'email': email,
            'phone_number': '555-0100',
            'address': {
                'address1': '123 Main St',
                'address2': None,
                'city': 'Chicago',
                'state': 'IL',
                'country': 'US',
                'zip_code': '60601',
            },
        },
        'products': [{'id': 55, 'sku': 'PRY-NUT-LG', 'quantity': 2}],
        'shipments': [shipment()] if shipments is None else shipments,
    }
    return rec


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

@pytest.fixture
def api():
    return API


@pytest.fixture
def token():
    return 'test-pat-token'


@pytest.fixture
def client(token):
    from shipbob.client import ShipBobClient
    # sleep is stubbed so retry/backoff tests run instantly
    return ShipBobClient(token, sleep=lambda _seconds: None)


@pytest.fixture
def s3_client():
    """Minimal stand-in for boto3's S3 client that records put_object calls."""
    class FakeS3:
        def __init__(self):
            self.puts = []
            self.error = None

        def put_object(self, **kwargs):
            if self.error:
                raise self.error
            self.puts.append(kwargs)
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}

        def body_for(self, key):
            for put in self.puts:
                if put['Key'] == key:
                    return put['Body']
            raise KeyError(key)

        @property
        def keys(self):
            return [put['Key'] for put in self.puts]

    return FakeS3()


@pytest.fixture
def athena():
    """Records the Athena statements a job issues (i.e. MSCK REPAIR TABLE)."""
    class FakeAthena:
        def __init__(self):
            self.calls = []

        def __call__(self, query):
            self.calls.append(query)

    return FakeAthena()
