"""Inventory extraction.

The 1.0 `GET /inventory` payload carried quantities *and* item attributes.
2026-07 split them across two endpoints, so `fetch_inventory` has to join
`/inventory-level` (quantities) to `/inventory` (attributes) and reproduce the
14-column contract that `shipbob_inventory_details` and every downstream job
already depend on.
"""
import pandas as pd
import responses

from conftest import API, cursor_page, inventory_item, inventory_level

from models import ShipbobInventory
from shipbob.extract import INVENTORY_COLUMNS, fetch_inventory
from utils import validate_dataframe


def stub_endpoints(levels, items):
    responses.get(f'{API}/inventory-level', json=cursor_page(levels))
    responses.get(f'{API}/inventory', json=cursor_page(items))


# --------------------------------------------------------------------------
# Schema contract - this is what keeps every downstream job working
# --------------------------------------------------------------------------

def test_column_contract_matches_the_glue_table_exactly():
    """shipbob_inventory_details/ddl.sql, in order. Changing this is a schema
    migration, not a refactor."""
    assert INVENTORY_COLUMNS == [
        'id', 'name', 'is_digital', 'is_case_pick', 'is_lot',
        'total_fulfillable_quantity', 'total_onhand_quantity',
        'total_committed_quantity', 'total_sellable_quantity',
        'total_awaiting_quantity', 'total_exception_quantity',
        'total_internal_transfer_quantity', 'total_backordered_quantity',
        'is_active',
    ]


@responses.activate
def test_returns_exactly_the_legacy_columns_in_order(client):
    stub_endpoints([inventory_level()], [inventory_item()])

    df = fetch_inventory(client)

    assert list(df.columns) == INVENTORY_COLUMNS


@responses.activate
def test_renames_2026_07_fields_back_to_legacy_names(client):
    stub_endpoints(
        [inventory_level(inventory_id=42, total_on_hand_quantity=120)],
        [inventory_item(inventory_id=42, is_case=True, is_lot=False,
                        is_active=False, is_digital=True)],
    )

    row = fetch_inventory(client).iloc[0]

    assert row['id'] == 42                    # was inventory_id
    assert row['total_onhand_quantity'] == 120  # was total_on_hand_quantity
    assert bool(row['is_case_pick']) is True    # was is_case
    assert bool(row['is_lot']) is False
    assert bool(row['is_active']) is False      # was variant.is_active
    assert bool(row['is_digital']) is True      # was variant.is_digital


@responses.activate
def test_carries_all_quantity_fields_through_unchanged(client):
    stub_endpoints([inventory_level()], [inventory_item()])

    row = fetch_inventory(client).iloc[0]

    assert row['total_fulfillable_quantity'] == 100
    assert row['total_committed_quantity'] == 20
    assert row['total_sellable_quantity'] == 130
    assert row['total_awaiting_quantity'] == 30
    assert row['total_exception_quantity'] == 0
    assert row['total_internal_transfer_quantity'] == 5
    assert row['total_backordered_quantity'] == 2


# --------------------------------------------------------------------------
# Joining the two endpoints
# --------------------------------------------------------------------------

@responses.activate
def test_joins_quantities_to_attributes_on_inventory_id(client):
    stub_endpoints(
        [inventory_level(inventory_id=1, total_fulfillable_quantity=10),
         inventory_level(inventory_id=2, total_fulfillable_quantity=20)],
        [inventory_item(inventory_id=2, is_lot=True),
         inventory_item(inventory_id=1, is_lot=False)],
    )

    df = fetch_inventory(client).set_index('id')

    assert df.loc[1, 'total_fulfillable_quantity'] == 10
    assert bool(df.loc[1, 'is_lot']) is False
    assert df.loc[2, 'total_fulfillable_quantity'] == 20
    assert bool(df.loc[2, 'is_lot']) is True


@responses.activate
def test_item_missing_from_attribute_endpoint_is_kept_with_false_flags(client):
    """Quantities are the operational signal. An item that /inventory-level
    reports but /inventory omits must not vanish from the snapshot, and must not
    arrive as NaN (which fails ShipbobInventory's bool fields and kills the job)."""
    stub_endpoints([inventory_level(inventory_id=1), inventory_level(inventory_id=999)],
                   [inventory_item(inventory_id=1)])

    df = fetch_inventory(client).set_index('id')

    assert 999 in df.index
    orphan = df.loc[999]
    assert bool(orphan['is_digital']) is False
    assert bool(orphan['is_case_pick']) is False
    assert bool(orphan['is_lot']) is False
    assert bool(orphan['is_active']) is False
    assert orphan['total_fulfillable_quantity'] == 100


@responses.activate
def test_attribute_only_items_are_dropped(client):
    """No quantities means nothing to snapshot; /inventory carries items that
    /inventory-level does not report on."""
    stub_endpoints([inventory_level(inventory_id=1)],
                   [inventory_item(inventory_id=1), inventory_item(inventory_id=2)])

    assert fetch_inventory(client)['id'].tolist() == [1]


@responses.activate
def test_paginates_both_endpoints(client):
    levels_page2 = f'{API}/inventory-level?Cursor=L2'
    items_page2 = f'{API}/inventory?Cursor=I2'
    responses.get(f'{API}/inventory-level',
                  json=cursor_page([inventory_level(inventory_id=1)], next_url=levels_page2))
    responses.get(levels_page2, json=cursor_page([inventory_level(inventory_id=2)]))
    responses.get(f'{API}/inventory',
                  json=cursor_page([inventory_item(inventory_id=1)], next_url=items_page2))
    responses.get(items_page2, json=cursor_page([inventory_item(inventory_id=2)]))

    assert sorted(fetch_inventory(client)['id'].tolist()) == [1, 2]


# --------------------------------------------------------------------------
# Edges
# --------------------------------------------------------------------------

@responses.activate
def test_empty_api_result_returns_empty_frame_with_columns(client):
    """Downstream code checks len(df) == 0; it must not hit a KeyError first."""
    stub_endpoints([], [])

    df = fetch_inventory(client)

    assert len(df) == 0
    assert list(df.columns) == INVENTORY_COLUMNS


@responses.activate
def test_quantity_columns_are_integers_not_floats(client):
    """A NaN anywhere in a column silently upcasts it to float, which writes
    '100.0' into the CSV and breaks the Glue int columns."""
    stub_endpoints([inventory_level(inventory_id=1), inventory_level(inventory_id=999)],
                   [inventory_item(inventory_id=1)])

    df = fetch_inventory(client)

    for col in INVENTORY_COLUMNS:
        if col.startswith('total_') or col == 'id':
            assert pd.api.types.is_integer_dtype(df[col]), f'{col} is {df[col].dtype}'


@responses.activate
def test_output_passes_pydantic_validation(client):
    stub_endpoints([inventory_level(inventory_id=1), inventory_level(inventory_id=999)],
                   [inventory_item(inventory_id=1)])

    valid, invalid = validate_dataframe(fetch_inventory(client), ShipbobInventory)

    assert invalid == []
    assert len(valid) == 2
