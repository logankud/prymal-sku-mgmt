"""Glue schema reconciliation for the Shopify tables.

Adding a column to the extract without adding it to the table is silent: the
CSV is headerless, Athena reads the first N fields by position, and the extra
values are discarded while every run reports success. These tests cover the
step that closes that gap, and the case where it must refuse to act.
"""
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import utils
from models import ShopifyLineItem, ShopifyOrder
from utils import SchemaDriftError, ddl_columns, ensure_table_columns

DDL = str(Path(__file__).resolve().parent.parent / 'src' / 'shopify_order_details' / 'ddl.sql')


@pytest.fixture
def glue(monkeypatch):
    """Stub the Glue client and capture any ALTER statement issued."""
    client = MagicMock()
    monkeypatch.setattr(utils.boto3, 'client', lambda *a, **k: client)
    issued = []
    monkeypatch.setattr(utils, 'run_athena_query_no_results',
                        lambda **kw: issued.append(kw['query']))
    client.issued = issued
    return client


def set_columns(glue, columns):
    glue.get_table.return_value = {
        'Table': {'StorageDescriptor': {
            'Columns': [{'Name': n, 'Type': t} for n, t in columns]}}}


# --------------------------------------------------------------------------
# Parsing the DDL
# --------------------------------------------------------------------------

def test_ddl_columns_reads_names_and_types_in_order():
    columns = ddl_columns(DDL, 'shopify_line_items')
    assert [name for name, _ in columns] == list(ShopifyLineItem.model_fields)
    assert dict(columns)['variant_id'] == 'bigint'


def test_ddl_columns_excludes_partition_keys():
    names = [name for name, _ in ddl_columns(DDL, 'shopify_orders')]
    assert 'year' not in names and 'month' not in names and 'day' not in names


def test_ddl_columns_matches_the_order_model():
    assert [n for n, _ in ddl_columns(DDL, 'shopify_orders')] == list(ShopifyOrder.model_fields)


def test_unknown_table_raises():
    with pytest.raises(ValueError, match='No CREATE EXTERNAL TABLE block'):
        ddl_columns(DDL, 'not_a_table')


# --------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------

def test_missing_columns_are_appended(glue):
    expected = ddl_columns(DDL, 'shopify_line_items')
    set_columns(glue, expected[:-3])          # the pre-2026-09 table

    added = ensure_table_columns('shopify_line_items', 'prymal', 'us-east-1',
                                 'bkt', expected)

    assert added == ['variant_id', 'product_id', 'line_discount']
    assert len(glue.issued) == 1
    assert glue.issued[0].startswith('ALTER TABLE shopify_line_items ADD COLUMNS (')
    assert 'variant_id bigint' in glue.issued[0]


def test_up_to_date_table_issues_no_ddl(glue):
    expected = ddl_columns(DDL, 'shopify_orders')
    set_columns(glue, expected)

    assert ensure_table_columns('shopify_orders', 'prymal', 'us-east-1', 'bkt',
                                expected) == []
    assert glue.issued == []


def test_reconciliation_is_idempotent(glue):
    """It runs on every job run, so the second run must do nothing."""
    expected = ddl_columns(DDL, 'shopify_orders')
    set_columns(glue, expected[:-7])
    ensure_table_columns('shopify_orders', 'prymal', 'us-east-1', 'bkt', expected)
    set_columns(glue, expected)               # as the table now stands
    assert ensure_table_columns('shopify_orders', 'prymal', 'us-east-1', 'bkt',
                                expected) == []
    assert len(glue.issued) == 1


def test_renamed_column_raises_instead_of_appending(glue):
    """Appending on top of a mismatch would bury it. Refuse."""
    expected = ddl_columns(DDL, 'shopify_orders')
    drifted = [('order_ref', 'bigint')] + expected[1:]
    set_columns(glue, drifted)

    with pytest.raises(SchemaDriftError, match='column 0'):
        ensure_table_columns('shopify_orders', 'prymal', 'us-east-1', 'bkt', expected)
    assert glue.issued == []


def test_retyped_column_raises(glue):
    expected = ddl_columns(DDL, 'shopify_line_items')
    drifted = list(expected)
    drifted[4] = (drifted[4][0], 'string')     # price double -> string
    set_columns(glue, drifted)

    with pytest.raises(SchemaDriftError):
        ensure_table_columns('shopify_line_items', 'prymal', 'us-east-1', 'bkt', expected)


def test_column_inserted_mid_list_raises(glue):
    """The exact hazard the appended-only rule exists to prevent."""
    expected = ddl_columns(DDL, 'shopify_line_items')
    drifted = expected[:4] + [('surprise', 'string')] + expected[4:]
    set_columns(glue, drifted)

    with pytest.raises(SchemaDriftError):
        ensure_table_columns('shopify_line_items', 'prymal', 'us-east-1', 'bkt', expected)


def test_glue_type_case_is_ignored(glue):
    expected = ddl_columns(DDL, 'shopify_line_items')
    set_columns(glue, [(n, t.upper()) for n, t in expected])
    assert ensure_table_columns('shopify_line_items', 'prymal', 'us-east-1',
                                'bkt', expected) == []
