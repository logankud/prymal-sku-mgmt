"""Glue schema reconciliation for the Shopify tables.

Adding a column to the extract without adding it to the table is silent: the
CSV is headerless, Athena reads the first N fields by position, and the extra
values are discarded while every run reports success. These tests cover the
step that closes that gap, and the case where it must refuse to act.
"""
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

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


# --------------------------------------------------------------------------
# Rendering values the declared type can actually read back
# --------------------------------------------------------------------------

def written_csv(df, columns):
    """The CSV text the job would upload, as a list of per-row field lists.

    An `order_id` column is prepended because pandas renders a null in a
    single-column frame as `""` rather than an empty field, to avoid emitting
    a blank line. Production frames always have many columns, so testing a
    lone column would assert behaviour that never occurs.
    """
    from io import StringIO
    df = df.copy()
    if 'order_id' not in df.columns:
        df.insert(0, 'order_id', range(1, len(df) + 1))
        columns = [('order_id', 'bigint')] + list(columns)
    buffer = StringIO()
    utils.format_df_for_s3(utils.coerce_frame_to_ddl(df, columns)).to_csv(
        buffer, index=False)
    return [line.split(',') for line in buffer.getvalue().splitlines()]


def test_nullable_bigint_is_not_written_as_a_float():
    """A gift-card line has no variant. That one null upcasts the whole column
    to float64, so variant_id would be written '111.0' and read back NULL."""
    df = pd.DataFrame({'variant_id': [111, None]})
    rows = written_csv(df, [('variant_id', 'bigint')])
    assert rows[1][1] == '111'
    assert rows[2][1] == ''


def test_midnight_timestamp_keeps_its_time_component():
    """to_csv drops the time when every value is midnight, writing a bare date
    that a Hive timestamp cannot parse. This is the shopify_orders.order_date
    bug: NULL in all 264,165 rows while created_at was fine."""
    df = pd.DataFrame({'order_date': pd.to_datetime(['2026-08-15', '2026-08-16'])})
    rows = written_csv(df, [('order_date', 'timestamp')])
    assert rows[1][1] == '2026-08-15 00:00:00'
    assert rows[2][1] == '2026-08-16 00:00:00'


def test_null_timestamp_is_written_empty():
    df = pd.DataFrame({'cancelled_at': [None, '2026-08-16T09:00:00-04:00']})
    rows = written_csv(df, [('cancelled_at', 'timestamp')])
    assert rows[1][1] == ''
    assert rows[2][1].startswith('2026-08-16 09:00:00')


def test_string_columns_are_left_alone():
    df = pd.DataFrame({'order_date': ['2026-08-15', '2026-08-16']})
    rows = written_csv(df, [('order_date', 'string')])
    assert rows[1][1] == '2026-08-15'


def test_columns_absent_from_the_frame_are_skipped():
    df = pd.DataFrame({'sku': ['16']})
    out = utils.coerce_frame_to_ddl(df, [('sku', 'string'), ('variant_id', 'bigint')])
    assert list(out.columns) == ['sku']


def test_coercion_does_not_mutate_the_caller_s_frame():
    df = pd.DataFrame({'variant_id': [111, None]})
    utils.coerce_frame_to_ddl(df, [('variant_id', 'bigint')])
    assert df['variant_id'].dtype == 'float64'


def test_every_shopify_column_round_trips():
    """One row through the real DDL for both tables, asserting nothing renders
    as a float-with-.0 or a bare date in a timestamp column."""
    import re as _re
    for table, row in (
        ('shopify_orders', {'order_id': 266585, 'shopify_order_id': 5544332211,
                            'customer_id': None, 'is_test': False,
                            'order_date': pd.Timestamp('2026-08-15'),
                            'cancelled_at': None}),
        ('shopify_line_items', {'order_id': 266585, 'variant_id': None,
                                'product_id': 222, 'quantity': 2}),
    ):
        columns = ddl_columns(DDL, table)
        rows = written_csv(pd.DataFrame([row]), columns)
        for value in rows[1]:
            assert not _re.fullmatch(r'-?\d+\.0', value), f'{table}: {value} is a float id'
        if table == 'shopify_orders':
            assert '2026-08-15 00:00:00' in rows[1]
