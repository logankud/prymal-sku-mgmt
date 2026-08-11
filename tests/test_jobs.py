"""End-to-end job behaviour: date defaults and the extract -> publish wiring.

These are the behaviours the run.sh scripts and workflow crons depend on.
"""
from datetime import datetime

import pandas as pd
import pytest
import responses
from botocore.exceptions import ClientError

from conftest import API, cursor_page, inventory_item, inventory_level, order

from shipbob.jobs import (default_order_window, default_partition_date,
                          local_context, run_inventory, run_orders)
from shipbob.load import INVENTORY_DETAILS, ORDER_DETAILS, s3_key

BUCKET = 'prymal-analytics'


# --------------------------------------------------------------------------
# Date defaults
# --------------------------------------------------------------------------

def test_inventory_snapshot_defaults_to_today_in_business_timezone():
    """The snapshot is stamped with the US/Eastern day, not the UTC day: 03:00
    UTC on the 8th is still the evening of the 7th on the floor."""
    utc_now = pd.Timestamp('2026-08-08T03:00:00Z')

    assert default_partition_date(now=utc_now) == '2026-08-07'


def test_inventory_snapshot_date_is_correct_in_winter_too():
    """Today's code subtracts a hard-coded 4 hours, which is wrong once the
    clocks go back."""
    utc_now = pd.Timestamp('2026-01-08T04:30:00Z')  # 23:30 on the 7th, EST

    assert default_partition_date(now=utc_now) == '2026-01-07'


def test_order_window_defaults_to_yesterday_through_today_utc():
    utc_now = pd.Timestamp('2026-08-08T07:00:00Z')

    assert default_order_window(now=utc_now) == ('2026-08-07', '2026-08-08')


# --------------------------------------------------------------------------
# Inventory job
# --------------------------------------------------------------------------

@responses.activate
def test_inventory_job_writes_one_snapshot_partition(client, s3_client, athena):
    responses.get(f'{API}/inventory-level', json=cursor_page([inventory_level()]))
    responses.get(f'{API}/inventory', json=cursor_page([inventory_item()]))

    written = run_inventory(client, s3_client=s3_client, bucket=BUCKET,
                            athena=athena, partition_date='2026-08-07')

    assert written == 1
    assert s3_client.keys == [s3_key(INVENTORY_DETAILS, '2026-08-07')]
    assert athena.calls == ['MSCK REPAIR TABLE shipbob_inventory_details']


@responses.activate
def test_inventory_job_does_nothing_when_api_returns_no_items(client, s3_client, athena):
    responses.get(f'{API}/inventory-level', json=cursor_page([]))
    responses.get(f'{API}/inventory', json=cursor_page([]))

    written = run_inventory(client, s3_client=s3_client, bucket=BUCKET,
                            athena=athena, partition_date='2026-08-07')

    assert written == 0
    assert s3_client.puts == []
    assert athena.calls == []


@responses.activate
def test_inventory_job_propagates_api_failure(client, s3_client, athena):
    """A dead API version must fail the workflow, not quietly write nothing."""
    from shipbob.client import ShipBobError

    responses.get(f'{API}/inventory-level', status=401, json={'message': 'Unauthorized'})

    with pytest.raises(ShipBobError):
        run_inventory(client, s3_client=s3_client, bucket=BUCKET, athena=athena,
                      partition_date='2026-08-07')

    assert s3_client.puts == []


# --------------------------------------------------------------------------
# Orders job
# --------------------------------------------------------------------------

@responses.activate
def test_orders_job_writes_one_partition_per_purchase_day(client, s3_client, athena):
    responses.get(f'{API}/order', json=[
        order(order_id=1, purchase_date='2026-08-07T09:00:00Z'),
        order(order_id=2, purchase_date='2026-08-07T22:00:00Z'),
        order(order_id=3, purchase_date='2026-08-08T02:00:00Z'),
    ])
    responses.get(f'{API}/order', json=[])

    written = run_orders(client, s3_client=s3_client, bucket=BUCKET, athena=athena,
                         start_date='2026-08-07', end_date='2026-08-08')

    assert written == 3
    assert s3_client.keys == [
        s3_key(ORDER_DETAILS, '2026-08-07'),
        s3_key(ORDER_DETAILS, '2026-08-08'),
    ]
    assert athena.calls == ['MSCK REPAIR TABLE shipbob_order_details']


@responses.activate
def test_orders_job_does_not_clobber_partitions_outside_the_window(client, s3_client, athena):
    responses.get(f'{API}/order', json=[
        order(order_id=1, purchase_date='2026-06-01T09:00:00Z'),
        order(order_id=2, purchase_date='2026-08-07T09:00:00Z'),
    ])
    responses.get(f'{API}/order', json=[])

    run_orders(client, s3_client=s3_client, bucket=BUCKET, athena=athena,
               start_date='2026-08-07', end_date='2026-08-08')

    assert s3_client.keys == [s3_key(ORDER_DETAILS, '2026-08-07')]


@responses.activate
def test_orders_job_is_a_noop_on_a_day_with_no_orders(client, s3_client, athena):
    responses.get(f'{API}/order', json=[])

    written = run_orders(client, s3_client=s3_client, bucket=BUCKET, athena=athena,
                         start_date='2026-08-07', end_date='2026-08-08')

    assert written == 0
    assert s3_client.puts == []
    assert athena.calls == []


@responses.activate
def test_orders_job_rerun_overwrites_the_same_key(client, s3_client, athena):
    """Backfills re-run the same date; the key must be deterministic so the
    partition is replaced rather than duplicated."""
    for _ in range(2):
        responses.get(f'{API}/order', json=[order(purchase_date='2026-08-07T09:00:00Z')])

    for _ in range(2):
        run_orders(client, s3_client=s3_client, bucket=BUCKET, athena=athena,
                   start_date='2026-08-07', end_date='2026-08-08')

    assert set(s3_client.keys) == {s3_key(ORDER_DETAILS, '2026-08-07')}
    assert len(s3_client.puts) == 2


# --------------------------------------------------------------------------
# Dry run - the same job, writing to disk instead of S3
# --------------------------------------------------------------------------

@responses.activate
def test_dry_run_writes_the_production_key_layout_under_a_local_dir(tmp_path, monkeypatch):
    """A dry run has to be diffable against a real partition, so it keeps the
    S3 key as the relative path."""
    monkeypatch.setenv('SHIPBOB_API_SECRET', 'test-pat-token')
    responses.get(f'{API}/inventory-level', json=cursor_page([inventory_level()]))
    responses.get(f'{API}/inventory', json=cursor_page([inventory_item()]))

    context = local_context(str(tmp_path))
    written = run_inventory(partition_date='2026-08-07', **context)

    expected = tmp_path / s3_key(INVENTORY_DETAILS, '2026-08-07')
    assert written == 1
    assert expected.is_file()
    assert expected.read_text().splitlines()[0].startswith('id,name,is_digital')


def test_dry_run_needs_no_aws_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv('SHIPBOB_API_SECRET', 'test-pat-token')
    for name in ('S3_BUCKET_NAME', 'GLUE_DATABASE_NAME', 'AWS_ACCESS_KEY',
                 'AWS_ACCESS_SECRET'):
        monkeypatch.delenv(name, raising=False)

    context = local_context(str(tmp_path))

    assert context['bucket'] == str(tmp_path)
    assert context['athena']('MSCK REPAIR TABLE anything') is None  # logged, not run


def test_dry_run_still_requires_the_shipbob_token(tmp_path, monkeypatch):
    monkeypatch.delenv('SHIPBOB_API_SECRET', raising=False)

    with pytest.raises(ValueError, match='SHIPBOB_API_SECRET'):
        local_context(str(tmp_path))
