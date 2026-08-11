"""Publishing a frame to S3 + Athena.

Both ShipBob jobs do the same four things today - validate, write one CSV per
partition, skip empties, then MSCK REPAIR - duplicated across two main.py files.
This is that logic once, with the silent-S3-failure hole closed.
"""
import io

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from models import ShipbobInventory
from shipbob.load import (INVENTORY_DETAILS, ORDER_DETAILS, ValidationFailed,
                          publish, s3_key, split_by_date)

BUCKET = 'prymal-analytics'


def inventory_frame(n=2):
    return pd.DataFrame([{
        'id': i, 'name': f'SKU {i}', 'is_digital': False, 'is_case_pick': False,
        'is_lot': True, 'total_fulfillable_quantity': 10, 'total_onhand_quantity': 12,
        'total_committed_quantity': 2, 'total_sellable_quantity': 13,
        'total_awaiting_quantity': 3, 'total_exception_quantity': 0,
        'total_internal_transfer_quantity': 0, 'total_backordered_quantity': 0,
        'is_active': True,
    } for i in range(1, n + 1)])


def read_csv(body):
    return pd.read_csv(io.StringIO(body))


# --------------------------------------------------------------------------
# S3 key layout - must match the existing partitions byte for byte
# --------------------------------------------------------------------------

def test_inventory_key_matches_todays_layout():
    assert s3_key(INVENTORY_DETAILS, '2026-08-07') == (
        'shipbob/inventory_details/partition_date=2026-08-07/'
        'shipbob_inventory_details_2026_08_07.csv')


def test_order_key_matches_todays_layout():
    assert s3_key(ORDER_DETAILS, '2026-08-07') == (
        'shipbob/order_details/order_date=2026-08-07/'
        'shipbob_order_details_2026_08_07.csv')


# --------------------------------------------------------------------------
# Happy path
# --------------------------------------------------------------------------

def test_writes_one_object_per_partition(s3_client, athena):
    publish({'2026-08-07': inventory_frame(), '2026-08-08': inventory_frame()},
            INVENTORY_DETAILS, s3_client=s3_client, bucket=BUCKET, athena=athena)

    assert s3_client.keys == [
        s3_key(INVENTORY_DETAILS, '2026-08-07'),
        s3_key(INVENTORY_DETAILS, '2026-08-08'),
    ]


def test_returns_row_count_written(s3_client, athena):
    written = publish({'2026-08-07': inventory_frame(3)}, INVENTORY_DETAILS,
                      s3_client=s3_client, bucket=BUCKET, athena=athena)

    assert written == 3


def test_repairs_the_table_once_after_writing(s3_client, athena):
    publish({'2026-08-07': inventory_frame(), '2026-08-08': inventory_frame()},
            INVENTORY_DETAILS, s3_client=s3_client, bucket=BUCKET, athena=athena)

    assert athena.calls == ['MSCK REPAIR TABLE shipbob_inventory_details']


def test_csv_is_written_without_an_index_column(s3_client, athena):
    publish({'2026-08-07': inventory_frame()}, INVENTORY_DETAILS,
            s3_client=s3_client, bucket=BUCKET, athena=athena)

    frame = read_csv(s3_client.puts[0]['Body'])
    assert list(frame.columns) == list(inventory_frame().columns)


def test_uploads_as_text_csv(s3_client, athena):
    publish({'2026-08-07': inventory_frame()}, INVENTORY_DETAILS,
            s3_client=s3_client, bucket=BUCKET, athena=athena)

    assert s3_client.puts[0]['ContentType'] == 'text/csv'
    assert s3_client.puts[0]['Bucket'] == BUCKET


# --------------------------------------------------------------------------
# CSV safety - Glue reads these as plain comma-delimited text
# --------------------------------------------------------------------------

def test_neutralises_delimiters_quotes_and_newlines_in_text(s3_client, athena):
    df = inventory_frame(1)
    df.loc[0, 'name'] = 'Vanilla, "Bulk"\nBag'

    publish({'2026-08-07': df}, INVENTORY_DETAILS, s3_client=s3_client,
            bucket=BUCKET, athena=athena)

    body = s3_client.puts[0]['Body']
    assert len(body.strip().splitlines()) == 2      # header + one record
    name = read_csv(body).loc[0, 'name']
    assert ',' not in name and '"' not in name and '\n' not in name


# --------------------------------------------------------------------------
# Empties
# --------------------------------------------------------------------------

def test_empty_partition_is_not_written(s3_client, athena):
    publish({'2026-08-07': inventory_frame(), '2026-08-08': inventory_frame(0)},
            INVENTORY_DETAILS, s3_client=s3_client, bucket=BUCKET, athena=athena)

    assert s3_client.keys == [s3_key(INVENTORY_DETAILS, '2026-08-07')]


def test_nothing_to_write_means_no_s3_call_and_no_table_repair(s3_client, athena):
    """Today's jobs skip the write *and* the MSCK when the API returns nothing;
    an empty repair is pointless Athena spend."""
    written = publish({}, INVENTORY_DETAILS, s3_client=s3_client,
                      bucket=BUCKET, athena=athena)

    assert written == 0
    assert s3_client.puts == []
    assert athena.calls == []


# --------------------------------------------------------------------------
# Failure modes
# --------------------------------------------------------------------------

def test_invalid_rows_abort_before_anything_is_written(s3_client, athena):
    """Fail loud, and fail before the first byte lands - a partially written
    day is worse than no day."""
    bad = inventory_frame(1).astype({'total_fulfillable_quantity': object})
    bad.loc[0, 'total_fulfillable_quantity'] = 'not-a-number'

    with pytest.raises(ValidationFailed):
        publish({'2026-08-07': inventory_frame(), '2026-08-08': bad},
                INVENTORY_DETAILS, s3_client=s3_client, bucket=BUCKET, athena=athena)

    assert s3_client.puts == []
    assert athena.calls == []


def test_validation_error_names_the_partition_and_the_field(s3_client, athena):
    bad = inventory_frame(1).astype({'total_fulfillable_quantity': object})
    bad.loc[0, 'total_fulfillable_quantity'] = 'not-a-number'

    with pytest.raises(ValidationFailed) as excinfo:
        publish({'2026-08-07': bad}, INVENTORY_DETAILS, s3_client=s3_client,
                bucket=BUCKET, athena=athena)

    message = str(excinfo.value)
    assert '2026-08-07' in message
    assert 'total_fulfillable_quantity' in message


def test_s3_failure_raises_instead_of_being_logged_and_ignored(s3_client, athena):
    """write_df_to_s3 currently catches ClientError, logs it and returns, so the
    job goes green having written nothing and then repairs the table anyway."""
    s3_client.error = ClientError({'Error': {'Code': 'AccessDenied'}}, 'PutObject')

    with pytest.raises(ClientError):
        publish({'2026-08-07': inventory_frame()}, INVENTORY_DETAILS,
                s3_client=s3_client, bucket=BUCKET, athena=athena)

    assert athena.calls == []


def test_table_is_not_repaired_if_a_later_partition_fails(s3_client, athena):
    class FailSecond:
        def __init__(self):
            self.puts = []

        def put_object(self, **kwargs):
            self.puts.append(kwargs)
            if len(self.puts) == 2:
                raise ClientError({'Error': {'Code': 'AccessDenied'}}, 'PutObject')
            return {}

    failing = FailSecond()
    with pytest.raises(ClientError):
        publish({'2026-08-07': inventory_frame(), '2026-08-08': inventory_frame()},
                INVENTORY_DETAILS, s3_client=failing, bucket=BUCKET, athena=athena)

    assert athena.calls == []


def test_frame_missing_a_contract_column_is_rejected(s3_client, athena):
    with pytest.raises(ValidationFailed):
        publish({'2026-08-07': inventory_frame().drop(columns=['is_active'])},
                INVENTORY_DETAILS, s3_client=s3_client, bucket=BUCKET, athena=athena)


# --------------------------------------------------------------------------
# split_by_date - replaces the day-walking loop in the orders job
# --------------------------------------------------------------------------

def test_splits_rows_into_one_partition_per_calendar_day():
    df = pd.DataFrame({'purchase_date': pd.to_datetime(
        ['2026-08-07T01:00:00Z', '2026-08-07T23:00:00Z', '2026-08-08T05:00:00Z'])})

    partitions = split_by_date(df, 'purchase_date')

    assert sorted(partitions) == ['2026-08-07', '2026-08-08']
    assert len(partitions['2026-08-07']) == 2
    assert len(partitions['2026-08-08']) == 1


def test_drops_rows_outside_the_requested_window():
    """The API filters on *created* date, so a response can contain an order
    purchased weeks earlier. Writing it would overwrite that old partition with
    a one-row file, so those rows are dropped - same as today's day loop, but
    deliberate."""
    df = pd.DataFrame({'purchase_date': pd.to_datetime(
        ['2026-07-01T01:00:00Z', '2026-08-07T01:00:00Z'])})

    partitions = split_by_date(df, 'purchase_date', window=('2026-08-07', '2026-08-08'))

    assert sorted(partitions) == ['2026-08-07']


def test_window_is_inclusive_of_both_ends():
    df = pd.DataFrame({'purchase_date': pd.to_datetime(
        ['2026-08-07T00:00:00Z', '2026-08-08T00:00:00Z'])})

    partitions = split_by_date(df, 'purchase_date', window=('2026-08-07', '2026-08-08'))

    assert sorted(partitions) == ['2026-08-07', '2026-08-08']


def test_empty_frame_splits_to_no_partitions():
    df = pd.DataFrame({'purchase_date': pd.to_datetime([])})

    assert split_by_date(df, 'purchase_date') == {}


def test_partition_column_is_not_leaked_into_the_written_csv(s3_client, athena):
    """order_date is a Hive partition key derived from the S3 prefix; it must
    not also appear as a data column."""
    df = inventory_frame()
    partitions = {'2026-08-07': df}

    publish(partitions, INVENTORY_DETAILS, s3_client=s3_client, bucket=BUCKET,
            athena=athena)

    columns = list(read_csv(s3_client.puts[0]['Body']).columns)
    assert 'partition_date' not in columns
    assert 'order_date' not in columns


# --------------------------------------------------------------------------
# Dataset definitions stay wired to the right models
# --------------------------------------------------------------------------

def test_datasets_declare_their_table_and_model():
    assert INVENTORY_DETAILS.table == 'shipbob_inventory_details'
    assert INVENTORY_DETAILS.model is ShipbobInventory
    assert ORDER_DETAILS.table == 'shipbob_order_details'
