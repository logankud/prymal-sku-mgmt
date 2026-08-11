"""run_athena_query must never hand back None.

Every caller does `len(df)` or indexes a column on the result, so returning
None on failure turns a real Athena error into an unrelated TypeError several
lines later, with the actual reason only visible in the log above it.
"""
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import utils
from utils import AthenaQueryError, run_athena_query

ARGS = ('SELECT * FROM shipbob_inventory_details', 'prymal', 'us-east-1', 'bucket')


class FakeAthena:
    """Stands in for boto3's Athena client with a scripted sequence of states."""

    def __init__(self, states, reason=None, rows=None, error=None,
                 columns=('id', 'name')):
        self.states = list(states)
        self.reason = reason
        self.rows = rows if rows is not None else []
        self.error = error
        self.columns = list(columns)
        self.polls = 0
        self.queries = []

    def start_query_execution(self, **kwargs):
        self.query = kwargs['QueryString']
        self.queries.append(self.query)
        return {'QueryExecutionId': 'qid-1'}

    def get_query_execution(self, **kwargs):
        self.polls += 1
        state = self.states.pop(0) if self.states else 'SUCCEEDED'
        status = {'State': state}
        if self.reason:
            status['StateChangeReason'] = self.reason
        return {'QueryExecution': {'Status': status}}

    def get_query_results(self, **kwargs):
        if self.error:
            raise self.error
        header = [{'Data': [{'VarCharValue': c} for c in self.columns]}]
        body = [{'Data': [{'VarCharValue': str(v)} if v is not None else {}
                          for v in row]} for row in self.rows]
        return {
            'ResultSet': {
                'ResultSetMetadata': {
                    'ColumnInfo': [{'Name': c} for c in self.columns]},
                'Rows': header + body,
            }
        }


@pytest.fixture
def athena_client(monkeypatch):
    """Swap boto3.client so run_athena_query talks to FakeAthena."""
    holder = {}

    def install(fake):
        holder['fake'] = fake
        monkeypatch.setattr(utils.boto3, 'client', lambda *a, **k: fake)
        return fake

    return install


def test_returns_dataframe_on_success(athena_client):
    fake = athena_client(FakeAthena(['SUCCEEDED'], rows=[(1, 'Vanilla')]))

    df = run_athena_query(*ARGS, sleep=lambda _: None)

    assert list(df.columns) == ['id', 'name']
    assert df.loc[0, 'name'] == 'Vanilla'


def test_polls_until_the_query_finishes(athena_client):
    slept = []
    fake = athena_client(FakeAthena(['QUEUED', 'RUNNING', 'RUNNING', 'SUCCEEDED']))

    run_athena_query(*ARGS, poll_seconds=2.0, sleep=slept.append)

    assert fake.polls == 4
    assert slept == [2.0, 2.0, 2.0]  # a sleep between polls, not a hot loop


def test_failed_query_raises_with_the_athena_reason(athena_client):
    athena_client(FakeAthena(['RUNNING', 'FAILED'],
                             reason='SYNTAX_ERROR: line 1:8: Column not found'))

    with pytest.raises(AthenaQueryError) as excinfo:
        run_athena_query(*ARGS, sleep=lambda _: None)

    assert 'FAILED' in str(excinfo.value)
    assert 'Column not found' in str(excinfo.value)
    assert 'shipbob_inventory_details' in str(excinfo.value)


def test_cancelled_query_raises_rather_than_reading_missing_results(athena_client):
    """A CANCELLED query used to fall out of the poll loop unlogged, then fail
    on get_query_results with 'Could not find results' and return None."""
    athena_client(FakeAthena(['RUNNING', 'CANCELLED'], reason='Query cancelled'))

    with pytest.raises(AthenaQueryError, match='CANCELLED'):
        run_athena_query(*ARGS, sleep=lambda _: None)


def test_unknown_terminal_state_raises(athena_client):
    athena_client(FakeAthena(['SOMETHING_NEW']))

    with pytest.raises(AthenaQueryError):
        run_athena_query(*ARGS, sleep=lambda _: None)


def test_client_error_raises_with_context(athena_client):
    athena_client(FakeAthena(
        ['SUCCEEDED'],
        error=ClientError({'Error': {'Code': 'InvalidRequestException',
                                     'Message': 'Could not find results'}},
                          'GetQueryResults')))

    with pytest.raises(AthenaQueryError) as excinfo:
        run_athena_query(*ARGS, sleep=lambda _: None)

    assert 'Could not find results' in str(excinfo.value)
    assert 'InvalidRequestException' in str(excinfo.value)


def test_empty_result_set_is_an_empty_frame_not_none(athena_client):
    """No matching partitions is a legitimate answer; callers guard on
    len(df) == 0 and must not get None instead."""
    athena_client(FakeAthena(['SUCCEEDED'], rows=[]))

    df = run_athena_query(*ARGS, sleep=lambda _: None)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

# --------------------------------------------------------------------------
# latest_partition - read the partition list from Glue, never from Athena
# --------------------------------------------------------------------------

class FakeGlue:
    """Stands in for boto3's Glue client with a fixed partition list."""

    def __init__(self, values, partition_keys=('partition_date',), pages=1):
        self.values = list(values)
        self.partition_keys = list(partition_keys)
        self.pages = pages

    def get_table(self, **kwargs):
        return {'Table': {'PartitionKeys': [{'Name': k} for k in self.partition_keys]}}

    def get_paginator(self, name):
        assert name == 'get_partitions'
        chunks = [self.values[i::self.pages] for i in range(self.pages)]
        return type('P', (), {
            'paginate': lambda _self, **kw: iter(
                [{'Partitions': [{'Values': v if isinstance(v, list) else [v]}
                                 for v in chunk]} for chunk in chunks])
        })()


@pytest.fixture
def glue_client(monkeypatch):
    def install(fake):
        monkeypatch.setattr(utils.boto3, 'client', lambda *a, **k: fake)
        return fake

    return install


def test_latest_partition_returns_the_max_value(glue_client):
    glue_client(FakeGlue(['2026-08-08', '2026-08-10', '2026-08-09']))

    assert utils.latest_partition('shipbob_inventory_details', 'prymal',
                                  'us-east-1') == '2026-08-10'


def test_latest_partition_never_runs_an_athena_query(glue_client, monkeypatch):
    """This is the whole point: Athena bills by bytes scanned and was
    cancelling on this lookup. Glue has no such notion."""
    def explode(*args, **kwargs):
        raise AssertionError('latest_partition must not query Athena')

    monkeypatch.setattr(utils, 'run_athena_query', explode)
    glue_client(FakeGlue(['2026-08-10']))

    assert utils.latest_partition('shipbob_inventory_details', 'prymal',
                                  'us-east-1') == '2026-08-10'


def test_latest_partition_applies_an_upper_bound(glue_client):
    glue_client(FakeGlue(['2026-07-25', '2026-07-26', '2026-08-10']))

    assert utils.latest_partition('shipbob_inventory_details', 'prymal',
                                  'us-east-1',
                                  on_or_before='2026-07-26') == '2026-07-26'


def test_latest_partition_is_none_when_nothing_matches_the_bound(glue_client):
    glue_client(FakeGlue(['2026-08-09', '2026-08-10']))

    assert utils.latest_partition('shipbob_inventory_details', 'prymal',
                                  'us-east-1', on_or_before='2026-07-01') is None


def test_latest_partition_is_none_when_the_table_has_no_partitions(glue_client):
    glue_client(FakeGlue([]))

    assert utils.latest_partition('shipbob_inventory_details', 'prymal',
                                  'us-east-1') is None


def test_latest_partition_walks_every_page(glue_client):
    """Glue pages at 1000 partitions; a table with years of daily data has more."""
    days = [f'2026-{m:02d}-{d:02d}' for m in range(1, 13) for d in range(1, 29)]
    glue_client(FakeGlue(days, pages=4))

    assert utils.latest_partition('t', 'prymal', 'us-east-1') == '2026-12-28'


def test_latest_partition_picks_the_right_key_in_a_composite_partition(glue_client):
    glue_client(FakeGlue([['shopify', '2026-08-09'], ['shopify', '2026-08-10']],
                         partition_keys=('channel', 'partition_date')))

    assert utils.latest_partition('t', 'prymal', 'us-east-1') == '2026-08-10'


def test_latest_partition_supports_other_partition_columns(glue_client):
    glue_client(FakeGlue(['2026-08-10'], partition_keys=('order_date',)))

    assert utils.latest_partition('shipbob_order_details', 'prymal', 'us-east-1',
                                  column='order_date') == '2026-08-10'


def test_unpartitioned_table_raises_a_clear_error(glue_client):
    """Diagnostic: if this fires, the table isn't partitioned the way the
    query assumes, which is worth knowing loudly."""
    glue_client(FakeGlue(['x'], partition_keys=()))

    with pytest.raises(ValueError, match='not partitioned'):
        utils.latest_partition('shipbob_inventory_details', 'prymal', 'us-east-1')
