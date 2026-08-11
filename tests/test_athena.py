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

    def __init__(self, states, reason=None, rows=None, error=None):
        self.states = list(states)
        self.reason = reason
        self.rows = rows if rows is not None else []
        self.error = error
        self.polls = 0

    def start_query_execution(self, **kwargs):
        self.query = kwargs['QueryString']
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
        header = [{'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}]}]
        body = [{'Data': [{'VarCharValue': str(i)}, {'VarCharValue': n}]}
                for i, n in self.rows]
        return {
            'ResultSet': {
                'ResultSetMetadata': {'ColumnInfo': [{'Name': 'id'}, {'Name': 'name'}]},
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
