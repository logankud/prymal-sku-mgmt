"""Transport behaviours: auth, version pinning, both pagination styles, error handling.

Today's code paginates off the 1.0 `Next-Page` response header and calls
json.loads(response.text) without checking the status code, so an auth failure
surfaces as a JSONDecodeError. 2026-07 removed those headers entirely, so the
client owns pagination and must fail loudly.
"""
import pytest
import responses

from conftest import API, cursor_page, inventory_level, order

from shipbob.client import ShipBobClient, ShipBobError


# --------------------------------------------------------------------------
# Auth + versioning
# --------------------------------------------------------------------------

@responses.activate
def test_sends_bearer_token_and_targets_pinned_version(client, token):
    responses.get(f'{API}/inventory-level', json=cursor_page([]))

    list(client.paginate_cursor('inventory-level'))

    assert responses.calls[0].request.headers['Authorization'] == f'Bearer {token}'
    assert responses.calls[0].request.url.startswith(f'{API}/inventory-level')


@responses.activate
def test_api_version_is_overridable_without_touching_call_sites(token):
    responses.get('https://api.shipbob.com/2027-01/inventory-level', json=cursor_page([]))

    client = ShipBobClient(token, version='2027-01', sleep=lambda _: None)
    list(client.paginate_cursor('inventory-level'))

    assert responses.calls[0].request.url.startswith(
        'https://api.shipbob.com/2027-01/inventory-level')


# --------------------------------------------------------------------------
# Cursor pagination (GET /inventory, /inventory-level)
# --------------------------------------------------------------------------

@responses.activate
def test_cursor_pagination_follows_next_url_until_null(client):
    page_two = f'{API}/inventory-level?Cursor=abc123'
    responses.get(f'{API}/inventory-level',
                  json=cursor_page([inventory_level(inventory_id=1)], next_url=page_two))
    responses.get(page_two,
                  json=cursor_page([inventory_level(inventory_id=2)], next_url=None))

    items = list(client.paginate_cursor('inventory-level'))

    assert [item['inventory_id'] for item in items] == [1, 2]


@responses.activate
def test_cursor_pagination_stops_on_single_page(client):
    responses.get(f'{API}/inventory-level',
                  json=cursor_page([inventory_level()], next_url=None))

    assert len(list(client.paginate_cursor('inventory-level'))) == 1
    assert len(responses.calls) == 1


@responses.activate
def test_cursor_pagination_tolerates_missing_or_null_items(client):
    responses.get(f'{API}/inventory-level', json={'items': None, 'next': None})

    assert list(client.paginate_cursor('inventory-level')) == []


# --------------------------------------------------------------------------
# Page/Limit pagination (GET /order)
# --------------------------------------------------------------------------

@responses.activate
def test_page_pagination_walks_pages_until_empty(client):
    responses.get(f'{API}/order', json=[order(order_id=1)])
    responses.get(f'{API}/order', json=[order(order_id=2)])
    responses.get(f'{API}/order', json=[])

    records = list(client.paginate_pages('order', params={'StartDate': '2026-08-07'},
                                         limit=1))

    assert [rec['id'] for rec in records] == [1, 2]
    pages = [responses.calls[i].request.params.get('Page') for i in range(3)]
    assert pages == ['1', '2', '3']


@responses.activate
def test_page_pagination_stops_on_short_page(client):
    """A page smaller than the requested Limit is the last page - don't spend a
    request proving it."""
    responses.get(f'{API}/order', json=[order(order_id=i) for i in range(3)])

    records = list(client.paginate_pages('order', limit=5))

    assert len(records) == 3
    assert len(responses.calls) == 1


@responses.activate
def test_page_pagination_forwards_caller_filters_on_every_page(client):
    responses.get(f'{API}/order', json=[order(order_id=1)])
    responses.get(f'{API}/order', json=[])

    list(client.paginate_pages('order', params={'StartDate': '2026-08-07',
                                                'EndDate': '2026-08-08'}, limit=1))

    for call in responses.calls:
        assert call.request.params['StartDate'] == '2026-08-07'
        assert call.request.params['EndDate'] == '2026-08-08'
        assert call.request.params['Limit'] == '1'


# --------------------------------------------------------------------------
# Failure handling
# --------------------------------------------------------------------------

@responses.activate
def test_auth_failure_raises_instead_of_returning_empty(client):
    """Today a 401 is parsed as JSON and yields an empty frame, so the job
    'succeeds' and writes nothing. It must raise."""
    responses.get(f'{API}/inventory-level', status=401,
                  json={'statusCode': 401, 'message': 'Unauthorized'})

    with pytest.raises(ShipBobError) as excinfo:
        list(client.paginate_cursor('inventory-level'))

    assert '401' in str(excinfo.value)


@responses.activate
def test_gone_version_raises_with_actionable_message(client):
    """API 1.0 is past end-of-support; a dead version must not look like 'no data'."""
    responses.get(f'{API}/inventory-level', status=410, json={'message': 'Gone'})

    with pytest.raises(ShipBobError):
        list(client.paginate_cursor('inventory-level'))


@responses.activate
def test_retries_429_honouring_x_retry_after(token):
    slept = []
    client = ShipBobClient(token, sleep=slept.append)
    responses.get(f'{API}/inventory-level', status=429,
                  headers={'x-retry-after': '7'},
                  json={'statusCode': 429, 'message': 'Rate limit is exceeded.'})
    responses.get(f'{API}/inventory-level', json=cursor_page([inventory_level()]))

    items = list(client.paginate_cursor('inventory-level'))

    assert len(items) == 1
    assert slept == [7.0]


@responses.activate
def test_retries_server_errors_then_succeeds(token):
    slept = []
    client = ShipBobClient(token, sleep=slept.append)
    responses.get(f'{API}/inventory-level', status=503, json={'message': 'unavailable'})
    responses.get(f'{API}/inventory-level', json=cursor_page([inventory_level()]))

    assert len(list(client.paginate_cursor('inventory-level'))) == 1
    assert len(slept) == 1


@responses.activate
def test_gives_up_after_max_retries(token):
    slept = []
    client = ShipBobClient(token, max_retries=3, sleep=slept.append)
    for _ in range(3):
        responses.get(f'{API}/inventory-level', status=503, json={'message': 'unavailable'})

    with pytest.raises(ShipBobError):
        list(client.paginate_cursor('inventory-level'))

    assert len(responses.calls) == 3


@responses.activate
def test_client_errors_are_not_retried(token):
    """A 400 will never succeed on retry - fail fast rather than burn the rate limit."""
    slept = []
    client = ShipBobClient(token, sleep=slept.append)
    responses.get(f'{API}/order', status=400, json={'message': 'bad request'})

    with pytest.raises(ShipBobError):
        list(client.paginate_pages('order'))

    assert len(responses.calls) == 1
    assert slept == []
