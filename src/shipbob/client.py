"""HTTP transport for the ShipBob API.

Version 1.0 paginated through `Next-Page` response headers. 2026-07 dropped
those in favour of two schemes, so pagination lives here rather than being
re-implemented in every extract function:

    paginate_cursor   GET /inventory, /inventory-level  -> {"items": [...], "next": url}
    paginate_pages    GET /order                        -> bare list + Page/Limit params

ShipBob allows 150 requests/minute and returns `x-retry-after` on a 429.
"""
import time
from typing import Any, Callable, Dict, Iterator, Optional

import requests
from loguru import logger

BASE_URL = 'https://api.shipbob.com'
API_VERSION = '2026-07'
PAGE_SIZE = 250

# 429 is rate limiting; 5xx is worth another go. Everything else is our fault.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class ShipBobError(RuntimeError):
    """A ShipBob request failed and will not be retried."""


class ShipBobClient:
    """A ShipBob REST client pinned to a single API version.

    Args:
        token: Personal Access Token, sent as a bearer token.
        version: API version segment. Bumping this is the whole migration.
        max_retries: total attempts per request, including the first.
        sleep: injected for tests; defaults to time.sleep.
    """

    def __init__(self, token: str, *, version: str = API_VERSION,
                 base_url: str = BASE_URL, session: Optional[requests.Session] = None,
                 max_retries: int = 5, timeout: int = 60,
                 sleep: Callable[[float], None] = time.sleep):
        if not token:
            raise ValueError('A ShipBob API token is required')
        self.version = version
        self.root = f"{base_url.rstrip('/')}/{version}"
        self.max_retries = max_retries
        self.timeout = timeout
        self._sleep = sleep
        self._session = session or requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
        })

    # -- requests ---------------------------------------------------------

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET a path (or an absolute URL, as returned in `next`) and return JSON.

        Raises ShipBobError on any non-2xx response that survives retries.
        """
        url = path if path.startswith('http') else f"{self.root}/{path.lstrip('/')}"

        for attempt in range(1, self.max_retries + 1):
            response = self._session.get(url, params=params, timeout=self.timeout)
            if response.ok:
                return response.json()

            retryable = response.status_code in RETRYABLE_STATUSES
            if not retryable or attempt == self.max_retries:
                raise ShipBobError(
                    f'{response.status_code} from {url} '
                    f'(API version {self.version}): {response.text[:500]}')

            delay = self._retry_delay(response, attempt)
            logger.warning(f'{response.status_code} from {url}; '
                           f'retry {attempt}/{self.max_retries - 1} in {delay}s')
            self._sleep(delay)

    @staticmethod
    def _retry_delay(response: requests.Response, attempt: int) -> float:
        """Honour ShipBob's x-retry-after, else back off exponentially."""
        header = response.headers.get('x-retry-after') or response.headers.get('Retry-After')
        if header:
            try:
                return float(header)
            except ValueError:
                pass
        return float(2 ** attempt)

    # -- pagination -------------------------------------------------------

    def paginate_cursor(self, path: str, params: Optional[Dict[str, Any]] = None,
                        page_size_param: Optional[str] = 'PageSize',
                        page_size: int = PAGE_SIZE) -> Iterator[dict]:
        """Yield `items` across every page, following the `next` URL until null.

        Page size is spelled differently per resource (`PageSize` on the
        inventory endpoints, `RecordsPerPage` on /channel), so it is named
        rather than hard-coded; pass None to leave it off entirely.
        """
        query = dict(params or {})
        if page_size_param:
            query[page_size_param] = page_size
        payload = self.get(path, params=query)
        pages = 0
        while True:
            pages += 1
            yield from (payload.get('items') or [])
            next_url = payload.get('next')
            if not next_url:
                logger.info(f'{path}: exhausted after {pages} page(s)')
                return
            payload = self.get(next_url)

    def paginate_pages(self, path: str, params: Optional[Dict[str, Any]] = None,
                       limit: int = PAGE_SIZE) -> Iterator[dict]:
        """Yield records across every page of a Page/Limit endpoint.

        A page shorter than `limit` is the last one, so we stop without spending
        a request to prove it.
        """
        page = 1
        while True:
            records = self.get(path, params={**(params or {}), 'Page': page,
                                             'Limit': limit}) or []
            yield from records
            if len(records) < limit:
                logger.info(f'{path}: exhausted after {page} page(s)')
                return
            page += 1
