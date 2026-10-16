"""Thin, testable client for the NEA PSI and PM2.5 endpoints on data.gov.sg.

Design notes
------------
* The client never raises on a failed fetch. It returns a ``FetchResult`` with
  ``ok=False`` and an ``error`` message so the caller can still write a row to
  the bronze table. Failed runs are then visible as data (useful for the gap
  handling in silver) and not only as a failed job.
* Retries cover 429, 5xx and network errors, with exponential backoff and
  respect for a ``Retry-After`` header. Other 4xx responses are not retried.
* The API can answer HTTP 200 with a non-zero ``code`` in the body, so the body
  is validated too.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests

BASE_URL = "https://api-open.data.gov.sg/v2/real-time/api"
ENDPOINTS = {
    "psi": f"{BASE_URL}/psi",
    "pm25": f"{BASE_URL}/pm25",
}

RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Pagination is documented for date-filtered requests only. The exact field name
# of the token in the response has not been confirmed against a live paginated
# response yet, so both spellings are accepted (see README "Open questions").
_TOKEN_KEYS = ("paginationToken", "pagination_token")


@dataclass
class FetchResult:
    endpoint: str
    request_date: Optional[str]  # YYYY-MM-DD or None for "latest"
    page: int
    fetched_at: datetime  # UTC
    http_status: Optional[int]
    ok: bool
    payload: Optional[str]  # raw response body, untouched
    error: Optional[str]

    def as_row(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "request_date": self.request_date,
            "page": self.page,
            "fetched_at": self.fetched_at,
            "http_status": self.http_status,
            "ok": self.ok,
            "payload": self.payload,
            "error": self.error,
        }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def validate_body(body: str) -> Optional[str]:
    """Return an error message if the body is not a usable API response."""
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        return f"invalid JSON: {exc}"
    if not isinstance(parsed, dict):
        return "unexpected response type"
    if parsed.get("code") != 0:
        return f"api error code={parsed.get('code')} msg={parsed.get('errorMsg')!r}"
    data = parsed.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        return "response has no data.items list"
    return None


def extract_pagination_token(body: str) -> Optional[str]:
    try:
        data = json.loads(body).get("data", {})
    except (json.JSONDecodeError, AttributeError):
        return None
    for key in _TOKEN_KEYS:
        token = data.get(key)
        if token:
            return str(token)
    return None


def reading_timestamps(body: str) -> list[str]:
    """Reading timestamps (as published, +08:00) found in a response body."""
    try:
        items = json.loads(body)["data"]["items"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return []
    return [i["timestamp"] for i in items if isinstance(i, dict) and "timestamp" in i]


def fetch(
    endpoint: str,
    date: Optional[str] = None,
    pagination_token: Optional[str] = None,
    page: int = 1,
    api_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
    max_retries: int = 4,
    backoff_seconds: float = 2.0,
    timeout: float = 20.0,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchResult:
    """Fetch one page from ``psi`` or ``pm25``. Never raises on API failure."""
    if endpoint not in ENDPOINTS:
        raise ValueError(f"unknown endpoint {endpoint!r}; expected one of {sorted(ENDPOINTS)}")

    http = session or requests.Session()
    headers = {"x-api-key": api_key} if api_key else {}
    params: dict[str, str] = {}
    if date:
        params["date"] = date
    if pagination_token:
        params["paginationToken"] = pagination_token

    last_status: Optional[int] = None
    last_error: Optional[str] = None
    last_body: Optional[str] = None

    for attempt in range(max_retries + 1):
        try:
            resp = http.get(ENDPOINTS[endpoint], params=params, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            last_status, last_body = None, None
            last_error = f"{type(exc).__name__}: {exc}"
        else:
            last_status, last_body = resp.status_code, resp.text
            if resp.status_code == 200:
                problem = validate_body(resp.text)
                if problem is None:
                    return FetchResult(endpoint, date, page, _now(), 200, True, resp.text, None)
                # A 200 with a bad body is not worth retrying hard, but one
                # retry covers truncated responses.
                last_error = problem
                if attempt >= 1:
                    break
            elif resp.status_code in RETRYABLE_STATUS:
                last_error = f"HTTP {resp.status_code}"
            else:
                last_error = f"HTTP {resp.status_code}"
                break  # non-retryable client error

        if attempt < max_retries:
            delay = backoff_seconds * (2**attempt)
            retry_after = None
            if last_status == 429:
                retry_after = resp.headers.get("Retry-After")  # type: ignore[possibly-undefined]
            if retry_after and retry_after.isdigit():
                delay = max(delay, float(retry_after))
            sleep(delay)

    return FetchResult(endpoint, date, page, _now(), last_status, False, last_body, last_error)


def fetch_all_pages(
    endpoint: str,
    date: Optional[str] = None,
    max_pages: int = 50,
    **kwargs: Any,
) -> list[FetchResult]:
    """Fetch a full day (``date`` set) following pagination, or just the latest reading.

    Stops at the first failed page and returns it, so callers see the failure.
    """
    results: list[FetchResult] = []
    token: Optional[str] = None
    for page in range(1, max_pages + 1):
        res = fetch(endpoint, date=date, pagination_token=token, page=page, **kwargs)
        results.append(res)
        if not res.ok or not date:
            break  # latest-reading requests are not paginated
        token = extract_pagination_token(res.payload or "")
        if not token:
            break
    return results
