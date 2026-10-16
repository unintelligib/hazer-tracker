import json
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hazer import nea_client as nc  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> str:
    return (FIXTURES / name).read_text()


class FakeResponse:
    def __init__(self, status=200, text="", headers=None):
        self.status_code = status
        self.text = text
        self.headers = headers or {}


class FakeSession:
    """Returns queued responses (or raises queued exceptions) in order."""

    def __init__(self, queue):
        self.queue = list(queue)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def no_sleep(_):
    pass


def test_fixtures_are_valid_bodies():
    assert nc.validate_body(load("psi_sample.json")) is None
    assert nc.validate_body(load("pm25_sample.json")) is None


def test_reading_timestamps_match_sample():
    assert nc.reading_timestamps(load("psi_sample.json")) == ["2026-10-14T20:00:00+08:00"]
    assert nc.reading_timestamps(load("pm25_sample.json")) == ["2026-10-14T21:00:00+08:00"]


def test_success_first_try():
    session = FakeSession([FakeResponse(200, load("psi_sample.json"))])
    res = nc.fetch("psi", session=session, sleep=no_sleep)
    assert res.ok and res.http_status == 200 and res.error is None
    assert res.payload == load("psi_sample.json")  # stored untouched
    assert session.calls[0]["url"].endswith("/psi")


def test_retries_429_then_succeeds_and_honors_retry_after():
    slept = []
    session = FakeSession(
        [FakeResponse(429, "", {"Retry-After": "30"}), FakeResponse(200, load("pm25_sample.json"))]
    )
    res = nc.fetch("pm25", session=session, sleep=slept.append)
    assert res.ok
    assert len(session.calls) == 2
    assert slept == [30.0]


def test_gives_up_after_max_retries_and_reports_failure():
    session = FakeSession([FakeResponse(503, "down")] * 3)
    res = nc.fetch("psi", session=session, max_retries=2, sleep=no_sleep)
    assert not res.ok and res.http_status == 503 and res.error == "HTTP 503"
    assert len(session.calls) == 3


def test_does_not_retry_404():
    session = FakeSession([FakeResponse(404, "nope")])
    res = nc.fetch("psi", session=session, sleep=no_sleep)
    assert not res.ok and res.http_status == 404
    assert len(session.calls) == 1


def test_network_error_is_retried_then_recovers():
    session = FakeSession([requests.ConnectionError("boom"), FakeResponse(200, load("psi_sample.json"))])
    res = nc.fetch("psi", session=session, sleep=no_sleep)
    assert res.ok and len(session.calls) == 2


def test_http_200_with_error_code_is_a_failure():
    body = json.dumps({"code": 1, "data": None, "errorMsg": "bad"})
    session = FakeSession([FakeResponse(200, body)] * 5)
    res = nc.fetch("psi", session=session, sleep=no_sleep)
    assert not res.ok and "code=1" in res.error
    assert res.payload == body  # raw body kept for debugging


def test_api_key_sent_as_header():
    session = FakeSession([FakeResponse(200, load("psi_sample.json"))])
    nc.fetch("psi", api_key="abc", session=session, sleep=no_sleep)
    assert session.calls[0]["headers"] == {"x-api-key": "abc"}


def test_unknown_endpoint_rejected():
    with pytest.raises(ValueError):
        nc.fetch("weather")


def test_pagination_followed_for_date_requests():
    page1 = json.loads(load("pm25_sample.json"))
    page1["data"]["paginationToken"] = "tok2"
    session = FakeSession(
        [FakeResponse(200, json.dumps(page1)), FakeResponse(200, load("pm25_sample.json"))]
    )
    results = nc.fetch_all_pages("pm25", date="2026-10-14", session=session, sleep=no_sleep)
    assert [r.page for r in results] == [1, 2]
    assert session.calls[1]["params"]["paginationToken"] == "tok2"
    assert session.calls[1]["params"]["date"] == "2026-10-14"


def test_latest_request_is_single_page():
    session = FakeSession([FakeResponse(200, load("psi_sample.json"))])
    results = nc.fetch_all_pages("psi", session=session, sleep=no_sleep)
    assert len(results) == 1


def test_pagination_stops_at_failed_page():
    session = FakeSession([FakeResponse(404, "x")])
    results = nc.fetch_all_pages("psi", date="2026-10-14", session=session, sleep=no_sleep)
    assert len(results) == 1 and not results[0].ok
