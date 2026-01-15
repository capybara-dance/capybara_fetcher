"""
Unit tests for KISAuth rate limit handling.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from capybara_fetcher.providers.korea_investment_auth import KISAuth


class _DummyResponse:
    def __init__(self, *, status_code: int, json_data: dict | None, text: str = ""):
        self.status_code = int(status_code)
        self._json_data = json_data
        self.text = text

    def json(self) -> dict:
        if self._json_data is None:
            raise ValueError("no json")
        return self._json_data


def _mk_auth(*, max_retries: int = 2) -> KISAuth:
    auth = KISAuth(
        appkey="k",
        appsecret="s",
        max_requests_per_second=0,  # disable limiter sleeps for tests
        max_retries=max_retries,
        backoff_base_seconds=0.01,
        backoff_max_seconds=0.01,
    )
    auth.token = "t"
    auth.token_expire = datetime.now() + timedelta(days=365)
    return auth


def test_fetch_api_retries_on_http_500_rate_limit(monkeypatch):
    auth = _mk_auth(max_retries=2)

    responses = [
        _DummyResponse(
            status_code=500,
            json_data={"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."},
            text='{"rt_cd":"1","msg_cd":"EGW00201","msg1":"초당 거래건수를 초과하였습니다."}',
        ),
        _DummyResponse(
            status_code=200,
            json_data={"rt_cd": "0", "output2": [{"stck_bsop_date": "20240102"}]},
            text='{"rt_cd":"0"}',
        ),
    ]

    calls: list[str] = []

    def fake_get(_url, headers=None, params=None):
        calls.append("get")
        return responses.pop(0)

    sleeps: list[float] = []

    def fake_sleep(seconds: float):
        sleeps.append(float(seconds))

    monkeypatch.setattr(auth._session, "get", fake_get)
    monkeypatch.setattr("capybara_fetcher.providers.korea_investment_auth.time.sleep", fake_sleep)

    out = auth.fetch_api("/x", "TR", {})
    assert out["rt_cd"] == "0"
    assert len(calls) == 2
    assert len(sleeps) >= 1


def test_fetch_api_retries_on_200_api_rate_limit(monkeypatch):
    auth = _mk_auth(max_retries=2)

    responses = [
        _DummyResponse(
            status_code=200,
            json_data={"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."},
            text='{"rt_cd":"1","msg_cd":"EGW00201","msg1":"초당 거래건수를 초과하였습니다."}',
        ),
        _DummyResponse(
            status_code=200,
            json_data={"rt_cd": "0", "output2": [{"stck_bsop_date": "20240102"}]},
            text='{"rt_cd":"0"}',
        ),
    ]

    def fake_get(_url, headers=None, params=None):
        return responses.pop(0)

    sleeps: list[float] = []

    def fake_sleep(seconds: float):
        sleeps.append(float(seconds))

    monkeypatch.setattr(auth._session, "get", fake_get)
    monkeypatch.setattr("capybara_fetcher.providers.korea_investment_auth.time.sleep", fake_sleep)

    out = auth.fetch_api("/x", "TR", {})
    assert out["rt_cd"] == "0"
    assert len(sleeps) >= 1


def test_fetch_api_non_rate_limit_http_error_raises(monkeypatch):
    auth = _mk_auth(max_retries=3)

    def fake_get(_url, headers=None, params=None):
        return _DummyResponse(
            status_code=500,
            json_data={"rt_cd": "1", "msg_cd": "E_OTHER", "msg1": "서버 오류"},
            text='{"rt_cd":"1","msg_cd":"E_OTHER","msg1":"서버 오류"}',
        )

    sleeps: list[float] = []

    def fake_sleep(seconds: float):
        sleeps.append(float(seconds))

    monkeypatch.setattr(auth._session, "get", fake_get)
    monkeypatch.setattr("capybara_fetcher.providers.korea_investment_auth.time.sleep", fake_sleep)

    with pytest.raises(RuntimeError) as e:
        auth.fetch_api("/x", "TR", {})

    # Should fail fast (no backoff sleeps) for non-rate-limit HTTP 500
    assert sleeps == []
    assert "HTTP error" in str(e.value)

