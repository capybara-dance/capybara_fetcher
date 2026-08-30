"""폐지 종목이 **실제로 수집 대상 목록에 들어가는지** 지킨다.

`run_cache_build()` 는 `load_stock_master()` 의 행이 아니라 `list_tickers()` 를 순회한다.
그런데 거래소에 "무엇이 상장돼 있나"를 묻는 provider 는 **생존자만 답할 수 있다** —
`KoreaInvestmentProvider` 는 KIS 마스터를, `FdrProvider` 는 `fdr.StockListing()` 을 쓴다.

그래서 마스터에 폐지 종목을 넣는 것만으로는 **아무 일도 일어나지 않는다.** 마스터는
커지고 릴리즈는 그대로다. 실측: 마스터의 폐지 492건 중 `CompositeProvider.list_tickers()`
에 들어간 것은 **1건**이었다.

`fetch_ohlcv` 가 폐지 종목을 준다는 확인은 이 경로를 통과하지 않는다 — 받을 수 있다는
것과 **요청된다**는 것은 다르다.
"""

import pandas as pd
import pytest

from capybara_fetcher.providers.provider_utils import add_delisted_from_master


def _master(rows):
    return pd.DataFrame(rows)


def _live(code, market="KOSPI"):
    return {"Code": code, "Market": market, "DelistingDate": None}


def _dead(code, market="KOSPI", date="2020-01-02"):
    return {"Code": code, "Market": market, "DelistingDate": date}


def test_delisted_codes_are_added_to_the_live_list():
    tickers, mapping = add_delisted_from_master(
        ["005930"], {"005930": "KOSPI"}, _master([_live("005930"), _dead("004320")])
    )

    assert "004320" in tickers, "폐지 종목이 목록에 없으면 영원히 수집되지 않는다"
    assert mapping["004320"] == "KOSPI"


def test_live_tickers_are_never_dropped():
    tickers, _ = add_delisted_from_master(
        ["005930", "000660"], {}, _master([_dead("004320")])
    )
    assert {"005930", "000660"} <= set(tickers)


def test_live_market_label_wins_over_a_stale_master():
    """로컬 마스터는 낡을 수 있고 거래소 목록은 그렇지 않다."""
    tickers, mapping = add_delisted_from_master(
        ["003670"], {"003670": "KOSPI"}, _master([_dead("003670", market="KOSDAQ")])
    )
    assert mapping["003670"] == "KOSPI"
    assert tickers.count("003670") == 1


def test_market_filter_does_not_leak_other_markets():
    tickers, _ = add_delisted_from_master(
        ["069500"], {"069500": "ETF"}, _master([_dead("004320", market="KOSPI")]),
        market="ETF",
    )
    assert "004320" not in tickers


def test_result_is_sorted_and_unique():
    tickers, _ = add_delisted_from_master(
        ["005930", "004320"], {}, _master([_dead("004320"), _dead("000420")])
    )
    assert tickers == sorted(set(tickers))


def test_master_without_the_column_is_a_no_op():
    """`DelistingDate` 가 없는 예전 마스터로도 깨지지 않아야 한다."""
    old = pd.DataFrame([{"Code": "005930", "Market": "KOSPI"}])
    tickers, mapping = add_delisted_from_master(["005930"], {"005930": "KOSPI"}, old)
    assert tickers == ["005930"] and mapping == {"005930": "KOSPI"}


def test_master_with_no_delisted_rows_is_a_no_op():
    """`--no-delisted` 로 만든 마스터."""
    tickers, _ = add_delisted_from_master(
        ["005930"], {}, _master([_live("005930")])
    )
    assert tickers == ["005930"]


@pytest.mark.external
def test_composite_list_tickers_contains_every_delisted_code():
    """기본 provider 의 **실제** 목록을 본다. 네트워크가 필요하다."""
    from capybara_fetcher.providers.composite_provider import CompositeProvider

    provider = CompositeProvider()
    tickers, _ = provider.list_tickers()
    master = provider.load_stock_master()
    delisted = set(master.loc[master["DelistingDate"].notna(), "Code"])

    assert delisted, "마스터에 폐지 종목이 없다 — 먼저 build_krx_stock_master.py 를 돌릴 것"
    missing = delisted - set(tickers)
    assert not missing, f"수집 대상에서 빠진 폐지 종목 {len(missing)}건: {sorted(missing)[:5]}"
