"""
Tests for scripts/build_krx_stock_master.py
"""
import sys
from pathlib import Path
import pandas as pd
import pytest

# Add the scripts directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from build_krx_stock_master import _update_names_from_fdr


def test_update_names_from_fdr_basic():
    """Test that _update_names_from_fdr updates stock names correctly."""
    # Create a sample DataFrame with some stock codes
    df = pd.DataFrame({
        'Code': ['005930', '000660', '005380'],
        'Name': ['Old Name 1', 'Old Name 2', 'Old Name 3'],
        'Market': ['KOSPI', 'KOSPI', 'KOSPI'],
    })
    
    # Call the function (this requires network access)
    result = _update_names_from_fdr(df, market='KOSPI')
    
    # Verify the function returns a DataFrame
    assert isinstance(result, pd.DataFrame)
    
    # Verify the shape is unchanged
    assert result.shape == df.shape
    
    # Verify columns are preserved
    assert list(result.columns) == list(df.columns)
    
    # Verify the stock codes are unchanged
    assert result['Code'].tolist() == df['Code'].tolist()


@pytest.mark.external
def test_update_names_from_fdr_kospi():
    """Test updating KOSPI stock names from FDR (requires network)."""
    # Create a sample DataFrame with known KOSPI stocks
    df = pd.DataFrame({
        'Code': ['005930', '000660'],  # Samsung Electronics, SK Hynix
        'Name': ['삼성전자 옛날이름', 'SK하이닉스 옛날이름'],
        'Market': ['KOSPI', 'KOSPI'],
        'IndustryLarge': ['전기전자', '전기전자'],
    })
    
    # Update names from FDR
    result = _update_names_from_fdr(df, market='KOSPI')
    
    # Verify that names were updated (they should be different from the original)
    # Note: We can't hardcode exact names as they may change, but we can verify they changed
    assert result.shape == df.shape
    assert '005930' in result['Code'].values
    assert '000660' in result['Code'].values


@pytest.mark.external
def test_update_names_from_fdr_kosdaq():
    """Test updating KOSDAQ stock names from FDR (requires network)."""
    # Create a sample DataFrame with a known KOSDAQ stock
    df = pd.DataFrame({
        'Code': ['196170'],  # 알테오젠 (a KOSDAQ stock)
        'Name': ['Old Name'],
        'Market': ['KOSDAQ'],
        'IndustryLarge': ['의료정밀'],
    })
    
    # Update names from FDR
    result = _update_names_from_fdr(df, market='KOSDAQ')
    
    # Verify the structure is preserved
    assert result.shape == df.shape
    assert '196170' in result['Code'].values


@pytest.mark.external
def test_update_names_from_fdr_specific_stock():
    """Test that code 240810 name is correctly updated from '원익아이피에스' to '원익IPS'."""
    # Create a DataFrame with stock code 240810 (원익IPS)
    df = pd.DataFrame({
        'Code': ['240810'],
        'Name': ['원익아이피에스'],  # Old incorrect name
        'Market': ['KOSDAQ'],
        'IndustryLarge': ['전기전자'],
    })
    
    # Update names from FDR
    result = _update_names_from_fdr(df, market='KOSDAQ')
    
    # Verify the name was updated correctly
    assert result.shape == df.shape
    assert result['Code'].iloc[0] == '240810'
    assert result['Name'].iloc[0] == '원익IPS', f"Expected '원익IPS' but got '{result['Name'].iloc[0]}'"
    
    # Verify other columns are preserved
    assert result['Market'].iloc[0] == 'KOSDAQ'
    assert result['IndustryLarge'].iloc[0] == '전기전자'


def test_update_names_from_fdr_empty_dataframe():
    """Test that empty DataFrame is handled correctly."""
    df = pd.DataFrame(columns=['Code', 'Name', 'Market'])
    
    result = _update_names_from_fdr(df, market='KOSPI')
    
    # Verify empty DataFrame is returned
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_update_names_from_fdr_preserves_other_columns():
    """Test that other columns are preserved during name update."""
    df = pd.DataFrame({
        'Code': ['005930'],
        'Name': ['Old Name'],
        'Market': ['KOSPI'],
        'IndustryLarge': ['전기전자'],
        'IndustryMid': ['반도체'],
        'SharesOutstanding': [1000000],
    })
    
    result = _update_names_from_fdr(df, market='KOSPI')
    
    # Verify all columns are preserved
    assert set(result.columns) == set(df.columns)
    
    # Verify non-Name columns are unchanged
    assert result['Code'].iloc[0] == '005930'
    assert result['Market'].iloc[0] == 'KOSPI'
    assert result['IndustryLarge'].iloc[0] == '전기전자'
    assert result['IndustryMid'].iloc[0] == '반도체'
    assert result['SharesOutstanding'].iloc[0] == 1000000


# ─────────────────────────────────────────────────────────────
# Delisted universe (survivorship bias)
#
# The master is built from a Seibro Excel snapshot of *currently listed* stocks, so
# delisted names were never even queried. That biases more than the missing rows:
# MRS_* are cross-sectional percentile ranks computed over whatever is in the frame,
# so ranking survivors only distorts the survivors' percentiles too.
# ─────────────────────────────────────────────────────────────

from build_krx_stock_master import _fetch_delisted_data, _merge_delisted  # noqa: E402


def _delisted_row(code, market="KOSDAQ", date="2020-01-02", name="폐지사"):
    return {
        "Code": code,
        "Name": name,
        "Market": market,
        "IndustryLarge": None,
        "IndustryMid": None,
        "IndustrySmall": None,
        "SharesOutstanding": 1000.0,
        "DelistingDate": date,
        "DelistingReason": "테스트",
    }


def _live_row(code, market="KOSDAQ", name="상장사", industry="IT"):
    return {
        "Code": code,
        "Name": name,
        "Market": market,
        "IndustryLarge": industry,
        "IndustryMid": industry,
        "IndustrySmall": industry,
        "SharesOutstanding": 9999.0,
    }


def test_merge_delisted_appends_names_not_in_the_master():
    master = pd.DataFrame([_live_row("005930")])
    delisted = pd.DataFrame([_delisted_row("004320")])

    out, appended, marked = _merge_delisted(master, delisted)

    assert appended == 1 and marked == 0
    assert set(out["Code"]) == {"005930", "004320"}
    assert out.loc[out["Code"] == "004320", "DelistingDate"].item() == "2020-01-02"


def test_merge_delisted_marks_stale_excel_rows_instead_of_duplicating():
    """**The Seibro Excel snapshot goes stale.**

    At the time of writing, 61 names KRX had already delisted in 2026 were still in it.
    A plain concat + drop_duplicates would keep the Excel row and leave them looking
    alive — exactly the bias this change removes.
    """
    master = pd.DataFrame([_live_row("032980", name="바이온")])
    delisted = pd.DataFrame([_delisted_row("032980", date="2026-07-01", name="바이온")])

    out, appended, marked = _merge_delisted(master, delisted)

    assert appended == 0, "이미 있는 코드를 또 넣으면 안 된다"
    assert marked == 1
    assert len(out) == 1
    assert out["DelistingDate"].item() == "2026-07-01"


def test_merge_delisted_keeps_the_richer_excel_fields():
    """The Excel row wins on industry/shares — the delisting listing has neither
    in the master's taxonomy."""
    master = pd.DataFrame([_live_row("032980", industry="산업재")])
    delisted = pd.DataFrame([_delisted_row("032980")])

    out, _, _ = _merge_delisted(master, delisted)

    assert out["IndustryLarge"].item() == "산업재"
    assert out["SharesOutstanding"].item() == 9999.0


def test_merge_delisted_treats_the_same_code_on_another_market_as_distinct():
    master = pd.DataFrame([_live_row("003670", market="KOSPI")])
    delisted = pd.DataFrame([_delisted_row("003670", market="KOSDAQ")])

    out, appended, marked = _merge_delisted(master, delisted)

    assert appended == 1 and marked == 0
    assert len(out) == 2


def test_merge_delisted_leaves_live_rows_unmarked():
    master = pd.DataFrame([_live_row("005930"), _live_row("000660")])
    delisted = pd.DataFrame([_delisted_row("004320")])

    out, _, _ = _merge_delisted(master, delisted)

    live = out[out["Code"].isin(["005930", "000660"])]
    assert live["DelistingDate"].isna().all(), (
        "상장 중인 종목에 폐지일이 붙으면 시계열이 잘린 것으로 오해된다"
    )


@pytest.mark.external
def test_fetch_delisted_data_returns_master_shaped_rows():
    """Requires network. One bulk query, no per-ticker loop."""
    df = _fetch_delisted_data(since="2015-01-01")

    assert not df.empty
    assert set(df.columns) == {
        "Code", "Name", "Market", "IndustryLarge", "IndustryMid",
        "IndustrySmall", "SharesOutstanding", "DelistingDate", "DelistingReason",
    }
    assert df["Code"].str.len().eq(6).all()
    for col in ["Code", "Name", "Market", "DelistingDate", "SharesOutstanding"]:
        assert df[col].notna().all(), f"{col} 에 결측이 있으면 안 된다"


@pytest.mark.external
def test_fetch_delisted_data_excludes_konex_by_default():
    """KONEX is not part of the listed universe this cache covers."""
    assert set(_fetch_delisted_data(since="2015-01-01")["Market"]) <= {"KOSPI", "KOSDAQ"}
    assert "KONEX" in set(
        _fetch_delisted_data(since="2015-01-01", include_konex=True)["Market"]
    )


@pytest.mark.external
def test_fetch_delisted_data_keeps_only_common_stock():
    """The listing also carries 신주인수권증서 · 수익증권 · 선박투자회사 etc."""
    df = _fetch_delisted_data(since="2015-01-01")
    # 주권 only — a few hundred, not the full ~1,600 rows of every security type.
    assert 300 < len(df) < 900


@pytest.mark.external
def test_fetch_delisted_data_respects_the_since_bound():
    recent = _fetch_delisted_data(since="2024-01-01")
    assert (recent["DelistingDate"] >= "2024-01-01").all()
    assert len(recent) < len(_fetch_delisted_data(since="2015-01-01"))


# ─────────────────────────────────────────────────────────────
# 회귀 (PR #40 리뷰)
# ─────────────────────────────────────────────────────────────

import json  # noqa: E402

from build_krx_stock_master import (  # noqa: E402
    DelistedFetchError,
    _write_master_json,
)


def test_fetch_delisted_data_raises_when_the_listing_fails(monkeypatch):
    """**빈 프레임으로 물러서면 안 된다.**

    `--include-delisted` 가 기본값이라, 네트워크 오류를 빈 결과로 바꾸면 빌드가 exit 0
    으로 끝나고 **생존편향을 그대로 가진 마스터가 "폐지 포함"처럼 릴리즈된다.**
    출력 어디에도 빠졌다는 말이 없다.
    """
    import build_krx_stock_master as mod

    monkeypatch.setattr(
        mod.fdr,
        "StockListing",
        lambda *a, **k: (_ for _ in ()).throw(ConnectionError("boom")),
    )

    with pytest.raises(DelistedFetchError, match="--no-delisted"):
        mod._fetch_delisted_data()


def test_fetch_delisted_data_raises_on_an_empty_listing(monkeypatch):
    """빈 응답은 시장에 대한 사실이 아니라 조회 실패다 — KRX 에 4,200건이 있다."""
    import build_krx_stock_master as mod

    monkeypatch.setattr(mod.fdr, "StockListing", lambda *a, **k: pd.DataFrame())

    with pytest.raises(DelistedFetchError, match="empty"):
        mod._fetch_delisted_data()


def test_fetch_delisted_data_raises_when_the_schema_changes(monkeypatch):
    """목록은 왔는데 필터 후 0건 → 스키마가 바뀐 것이다."""
    import build_krx_stock_master as mod

    monkeypatch.setattr(
        mod.fdr,
        "StockListing",
        lambda *a, **k: pd.DataFrame(
            {
                "Symbol": ["004320"],
                "Name": ["울트라건설"],
                "Market": ["KOSPI"],
                "SecuGroup": ["신주인수권증서"],  # 주권이 아니다
                "DelistingDate": ["2015-04-13"],
                "Reason": ["x"],
                "ListingShares": [100],
            }
        ),
    )

    with pytest.raises(DelistedFetchError, match="schema change"):
        mod._fetch_delisted_data()


def _reject_constants(constant: str) -> None:
    raise ValueError(f"non-standard {constant}")


def test_write_master_json_emits_null_not_nan(tmp_path):
    """pandas 의 NaN 을 그대로 쓰면 `NaN` 토큰이 되어 **유효한 JSON 이 아니다.**

    엄격한 파서가 거부하고, README 의 "상장 중인 종목은 null" 계약과도 어긋난다.
    `main()` 이 쓰는 바로 그 함수를 시험한다 — 직렬화 패턴을 테스트가 재현하면
    `main()` 이 다른 짓을 해도 안 잡힌다.
    """
    merged, _, _ = _merge_delisted(
        pd.DataFrame([_live_row("005930")]),
        pd.DataFrame([_delisted_row("004320", market="KOSPI")]),
    )
    assert merged["DelistingDate"].isna().any(), "라이브 행에 결측이 있어야 하는 상황"

    path = tmp_path / "master.json"
    _write_master_json(merged, path)

    assert "NaN" not in path.read_text(encoding="utf-8")
    loaded = json.loads(path.read_text(encoding="utf-8"), parse_constant=_reject_constants)
    live = next(r for r in loaded if r["Code"] == "005930")
    assert live["DelistingDate"] is None
    assert live["DelistingReason"] is None


def test_committed_master_json_is_standard_json():
    """저장소에 커밋된 마스터 자체를 지킨다.

    함수만 시험하면 **이미 잘못 쓰인 파일**은 계속 남는다. 실제로 그랬다.
    """
    path = Path(__file__).parent.parent / "data" / "krx_stock_master.json"
    text = path.read_text(encoding="utf-8")

    records = json.loads(text, parse_constant=_reject_constants)
    assert records, "마스터가 비어 있다"

    live = [r for r in records if r.get("DelistingDate") is None]
    dead = [r for r in records if r.get("DelistingDate") is not None]
    assert live and dead, "상장 종목과 폐지 종목이 모두 있어야 한다"
