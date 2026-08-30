import argparse
import json
import sys
import warnings
from pathlib import Path

import pandas as pd
import FinanceDataReader as fdr

# Add the parent directory to the path to import from capybara_fetcher
sys.path.insert(0, str(Path(__file__).parent.parent))

from capybara_fetcher.providers.fdr_provider import FdrProvider


def _read_master_xlsx(path: Path, market: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    # Normalize column names (strip whitespace)
    df.columns = [str(c).strip() for c in df.columns]

    required = ["종목코드", "종목명", "업종(대분류)", "업종(중분류)", "업종(소분류)", "발행주식수"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {path.name}: {missing}")

    shares = (
        pd.to_numeric(
            df["발행주식수"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip(),
            errors="coerce",
        )
        .round()
    )
    # Ensure JSON-serializable python ints (or None)
    shares_py = [int(x) if pd.notna(x) else None for x in shares.tolist()]

    out = pd.DataFrame(
        {
            "Code": df["종목코드"].astype(str).str.strip().str.zfill(6),
            "Name": df["종목명"].astype(str).str.strip(),
            "Market": market,
            "IndustryLarge": df["업종(대분류)"].astype(str).str.strip(),
            "IndustryMid": df["업종(중분류)"].astype(str).str.strip(),
            "IndustrySmall": df["업종(소분류)"].astype(str).str.strip(),
            "SharesOutstanding": shares_py,
        }
    )
    out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"])
    return out


def _update_names_from_fdr(df: pd.DataFrame, market: str) -> pd.DataFrame:
    """
    Update stock names from FinanceDataReader to ensure accuracy.
    
    Args:
        df: DataFrame with stock data (must have 'Code' and 'Name' columns)
        market: Market name ('KOSPI' or 'KOSDAQ')
    
    Returns:
        DataFrame with updated names from FDR
    """
    try:
        # Fetch stock listing from FDR
        fdr_df = fdr.StockListing(market)
        
        if fdr_df.empty:
            warnings.warn(f"No data fetched from FDR for {market}")
            return df
        
        # Ensure Code column is properly formatted in FDR data
        fdr_df['Code'] = fdr_df['Code'].astype(str).str.strip().str.zfill(6)
        
        # Create a mapping of Code -> Name from FDR data
        fdr_name_map = dict(zip(fdr_df['Code'], fdr_df['Name']))
        
        # Update names in the dataframe using vectorized operations
        df = df.copy()
        original_count = len(df)
        
        # Count how many names will be updated before updating
        old_names = df['Name'].copy()
        new_names = df['Code'].map(fdr_name_map)
        
        # Only update where we have FDR data
        mask = new_names.notna()
        df.loc[mask, 'Name'] = new_names[mask]
        
        # Count how many names actually changed
        updated_count = (old_names != df['Name']).sum()
        
        print(f"Updated {updated_count} stock names from FDR for {market} (total: {original_count})")
        return df
        
    except Exception as e:
        warnings.warn(f"Failed to update names from FDR for {market}: {str(e)}")
        return df


def _fetch_etf_data(master_json_path: str) -> pd.DataFrame:
    """Fetch ETF data using FdrProvider.list_tickers()."""
    try:
        # Use FdrProvider's list_tickers with market='ETF' to fetch ETF data
        fdr_provider = FdrProvider(master_json_path=master_json_path, source="KRX")
        tickers, market_by_ticker = fdr_provider.list_tickers(market='ETF')
        
        if not tickers:
            warnings.warn("No ETF data fetched via FdrProvider")
            return pd.DataFrame()
        
        # We need to fetch the full ETF data with names
        # Since list_tickers only returns codes, we need to use the internal fetch
        df_etf = fdr.StockListing('ETF/KR')
        
        if df_etf.empty:
            warnings.warn("No ETF data fetched")
            return pd.DataFrame()
        
        # Map ETF columns to master format
        # ETF data has: Symbol, Name, and other fields
        etf_master = pd.DataFrame({
            'Code': df_etf['Symbol'].astype(str).str.strip().str.zfill(6),
            'Name': df_etf['Name'].astype(str).str.strip(),
            'Market': 'ETF',
            'IndustryLarge': None,
            'IndustryMid': None,
            'IndustrySmall': None,
            'SharesOutstanding': None,
        })
        
        etf_master = etf_master.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"])
        print(f"Fetched {len(etf_master)} ETF entries via FdrProvider")
        return etf_master
        
    except Exception as e:
        warnings.warn(f"Failed to fetch ETF data: {str(e)}")
        return pd.DataFrame()


#: 릴리즈 프레임이 2015-01-02 부터라 그 이전 폐지분은 쓸 데가 없다.
DEFAULT_DELISTED_FROM = "2015-01-01"


class DelistedFetchError(RuntimeError):
    """The delisting listing could not be fetched.

    **This has to be fatal.** ``--include-delisted`` is the default, so degrading to an
    empty frame would write a master that *looks* survivorship-free — the build exits 0,
    the release publishes, and every consumer inherits the full bias with nothing in the
    output saying so. Failing loudly is the only way the omission is noticed.

    ``--no-delisted`` is the way to build without them **on purpose**.
    """


def _fetch_delisted_data(
    *, since: str = DEFAULT_DELISTED_FROM, include_konex: bool = False
) -> pd.DataFrame:
    """Fetch delisted stocks so the universe is not survivorship-biased.

    The master is built from Seibro Excel, which lists **currently listed** stocks only.
    Delisted names are therefore never even queried — they are missing from the release
    not because the fetch failed but because they were never in the ticker list.

    This matters more than it looks. ``MRS_*`` are **cross-sectional percentile ranks**
    computed in the orchestrator over whatever is in the frame. Ranking survivors only
    distorts the percentile of the survivors too: a stock in the top 30% of its year
    looks like the top 20% once the names that died that year are dropped. Consumers
    cannot repair that by appending delisted OHLCV afterwards — the universe has to be
    right *before* the percentiles are computed.

    ``fdr.StockListing('KRX-DELISTING')`` is a single bulk query (~4,200 rows covering
    1960~present) that carries the delisting date and reason.
    """
    try:
        df = fdr.StockListing("KRX-DELISTING")
    except Exception as e:
        raise DelistedFetchError(
            f"Failed to fetch the delisting listing: {e}\n"
            "Refusing to build a master that silently omits delisted names — the "
            "release would look survivorship-free while carrying the full bias.\n"
            "Pass --no-delisted to build without them on purpose."
        ) from e

    if df is None or df.empty:
        raise DelistedFetchError(
            "The delisting listing came back empty.\n"
            "This is a fetch failure, not a fact about the market — KRX has ~4,200 "
            "delisted securities on record. Pass --no-delisted to skip on purpose."
        )

    df = df.copy()
    df["DelistingDate"] = pd.to_datetime(df["DelistingDate"], errors="coerce")

    # 주권 only. The listing also carries 신주인수권증서 · 수익증권 · 선박투자회사 etc.,
    # which are not part of the equity universe this cache is about.
    df = df[df["SecuGroup"] == "주권"]
    df = df[df["DelistingDate"] >= pd.Timestamp(since)]

    markets = ["KOSPI", "KOSDAQ"] + (["KONEX"] if include_konex else [])
    df = df[df["Market"].isin(markets)]

    if df.empty:
        raise DelistedFetchError(
            f"No delisted 주권 found since {since} in markets {markets}.\n"
            "The listing was non-empty, so this points at a schema change "
            "(SecuGroup / Market / DelistingDate) rather than at the market."
        )

    out = pd.DataFrame(
        {
            "Code": df["Symbol"].astype(str).str.strip().str.zfill(6),
            "Name": df["Name"].astype(str).str.strip(),
            "Market": df["Market"].astype(str).str.strip(),
            # The delisting listing carries a **single-level** ``Industry`` ('유통',
            # '통신장비') that does not match Seibro's large/mid/small taxonomy. Forcing
            # it into those columns would invent industry groups that no other row uses,
            # so we leave them null — the same thing ETF rows already do, and
            # ``industry.py`` maps null to "Unknown".
            "IndustryLarge": None,
            "IndustryMid": None,
            "IndustrySmall": None,
            "SharesOutstanding": pd.to_numeric(df["ListingShares"], errors="coerce"),
            "DelistingDate": df["DelistingDate"].dt.strftime("%Y-%m-%d"),
            # Kept so consumers can tell "went to zero" from "merged into another
            # ticker" — 피흡수합병 alone is ~115 of these rows.
            "DelistingReason": df["Reason"].astype(str).str.strip(),
        }
    )
    out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"])
    print(f"Fetched {len(out)} delisted entries since {since} (markets: {markets})")
    return out


def _write_master_json(master: pd.DataFrame, out_path: Path) -> None:
    """Write the master as **standard** JSON.

    pandas leaves ``NaN`` where a live row has no delisting fields, and
    ``json.dumps`` writes those as a bare ``NaN`` token — **which is not valid JSON**.
    Strict parsers reject the file and README promises ``null``.

    Missing values are converted first, and ``allow_nan=False`` makes any survivor a
    hard error so this cannot silently regress.
    """
    records = master.astype(object).where(master.notna(), None).to_dict(orient="records")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )


def _merge_delisted(
    master: pd.DataFrame, delisted: pd.DataFrame
) -> tuple[pd.DataFrame, int, int]:
    """Merge the delisting listing into the master.

    A plain concat is wrong here. **The Seibro Excel snapshot is stale**: as of this
    writing 61 names that KRX already delisted in 2026 are still in it. Dropping the
    duplicate and keeping the Excel row would leave those 61 looking alive, which is
    exactly the bias this change is meant to remove.

    So the two sources are merged by role rather than by precedence:

    - **code in both** → keep the Excel row (richer: real industry taxonomy, share
      count) but **stamp the delisting date and reason onto it**
    - **code only in the delisting listing** → append it

    Returns ``(master, appended, marked)``.
    """
    key = ["Code", "Market"]
    facts = delisted[[*key, "DelistingDate", "DelistingReason"]]

    merged = master.merge(facts, on=key, how="left")
    marked = int(merged["DelistingDate"].notna().sum())

    known = set(map(tuple, master[key].itertuples(index=False, name=None)))
    fresh = delisted[
        ~delisted[key].apply(tuple, axis=1).isin(known)
    ]
    appended = len(fresh)

    out = pd.concat([merged, fresh], ignore_index=True)
    return out, appended, marked


def main() -> None:
    p = argparse.ArgumentParser(description="Build KRX stock master JSON from Seibro Excel files and FDR ETF data")
    p.add_argument("--kospi-xlsx", type=str, default="/workspace/data/kospi.xlsx")
    p.add_argument("--kosdaq-xlsx", type=str, default="/workspace/data/kosdaq.xlsx")
    p.add_argument("--output-json", type=str, default="/workspace/data/krx_stock_master.json")
    p.add_argument("--include-etf", action="store_true", default=True, help="Include ETF data from FinanceDataReader (default: True)")
    p.add_argument("--no-etf", dest="include_etf", action="store_false", help="Exclude ETF data")
    p.add_argument(
        "--include-delisted",
        action="store_true",
        default=True,
        help="Include delisted stocks so the universe is not survivorship-biased (default: True)",
    )
    p.add_argument("--no-delisted", dest="include_delisted", action="store_false")
    p.add_argument(
        "--delisted-from",
        type=str,
        default=DEFAULT_DELISTED_FROM,
        help=f"Earliest delisting date to include (default: {DEFAULT_DELISTED_FROM})",
    )
    p.add_argument(
        "--include-konex",
        action="store_true",
        default=False,
        help="Also include delisted KONEX names (default: False — KONEX is not in the listed universe)",
    )
    args = p.parse_args()

    kospi = _read_master_xlsx(Path(args.kospi_xlsx), market="KOSPI")
    kosdaq = _read_master_xlsx(Path(args.kosdaq_xlsx), market="KOSDAQ")
    
    # Update stock names from FDR to ensure accuracy
    kospi = _update_names_from_fdr(kospi, market="KOSPI")
    kosdaq = _update_names_from_fdr(kosdaq, market="KOSDAQ")

    master = pd.concat([kospi, kosdaq], ignore_index=True)
    
    # Fetch and add ETF data if requested
    if args.include_etf:
        etf_data = _fetch_etf_data(args.output_json)
        if not etf_data.empty:
            master = pd.concat([master, etf_data], ignore_index=True)
    
    delisted_count = 0
    marked_count = 0
    if args.include_delisted:
        delisted = _fetch_delisted_data(
            since=args.delisted_from, include_konex=args.include_konex
        )
        if not delisted.empty:
            master, delisted_count, marked_count = _merge_delisted(master, delisted)

    master = master.sort_values(["Market", "Code"]).reset_index(drop=True)

    out_path = Path(args.output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _write_master_json(master, out_path)

    print(f"Wrote {len(master)} rows -> {out_path}")
    etf_count = len(master) - len(kospi) - len(kosdaq) - delisted_count
    print(
        f"  KOSPI: {len(kospi)}, KOSDAQ: {len(kosdaq)}, "
        f"ETF: {etf_count}, delisted appended: {delisted_count}"
    )
    if marked_count:
        print(
            f"  ⚠️  {marked_count} rows already in the Seibro Excel are in fact delisted "
            f"— the Excel snapshot is stale. They were marked, not duplicated."
        )


if __name__ == "__main__":
    main()

