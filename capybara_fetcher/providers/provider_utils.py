"""
Common utility functions for data providers.
"""
from __future__ import annotations

import json
import pandas as pd


_MASTER_COLS = [
    "Code",
    "Name",
    "Market",
    "IndustryLarge",
    "IndustryMid",
    "IndustrySmall",
    "SharesOutstanding",
    # Present only for rows that came from the delisting listing; null for live names.
    # Consumers need this to tell "the series legitimately ends here" from "the fetch
    # has a hole", and to tell 피흡수합병 from a stock that went to zero.
    "DelistingDate",
    "DelistingReason",
]


def load_master_json(path: str) -> pd.DataFrame:
    """
    Load stock master data from JSON file.
    
    This function is shared across providers that use local stock master files.
    
    Args:
        path: Path to JSON file containing stock master data
        
    Returns:
        DataFrame with standardized columns and types
        
    Raises:
        ValueError: If file is empty or has no valid rows
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError(f"stock master is empty: {path}")

    for c in _MASTER_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    out = df[_MASTER_COLS].copy()
    out["Code"] = out["Code"].astype(str).str.strip().str.zfill(6)
    out["Name"] = out["Name"].astype(str).str.strip()
    out["Market"] = out["Market"].astype(str).str.strip()
    out["IndustryLarge"] = out["IndustryLarge"].astype(str).str.strip()
    out["IndustryMid"] = out["IndustryMid"].astype(str).str.strip()
    out["IndustrySmall"] = out["IndustrySmall"].astype(str).str.strip()
    out["SharesOutstanding"] = pd.to_numeric(out["SharesOutstanding"], errors="coerce").astype("Int64")
    # Left as-is (nullable strings): live rows have no delisting date, and coercing
    # them to NaT/"" would make "still listed" indistinguishable from "unknown".
    out["DelistingDate"] = out["DelistingDate"].where(out["DelistingDate"].notna(), None)
    out["DelistingReason"] = out["DelistingReason"].where(
        out["DelistingReason"].notna(), None
    )
    out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"]).sort_values(["Market", "Code"])
    if out.empty:
        raise ValueError(f"stock master has no valid rows: {path}")
    return out


def add_delisted_from_master(
    tickers: list[str],
    market_by_ticker: dict[str, str],
    master: pd.DataFrame,
    *,
    market: str | None = None,
) -> tuple[list[str], dict[str, str]]:
    """Union a live ticker list with the delisted names in the local master.

    **Providers that ask an exchange "what is listed?" can only ever answer with
    survivors.** `KoreaInvestmentProvider` downloads the KIS master and `FdrProvider`
    calls `fdr.StockListing()`; both describe today. The orchestrator iterates
    `list_tickers()`, not the rows of `load_stock_master()`, so delisted names added to
    the local JSON are never fetched — the master grows and the release does not change.

    That was measured: of 492 delisted codes in the master, exactly **1** appeared in
    `CompositeProvider.list_tickers()`.

    Live entries win on the market label — the local master can be stale, the exchange
    listing is not.
    """
    if master is None or master.empty or "DelistingDate" not in master.columns:
        return tickers, market_by_ticker

    dead = master[master["DelistingDate"].notna()]
    if market:
        dead = dead[dead["Market"].astype(str).str.strip() == str(market).strip()]
    if dead.empty:
        return tickers, market_by_ticker

    known = set(tickers)
    merged = dict(market_by_ticker)
    for code, mkt in zip(dead["Code"], dead["Market"]):
        code = str(code).strip().zfill(6)
        if code not in known:
            known.add(code)
            merged[code] = str(mkt).strip()

    return sorted(known), merged
