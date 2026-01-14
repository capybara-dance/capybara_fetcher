from __future__ import annotations

import json
from dataclasses import dataclass
import datetime as dt

import pandas as pd
from pykrx import stock

from ..provider import DataProvider


_MASTER_COLS = [
    "Code",
    "Name",
    "Market",
    "IndustryLarge",
    "IndustryMid",
    "IndustrySmall",
    "SharesOutstanding",
]


def _load_master_json(path: str) -> pd.DataFrame:
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
    out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"]).sort_values(["Market", "Code"])
    if out.empty:
        raise ValueError(f"stock master has no valid rows: {path}")
    return out


@dataclass(frozen=True)
class PykrxProvider(DataProvider):
    """
    DataProvider implementation:
    - tickers/master: local Seibro-derived JSON
    - ohlcv: pykrx
    """

    master_json_path: str
    name: str = "pykrx"

    def load_stock_master(self, *, asof_date: dt.date | None = None) -> pd.DataFrame:
        # asof_date reserved for future providers
        return _load_master_json(self.master_json_path)

    def list_tickers(
        self,
        *,
        asof_date: dt.date | None = None,
        market: str | None = None,
    ) -> tuple[list[str], dict[str, str]]:
        master = self.load_stock_master(asof_date=asof_date)
        if market:
            m = str(market).strip()
            master = master[master["Market"] == m]
        tickers = master["Code"].astype(str).str.zfill(6).unique().tolist()
        tickers = sorted(tickers)
        market_by_ticker = dict(zip(master["Code"].tolist(), master["Market"].tolist()))
        return tickers, market_by_ticker

    def fetch_ohlcv(
        self,
        *,
        ticker: str,
        start_date: str,
        end_date: str,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        # pykrx returns DatetimeIndex + korean column names
        return stock.get_market_ohlcv(start_date, end_date, str(ticker).zfill(6), adjusted=bool(adjusted))

