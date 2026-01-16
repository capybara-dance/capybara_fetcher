"""
FinanceDataReader (FDR) data provider.

This provider uses FinanceDataReader library to fetch stock data:
https://github.com/FinanceData/FinanceDataReader
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pandas as pd
import FinanceDataReader as fdr

from ..provider import DataProvider
from .provider_utils import load_master_json


@dataclass(frozen=True)
class FdrProvider(DataProvider):
    """
    DataProvider implementation using FinanceDataReader:
    - tickers/master: local Seibro-derived JSON (same as other providers)
    - ohlcv: FinanceDataReader (FDR) library
    
    FDR supports multiple data sources:
    - KRX (Korean Exchange) - default for Korean stocks
    - NAVER Finance
    - Yahoo Finance
    
    The provider uses KRX as the default source for Korean stock data.
    """

    master_json_path: str
    source: str = "KRX"  # Data source: "KRX", "NAVER", or "YAHOO"
    name: str = "fdr"

    def load_stock_master(self, *, asof_date: dt.date | None = None) -> pd.DataFrame:
        """Load stock master from local JSON file."""
        # asof_date reserved for future providers
        return load_master_json(self.master_json_path)

    def list_tickers(
        self,
        *,
        asof_date: dt.date | None = None,
        market: str | None = None,
    ) -> tuple[list[str], dict[str, str]]:
        """List tickers from stock master."""
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
        """
        Fetch OHLCV data using FinanceDataReader.
        
        Args:
            ticker: 6-digit stock code
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            adjusted: Whether to use adjusted prices (default: True)
                     Note: FDR's KRX source provides adjusted prices by default
        
        Returns:
            DataFrame with Korean column names (matching pykrx format)
            for consistency with standardization layer.
        """
        ticker_code = str(ticker).zfill(6)
        
        # Build the symbol based on source
        if self.source.upper() == "KRX":
            symbol = f"KRX:{ticker_code}"
        elif self.source.upper() == "NAVER":
            symbol = f"NAVER:{ticker_code}"
        elif self.source.upper() == "YAHOO":
            # Yahoo Finance requires .KS or .KQ suffix for Korean stocks
            # We'll use .KS as default (KOSPI format)
            symbol = f"YAHOO:{ticker_code}.KS"
        else:
            # Default to ticker code without prefix (FDR will use NAVER)
            symbol = ticker_code
        
        try:
            # Fetch data from FDR
            df = fdr.DataReader(symbol, start_date, end_date)
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            # FDR returns DataFrame with English column names
            # Common FDR columns: Date (index), Open, High, Low, Close, Volume, Change
            # Map to Korean column names (matching pykrx format)
            column_mapping = {
                "Open": "시가",
                "High": "고가",
                "Low": "저가",
                "Close": "종가",
                "Volume": "거래량",
                "Change": "등락률",
            }
            
            # Only rename columns that exist
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
            
            # Ensure index is DatetimeIndex with name "날짜"
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            
            # Sort by date
            df = df.sort_index()
            
            # Add 거래대금 (trading value) if not present
            # Trading value = Volume * Close (approximate)
            if "거래대금" not in df.columns and "거래량" in df.columns and "종가" in df.columns:
                df["거래대금"] = df["거래량"] * df["종가"]
            
            return df
            
        except Exception as e:
            # Raise error for fail-fast behavior
            raise RuntimeError(f"Failed to fetch OHLCV from FDR for {ticker} (source: {self.source}): {str(e)}") from e
