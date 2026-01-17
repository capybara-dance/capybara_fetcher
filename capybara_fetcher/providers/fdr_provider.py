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
        # Use zero-filled ticker codes for dictionary keys to match ticker format
        ticker_codes = master["Code"].astype(str).str.zfill(6).tolist()
        market_by_ticker = dict(zip(ticker_codes, master["Market"].tolist()))
        return tickers, market_by_ticker

    def _split_date_range_into_years(self, start_date: str, end_date: str) -> list[str]:
        """
        Split a date range into year strings for year-by-year fetching.
        
        FDR supports fetching by year using format: fdr.DataReader(symbol, '2020')
        This is simpler and more reliable than chunking by days.
        
        Args:
            start_date: Start date in YYYY-MM-DD or YYYYMMDD format
            end_date: End date in YYYY-MM-DD or YYYYMMDD format
            
        Returns:
            List of year strings (e.g., ['2020', '2021', '2022'])
        """
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        start_year = start.year
        end_year = end.year
        
        # Generate list of years as strings
        years = [str(year) for year in range(start_year, end_year + 1)]
        
        return years

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
        # Note: KRX source doesn't support all tickers (e.g., ETFs like 069500)
        # We fetch year-by-year to avoid API limits
        # We'll try KRX first, then fall back to NAVER if it fails
        if self.source.upper() == "KRX":
            symbol = f"KRX:{ticker_code}"
            fallback_symbol = f"NAVER:{ticker_code}"
            use_chunking = True  # KRX fetches year-by-year
        elif self.source.upper() == "NAVER":
            symbol = f"NAVER:{ticker_code}"
            fallback_symbol = None
            use_chunking = False
        elif self.source.upper() == "YAHOO":
            # Yahoo Finance requires .KS (KOSPI) or .KQ (KOSDAQ) suffix
            # We'll default to NAVER for Yahoo to avoid market determination complexity
            # Users needing Yahoo should specify the full symbol externally
            symbol = f"NAVER:{ticker_code}"
            fallback_symbol = None
            use_chunking = False
        else:
            # Default to ticker code without prefix (FDR will use NAVER)
            symbol = ticker_code
            fallback_symbol = None
            use_chunking = False
        
        # Fetch data with year-by-year chunking if needed (for KRX source)
        if use_chunking:
            years = self._split_date_range_into_years(start_date, end_date)
            dfs = []
            
            for year in years:
                try:
                    # Fetch data for entire year using FDR's year format
                    year_df = fdr.DataReader(symbol, year)
                    if year_df is not None and not year_df.empty:
                        dfs.append(year_df)
                except ValueError as e:
                    # KRX source may not support certain tickers (e.g., ETFs)
                    # Fall back to NAVER if available
                    if fallback_symbol and "is not supported" in str(e):
                        try:
                            year_df = fdr.DataReader(fallback_symbol, year)
                            if year_df is not None and not year_df.empty:
                                dfs.append(year_df)
                        except Exception as fallback_error:
                            raise RuntimeError(
                                f"Failed to fetch OHLCV from FDR for {ticker} (year {year}): "
                                f"KRX source failed ({str(e)}), NAVER fallback also failed ({str(fallback_error)})"
                            ) from fallback_error
                    else:
                        raise RuntimeError(
                            f"Failed to fetch OHLCV from FDR for {ticker} (year {year}, source: {self.source}): {str(e)}"
                        ) from e
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to fetch OHLCV from FDR for {ticker} (year {year}, source: {self.source}): {str(e)}"
                    ) from e
            
            # Concatenate all years
            if dfs:
                df = pd.concat(dfs, axis=0)
                # Remove duplicates that might occur at year boundaries
                df = df[~df.index.duplicated(keep='first')]
                df = df.sort_index()
                
                # Filter to requested date range
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                df = df[(df.index >= start_dt) & (df.index <= end_dt)]
            else:
                df = pd.DataFrame()
        else:
            # No chunking needed for non-KRX sources
            try:
                df = fdr.DataReader(symbol, start_date, end_date)
            except ValueError as e:
                if fallback_symbol and "is not supported" in str(e):
                    try:
                        df = fdr.DataReader(fallback_symbol, start_date, end_date)
                    except Exception as fallback_error:
                        raise RuntimeError(
                            f"Failed to fetch OHLCV from FDR for {ticker}: "
                            f"{self.source} source failed ({str(e)}), fallback also failed ({str(fallback_error)})"
                        ) from fallback_error
                else:
                    raise RuntimeError(f"Failed to fetch OHLCV from FDR for {ticker} (source: {self.source}): {str(e)}") from e
            except Exception as e:
                raise RuntimeError(f"Failed to fetch OHLCV from FDR for {ticker} (source: {self.source}): {str(e)}") from e
        
        try:
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
            # Trading value approximation: Volume * Close
            # Note: This is an approximation as true trading value would be the sum of
            # (price * volume) for each individual trade throughout the day. Using
            # Volume * Close provides a reasonable estimate when intraday data is unavailable.
            if "거래대금" not in df.columns and "거래량" in df.columns and "종가" in df.columns:
                df["거래대금"] = df["거래량"] * df["종가"]
            
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to process OHLCV data from FDR for {ticker}: {str(e)}") from e
