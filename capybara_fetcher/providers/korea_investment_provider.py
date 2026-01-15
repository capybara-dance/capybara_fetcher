"""
Korea Investment Securities API data provider.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import datetime as dt
import threading

import pandas as pd

from ..provider import DataProvider
from .korea_investment_auth import KISAuth
from .provider_utils import load_master_json


@dataclass(frozen=True)
class KoreaInvestmentProvider(DataProvider):
    """
    DataProvider implementation using Korea Investment Securities API:
    - tickers/master: local Seibro-derived JSON (same as pykrx)
    - ohlcv: Korea Investment API
    """

    master_json_path: str
    appkey: str
    appsecret: str
    base_url: str = "https://openapi.koreainvestment.com:9443"
    name: str = "korea_investment"
    _auth: KISAuth | None = field(default=None, init=False, repr=False, compare=False)
    _auth_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

    def _get_auth(self) -> KISAuth:
        """Get KIS authentication instance (cached to reuse token across session)."""
        # Use object.__setattr__ to bypass frozen dataclass restriction
        # Thread-safe lazy initialization with double-checked locking
        auth = object.__getattribute__(self, '_auth')
        if auth is None:
            lock = object.__getattribute__(self, '_auth_lock')
            with lock:
                # Double-check after acquiring lock
                auth = object.__getattribute__(self, '_auth')
                if auth is None:
                    auth = KISAuth(self.appkey, self.appsecret, self.base_url)
                    object.__setattr__(self, '_auth', auth)
        return auth

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
        Fetch OHLCV data using Korea Investment API.
        
        Uses the inquire-daily-itemchartprice API endpoint which supports
        date range queries (up to 100 days per call).
        
        Returns DataFrame with Korean column names (like pykrx) for consistency
        with standardization layer.
        """
        auth = self._get_auth()
        
        # API endpoint for daily item chart price
        # Based on /domestic_stock/inquire_daily_itemchartprice
        api_path = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        tr_id = "FHKST03010100"
        
        # Format dates as YYYYMMDD
        start_str = start_date.replace("-", "")
        end_str = end_date.replace("-", "")
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # J=주식, ETF
            "FID_INPUT_ISCD": ticker.zfill(6),
            "FID_INPUT_DATE_1": start_str,
            "FID_INPUT_DATE_2": end_str,
            "FID_PERIOD_DIV_CODE": "D",  # D=일봉
            "FID_ORG_ADJ_PRC": "0" if adjusted else "1",  # 0=수정주가, 1=원주가
        }
        
        try:
            result = auth.fetch_api(api_path, tr_id, params)
            
            # The API returns output2 as array of daily data
            if "output2" not in result:
                return pd.DataFrame()
            
            df = pd.DataFrame(result["output2"])
            
            if df.empty:
                return pd.DataFrame()
            
            # Map API column names to Korean names (matching pykrx format)
            # API columns: stck_bsop_date, stck_oprc, stck_hgpr, stck_lwpr, stck_clpr, acml_vol, acml_tr_pbmn
            column_mapping = {
                "stck_bsop_date": "날짜",
                "stck_oprc": "시가",
                "stck_hgpr": "고가",
                "stck_lwpr": "저가",
                "stck_clpr": "종가",
                "acml_vol": "거래량",
                "acml_tr_pbmn": "거래대금",
            }
            
            # Only map columns that exist
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
            
            # Convert date to datetime index
            if "날짜" in df.columns:
                df["날짜"] = pd.to_datetime(df["날짜"], format="%Y%m%d")
                df = df.set_index("날짜")
                df = df.sort_index()
            
            return df
            
        except Exception as e:
            # Log error and return empty DataFrame (fail-fast will be handled by orchestrator)
            raise RuntimeError(f"Failed to fetch OHLCV for {ticker}: {str(e)}") from e
