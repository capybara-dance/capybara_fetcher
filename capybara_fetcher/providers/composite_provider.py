"""
Composite data provider that combines functionalities from multiple providers.

This provider allows selecting and combining features from existing providers,
enabling flexible fallback strategies and multi-source data fetching.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Sequence

import pandas as pd

from ..provider import DataProvider


@dataclass(frozen=True)
class CompositeProvider(DataProvider):
    """
    Composite DataProvider that combines multiple providers.
    
    This provider wraps multiple DataProvider instances and provides
    an interface to select and combine their functionalities appropriately.
    
    The actual implementation strategy (fallback, merge, priority, etc.)
    will be defined based on specific requirements.
    
    Args:
        providers: Sequence of DataProvider instances to compose
        name: Provider name (default: "composite")
    
    Example:
        >>> pykrx_provider = PykrxProvider(master_json_path="...")
        >>> fdr_provider = FdrProvider(master_json_path="...")
        >>> composite = CompositeProvider(
        ...     providers=[pykrx_provider, fdr_provider],
        ...     name="composite"
        ... )
    """

    providers: Sequence[DataProvider]
    name: str = "composite"

    def __post_init__(self):
        """Validate that at least one provider is provided."""
        if not self.providers or len(self.providers) == 0:
            raise ValueError("CompositeProvider requires at least one provider")

    def list_tickers(
        self,
        *,
        asof_date: dt.date | None = None,
        market: str | None = None,
    ) -> tuple[list[str], dict[str, str]]:
        """
        List tickers from providers.
        
        TODO: Implementation strategy to be determined.
        Possible strategies:
        - Use first provider
        - Merge tickers from all providers
        - Use specific provider based on market
        
        Args:
            asof_date: Optional date for historical ticker list
            market: Optional market filter (e.g., "KOSPI", "KOSDAQ")
        
        Returns:
            Tuple of (ticker list, market mapping dict)
        
        Raises:
            NotImplementedError: Implementation pending
        """
        raise NotImplementedError(
            "list_tickers implementation strategy needs to be determined. "
            "Possible approaches: use first provider, merge all providers, "
            "or select by market."
        )

    def load_stock_master(
        self,
        *,
        asof_date: dt.date | None = None,
    ) -> pd.DataFrame:
        """
        Load stock master data from providers.
        
        TODO: Implementation strategy to be determined.
        Possible strategies:
        - Use first provider
        - Merge master data from all providers
        - Use most complete provider
        
        Args:
            asof_date: Optional date for historical master data
        
        Returns:
            DataFrame with stock master information
        
        Raises:
            NotImplementedError: Implementation pending
        """
        raise NotImplementedError(
            "load_stock_master implementation strategy needs to be determined. "
            "Possible approaches: use first provider, merge all providers, "
            "or select most complete source."
        )

    def fetch_ohlcv(
        self,
        *,
        ticker: str,
        start_date: str,
        end_date: str,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data for a ticker.
        
        TODO: Implementation strategy to be determined.
        Possible strategies:
        - Try providers in order (failover)
        - Use specific provider based on ticker or date range
        - Merge data from multiple providers
        
        Args:
            ticker: 6-digit stock code
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            adjusted: Whether to use adjusted prices
        
        Returns:
            DataFrame with OHLCV data
        
        Raises:
            NotImplementedError: Implementation pending
        """
        raise NotImplementedError(
            "fetch_ohlcv implementation strategy needs to be determined. "
            "Possible approaches: failover (try in order), provider selection "
            "by ticker/date, or data merging."
        )
