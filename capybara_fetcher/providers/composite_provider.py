"""
Composite data provider that combines functionalities from multiple providers.

This provider allows selecting and combining features from existing providers,
enabling flexible fallback strategies and multi-source data fetching.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Sequence, Literal

import pandas as pd

from ..provider import DataProvider
from .pykrx_provider import PykrxProvider
from .fdr_provider import FdrProvider
from .korea_investment_provider import KoreaInvestmentProvider


ProviderType = Literal["pykrx", "fdr", "korea_investment"]


@dataclass(frozen=True)
class CompositeProvider(DataProvider):
    """
    Composite DataProvider that combines multiple providers.
    
    This provider internally creates and manages multiple DataProvider instances
    based on configuration. External users don't need to know about individual
    provider implementations.
    
    The actual implementation strategy (fallback, merge, priority, etc.)
    will be defined based on specific requirements.
    
    Args:
        master_json_path: Path to stock master JSON file
        provider_types: List of provider types to use (e.g., ["pykrx", "fdr"])
        fdr_source: Source for FDR provider (default: "KRX")
        korea_investment_appkey: Optional app key for Korea Investment provider
        korea_investment_appsecret: Optional app secret for Korea Investment provider
        korea_investment_base_url: Optional base URL for Korea Investment provider
        name: Provider name (default: "composite")
    
    Example:
        >>> # Simple usage with default providers
        >>> composite = CompositeProvider(
        ...     master_json_path="data/krx_stock_master.json",
        ...     provider_types=["pykrx", "fdr"]
        ... )
        >>> 
        >>> # With Korea Investment provider
        >>> composite = CompositeProvider(
        ...     master_json_path="data/krx_stock_master.json",
        ...     provider_types=["korea_investment", "fdr"],
        ...     korea_investment_appkey="your_key",
        ...     korea_investment_appsecret="your_secret"
        ... )
    """

    master_json_path: str
    provider_types: Sequence[ProviderType] = field(default_factory=lambda: ["pykrx"])
    fdr_source: str = "KRX"
    korea_investment_appkey: str | None = None
    korea_investment_appsecret: str | None = None
    korea_investment_base_url: str = "https://openapi.koreainvestment.com:9443"
    name: str = "composite"
    
    # Internal field to cache providers
    _providers: Sequence[DataProvider] | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self):
        """Initialize and validate providers."""
        if not self.provider_types or len(self.provider_types) == 0:
            raise ValueError("CompositeProvider requires at least one provider type")
        
        # Create provider instances based on configuration
        providers = []
        for provider_type in self.provider_types:
            if provider_type == "pykrx":
                providers.append(PykrxProvider(master_json_path=self.master_json_path))
            elif provider_type == "fdr":
                providers.append(FdrProvider(
                    master_json_path=self.master_json_path,
                    source=self.fdr_source
                ))
            elif provider_type == "korea_investment":
                if not self.korea_investment_appkey or not self.korea_investment_appsecret:
                    raise ValueError(
                        "korea_investment provider requires both appkey and appsecret"
                    )
                providers.append(KoreaInvestmentProvider(
                    master_json_path=self.master_json_path,
                    appkey=self.korea_investment_appkey,
                    appsecret=self.korea_investment_appsecret,
                    base_url=self.korea_investment_base_url
                ))
            else:
                raise ValueError(f"Unknown provider type: {provider_type}")
        
        # Use object.__setattr__ to bypass frozen dataclass restriction
        object.__setattr__(self, '_providers', providers)
    
    @property
    def providers(self) -> Sequence[DataProvider]:
        """Get the list of internal providers."""
        return self._providers

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
