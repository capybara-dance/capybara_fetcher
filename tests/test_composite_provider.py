"""
Tests for CompositeProvider.
"""
import pytest
import pandas as pd
import datetime as dt
from pathlib import Path
from capybara_fetcher.providers import CompositeProvider, PykrxProvider, FdrProvider


@pytest.fixture
def master_json_path():
    """Path to test stock master JSON."""
    repo_root = Path(__file__).parent.parent
    return str(repo_root / "data" / "krx_stock_master.json")


@pytest.fixture
def pykrx_provider(master_json_path):
    """Create PykrxProvider instance."""
    return PykrxProvider(master_json_path=master_json_path)


@pytest.fixture
def fdr_provider(master_json_path):
    """Create FdrProvider instance."""
    return FdrProvider(master_json_path=master_json_path, source="KRX")


@pytest.fixture
def composite_provider(pykrx_provider, fdr_provider):
    """Create CompositeProvider with multiple providers."""
    return CompositeProvider(providers=[pykrx_provider, fdr_provider])


def test_composite_provider_creation(pykrx_provider, fdr_provider):
    """Test creating a CompositeProvider with multiple providers."""
    composite = CompositeProvider(providers=[pykrx_provider, fdr_provider])
    
    assert composite.name == "composite"
    assert len(composite.providers) == 2
    assert composite.providers[0] == pykrx_provider
    assert composite.providers[1] == fdr_provider


def test_composite_provider_custom_name(pykrx_provider):
    """Test creating a CompositeProvider with custom name."""
    composite = CompositeProvider(
        providers=[pykrx_provider],
        name="my_composite"
    )
    
    assert composite.name == "my_composite"


def test_composite_provider_empty_providers():
    """Test that CompositeProvider requires at least one provider."""
    with pytest.raises(ValueError) as exc_info:
        CompositeProvider(providers=[])
    
    assert "at least one provider" in str(exc_info.value)


def test_composite_provider_single_provider(pykrx_provider):
    """Test CompositeProvider with single provider."""
    composite = CompositeProvider(providers=[pykrx_provider])
    
    assert len(composite.providers) == 1
    assert composite.providers[0] == pykrx_provider


def test_composite_provider_list_tickers_not_implemented(composite_provider):
    """Test that list_tickers raises NotImplementedError."""
    with pytest.raises(NotImplementedError) as exc_info:
        composite_provider.list_tickers()
    
    assert "list_tickers implementation strategy" in str(exc_info.value)


def test_composite_provider_load_stock_master_not_implemented(composite_provider):
    """Test that load_stock_master raises NotImplementedError."""
    with pytest.raises(NotImplementedError) as exc_info:
        composite_provider.load_stock_master()
    
    assert "load_stock_master implementation strategy" in str(exc_info.value)


def test_composite_provider_fetch_ohlcv_not_implemented(composite_provider):
    """Test that fetch_ohlcv raises NotImplementedError."""
    with pytest.raises(NotImplementedError) as exc_info:
        composite_provider.fetch_ohlcv(
            ticker="005930",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
    
    assert "fetch_ohlcv implementation strategy" in str(exc_info.value)


def test_composite_provider_providers_immutable(composite_provider):
    """Test that providers sequence is immutable (frozen dataclass)."""
    # Frozen dataclass should prevent attribute modification
    with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
        composite_provider.providers = []


def test_composite_provider_multiple_providers_same_type(master_json_path):
    """Test CompositeProvider with multiple providers of same type."""
    provider1 = FdrProvider(master_json_path=master_json_path, source="KRX")
    provider2 = FdrProvider(master_json_path=master_json_path, source="NAVER")
    
    composite = CompositeProvider(providers=[provider1, provider2])
    
    assert len(composite.providers) == 2
    assert composite.providers[0].source == "KRX"
    assert composite.providers[1].source == "NAVER"


def test_composite_provider_interface_compliance(composite_provider):
    """Test that CompositeProvider has all required DataProvider methods."""
    # Check that all DataProvider protocol methods exist
    assert hasattr(composite_provider, "name")
    assert hasattr(composite_provider, "list_tickers")
    assert hasattr(composite_provider, "load_stock_master")
    assert hasattr(composite_provider, "fetch_ohlcv")
    
    # Check they are callable
    assert callable(composite_provider.list_tickers)
    assert callable(composite_provider.load_stock_master)
    assert callable(composite_provider.fetch_ohlcv)


def test_composite_provider_docstrings(composite_provider):
    """Test that CompositeProvider methods have documentation."""
    assert composite_provider.list_tickers.__doc__ is not None
    assert composite_provider.load_stock_master.__doc__ is not None
    assert composite_provider.fetch_ohlcv.__doc__ is not None
    assert "TODO" in composite_provider.list_tickers.__doc__
    assert "TODO" in composite_provider.load_stock_master.__doc__
    assert "TODO" in composite_provider.fetch_ohlcv.__doc__
