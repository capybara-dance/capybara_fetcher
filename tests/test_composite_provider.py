"""
Tests for CompositeProvider.
"""
import pytest
import pandas as pd
import datetime as dt
from pathlib import Path
from capybara_fetcher.providers import CompositeProvider


@pytest.fixture
def master_json_path():
    """Path to test stock master JSON."""
    repo_root = Path(__file__).parent.parent
    return str(repo_root / "data" / "krx_stock_master.json")


def test_composite_provider_default_creation(master_json_path):
    """Test creating a CompositeProvider with default settings."""
    composite = CompositeProvider(master_json_path=master_json_path)
    
    assert composite.name == "composite"
    assert len(composite.providers) == 1
    assert composite.providers[0].name == "pykrx"


def test_composite_provider_multiple_providers(master_json_path):
    """Test creating a CompositeProvider with multiple provider types."""
    composite = CompositeProvider(
        master_json_path=master_json_path,
        provider_types=["pykrx", "fdr"]
    )
    
    assert composite.name == "composite"
    assert len(composite.providers) == 2
    assert composite.providers[0].name == "pykrx"
    assert composite.providers[1].name == "fdr"


def test_composite_provider_custom_name(master_json_path):
    """Test creating a CompositeProvider with custom name."""
    composite = CompositeProvider(
        master_json_path=master_json_path,
        provider_types=["pykrx"],
        name="my_composite"
    )
    
    assert composite.name == "my_composite"


def test_composite_provider_empty_providers(master_json_path):
    """Test that CompositeProvider requires at least one provider type."""
    with pytest.raises(ValueError) as exc_info:
        CompositeProvider(
            master_json_path=master_json_path,
            provider_types=[]
        )
    
    assert "at least one provider type" in str(exc_info.value)


def test_composite_provider_fdr_with_custom_source(master_json_path):
    """Test CompositeProvider with FDR provider using custom source."""
    composite = CompositeProvider(
        master_json_path=master_json_path,
        provider_types=["fdr"],
        fdr_source="NAVER"
    )
    
    assert len(composite.providers) == 1
    assert composite.providers[0].name == "fdr"
    assert composite.providers[0].source == "NAVER"


def test_composite_provider_korea_investment_requires_credentials(master_json_path):
    """Test that korea_investment provider requires credentials."""
    with pytest.raises(ValueError) as exc_info:
        CompositeProvider(
            master_json_path=master_json_path,
            provider_types=["korea_investment"]
        )
    
    assert "requires both appkey and appsecret" in str(exc_info.value)


def test_composite_provider_korea_investment_with_credentials(master_json_path):
    """Test CompositeProvider with Korea Investment provider."""
    composite = CompositeProvider(
        master_json_path=master_json_path,
        provider_types=["korea_investment"],
        korea_investment_appkey="test_key",
        korea_investment_appsecret="test_secret"
    )
    
    assert len(composite.providers) == 1
    assert composite.providers[0].name == "korea_investment"


def test_composite_provider_unknown_provider_type(master_json_path):
    """Test that unknown provider type raises error."""
    with pytest.raises(ValueError) as exc_info:
        CompositeProvider(
            master_json_path=master_json_path,
            provider_types=["unknown_provider"]
        )
    
    assert "Unknown provider type" in str(exc_info.value)


def test_composite_provider_list_tickers_not_implemented(master_json_path):
    """Test that list_tickers raises NotImplementedError."""
    composite = CompositeProvider(master_json_path=master_json_path)
    
    with pytest.raises(NotImplementedError) as exc_info:
        composite.list_tickers()
    
    assert "list_tickers implementation strategy" in str(exc_info.value)


def test_composite_provider_load_stock_master_not_implemented(master_json_path):
    """Test that load_stock_master raises NotImplementedError."""
    composite = CompositeProvider(master_json_path=master_json_path)
    
    with pytest.raises(NotImplementedError) as exc_info:
        composite.load_stock_master()
    
    assert "load_stock_master implementation strategy" in str(exc_info.value)


def test_composite_provider_fetch_ohlcv_not_implemented(master_json_path):
    """Test that fetch_ohlcv raises NotImplementedError."""
    composite = CompositeProvider(master_json_path=master_json_path)
    
    with pytest.raises(NotImplementedError) as exc_info:
        composite.fetch_ohlcv(
            ticker="005930",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
    
    assert "fetch_ohlcv implementation strategy" in str(exc_info.value)


def test_composite_provider_providers_immutable(master_json_path):
    """Test that providers property cannot be modified directly."""
    composite = CompositeProvider(master_json_path=master_json_path)
    
    # Frozen dataclass should prevent attribute modification
    with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
        composite.master_json_path = "new_path"


def test_composite_provider_interface_compliance(master_json_path):
    """Test that CompositeProvider has all required DataProvider methods."""
    composite = CompositeProvider(master_json_path=master_json_path)
    
    # Check that all DataProvider protocol methods exist
    assert hasattr(composite, "name")
    assert hasattr(composite, "list_tickers")
    assert hasattr(composite, "load_stock_master")
    assert hasattr(composite, "fetch_ohlcv")
    
    # Check they are callable
    assert callable(composite.list_tickers)
    assert callable(composite.load_stock_master)
    assert callable(composite.fetch_ohlcv)


def test_composite_provider_docstrings(master_json_path):
    """Test that CompositeProvider methods have documentation."""
    composite = CompositeProvider(master_json_path=master_json_path)
    
    assert composite.list_tickers.__doc__ is not None
    assert composite.load_stock_master.__doc__ is not None
    assert composite.fetch_ohlcv.__doc__ is not None
    assert "TODO" in composite.list_tickers.__doc__
    assert "TODO" in composite.load_stock_master.__doc__
    assert "TODO" in composite.fetch_ohlcv.__doc__


def test_composite_provider_all_provider_types(master_json_path):
    """Test CompositeProvider with all provider types."""
    composite = CompositeProvider(
        master_json_path=master_json_path,
        provider_types=["pykrx", "fdr", "korea_investment"],
        fdr_source="KRX",
        korea_investment_appkey="test_key",
        korea_investment_appsecret="test_secret"
    )
    
    assert len(composite.providers) == 3
    assert composite.providers[0].name == "pykrx"
    assert composite.providers[1].name == "fdr"
    assert composite.providers[2].name == "korea_investment"
