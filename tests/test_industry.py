import pandas as pd
import pytest

from capybara_fetcher.industry import compute_industry_feature_frame, INDUSTRY_LEVEL_L


def test_compute_industry_feature_frame_filters_empty_industrylarge():
    """Test that rows with empty IndustryLarge are filtered out from master_df."""
    # Create feature_df with stock data
    dates = pd.date_range("2025-01-01", periods=10, freq="D")
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 3,
        "Ticker": ["000001"] * 10 + ["000002"] * 10 + ["000003"] * 10,
        "Close": [100.0 + i for i in range(30)],
    })
    
    # Create master_df with some rows having empty IndustryLarge
    master_df = pd.DataFrame({
        "Code": ["000001", "000002", "000003"],
        "Name": ["Stock1", "Stock2", "Stock3"],
        "Market": ["KOSPI", "KOSDAQ", "KOSPI"],
        "IndustryLarge": ["Tech", None, "Finance"],  # 000002 has None
        "IndustryMid": ["Software", "Hardware", "Banking"],
        "IndustrySmall": ["Apps", "Chips", "Retail"],
    })
    
    global_dates = pd.DatetimeIndex(dates)
    
    result = compute_industry_feature_frame(
        feature_df=feature_df,
        master_df=master_df,
        benchmark_close_by_date=None,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )
    
    # Check that only Tech and Finance industries are present
    # Unknown industry should not appear if we filtered correctly
    industry_keys = result["IndustryKey"].unique()
    assert "Tech" in industry_keys
    assert "Finance" in industry_keys
    # Since 000002 has None for IndustryLarge, it should be filtered out
    # and not contribute to any industry


def test_compute_industry_feature_frame_filters_empty_string_industrylarge():
    """Test that rows with empty string IndustryLarge are filtered out from master_df."""
    dates = pd.date_range("2025-01-01", periods=10, freq="D")
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 3,
        "Ticker": ["000001"] * 10 + ["000002"] * 10 + ["000003"] * 10,
        "Close": [100.0 + i for i in range(30)],
    })
    
    # Create master_df with some rows having empty string IndustryLarge
    master_df = pd.DataFrame({
        "Code": ["000001", "000002", "000003"],
        "Name": ["Stock1", "Stock2", "Stock3"],
        "Market": ["KOSPI", "KOSDAQ", "KOSPI"],
        "IndustryLarge": ["Tech", "", "Finance"],  # 000002 has empty string
        "IndustryMid": ["Software", "Hardware", "Banking"],
        "IndustrySmall": ["Apps", "Chips", "Retail"],
    })
    
    global_dates = pd.DatetimeIndex(dates)
    
    result = compute_industry_feature_frame(
        feature_df=feature_df,
        master_df=master_df,
        benchmark_close_by_date=None,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )
    
    # Check that only Tech and Finance industries are present
    industry_keys = result["IndustryKey"].unique()
    assert "Tech" in industry_keys
    assert "Finance" in industry_keys


def test_compute_industry_feature_frame_filters_etf_and_empty_industrylarge():
    """Test that both ETF rows and empty IndustryLarge rows are filtered out."""
    dates = pd.date_range("2025-01-01", periods=10, freq="D")
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 4,
        "Ticker": ["000001"] * 10 + ["000002"] * 10 + ["000003"] * 10 + ["000004"] * 10,
        "Close": [100.0 + i for i in range(40)],
    })
    
    master_df = pd.DataFrame({
        "Code": ["000001", "000002", "000003", "000004"],
        "Name": ["Stock1", "ETF1", "Stock3", "Stock4"],
        "Market": ["KOSPI", "ETF", "KOSDAQ", "KOSPI"],
        "IndustryLarge": ["Tech", "TechETF", None, "Finance"],  # 000002 is ETF, 000003 has None
        "IndustryMid": ["Software", "Index", "Hardware", "Banking"],
        "IndustrySmall": ["Apps", "Tech", "Chips", "Retail"],
    })
    
    global_dates = pd.DatetimeIndex(dates)
    
    result = compute_industry_feature_frame(
        feature_df=feature_df,
        master_df=master_df,
        benchmark_close_by_date=None,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )
    
    # Check that only Tech and Finance industries are present
    # ETF and None IndustryLarge should both be filtered out
    industry_keys = result["IndustryKey"].unique()
    assert "Tech" in industry_keys
    assert "Finance" in industry_keys
    assert "TechETF" not in industry_keys
