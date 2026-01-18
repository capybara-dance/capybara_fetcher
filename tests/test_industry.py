import pandas as pd
import pytest

from capybara_fetcher.industry import (
    INDUSTRY_LEVEL_L,
    compute_industry_feature_frame,
    compute_universe_equal_weight_benchmark_close_by_date,
)


def test_compute_industry_feature_frame_filters_empty_industry_large():
    """Test that empty IndustryLarge values are filtered out from master_df"""
    # Create test data with some empty IndustryLarge values
    dates = pd.date_range("2025-01-01", periods=5, freq="D")
    
    # Feature data for 2 stocks
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 2,
        "Ticker": ["000001"] * 5 + ["000002"] * 5,
        "Close": [100, 101, 102, 103, 104] * 2,
    })
    
    # Master data: stock 1 has valid IndustryLarge, stock 2 has NaN
    # After filtering, only stock 1 should remain in master_df
    # Stock 2 will then get NaN in merge -> normalized to "" -> mapped to "Unknown"
    master_df = pd.DataFrame({
        "Code": ["000001", "000002"],
        "Name": ["StockA", "StockB"],
        "Market": ["KOSPI", "KOSPI"],
        "IndustryLarge": ["Finance", None],  # Valid, NaN
        "IndustryMid": ["Banking", "Tech"],
        "IndustrySmall": ["Commercial", "Software"],
        "SharesOutstanding": [1000, 2000],
    })
    
    global_dates = pd.DatetimeIndex(dates)
    benchmark = compute_universe_equal_weight_benchmark_close_by_date(
        feature_df, global_dates=global_dates
    )
    
    # Call the function
    result = compute_industry_feature_frame(
        feature_df,
        master_df=master_df,
        benchmark_close_by_date=benchmark,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )
    
    # Verify that the filtering worked:
    # Stock 000001 has IndustryLarge="Finance" and contributes to Finance industry
    # Stock 000002 has IndustryLarge=None in master_df, gets filtered out,
    #   then in merge gets NaN -> "" -> "Unknown"
    assert not result.empty
    unique_industries = result["IndustryLarge"].unique()
    
    # Should have "Finance" and "Unknown" industries
    assert "Finance" in unique_industries
    assert "Unknown" in unique_industries
    

def test_compute_industry_feature_frame_filters_etf():
    """Test that ETF items are filtered out"""
    dates = pd.date_range("2025-01-01", periods=5, freq="D")
    
    # Feature data for 2 stocks
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 2,
        "Ticker": ["000001"] * 5 + ["000002"] * 5,
        "Close": [100, 101, 102, 103, 104] * 2,
    })
    
    # Master data: stock 1 is normal, stock 2 is ETF
    master_df = pd.DataFrame({
        "Code": ["000001", "000002"],
        "Name": ["StockA", "ETF_B"],
        "Market": ["KOSPI", "ETF"],
        "IndustryLarge": ["Finance", "Unknown"],
        "IndustryMid": ["Banking", ""],
        "IndustrySmall": ["Commercial", ""],
        "SharesOutstanding": [1000, 2000],
    })
    
    global_dates = pd.DatetimeIndex(dates)
    benchmark = compute_universe_equal_weight_benchmark_close_by_date(
        feature_df, global_dates=global_dates
    )
    
    # Call the function
    result = compute_industry_feature_frame(
        feature_df,
        master_df=master_df,
        benchmark_close_by_date=benchmark,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )
    
    # Verify that stock 000001 contributes to Finance industry
    # Stock 000002 (ETF) is filtered from master_df, so it becomes Unknown in merge
    assert not result.empty
    unique_industries = result["IndustryLarge"].unique()
    
    # Should have "Finance" (from stock 1) and "Unknown" (from ETF stock 2)
    assert "Finance" in unique_industries
    assert "Unknown" in unique_industries


def test_compute_industry_feature_frame_without_market_column():
    """Test that function works when Market column is not present"""
    dates = pd.date_range("2025-01-01", periods=5, freq="D")
    
    # Feature data
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 2,
        "Ticker": ["000001"] * 5 + ["000002"] * 5,
        "Close": [100, 101, 102, 103, 104] * 2,
    })
    
    # Master data without Market column
    master_df = pd.DataFrame({
        "Code": ["000001", "000002"],
        "Name": ["StockA", "StockB"],
        "IndustryLarge": ["Finance", "Tech"],
        "IndustryMid": ["Banking", "Software"],
        "IndustrySmall": ["Commercial", "Enterprise"],
        "SharesOutstanding": [1000, 2000],
    })
    
    global_dates = pd.DatetimeIndex(dates)
    benchmark = compute_universe_equal_weight_benchmark_close_by_date(
        feature_df, global_dates=global_dates
    )
    
    # Call the function - should not raise error even without Market column
    result = compute_industry_feature_frame(
        feature_df,
        master_df=master_df,
        benchmark_close_by_date=benchmark,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )
    
    # Verify both industries are present
    assert not result.empty
    unique_industries = result["IndustryLarge"].unique()
    assert len(unique_industries) == 2
    assert "Finance" in unique_industries
    assert "Tech" in unique_industries
