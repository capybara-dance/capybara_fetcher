import pandas as pd
import pytest

from capybara_fetcher.industry import compute_industry_feature_frame, INDUSTRY_LEVEL_L


def test_compute_industry_feature_frame_filters_empty_industry_large():
    """Test that rows with empty or NaN IndustryLarge values are filtered out from master_df."""
    # Create feature_df with some sample data
    dates = pd.date_range("2025-01-01", periods=5, freq="D")
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 3,
        "Ticker": ["000001"] * 5 + ["000002"] * 5 + ["000003"] * 5,
        "Close": [100, 101, 102, 103, 104] * 3,
    })

    # Create master_df with mixed IndustryLarge values:
    # - 000001: valid IndustryLarge
    # - 000002: NaN IndustryLarge (should be filtered)
    # - 000003: empty string IndustryLarge (should be filtered)
    master_df = pd.DataFrame({
        "Code": ["000001", "000002", "000003"],
        "Name": ["A", "B", "C"],
        "Market": ["KOSPI", "KOSPI", "KOSDAQ"],
        "IndustryLarge": ["Technology", None, ""],
        "IndustryMid": ["Software", "Hardware", "Services"],
        "IndustrySmall": ["Cloud", "Chips", "Consulting"],
    })

    global_dates = pd.date_range("2025-01-01", periods=5, freq="D")
    
    # Run the function
    result = compute_industry_feature_frame(
        feature_df,
        master_df=master_df,
        benchmark_close_by_date=None,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )

    # Verify that Technology industry is present with only 1 constituent (000001)
    tech_data = result[result["IndustryLarge"] == "Technology"]
    assert len(tech_data) > 0
    assert tech_data["ConstituentCount"].max() == 1
    
    # Verify that Unknown industry is present with 2 constituents (000002, 000003)
    # These tickers are filtered from master_df, so they end up as "Unknown"
    unknown_data = result[result["IndustryLarge"] == "Unknown"]
    assert len(unknown_data) > 0
    assert unknown_data["ConstituentCount"].max() == 2
    
    # The key point: tickers with empty IndustryLarge in master_df 
    # are excluded from industry classification and grouped as "Unknown"
    assert set(result["IndustryLarge"].unique()) == {"Technology", "Unknown"}


def test_compute_industry_feature_frame_keeps_all_valid_industries():
    """Test that rows with valid IndustryLarge values are retained."""
    dates = pd.date_range("2025-01-01", periods=5, freq="D")
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 2,
        "Ticker": ["000001"] * 5 + ["000002"] * 5,
        "Close": [100, 101, 102, 103, 104] * 2,
    })

    master_df = pd.DataFrame({
        "Code": ["000001", "000002"],
        "Name": ["A", "B"],
        "Market": ["KOSPI", "KOSDAQ"],
        "IndustryLarge": ["Technology", "Finance"],
        "IndustryMid": ["Software", "Banking"],
        "IndustrySmall": ["Cloud", "Retail"],
    })

    global_dates = pd.date_range("2025-01-01", periods=5, freq="D")
    
    result = compute_industry_feature_frame(
        feature_df,
        master_df=master_df,
        benchmark_close_by_date=None,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )

    # Both industries should be present
    unique_industries = sorted(result["IndustryLarge"].unique())
    assert "Technology" in unique_industries
    assert "Finance" in unique_industries


def test_compute_industry_feature_frame_filters_whitespace_industry_large():
    """Test that rows with whitespace-only IndustryLarge values are filtered out."""
    dates = pd.date_range("2025-01-01", periods=5, freq="D")
    feature_df = pd.DataFrame({
        "Date": dates.tolist() * 2,
        "Ticker": ["000001"] * 5 + ["000002"] * 5,
        "Close": [100, 101, 102, 103, 104] * 2,
    })

    # 000002 has whitespace-only IndustryLarge (should be filtered)
    master_df = pd.DataFrame({
        "Code": ["000001", "000002"],
        "Name": ["A", "B"],
        "Market": ["KOSPI", "KOSDAQ"],
        "IndustryLarge": ["Technology", "   "],
        "IndustryMid": ["Software", "Banking"],
        "IndustrySmall": ["Cloud", "Retail"],
    })

    global_dates = pd.date_range("2025-01-01", periods=5, freq="D")
    
    result = compute_industry_feature_frame(
        feature_df,
        master_df=master_df,
        benchmark_close_by_date=None,
        level=INDUSTRY_LEVEL_L,
        global_dates=global_dates,
    )

    # Only Technology should have constituent count of 1
    # 000002 should be filtered and grouped as Unknown
    tech_data = result[result["IndustryLarge"] == "Technology"]
    assert tech_data["ConstituentCount"].max() == 1
    
    unknown_data = result[result["IndustryLarge"] == "Unknown"]
    assert unknown_data["ConstituentCount"].max() == 1
