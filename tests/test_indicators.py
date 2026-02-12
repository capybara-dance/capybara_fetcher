import pandas as pd

from capybara_fetcher.indicators import compute_features, MA_WINDOWS, MRS_WINDOWS, VMA_WINDOWS


def test_compute_features_adds_columns_and_new_high_flag():
    dates = pd.date_range("2025-01-01", periods=260, freq="D")
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": range(1, 261),
            "High": range(1, 261),
            "Low": range(1, 261),
            "Close": range(1, 261),
            "Volume": [100] * 260,
            "TradingValue": [None] * 260,
            "Change": [None] * 260,
            "Ticker": ["000001"] * 260,
        }
    )
    bench = pd.Series([100.0] * 260, index=dates.normalize())

    out = compute_features(df, benchmark_close_by_date=bench)
    for w in MA_WINDOWS:
        assert f"SMA_{w}" in out.columns
    for w in VMA_WINDOWS:
        assert f"VMA_{w}" in out.columns
    assert "MansfieldRS" in out.columns
    assert "IsNewHigh1Y" in out.columns
    # At the very end, close is increasing so last point should be new high (after 252 days)
    assert bool(out["IsNewHigh1Y"].iloc[-1]) is True


def test_compute_features_handles_duplicate_benchmark_index():
    dates = pd.date_range("2025-01-01", periods=260, freq="D")
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": range(1, 261),
            "High": range(1, 261),
            "Low": range(1, 261),
            "Close": range(1, 261),
            "Volume": [100] * 260,
            "TradingValue": [None] * 260,
            "Change": [None] * 260,
            "Ticker": ["000001"] * 260,
        }
    )
    # Duplicate benchmark index on purpose
    bench = pd.Series([100.0] * 260, index=dates.normalize())
    bench2 = pd.concat([bench, bench])  # duplicate dates

    out = compute_features(df, benchmark_close_by_date=bench2)
    assert "MansfieldRS" in out.columns


def test_compute_features_adds_mrs_raw_columns():
    """Test that multi-timeframe MRS raw columns are added."""
    dates = pd.date_range("2025-01-01", periods=260, freq="D")
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": range(1, 261),
            "High": range(1, 261),
            "Low": range(1, 261),
            "Close": range(1, 261),
            "Volume": [100] * 260,
            "TradingValue": [None] * 260,
            "Change": [None] * 260,
            "Ticker": ["000001"] * 260,
        }
    )
    bench = pd.Series([100.0] * 260, index=dates.normalize())

    out = compute_features(df, benchmark_close_by_date=bench)
    
    # Check that all MRS raw columns are present
    for col_name in MRS_WINDOWS.keys():
        assert f"{col_name}_raw" in out.columns, f"{col_name}_raw not found"
    
    # Verify that MRS_12M_raw has valid values after 250 days
    assert out[f"MRS_12M_raw"].notna().sum() > 0, "MRS_12M_raw should have non-null values"


def test_mrs_percentile_conversion():
    """Test that percentile conversion works correctly across multiple stocks."""
    dates = pd.date_range("2025-01-01", periods=260, freq="D")
    
    # Create 3 stocks with different performance
    dfs = []
    for i, ticker in enumerate(["000001", "000002", "000003"]):
        df = pd.DataFrame(
            {
                "Date": dates,
                "Close": range(1 + i * 100, 261 + i * 100),  # Different price levels
                "Ticker": [ticker] * 260,
            }
        )
        dfs.append(df)
    
    combined = pd.concat(dfs, ignore_index=True)
    bench = pd.Series([100.0] * 260, index=dates.normalize())
    
    # Simulate adding raw MRS columns (using a simple value for testing)
    combined["MRS_1M_raw"] = combined["Close"] * 0.1  # Proportional to close
    
    # Calculate percentiles (same logic as in orchestrator)
    combined["MRS_1M"] = (
        combined.groupby("Date")["MRS_1M_raw"]
        .rank(pct=True, method="average")
        .mul(100.0)
        .round(2)
    )
    
    # Check that percentiles are in valid range
    assert combined["MRS_1M"].min() >= 0.0
    assert combined["MRS_1M"].max() <= 100.0
    
    # Check that highest performer gets highest percentile on each date
    for date in dates[:10]:  # Check first 10 dates
        date_data = combined[combined["Date"] == date].sort_values("MRS_1M_raw", ascending=False)
        if len(date_data) == 3:
            # Highest raw value should have highest percentile
            assert date_data.iloc[0]["MRS_1M"] == date_data["MRS_1M"].max()
            # Lowest raw value should have lowest percentile
            assert date_data.iloc[-1]["MRS_1M"] == date_data["MRS_1M"].min()


def test_volume_moving_averages():
    """Test that volume moving averages are calculated correctly."""
    dates = pd.date_range("2025-01-01", periods=100, freq="D")
    # Create increasing volume
    volumes = list(range(100, 200))
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": [100] * 100,
            "High": [110] * 100,
            "Low": [90] * 100,
            "Close": [100] * 100,
            "Volume": volumes,
            "TradingValue": [None] * 100,
            "Change": [None] * 100,
            "Ticker": ["000001"] * 100,
        }
    )
    bench = pd.Series([100.0] * 100, index=dates.normalize())
    
    out = compute_features(df, benchmark_close_by_date=bench)
    
    # Check that VMA columns exist
    assert "VMA_20" in out.columns
    assert "VMA_50" in out.columns
    
    # Check that VMA_20 has values after 20 days
    assert out["VMA_20"].notna().sum() == 81  # 100 - 20 + 1
    assert pd.isna(out["VMA_20"].iloc[18])  # 19th day should be NA (0-indexed)
    assert pd.notna(out["VMA_20"].iloc[19])  # 20th day should have a value
    
    # Check that VMA_50 has values after 50 days
    assert out["VMA_50"].notna().sum() == 51  # 100 - 50 + 1
    assert pd.isna(out["VMA_50"].iloc[48])  # 49th day should be NA (0-indexed)
    assert pd.notna(out["VMA_50"].iloc[49])  # 50th day should have a value
    
    # Verify calculation for VMA_20 at day 20 (index 19)
    # Should be average of volumes[0:20] = range(100, 120)
    expected_vma_20 = sum(range(100, 120)) / 20
    assert abs(out["VMA_20"].iloc[19] - expected_vma_20) < 0.01


