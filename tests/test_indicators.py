import pandas as pd

from capybara_fetcher.indicators import compute_features, MA_WINDOWS


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
    assert "MansfieldRS" in out.columns
    assert "IsNewHigh1Y" in out.columns
    # At the very end, close is increasing so last point should be new high (after 252 days)
    assert bool(out["IsNewHigh1Y"].iloc[-1]) is True

