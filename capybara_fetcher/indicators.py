from __future__ import annotations

import pandas as pd


MA_WINDOWS = [5, 10, 20, 60, 120, 200]
NEW_HIGH_WINDOW_TRADING_DAYS = 252
MANSFIELD_RS_SMA_WINDOW = 200


def compute_features(
    ohlcv_df: pd.DataFrame,
    *,
    benchmark_close_by_date: pd.Series | None,
) -> pd.DataFrame:
    """
    Add feature columns to standardized OHLCV.

    Expected input:
      Date, Open, High, Low, Close, Volume, TradingValue, Change, Ticker
    """
    if ohlcv_df is None or ohlcv_df.empty:
        raise ValueError("ohlcv_df is empty")

    df = ohlcv_df.copy()
    if "Date" not in df.columns or "Close" not in df.columns:
        raise ValueError("ohlcv_df missing Date/Close")

    df = df.sort_values("Date")
    close = pd.to_numeric(df["Close"], errors="raise")
    high = pd.to_numeric(df["High"], errors="raise")

    # Moving averages
    for w in MA_WINDOWS:
        df[f"SMA_{w}"] = close.rolling(window=w, min_periods=w).mean()

    # Mansfield Relative Strength (vs benchmark)
    if benchmark_close_by_date is not None and not benchmark_close_by_date.empty:
        # Pandas requires unique index for fast mapping; enforce here (fail-fast elsewhere).
        # If duplicates exist, keep the last observed value for each date.
        if not benchmark_close_by_date.index.is_unique:
            benchmark_close_by_date = benchmark_close_by_date[~benchmark_close_by_date.index.duplicated(keep="last")]

        bench = df["Date"].dt.normalize().map(benchmark_close_by_date)
        bench = pd.to_numeric(bench, errors="coerce")
        rs_raw = close / bench
        rs_sma = rs_raw.rolling(window=MANSFIELD_RS_SMA_WINDOW, min_periods=MANSFIELD_RS_SMA_WINDOW).mean()
        df["MansfieldRS"] = (rs_raw / rs_sma - 1.0) * 100.0
    else:
        df["MansfieldRS"] = pd.NA

    # 1Y new high (High is the highest high in last ~1 year trading days, inclusive)
    roll_max = high.rolling(window=NEW_HIGH_WINDOW_TRADING_DAYS, min_periods=NEW_HIGH_WINDOW_TRADING_DAYS).max()
    df["IsNewHigh1Y"] = high.eq(roll_max).astype("boolean")

    return df

