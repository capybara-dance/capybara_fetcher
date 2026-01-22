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

    # Moving averages
    for w in MA_WINDOWS:
        df[f"SMA_{w}"] = close.rolling(window=w, min_periods=w).mean()

    # Mansfield Relative Strength (vs benchmark)
    # Mansfield RS는 종목의 벤치마크 대비 상대적 강도를 측정하는 지표입니다.
    # 계산 공식: MansfieldRS = (RS_raw / RS_sma - 1) × 100
    # - RS_raw: 종목 종가 / 벤치마크 종가
    # - RS_sma: RS_raw의 200일 이동평균
    # 자세한 설명은 docs/MANSFIELD_RS.md 참조
    if benchmark_close_by_date is not None and not benchmark_close_by_date.empty:
        # Pandas requires unique index for fast mapping; enforce here (fail-fast elsewhere).
        # If duplicates exist, keep the last observed value for each date.
        if not benchmark_close_by_date.index.is_unique:
            benchmark_close_by_date = benchmark_close_by_date[~benchmark_close_by_date.index.duplicated(keep="last")]

        # Step 1: 날짜별 벤치마크 매핑 및 Raw RS 계산
        bench = df["Date"].dt.normalize().map(benchmark_close_by_date)
        bench = pd.to_numeric(bench, errors="coerce")
        rs_raw = close / bench  # RS_raw(t) = Close_ticker(t) / Close_benchmark(t)
        
        # Step 2: RS의 200일 이동평균 계산
        rs_sma = rs_raw.rolling(window=MANSFIELD_RS_SMA_WINDOW, min_periods=MANSFIELD_RS_SMA_WINDOW).mean()
        
        # Step 3: Mansfield RS 계산 (백분율)
        # 양수: 최근 200일 평균보다 강함 (상승 추세)
        # 음수: 최근 200일 평균보다 약함 (하락 추세)
        df["MansfieldRS"] = (rs_raw / rs_sma - 1.0) * 100.0
    else:
        df["MansfieldRS"] = pd.NA

    # 1Y new high (Close is the highest close in last ~1 year trading days, inclusive)
    roll_max = close.rolling(window=NEW_HIGH_WINDOW_TRADING_DAYS, min_periods=NEW_HIGH_WINDOW_TRADING_DAYS).max()
    df["IsNewHigh1Y"] = close.eq(roll_max).astype("boolean")

    return df

