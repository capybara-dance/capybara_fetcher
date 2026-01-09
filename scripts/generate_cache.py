import os
import argparse
import datetime
import json
import platform
import sys
import pandas as pd
from pykrx import stock
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time
from time import perf_counter
from importlib.metadata import version as pkg_version, PackageNotFoundError

MA_WINDOWS = [5, 10, 20, 60, 120, 200]
NEW_HIGH_WINDOW_TRADING_DAYS = 252
MANSFIELD_BENCHMARK_TICKER = "069500"
MANSFIELD_RS_SMA_WINDOW = 200

KRX_STOCK_MASTER_JSON_DEFAULT = "/workspace/data/krx_stock_master.json"

INDUSTRY_LEVEL_L = "L"
INDUSTRY_LEVEL_LM = "LM"
INDUSTRY_LEVEL_LMS = "LMS"
INDUSTRY_LEVELS = [INDUSTRY_LEVEL_L, INDUSTRY_LEVEL_LM, INDUSTRY_LEVEL_LMS]

INDUSTRY_BENCHMARK_069500 = "069500"
INDUSTRY_BENCHMARK_UNIVERSE = "universe"

def load_krx_stock_master_df(path: str) -> tuple[pd.DataFrame, str | None]:
    """
    KRX 종목 마스터(JSON)을 DataFrame으로 로드합니다.
    Returns:
      - df: 최소 컬럼(Code, Market, IndustryLarge/Mid/Small 포함)
      - error: 실패 시 에러 문자열, 성공 시 None
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        if df.empty:
            return pd.DataFrame(), "EmptyMasterError: krx_stock_master.json is empty"

        # Ensure required columns exist
        cols = ["Code", "Market", "Name", "IndustryLarge", "IndustryMid", "IndustrySmall"]
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA

        out = df[cols].copy()
        out["Code"] = out["Code"].astype(str).str.strip().str.zfill(6)
        out["Market"] = out["Market"].astype(str).str.strip()
        out["Name"] = out["Name"].astype(str).str.strip()
        out["IndustryLarge"] = out["IndustryLarge"].astype(str).str.strip()
        out["IndustryMid"] = out["IndustryMid"].astype(str).str.strip()
        out["IndustrySmall"] = out["IndustrySmall"].astype(str).str.strip()

        out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"])
        return out, None
    except Exception as e:
        return pd.DataFrame(), f"{type(e).__name__}: {e}"

def _normalize_industry_value(v: object) -> str:
    s = "" if v is None else str(v).strip()
    if s.lower() in {"nan", "none"}:
        return ""
    return s

def _industry_key_large(large: str) -> str:
    return large or "Unknown"

def _industry_key_large_mid(large: str, mid: str) -> str:
    return f"{large or 'Unknown'}||{mid or 'Unknown'}"

def _industry_key_large_mid_small(large: str, mid: str, small: str) -> str:
    return f"{large or 'Unknown'}||{mid or 'Unknown'}||{small or 'Unknown'}"

def _compute_industry_level_frame(
    feature_df: pd.DataFrame,
    master_df: pd.DataFrame,
    benchmark_close_by_date: pd.Series | None,
    level: str,
    global_dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    A안(업종 지수 생성 후 Mansfield RS 적용), 동일가중(일간 수익률 평균) 방식으로 업종 지표를 생성합니다.
    출력은 long format: Date, Level, Industry(L/M/S), IndustryClose, IndustryReturn, ConstituentCount, MansfieldRS
    """
    if feature_df is None or feature_df.empty:
        return pd.DataFrame()

    df = feature_df[["Date", "Ticker", "Close"]].copy()
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.zfill(6)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df.dropna(subset=["Date", "Ticker", "Close"]).sort_values(["Ticker", "Date"])

    m = master_df.copy()
    if "Code" in m.columns:
        m["Code"] = m["Code"].astype(str).str.strip().str.zfill(6)
    for c in ["IndustryLarge", "IndustryMid", "IndustrySmall"]:
        if c not in m.columns:
            m[c] = ""
        m[c] = m[c].apply(_normalize_industry_value)

    df = df.merge(m[["Code", "IndustryLarge", "IndustryMid", "IndustrySmall"]], left_on="Ticker", right_on="Code", how="left")
    df["IndustryLarge"] = df["IndustryLarge"].apply(_normalize_industry_value)
    df["IndustryMid"] = df["IndustryMid"].apply(_normalize_industry_value)
    df["IndustrySmall"] = df["IndustrySmall"].apply(_normalize_industry_value)
    df = df.drop(columns=["Code"])

    # IndustryKey + output columns by level
    if level == INDUSTRY_LEVEL_L:
        df["IndustryKey"] = df["IndustryLarge"].map(_industry_key_large)
        out_large = df["IndustryLarge"].map(lambda x: x or "Unknown")
        out_mid = pd.Series([""] * len(df), index=df.index)
        out_small = pd.Series([""] * len(df), index=df.index)
    elif level == INDUSTRY_LEVEL_LM:
        df["IndustryKey"] = [
            _industry_key_large_mid(a, b) for a, b in zip(df["IndustryLarge"].tolist(), df["IndustryMid"].tolist())
        ]
        out_large = df["IndustryLarge"].map(lambda x: x or "Unknown")
        out_mid = df["IndustryMid"].map(lambda x: x or "Unknown")
        out_small = pd.Series([""] * len(df), index=df.index)
    elif level == INDUSTRY_LEVEL_LMS:
        df["IndustryKey"] = [
            _industry_key_large_mid_small(a, b, c)
            for a, b, c in zip(df["IndustryLarge"].tolist(), df["IndustryMid"].tolist(), df["IndustrySmall"].tolist())
        ]
        out_large = df["IndustryLarge"].map(lambda x: x or "Unknown")
        out_mid = df["IndustryMid"].map(lambda x: x or "Unknown")
        out_small = df["IndustrySmall"].map(lambda x: x or "Unknown")
    else:
        raise ValueError(f"Invalid industry level: {level}")

    df["IndustryLargeOut"] = out_large
    df["IndustryMidOut"] = out_mid
    df["IndustrySmallOut"] = out_small

    # Per-ticker returns
    df["Ret"] = df.groupby("Ticker", sort=False)["Close"].pct_change()

    # Equal-weight daily return per industry (mean of available constituent returns)
    g = (
        df.groupby(["IndustryKey", "Date"], sort=True)
        .agg(IndustryReturn=("Ret", "mean"), ConstituentCount=("Ret", "count"))
        .reset_index()
    )

    # Expand to full date grid per IndustryKey (keeps chart/rolling stable)
    keys = sorted(g["IndustryKey"].unique().tolist())
    if not keys:
        return pd.DataFrame()

    full_idx = pd.MultiIndex.from_product([keys, global_dates], names=["IndustryKey", "Date"])
    g = g.set_index(["IndustryKey", "Date"]).reindex(full_idx).sort_index()
    g["IndustryReturn"] = pd.to_numeric(g["IndustryReturn"], errors="coerce").fillna(0.0)
    g["ConstituentCount"] = pd.to_numeric(g["ConstituentCount"], errors="coerce").fillna(0).astype("int64")

    # Industry index (base 100)
    g["IndustryClose"] = (1.0 + g["IndustryReturn"]).groupby(level=0, sort=False).cumprod() * 100.0

    out = g.reset_index()
    out["Level"] = level

    # Attach industry L/M/S labels per key (dedupe mapping from original df)
    key_map = (
        df[["IndustryKey", "IndustryLargeOut", "IndustryMidOut", "IndustrySmallOut"]]
        .drop_duplicates(subset=["IndustryKey"])
        .set_index("IndustryKey")
    )
    out["IndustryLarge"] = out["IndustryKey"].map(key_map["IndustryLargeOut"]).fillna("Unknown")
    out["IndustryMid"] = out["IndustryKey"].map(key_map["IndustryMidOut"]).fillna("")
    out["IndustrySmall"] = out["IndustryKey"].map(key_map["IndustrySmallOut"]).fillna("")

    # Mansfield RS vs benchmark
    if benchmark_close_by_date is not None and not benchmark_close_by_date.empty:
        b = out["Date"].map(benchmark_close_by_date)
        b = pd.to_numeric(b, errors="coerce")
        rs_raw = out["IndustryClose"] / b
        rs_sma = rs_raw.groupby(out["IndustryKey"]).transform(
            lambda s: s.rolling(window=MANSFIELD_RS_SMA_WINDOW, min_periods=MANSFIELD_RS_SMA_WINDOW).mean()
        )
        out["MansfieldRS"] = (rs_raw / rs_sma - 1.0) * 100.0
    else:
        out["MansfieldRS"] = pd.NA

    # Column order
    out = out[
        [
            "Date",
            "Level",
            "IndustryLarge",
            "IndustryMid",
            "IndustrySmall",
            "IndustryKey",
            "IndustryClose",
            "IndustryReturn",
            "ConstituentCount",
            "MansfieldRS",
        ]
    ].sort_values(["Level", "IndustryLarge", "IndustryMid", "IndustrySmall", "Date"])
    return out

def _compute_universe_equal_weight_benchmark_close_by_date(
    feature_df: pd.DataFrame,
    global_dates: pd.DatetimeIndex,
) -> pd.Series:
    """
    유니버스(전 종목) 동일가중 벤치마크 지수(기준값 100) 시계열을 생성합니다.
    - 일간 수익률: 종목별 pct_change
    - 유니버스 수익률: 해당 일자 ret가 있는 종목들의 평균
    - 지수: (1+ret).cumprod * 100
    Returns:
      - Series indexed by normalized Date with float values (benchmark close)
    """
    df = feature_df[["Date", "Ticker", "Close"]].copy()
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.zfill(6)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df.dropna(subset=["Date", "Ticker", "Close"]).sort_values(["Ticker", "Date"])

    df["Ret"] = df.groupby("Ticker", sort=False)["Close"].pct_change()
    u = df.groupby("Date", sort=True).agg(UniverseReturn=("Ret", "mean")).reset_index()

    u = u.set_index("Date").reindex(global_dates).sort_index()
    u["UniverseReturn"] = pd.to_numeric(u["UniverseReturn"], errors="coerce").fillna(0.0)
    bench = (1.0 + u["UniverseReturn"]).cumprod() * 100.0
    bench.name = "UniverseClose"
    bench.index = pd.DatetimeIndex(pd.to_datetime(bench.index, errors="coerce")).normalize()
    return bench

def load_universe_from_krx_stock_master_json(path: str) -> tuple[list[str], dict[str, str], str | None]:
    """
    KRX 종목 마스터(JSON)에서 유니버스를 구성합니다.
    Returns:
      - tickers: 종목코드(6자리 문자열) 리스트
      - market_by_ticker: {종목코드: 시장(KOSPI/KOSDAQ)}
      - error: 실패 시 에러 문자열, 성공 시 None
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        if df.empty:
            return [], {}, "EmptyMasterError: krx_stock_master.json is empty"
        if "Code" not in df.columns or "Market" not in df.columns:
            return [], {}, "InvalidMasterError: missing Code/Market fields"
        df["Code"] = df["Code"].astype(str).str.strip().str.zfill(6)
        df["Market"] = df["Market"].astype(str).str.strip()
        df = df.dropna(subset=["Code", "Market"])
        tickers = sorted(df["Code"].unique().tolist())
        market_by_ticker = dict(zip(df["Code"].tolist(), df["Market"].tolist()))
        return tickers, market_by_ticker, None
    except Exception as e:
        return [], {}, f"{type(e).__name__}: {e}"

def fetch_data(ticker: str, start_date: str, end_date: str, benchmark_close_by_date: pd.Series | None):
    """
    개별 종목의 OHLCV 데이터를 수집합니다.
    Feature 계산 로직을 이곳에 추가할 수 있습니다.
    """
    try:
        # pykrx 수집 (adjusted=True for 수정주가)
        df = stock.get_market_ohlcv(start_date, end_date, ticker, adjusted=True)
        
        if df is None or df.empty:
            return None
            
        # 컬럼명 영문 변환
        rename_map = {
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
            "거래대금": "TradingValue",
            "등락률": "Change"
        }
        df = df.rename(columns=rename_map)
        
        # 날짜 인덱스 처리
        df.index.name = 'Date'
        df = df.reset_index()
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        
        # 티커 컬럼 추가
        df["Ticker"] = ticker

        # --- Indicators ---
        close = pd.to_numeric(df["Close"], errors="coerce")

        # Moving averages
        for w in MA_WINDOWS:
            df[f"SMA_{w}"] = close.rolling(window=w, min_periods=w).mean()

        # Mansfield Relative Strength (vs benchmark)
        if benchmark_close_by_date is not None and not benchmark_close_by_date.empty:
            # Map benchmark close to this ticker's dates
            b = df["Date"].dt.normalize().map(benchmark_close_by_date)
            b = pd.to_numeric(b, errors="coerce")
            rs_raw = close / b
            rs_sma = rs_raw.rolling(window=MANSFIELD_RS_SMA_WINDOW, min_periods=MANSFIELD_RS_SMA_WINDOW).mean()
            df["MansfieldRS"] = (rs_raw / rs_sma - 1.0) * 100.0
        else:
            df["MansfieldRS"] = pd.NA

        # 1Y new high (Close is the highest close in last ~1 year trading days, inclusive)
        roll_max = close.rolling(window=NEW_HIGH_WINDOW_TRADING_DAYS, min_periods=NEW_HIGH_WINDOW_TRADING_DAYS).max()
        df["IsNewHigh1Y"] = close.eq(roll_max).astype("boolean")
        
        return df
    except Exception as e:
        # 로그가 너무 많아질 수 있으므로 에러 발생 시 None 반환
        return None

def _write_parquet(df: pd.DataFrame, output_path: str) -> None:
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    df.to_parquet(output_path, compression="zstd")

def _write_json(data: dict, output_path: str) -> None:
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)

def _empty_feature_frame() -> pd.DataFrame:
    # 최소 스키마(다운스트림에서 파일 존재/파싱 가능하도록)
    return pd.DataFrame(
        columns=[
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "TradingValue",
            "Change",
            "Ticker",
            *[f"SMA_{w}" for w in MA_WINDOWS],
            "MansfieldRS",
            "IsNewHigh1Y",
        ]
    )

def _safe_pkg_version(dist_name: str) -> str | None:
    try:
        return pkg_version(dist_name)
    except PackageNotFoundError:
        return None

def _safe_file_size_bytes(path: str) -> int | None:
    try:
        return int(os.path.getsize(path))
    except OSError:
        return None

def _bytes_to_mb(size_bytes: int | None) -> float | None:
    if size_bytes is None:
        return None
    return round(size_bytes / (1024 * 1024), 4)

def main():
    parser = argparse.ArgumentParser(description="Generate Korea Universe Feature Cache")
    parser.add_argument("--start-date", type=str, default=(datetime.datetime.now() - datetime.timedelta(days=365*3)).strftime("%Y%m%d"), help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", type=str, default=datetime.datetime.now().strftime("%Y%m%d"), help="End date (YYYYMMDD)")
    parser.add_argument("--output", type=str, default="korea_universe_feature_frame.parquet", help="Output parquet file path")
    parser.add_argument("--meta-output", type=str, default="", help="Output metadata json file path (default: <output>.meta.json)")
    parser.add_argument("--industry-output", type=str, default="", help="Output industry parquet file path (optional)")
    parser.add_argument("--industry-meta-output", type=str, default="", help="Output industry metadata json path (optional)")
    parser.add_argument(
        "--industry-benchmark",
        type=str,
        default=INDUSTRY_BENCHMARK_UNIVERSE,
        choices=[INDUSTRY_BENCHMARK_UNIVERSE, INDUSTRY_BENCHMARK_069500],
        help="Industry Mansfield RS benchmark: 'universe' (equal-weight all stocks) or '069500'",
    )
    parser.add_argument("--krx-stock-master-json", type=str, default=KRX_STOCK_MASTER_JSON_DEFAULT, help="Path to krx_stock_master.json")
    parser.add_argument("--max-workers", type=int, default=8, help="Number of threads")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit number of tickers for testing (0 for all)")
    
    args = parser.parse_args()
    
    print(f"Start generating cache from {args.start_date} to {args.end_date}...")
    t0 = perf_counter()
    started_at = datetime.datetime.now(datetime.timezone.utc)
    meta_output = args.meta_output or f"{args.output}.meta.json"
    industry_output = args.industry_output.strip() if args.industry_output else ""
    industry_meta_output = args.industry_meta_output.strip() if args.industry_meta_output else ""
    if industry_output and not industry_meta_output:
        industry_meta_output = f"{industry_output}.meta.json"
    if industry_meta_output and not industry_output:
        # Treat as no-op to avoid confusion
        industry_meta_output = ""
    
    # 1. 유니버스 구성 (KRX master JSON 기준)
    t_universe0 = perf_counter()
    tickers, market_by_ticker, universe_error = load_universe_from_krx_stock_master_json(args.krx_stock_master_json)
    t_universe1 = perf_counter()
    if not tickers:
        print("No tickers found. Writing metadata only.")
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "universe_fetch": {
                    "success": False,
                    "last_error": universe_error,
                },
                "universe_source": "krx_stock_master_json",
                "universe_input": {
                    "krx_stock_master_json": args.krx_stock_master_json,
                },
                "tickers": [],
                "ticker_count": 0,
                "rows": 0,
                "columns": list(_empty_feature_frame().columns),
                "features": [],
                "indicators": {
                    "moving_averages": MA_WINDOWS,
                    "mansfield_rs": {
                        "benchmark_ticker": MANSFIELD_BENCHMARK_TICKER,
                        "sma_window": MANSFIELD_RS_SMA_WINDOW,
                        "benchmark_fetch": {"success": False, "ticker": MANSFIELD_BENCHMARK_TICKER, "error": "Universe fetch failed; benchmark not attempted"},
                    },
                    "new_high_1y": {
                        "window_trading_days": NEW_HIGH_WINDOW_TRADING_DAYS,
                    },
                },
                "data_file": {
                    "path": args.output,
                    "generated": False,
                    "size_mb": None,
                },
                "notes": "Universe fetch failed; metadata-only release.",
                "timing_seconds": {
                    "universe_load": round(t_universe1 - t_universe0, 4),
                    "total": round(perf_counter() - t0, 4),
                },
                "env": {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "pandas": _safe_pkg_version("pandas"),
                    "pyarrow": _safe_pkg_version("pyarrow"),
                    "pykrx": _safe_pkg_version("pykrx"),
                },
            },
            meta_output,
        )
        return

    # Full master DF for industry mapping (optional)
    master_df, master_error = load_krx_stock_master_df(args.krx_stock_master_json)

    # Benchmark (for Mansfield RS)
    t_bench0 = perf_counter()
    benchmark_fetch = {"success": True, "ticker": MANSFIELD_BENCHMARK_TICKER, "error": None}
    benchmark_close_by_date: pd.Series | None
    try:
        bench = stock.get_market_ohlcv(args.start_date, args.end_date, MANSFIELD_BENCHMARK_TICKER, adjusted=True)
        if bench is None or bench.empty:
            raise ValueError("Empty benchmark OHLCV")
        bench = bench.rename(columns={"종가": "Close"})
        bench.index.name = "Date"
        bench = bench.reset_index()
        bench["Date"] = pd.to_datetime(bench["Date"], errors="coerce").dt.normalize()
        bench = bench.dropna(subset=["Date"])
        benchmark_close_by_date = pd.to_numeric(bench["Close"], errors="coerce")
        benchmark_close_by_date.index = bench["Date"]
    except Exception as e:
        benchmark_fetch = {"success": False, "ticker": MANSFIELD_BENCHMARK_TICKER, "error": f"{type(e).__name__}: {e}"}
        benchmark_close_by_date = None
    t_bench1 = perf_counter()

    if args.test_limit > 0:
        print(f"Testing with first {args.test_limit} tickers only.")
        tickers = tickers[:args.test_limit]

    # 2. 병렬 데이터 수집
    t_fetch0 = perf_counter()
    results = []
    print(f"Fetching data for {len(tickers)} tickers with {args.max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(fetch_data, ticker, args.start_date, args.end_date, benchmark_close_by_date): ticker
            for ticker in tickers
        }
        
        for future in tqdm(futures, total=len(tickers)):
            res = future.result()
            if res is not None:
                results.append(res)
    t_fetch1 = perf_counter()
    
    print(f"Fetched {len(results)}/{len(tickers)} tickers successfully.")
    
    # 3. 병합 및 저장
    if results:
        t_save0 = perf_counter()
        print("Concatenating data...")
        full_df = pd.concat(results, ignore_index=True)
        
        # Date, Ticker 기준으로 정렬
        if "Date" in full_df.columns and "Ticker" in full_df.columns:
            full_df = full_df.sort_values(by=["Date", "Ticker"])
        
        print(f"Saving to {args.output}...")
        _write_parquet(full_df, args.output)
        t_save1 = perf_counter()

        # 3.5) Industry cache (A안, 동일가중) 저장 (optional)
        industry_generated = False
        industry_rows = 0
        industry_cols: list[str] = []
        industry_error: str | None = None
        if industry_output:
            try:
                if master_error is not None or master_df is None or master_df.empty:
                    raise ValueError(f"KRX stock master load failed: {master_error}")

                # Use feature date range as the industry grid
                global_dates = pd.to_datetime(full_df["Date"], errors="coerce").dt.normalize().dropna().sort_values().unique()
                global_dates = pd.DatetimeIndex(global_dates)

                # Industry Mansfield RS benchmark
                industry_benchmark_fetch: dict
                if args.industry_benchmark == INDUSTRY_BENCHMARK_UNIVERSE:
                    industry_benchmark_close_by_date = _compute_universe_equal_weight_benchmark_close_by_date(full_df, global_dates)
                    industry_benchmark_fetch = {
                        "type": INDUSTRY_BENCHMARK_UNIVERSE,
                        "method": "equal_weighted_daily_return_mean_then_cumprod_base_100",
                        "success": True,
                        "error": None,
                    }
                else:
                    industry_benchmark_close_by_date = benchmark_close_by_date
                    industry_benchmark_fetch = {
                        "type": INDUSTRY_BENCHMARK_069500,
                        "ticker": MANSFIELD_BENCHMARK_TICKER,
                        "success": bool(benchmark_fetch.get("success")),
                        "error": benchmark_fetch.get("error"),
                    }

                ind_frames = []
                for lvl in INDUSTRY_LEVELS:
                    ind_frames.append(
                        _compute_industry_level_frame(full_df, master_df, industry_benchmark_close_by_date, lvl, global_dates)
                    )
                industry_df = pd.concat([f for f in ind_frames if f is not None and not f.empty], ignore_index=True)

                if industry_df is not None and not industry_df.empty:
                    _write_parquet(industry_df, industry_output)
                    industry_generated = True
                    industry_rows = int(len(industry_df))
                    industry_cols = list(industry_df.columns)
                else:
                    industry_error = "NoIndustryData: industry frame is empty"
            except Exception as e:
                industry_error = f"{type(e).__name__}: {e}"
                industry_benchmark_fetch = {
                    "type": args.industry_benchmark,
                    "success": False,
                    "error": industry_error,
                }

            # Industry metadata (written even if industry failed; useful for debugging)
            _write_json(
                {
                    "generated_at_utc": started_at.isoformat(),
                    "start_date": args.start_date,
                    "end_date": args.end_date,
                    "source_feature_parquet": args.output,
                    "industry_levels": INDUSTRY_LEVELS,
                    "method": {
                        "industry_index": "equal_weighted_daily_return_mean_then_cumprod_base_100",
                        "mansfield_rs": {
                            "benchmark": args.industry_benchmark,
                            "sma_window": MANSFIELD_RS_SMA_WINDOW,
                        },
                    },
                    "master_source": {
                        "krx_stock_master_json": args.krx_stock_master_json,
                        "load_success": master_error is None and master_df is not None and not master_df.empty,
                        "error": master_error,
                    },
                    "benchmark_fetch": industry_benchmark_fetch,
                    "data_file": {
                        "path": industry_output,
                        "generated": industry_generated,
                        "rows": industry_rows,
                        "columns": industry_cols,
                        "size_mb": _bytes_to_mb(_safe_file_size_bytes(industry_output)) if industry_generated else None,
                        "error": industry_error,
                    },
                },
                industry_meta_output or f"{industry_output}.meta.json",
            )

        # 메타데이터 저장
        t_meta0 = perf_counter()
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "universe_fetch": {
                    "success": True,
                    "last_error": None,
                },
                "universe_source": "krx_stock_master_json",
                "universe_input": {
                    "krx_stock_master_json": args.krx_stock_master_json,
                },
                "tickers": tickers,
                "ticker_count": len(tickers),
                "fetched_ticker_count": len(results),
                "rows": int(len(full_df)),
                "columns": list(full_df.columns),
                "features": [f"SMA_{w}" for w in MA_WINDOWS] + ["MansfieldRS", "IsNewHigh1Y"],
                "indicators": {
                    "moving_averages": MA_WINDOWS,
                    "mansfield_rs": {
                        "benchmark_ticker": MANSFIELD_BENCHMARK_TICKER,
                        "sma_window": MANSFIELD_RS_SMA_WINDOW,
                        "benchmark_fetch": benchmark_fetch,
                    },
                    "new_high_1y": {
                        "window_trading_days": NEW_HIGH_WINDOW_TRADING_DAYS,
                    },
                },
                "industry_cache": {
                    "enabled": bool(industry_output),
                    "output": industry_output or None,
                    "meta_output": industry_meta_output or (f"{industry_output}.meta.json" if industry_output else None),
                },
                "data_file": {
                    "path": args.output,
                    "generated": True,
                    "size_mb": _bytes_to_mb(_safe_file_size_bytes(args.output)),
                },
                "args": {
                    "max_workers": args.max_workers,
                    "test_limit": args.test_limit,
                    "output": args.output,
                    "meta_output": meta_output,
                    "industry_output": industry_output,
                    "industry_meta_output": industry_meta_output,
                    "industry_benchmark": args.industry_benchmark,
                    "krx_stock_master_json": args.krx_stock_master_json,
                },
                "timing_seconds": {
                    "universe_load": round(t_universe1 - t_universe0, 4),
                    "benchmark_fetch": round(t_bench1 - t_bench0, 4),
                    "data_fetch_and_indicators": round(t_fetch1 - t_fetch0, 4),
                    "concat_and_save": round(t_save1 - t_save0, 4),
                    "meta_write": round(perf_counter() - t_meta0, 4),
                    "total": round(perf_counter() - t0, 4),
                },
                "env": {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "pandas": _safe_pkg_version("pandas"),
                    "pyarrow": _safe_pkg_version("pyarrow"),
                    "pykrx": _safe_pkg_version("pykrx"),
                },
            },
            meta_output,
        )
        elapsed = perf_counter() - t0
        
        print(f"Done. File saved to {args.output}. Total time: {elapsed:.2f}s")
        print(f"Total Rows: {len(full_df)}")
    else:
        print("No data fetched. Writing metadata only.")
        t_meta0 = perf_counter()
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "universe_fetch": {
                    "success": True,
                    "last_error": None,
                },
                "universe_source": "krx_stock_master_json",
                "universe_input": {
                    "krx_stock_master_json": args.krx_stock_master_json,
                },
                "tickers": tickers,
                "ticker_count": len(tickers),
                "fetched_ticker_count": 0,
                "rows": 0,
                "columns": list(_empty_feature_frame().columns),
                "features": [f"SMA_{w}" for w in MA_WINDOWS] + ["MansfieldRS", "IsNewHigh1Y"],
                "indicators": {
                    "moving_averages": MA_WINDOWS,
                    "mansfield_rs": {
                        "benchmark_ticker": MANSFIELD_BENCHMARK_TICKER,
                        "sma_window": MANSFIELD_RS_SMA_WINDOW,
                        "benchmark_fetch": benchmark_fetch,
                    },
                    "new_high_1y": {
                        "window_trading_days": NEW_HIGH_WINDOW_TRADING_DAYS,
                    },
                },
                "data_file": {
                    "path": args.output,
                    "generated": False,
                    "size_mb": None,
                },
                "notes": "No OHLCV data fetched; metadata-only release.",
                "args": {
                    "max_workers": args.max_workers,
                    "test_limit": args.test_limit,
                    "output": args.output,
                    "meta_output": meta_output,
                    "industry_output": industry_output,
                    "industry_meta_output": industry_meta_output,
                    "krx_stock_master_json": args.krx_stock_master_json,
                },
                "timing_seconds": {
                    "universe_load": round(t_universe1 - t_universe0, 4),
                    "benchmark_fetch": round(t_bench1 - t_bench0, 4),
                    "data_fetch_and_indicators": round(t_fetch1 - t_fetch0, 4),
                    "meta_write": round(perf_counter() - t_meta0, 4),
                    "total": round(perf_counter() - t0, 4),
                },
                "env": {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "pandas": _safe_pkg_version("pandas"),
                    "pyarrow": _safe_pkg_version("pyarrow"),
                    "pykrx": _safe_pkg_version("pykrx"),
                },
            },
            meta_output,
        )

if __name__ == "__main__":
    main()
