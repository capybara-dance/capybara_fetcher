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
    parser.add_argument("--krx-stock-master-json", type=str, default=KRX_STOCK_MASTER_JSON_DEFAULT, help="Path to krx_stock_master.json")
    parser.add_argument("--max-workers", type=int, default=8, help="Number of threads")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit number of tickers for testing (0 for all)")
    
    args = parser.parse_args()
    
    print(f"Start generating cache from {args.start_date} to {args.end_date}...")
    t0 = perf_counter()
    started_at = datetime.datetime.now(datetime.timezone.utc)
    meta_output = args.meta_output or f"{args.output}.meta.json"
    
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
