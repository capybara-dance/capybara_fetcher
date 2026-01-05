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
from importlib.metadata import version as pkg_version, PackageNotFoundError

TEMP_KOSPI_TICKERS = [
    # 임시 유니버스 (KOSPI 5)
    "005930",  # 삼성전자
    "000660",  # SK하이닉스
    "005380",  # 현대차
    "051910",  # LG화학
    "035420",  # NAVER
]

TEMP_KOSDAQ_TICKERS = [
    # 임시 유니버스 (KOSDAQ 5)
    "247540",  # 에코프로비엠
    "263750",  # 펄어비스
    "293490",  # 카카오게임즈
    "091990",  # 셀트리온헬스케어(과거 KOSDAQ, 현재는 변동 가능)
    "278280",  # 천보
]

def build_korea_full_universe():
    """
    임시 유니버스: KOSPI 5개 + KOSDAQ 5개 티커를 반환합니다.
    (stock.get_market_ticker_list 이슈는 추후 해결)
    """
    tickers = sorted(list(set(TEMP_KOSPI_TICKERS + TEMP_KOSDAQ_TICKERS)))
    market_by_ticker: dict[str, str] = {}
    for t in TEMP_KOSPI_TICKERS:
        market_by_ticker[t] = "KOSPI"
    for t in TEMP_KOSDAQ_TICKERS:
        market_by_ticker[t] = "KOSDAQ"
    return tickers, market_by_ticker, None, None

def fetch_data(ticker, start_date, end_date):
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
        
        # 티커 컬럼 추가 (종목명은 별도 맵 파일로 저장)
        df["Ticker"] = ticker
        
        # TODO: Feature Calculation Logic here
        # e.g., df['sma_20'] = df['Close'].rolling(20).mean()
        
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
        ]
    )

def _build_ticker_info_map(tickers: list[str], market_by_ticker: dict[str, str]) -> pd.DataFrame:
    """
    티커 정보를 별도 DF로 구성합니다. (Ticker/Name/Market)
    (메인 데이터에 종목명을 중복 저장하지 않기 위함)
    """
    rows: list[dict[str, str]] = []
    for t in tickers:
        try:
            name = stock.get_market_ticker_name(t) or ""
        except Exception:
            name = ""
        market = market_by_ticker.get(t) or "UNKNOWN"
        rows.append({"Ticker": t, "Name": name, "Market": market})
    return pd.DataFrame(rows).drop_duplicates(subset=["Ticker"]).sort_values("Ticker")

def _empty_ticker_info_map() -> pd.DataFrame:
    return pd.DataFrame(columns=["Ticker", "Name", "Market"])

def _default_ticker_info_map_path(output_parquet_path: str) -> str:
    # e.g. cache/foo.parquet -> cache/foo_ticker_info_map.parquet
    base, ext = os.path.splitext(output_parquet_path)
    if not ext:
        return f"{output_parquet_path}_ticker_info_map.parquet"
    return f"{base}_ticker_info_map{ext}"

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
    parser.add_argument(
        "--ticker-info-map-output",
        dest="ticker_info_map_output",
        type=str,
        default="",
        help="Output parquet file path for ticker info map (default: <output>_ticker_info_map.parquet)",
    )
    # Backward-compatible alias
    parser.add_argument(
        "--ticker-name-map-output",
        dest="ticker_info_map_output",
        type=str,
        default="",
        help="DEPRECATED: use --ticker-info-map-output instead",
    )
    parser.add_argument("--max-workers", type=int, default=8, help="Number of threads")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit number of tickers for testing (0 for all)")
    
    args = parser.parse_args()
    
    print(f"Start generating cache from {args.start_date} to {args.end_date}...")
    started_at = datetime.datetime.now(datetime.timezone.utc)
    meta_output = args.meta_output or f"{args.output}.meta.json"
    ticker_info_map_output = args.ticker_info_map_output or _default_ticker_info_map_path(args.output)
    
    # 1. 유니버스 구성
    tickers, market_by_ticker, universe_date, universe_error = build_korea_full_universe()
    if not tickers:
        print("No tickers found. Writing metadata only.")
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "universe_date": universe_date,
                "universe_fetch": {
                    "success": False,
                    "last_error": universe_error,
                },
                "universe_source": "temporary_static_list",
                "tickers": [],
                "ticker_count": 0,
                "rows": 0,
                "columns": list(_empty_feature_frame().columns),
                "features": [],
                "data_file": {
                    "path": args.output,
                    "generated": False,
                    "size_mb": None,
                },
                "ticker_info_map": {
                    "path": ticker_info_map_output,
                    "rows": 0,
                    "generated": False,
                    "size_mb": None,
                },
                "notes": "Universe fetch failed; metadata-only release.",
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

    # GitHub Actions 환경에서는 실수로 전체 유니버스(약 2,600 종목)를 돌려
    # 너무 오래 걸리는 것을 방지하기 위해 기본 테스트 제한을 둡니다.
    if os.getenv("GITHUB_ACTIONS", "").lower() == "true" and args.test_limit == 0:
        args.test_limit = int(os.getenv("CI_TEST_LIMIT", "10"))
        print(f"Detected GitHub Actions. Applying CI_TEST_LIMIT={args.test_limit}.")

    if args.test_limit > 0:
        print(f"Testing with first {args.test_limit} tickers only.")
        tickers = tickers[:args.test_limit]

    # 티커 정보 맵 저장 (메인 데이터와 분리)
    ticker_info_map_df = _build_ticker_info_map(tickers, market_by_ticker)
    _write_parquet(ticker_info_map_df, ticker_info_map_output)
    
    # 2. 병렬 데이터 수집
    results = []
    print(f"Fetching data for {len(tickers)} tickers with {args.max_workers} workers...")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(fetch_data, ticker, args.start_date, args.end_date): ticker for ticker in tickers}
        
        for future in tqdm(futures, total=len(tickers)):
            res = future.result()
            if res is not None:
                results.append(res)
    
    print(f"Fetched {len(results)}/{len(tickers)} tickers successfully.")
    
    # 3. 병합 및 저장
    if results:
        print("Concatenating data...")
        full_df = pd.concat(results, ignore_index=True)
        
        # Date, Ticker 기준으로 정렬
        if "Date" in full_df.columns and "Ticker" in full_df.columns:
            full_df = full_df.sort_values(by=["Date", "Ticker"])
        
        print(f"Saving to {args.output}...")
        _write_parquet(full_df, args.output)

        # 메타데이터 저장
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "universe_date": universe_date,
                "universe_fetch": {
                    "success": True,
                    "last_error": None,
                },
                "universe_source": "temporary_static_list",
                "tickers": tickers,
                "ticker_count": len(tickers),
                "fetched_ticker_count": len(results),
                "rows": int(len(full_df)),
                "columns": list(full_df.columns),
                # 현재 스크립트는 별도 지표 계산을 하지 않으므로, "features"는 컬럼으로부터 추정하거나 빈 리스트로 둡니다.
                "features": [],
                "data_file": {
                    "path": args.output,
                    "generated": True,
                    "size_mb": _bytes_to_mb(_safe_file_size_bytes(args.output)),
                },
                "ticker_info_map": {
                    "path": ticker_info_map_output,
                    "rows": int(len(ticker_info_map_df)),
                    "generated": True,
                    "size_mb": _bytes_to_mb(_safe_file_size_bytes(ticker_info_map_output)),
                },
                "args": {
                    "max_workers": args.max_workers,
                    "test_limit": args.test_limit,
                    "output": args.output,
                    "meta_output": meta_output,
                    "ticker_info_map_output": ticker_info_map_output,
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
        
        elapsed = time.time() - start_time
        print(f"Done. File saved to {args.output}. Total time: {elapsed:.2f}s")
        print(f"Total Rows: {len(full_df)}")
    else:
        print("No data fetched. Writing metadata only.")
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "universe_date": universe_date,
                "universe_fetch": {
                    "success": True,
                    "last_error": None,
                },
                "universe_source": "temporary_static_list",
                "tickers": tickers,
                "ticker_count": len(tickers),
                "fetched_ticker_count": 0,
                "rows": 0,
                "columns": list(_empty_feature_frame().columns),
                "features": [],
                "data_file": {
                    "path": args.output,
                    "generated": False,
                    "size_mb": None,
                },
                "ticker_info_map": {
                    "path": ticker_info_map_output,
                    "rows": int(len(ticker_info_map_df)),
                    "generated": True,
                    "size_mb": _bytes_to_mb(_safe_file_size_bytes(ticker_info_map_output)),
                },
                "notes": "No OHLCV data fetched; metadata-only release.",
                "args": {
                    "max_workers": args.max_workers,
                    "test_limit": args.test_limit,
                    "output": args.output,
                    "meta_output": meta_output,
                    "ticker_info_map_output": ticker_info_map_output,
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
