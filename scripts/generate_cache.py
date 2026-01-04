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

FALLBACK_TICKERS = [
    # KOSPI / KOSDAQ 대표 종목 일부 (pykrx 조회 실패 시 최소 캐시 생성용)
    "005930",  # 삼성전자
    "000660",  # SK하이닉스
    "035420",  # NAVER
    "051910",  # LG화학
    "068270",  # 셀트리온
    "035720",  # 카카오
    "207940",  # 삼성바이오로직스
    "005380",  # 현대차
    "105560",  # KB금융
    "012330",  # 현대모비스
]

def build_korea_full_universe(target_date=None):
    """
    KOSPI 및 KOSDAQ 전 종목 티커 리스트를 반환합니다.
    데이터가 없는 경우(휴일 등), 과거 날짜로 이동하며 데이터를 찾습니다.
    pykrx 조회 실패 시, 최소 동작을 위한 fallback 티커 리스트를 반환합니다.
    """
    if target_date is None:
        target_date = datetime.datetime.now()
    else:
        try:
            target_date = datetime.datetime.strptime(target_date, "%Y%m%d")
        except:
            target_date = datetime.datetime.now()

    # 최대 10일 전까지 조회 시도
    for _ in range(10):
        str_date = target_date.strftime("%Y%m%d")
        try:
            kospi = stock.get_market_ticker_list(str_date, market="KOSPI")
            kosdaq = stock.get_market_ticker_list(str_date, market="KOSDAQ")
            
            if len(kospi) > 0 or len(kosdaq) > 0:
                print(f"Target Date: {str_date}")
                print(f"KOSPI: {len(kospi)} items, KOSDAQ: {len(kosdaq)} items")
                return sorted(list(set(kospi + kosdaq)))
        except Exception as e:
            pass
        
        # 하루 전으로 이동
        target_date -= datetime.timedelta(days=1)
    
    print("Error: Could not find tickers via pykrx after multiple attempts. Using fallback tickers.")
    return FALLBACK_TICKERS.copy()

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
        
        # 티커/종목명 컬럼 추가
        df["Ticker"] = ticker
        try:
            df["Name"] = stock.get_market_ticker_name(ticker) or ""
        except Exception:
            df["Name"] = ""
        
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
            "Name",
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
    parser.add_argument("--max-workers", type=int, default=8, help="Number of threads")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit number of tickers for testing (0 for all)")
    
    args = parser.parse_args()
    
    print(f"Start generating cache from {args.start_date} to {args.end_date}...")
    started_at = datetime.datetime.now(datetime.timezone.utc)
    meta_output = args.meta_output or f"{args.output}.meta.json"
    
    # 1. 유니버스 구성
    tickers = build_korea_full_universe()
    if not tickers:
        print("No tickers found. Writing empty cache file.")
        _write_parquet(_empty_feature_frame(), args.output)
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "tickers": [],
                "ticker_count": 0,
                "rows": 0,
                "columns": list(_empty_feature_frame().columns),
                "features": [],
                "data_file": {
                    "path": args.output,
                    "size_mb": _bytes_to_mb(_safe_file_size_bytes(args.output)),
                },
                "notes": "No tickers found via data source; wrote empty cache.",
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
                "tickers": tickers,
                "ticker_count": len(tickers),
                "fetched_ticker_count": len(results),
                "rows": int(len(full_df)),
                "columns": list(full_df.columns),
                # 현재 스크립트는 별도 지표 계산을 하지 않으므로, "features"는 컬럼으로부터 추정하거나 빈 리스트로 둡니다.
                "features": [],
                "data_file": {
                    "path": args.output,
                    "size_mb": _bytes_to_mb(_safe_file_size_bytes(args.output)),
                },
                "args": {
                    "max_workers": args.max_workers,
                    "test_limit": args.test_limit,
                    "output": args.output,
                    "meta_output": meta_output,
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
        print("No data fetched. Writing empty cache file.")
        _write_parquet(_empty_feature_frame(), args.output)
        _write_json(
            {
                "generated_at_utc": started_at.isoformat(),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "tickers": tickers,
                "ticker_count": len(tickers),
                "fetched_ticker_count": 0,
                "rows": 0,
                "columns": list(_empty_feature_frame().columns),
                "features": [],
                "data_file": {
                    "path": args.output,
                    "size_mb": _bytes_to_mb(_safe_file_size_bytes(args.output)),
                },
                "notes": "No OHLCV data fetched; wrote empty cache.",
                "args": {
                    "max_workers": args.max_workers,
                    "test_limit": args.test_limit,
                    "output": args.output,
                    "meta_output": meta_output,
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
