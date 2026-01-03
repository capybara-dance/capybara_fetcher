import os
import argparse
import datetime
import pandas as pd
from pykrx import stock
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time

def build_korea_full_universe(target_date=None):
    """
    KOSPI 및 KOSDAQ 전 종목 티커 리스트를 반환합니다.
    데이터가 없는 경우(휴일 등), 과거 날짜로 이동하며 데이터를 찾습니다.
    pykrx 조회 실패 시, 테스트를 위한 주요 종목 리스트를 반환합니다.
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
                
                # TEMPORARY: Limit to 5 KOSPI + 5 KOSDAQ for quick verification
                print("!! QUICK VERIFICATION MODE: Limiting to 5 KOSPI + 5 KOSDAQ tickers !!")
                kospi = kospi[:5]
                kosdaq = kosdaq[:5]
                
                return sorted(list(set(kospi + kosdaq)))
        except Exception as e:
            pass
        
        # 하루 전으로 이동
        target_date -= datetime.timedelta(days=1)
    
    print("Warning: Could not find tickers via pykrx. Using fallback list for testing.")
    # Fallback list (5 KOSPI + 5 KOSDAQ)
    # KOSPI: Samsung, SK Hynix, Naver, Kakao, Hyundai Motor
    # KOSDAQ: Ecopro BM, Ecopro, HLB, Pearl Abyss, Celltrion Pharm
    return [
        "005930", "000660", "035420", "035720", "005380", # KOSPI
        "247540", "086520", "028300", "263750", "068760"  # KOSDAQ
    ]

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
        
        # 티커 컬럼 추가
        df['Code'] = ticker
        
        # TODO: Feature Calculation Logic here
        # e.g., df['sma_20'] = df['Close'].rolling(20).mean()
        
        return df
    except Exception as e:
        # 로그가 너무 많아질 수 있으므로 에러 발생 시 None 반환
        return None

def main():
    parser = argparse.ArgumentParser(description="Generate Korea Universe Feature Cache")
    parser.add_argument("--start-date", type=str, default=(datetime.datetime.now() - datetime.timedelta(days=365*3)).strftime("%Y%m%d"), help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", type=str, default=datetime.datetime.now().strftime("%Y%m%d"), help="End date (YYYYMMDD)")
    parser.add_argument("--output", type=str, default="korea_universe_feature_frame.parquet", help="Output parquet file path")
    parser.add_argument("--max-workers", type=int, default=8, help="Number of threads")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit number of tickers for testing (0 for all)")
    
    args = parser.parse_args()
    
    print(f"Start generating cache from {args.start_date} to {args.end_date}...")
    
    # 1. 유니버스 구성
    tickers = build_korea_full_universe()
    if not tickers:
        print("No tickers found.")
        return

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
        
        # Date, Code 기준으로 정렬
        if 'Date' in full_df.columns and 'Code' in full_df.columns:
             full_df = full_df.sort_values(by=['Date', 'Code'])
        
        print(f"Saving to {args.output}...")
        # 디렉토리가 없으면 생성
        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            
        full_df.to_parquet(args.output, compression='zstd')
        
        elapsed = time.time() - start_time
        print(f"Done. File saved to {args.output}. Total time: {elapsed:.2f}s")
        print(f"Total Rows: {len(full_df)}")
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
