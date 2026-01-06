# Feature Cache Repository Architecture

## 1. Project Overview
이 레포지토리는 KOSPI 및 KOSDAQ 시장의 주가 데이터를 미리 수집하고 가공하여 **Feature Cache (Parquet)** 형태로 제공하는 역할을 합니다.  
백테스트 및 분석 시스템(`capybara_share` 등)은 이 레포지토리의 **GitHub Releases**에서 최신 캐시 파일을 다운로드하여 사용함으로써 데이터 수집 시간을 단축합니다.

## 2. Architecture

### Data Flow
1.  **Universe Construction (Temporary)**: 현재는 `stock.get_market_ticker_list` 이슈로 인해
    **임시 유니버스(KOSPI 5 + KOSDAQ 5)** 고정 티커로만 진행.
    - 전 종목 유니버스는 추후 `get_market_ticker_list` 안정화 후 재도입 예정
2.  **KRX Stock Master Build (Static)**: Seibro에서 수집한 원본 엑셀(코스피/코스닥)로부터
    종목 마스터를 추출하여 레포에 `data/krx_stock_master.json`으로 저장.
    - 원본 엑셀 출처: `https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/stock/BIP_CNTS02004V.xml&menuNo=41`
    - 릴리즈 시에는 JSON을 DataFrame(Parquet)으로 변환하여 `cache/krx_stock_master.parquet`로 함께 배포
3.  **Ticker Info Map Build**: 티커별 메타(종목명, 시장구분)를 **별도 Parquet**(`Ticker Info Map`)으로 저장.
4.  **Data Fetching**: `pykrx`를 통해 각 종목의 OHLCV 데이터 병렬 수집.
5.  **Standardization**: 컬럼명 영문 변환 (`시가` -> `Open` 등) 및 날짜 인덱스 처리.
6.  **Indicator Calculation**:
    - 단순 OHLCV 외에 아래 지표를 추가로 계산하여 컬럼으로 저장
      - 이동평균: 5/10/20/60/120/200일 (`SMA_5`, `SMA_10`, ... `SMA_200`)
      - Mansfield Relative Strength: 벤치마크 `069500` 대비 (`MansfieldRS`)
      - 1년 신고가 여부(종가 기준): `IsNewHigh1Y` (최근 252 거래일 롤링)
7.  **Serialization**: 수집된 전체 데이터를 단일 `Parquet` 파일로 저장 (zstd 압축).
8.  **Metadata Export**: 날짜 범위/티커 목록/파일 크기 등 실행 정보를 `meta.json`으로 저장.
9.  **Distribution**: GitHub Actions를 통해 산출물들을 **GitHub Releases**에 자동 업로드.

## 3. Directory Structure

```
/workspace/
├── 코스피.xlsx                         # (원본) Seibro 수집 KOSPI 종목 정보
├── 코스닥.xlsx                         # (원본) Seibro 수집 KOSDAQ 종목 정보
├── .github/
│   └── workflows/
│       └── update_feature_cache.yml  # 캐시 생성 및 릴리스 자동화 워크플로우
├── data/
│   └── krx_stock_master.json          # (생성) 종목 마스터 JSON (Code/Name/Market/Industry*)
├── scripts/
│   └── generate_cache.py             # 데이터 수집 및 Parquet 생성 스크립트
│   └── build_krx_stock_master.py      # 코스피/코스닥 엑셀 -> 종목 마스터 JSON 생성
│   └── export_krx_stock_master_parquet.py # 종목 마스터 JSON -> Parquet 변환(릴리즈용)
├── requirements.txt                  # 의존성 패키지 목록 (pandas, pykrx, pyarrow, streamlit 등)
├── streamlit_app.py                  # 릴리스 데이터 검증용 Streamlit 웹앱
└── arch.md                           # 아키텍처 및 구현 현황 문서
```

## 4. Current Implementation Status

### ✅ Implemented
*   **Parallel Data Fetching**: `ThreadPoolExecutor`를 사용하여 고속으로 데이터 수집.
*   **Parquet Storage**: `pyarrow` 엔진을 사용한 고효율 데이터 저장.
*   **Automated Workflow**: `workflow_dispatch` 트리거를 통한 수동 실행 및 GitHub Releases 자동 업로드.
*   **Verification Tool**: `streamlit_app.py`를 통해 생성된 캐시 파일의 내용을 웹에서 즉시 확인 가능.
*   **Meta + Map Artifacts**: `meta.json` 및 `Ticker Info Map`(Ticker/Name/Market) Parquet를 함께 배포.
*   **CI Safety Limit**: GitHub Actions에서는 기본적으로 `CI_TEST_LIMIT`(기본 10)만 수집하도록 제한 가능.
*   **Release Permission Fix**: 워크플로에 `permissions: contents: write` 설정으로 릴리스 생성 403 방지.

### ⚠️ Temporary Limitations
*   **Basic Data Only**: 현재는 OHLCV(가격/거래량) 데이터만 포함하며, 이동평균선 등 파생 변수(Feature)는 계산하지 않음.
*   **Temporary Universe**: 현재는 임시 10개 티커만 수집/생성함. (KOSPI 5 + KOSDAQ 5)

## 5. Remaining Tasks

### Short-term
1.  **Full Universe in CI**: 전 종목 수집은 시간이 오래 걸릴 수 있으므로, CI에서는 `CI_TEST_LIMIT` 정책/스케줄링을 정리하고 전체 빌드는 별도 스케줄로 분리 검토.
2.  **Feature Logic Integration**: 단순 OHLCV 외에 기술적 지표(MA, RSI, Bollinger Bands 등) 계산 로직 추가.
3.  **Scheduler**: 주간/일간 자동 실행을 위한 `schedule` (cron) 트리거 활성화.

## 6. Release Artifacts

릴리즈에는 아래 파일들이 함께 올라갑니다.

- **Feature Data**: `cache/korea_universe_feature_frame.parquet`
  - 주요 컬럼: `Date`, `Open`, `High`, `Low`, `Close`, `Volume`, `TradingValue`, `Change`, `Ticker`
  - 지표 컬럼: `SMA_5`, `SMA_10`, `SMA_20`, `SMA_60`, `SMA_120`, `SMA_200`, `MansfieldRS`, `IsNewHigh1Y`
- **Metadata**: `cache/korea_universe_feature_frame.meta.json`
  - 날짜 범위, 티커 목록/개수, 파일 크기(MB), 실행 환경 버전, 유니버스 기준일(`universe_date`) 등
- **Ticker Info Map**: `cache/korea_universe_ticker_info_map.parquet`
  - 컬럼: `Ticker`, `Name`, `Market`(KOSPI/KOSDAQ/UNKNOWN)
- **KRX Stock Master (DataFrame)**: `cache/krx_stock_master.parquet`
  - 원본: 레포 내 `코스피.xlsx`, `코스닥.xlsx` (Seibro 수집)
  - 컬럼: `Code`, `Name`, `Market`, `IndustryLarge`, `IndustryMid`, `IndustrySmall`

### Long-term
1.  **Incremental Update**: 전체 재수집 대신 최신 데이터만 추가하는 증분 업데이트 구현.
2.  **Partitioning**: 데이터 크기 증가 시 연도별 또는 종목별 파일 분할 저장 고려.
