# Feature Cache Repository Architecture

## 1. Project Overview
이 레포지토리는 KOSPI 및 KOSDAQ 시장의 주가 데이터를 미리 수집하고 가공하여 **Feature Cache (Parquet)** 형태로 제공하는 역할을 합니다.  
백테스트 및 분석 시스템(`capybara_share` 등)은 이 레포지토리의 **GitHub Releases**에서 최신 캐시 파일을 다운로드하여 사용함으로써 데이터 수집 시간을 단축합니다.

## 2. Architecture

### Data Flow
1.  **Universe Construction (Full)**: `data/krx_stock_master.json`의 전 종목(KOSPI+KOSDAQ) 코드를 유니버스로 사용.
2.  **KRX Stock Master Build (Static)**: Seibro에서 수집한 원본 엑셀(코스피/코스닥)로부터
    종목 마스터를 추출하여 레포에 `data/krx_stock_master.json`으로 저장.
    - 원본 엑셀 출처: `https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/stock/BIP_CNTS02004V.xml&menuNo=41`
    - 릴리즈 시에는 JSON을 DataFrame(Parquet)으로 변환하여 `cache/krx_stock_master.parquet`로 함께 배포
3.  **Ticker/Market Lookup**: 종목명/시장/업종 정보는 `KRX Stock Master`를 기준으로 조회(별도 ticker map 파일은 생성하지 않음).
4.  **Data Fetching**: `pykrx`를 통해 각 종목의 OHLCV 데이터 병렬 수집.
5.  **Standardization**: 컬럼명 영문 변환 (`시가` -> `Open` 등) 및 날짜 인덱스 처리.
6.  **Indicator Calculation**:
    - 단순 OHLCV 외에 아래 지표를 추가로 계산하여 컬럼으로 저장
      - 이동평균: 5/10/20/60/120/200일 (`SMA_5`, `SMA_10`, ... `SMA_200`)
      - Mansfield Relative Strength (`MansfieldRS`)
        - 벤치마크: `069500` (수정주가, `adjusted=True`)
        - 윈도우: 200 거래일 SMA (`min_periods=200` → 초반 구간은 `NA`)
        - 계산:
          - \(RS_{raw}(t) = Close_{ticker}(t) / Close_{benchmark}(t)\)
          - \(RS_{sma}(t) = SMA_{200}(RS_{raw}(t))\)
          - \(MansfieldRS(t) = (RS_{raw}(t) / RS_{sma}(t) - 1) * 100\)
      - 1년 신고가 여부(종가 기준): `IsNewHigh1Y` (최근 252 거래일 롤링, `min_periods=252`)
7.  **Industry Strength (A안)**:
    - `KRX Stock Master`의 업종 분류(대/중/소)에 따라 종목을 그룹핑
    - 업종 지수(기준값 100)를 **동일가중(일간 수익률 평균 → 누적)**으로 생성
      - 종목별 일간 수익률: `Close.pct_change()`
      - 업종 일간 수익률: 해당 일자 업종 구성종목 수익률의 평균
      - 업종 지수: \((1+업종수익률)\) 누적곱 × 100
      - 업종 데이터는 업종별 full date grid로 확장(reindex)하여 날짜축/롤링 계산을 안정화(수익률 결측은 0으로 보정)
    - 업종 지수에 대해 종목과 동일한 방식으로 **Mansfield RS** 계산
      - 업종 벤치마크는 실행 인자로 선택: `--industry-benchmark {universe|069500}` (기본값: `universe`)
        - `universe`: 전 종목(유니버스) 동일가중 지수(기준 100)
        - `069500`: 종목 RS와 동일한 `069500` 벤치마크 사용
    - 산출물: `cache/korea_industry_feature_frame.parquet` (+ meta)
8.  **Serialization**: 수집된 전체 데이터를 단일 `Parquet` 파일로 저장 (zstd 압축).
9.  **Metadata Export**: 날짜 범위/티커 목록/파일 크기 등 실행 정보를 `meta.json`으로 저장.
10. **Distribution**: GitHub Actions를 통해 산출물들을 **GitHub Releases**에 자동 업로드.

### Data Consumption (How to read large releases)
전 종목 Feature Data parquet는 용량이 커서(전량 다운로드/로딩 시 메모리 사용 급증) 클라이언트가 쉽게 OOM(메모리 부족)으로 종료될 수 있습니다.  
따라서 데이터 소비는 아래 방식 중 하나를 권장합니다.

1) **On-demand query (권장)**: 원격 Parquet를 전체 다운로드하지 않고, 필요한 티커/기간/컬럼만 쿼리로 가져오기
- **Streamlit 앱(`streamlit_app.py`) 구현 방식**
  - DuckDB + `httpfs`를 사용해 GitHub Releases의 parquet asset URL을 대상으로 `read_parquet(url)` 실행
  - 티커/기간 필터를 SQL WHERE로 적용하여 필요한 row만 가져옴:
    - 예: `WHERE Ticker='005930' AND Date BETWEEN '2025-01-01' AND '2026-01-01'`
  - 날짜 슬라이더 범위도 `SELECT min(Date), max(Date)`로 구함(전체 로드 없음)
- 장점: **다운로드/메모리 사용량 최소화**, 대용량 릴리즈에서도 안정적

2) **Full download (주의)**: parquet를 로컬에 다운로드 후 Pandas로 전체 로드
- 파일이 크면 Streamlit/노트북 환경에서 OOM이 날 수 있으므로, 충분한 메모리가 있는 환경에서만 권장

## 3. Directory Structure

```
/workspace/
├── .github/
│   └── workflows/
│       └── update_feature_cache.yml  # 캐시 생성 및 릴리스 자동화 워크플로우
├── data/
│   ├── 코스피.xlsx                     # (원본) Seibro 수집 KOSPI 종목 정보
│   ├── 코스닥.xlsx                     # (원본) Seibro 수집 KOSDAQ 종목 정보
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
*   **Meta + Master Artifacts**: `meta.json` 및 `KRX Stock Master` Parquet를 함께 배포.
*   **Release Permission Fix**: 워크플로에 `permissions: contents: write` 설정으로 릴리스 생성 403 방지.

### ⚠️ Temporary Limitations
*   **Runtime/Scale**: 전 종목 수집은 시간이 오래 걸 수 있으며, 네트워크/소스 상태에 영향을 받음. (소요시간은 meta에 기록)

## 5. Remaining Tasks

## 5.1 Data Source Layer Refactoring Plan (데이터 수집부 교체 용이화)

### 배경 / 문제 정의
현재 데이터 수집은 `pykrx` 중심으로 구성되어 있으며, 일부 API(예: 코스피/코스닥 종목 리스트, 업종 분류 등)가 **일시적으로 동작하지 않는 경우**를 대비해 `data/krx_stock_master.json`(Seibro 엑셀 기반) 같은 **로컬 정적 데이터**에 의존하고 있습니다.  
향후 이 로컬 의존 지점을 **증권사 API(또는 대체 데이터 소스)** 로 교체할 계획인데, 현 구조는 다음 이유로 변경 비용이 큽니다.

- `scripts/generate_cache.py`가 **유니버스 구성 + 마스터 로드 + 가격 수집 + 지표 계산 + 저장/메타 기록**을 한 곳에서 처리 (결합도 높음)
- 데이터 소스 교체 시 **여러 로직(수집/표준화/에러 처리/메타)** 를 동시 수정해야 함
- 동일한 “가격 데이터”라도 소스마다 **스키마/타임존/보정주가/결측 처리**가 달라 쉽게 깨질 수 있음

### 목표
- **수집부를 Provider(플러그인)로 추상화**하여 소스 교체를 “구현 클래스 교체”로 제한
- 기존 파이프라인(Feature 계산/Parquet 저장/메타 기록/UI)은 **동작을 유지**하면서 내부 구조만 개선
- Provider 장애/제한을 대비해 **Fallback(우선순위) 전략**과 **관측 가능성(메타/로그)** 을 강화

### 핵심 설계 원칙
- **Contracts First**: Provider가 반드시 지켜야 할 입출력 계약(컬럼/타입/의미)을 문서/테스트로 고정
- **Orchestrator는 순수 파이프라인**: “무엇을 만들지”만 알고 “어디서 가져오는지”는 모름
- **표준화는 단일 지점**: 원천 데이터의 컬럼/타입/날짜 정규화는 공통 모듈에서 일관되게 수행
- **에러는 숨기지 않고 기록**: 실패/부분 성공/폴백 발생을 `meta.json`에 구조적으로 남김

### 제안 아키텍처(계층/모듈)
아래는 “데이터 소스 교체”를 위한 최소 단위 인터페이스 분리안입니다.

#### 1) Provider Contracts (인터페이스)
- **`UniverseProvider`**: 유니버스(티커 리스트) 제공
  - 입력: `asof_date`(선택), 시장 필터(선택)
  - 출력: `tickers: list[str]` (6자리 문자열), `market_by_ticker: dict[str,str]` (가능하면)
- **`StockMasterProvider`**: 종목 마스터(시장/업종/상장주식수 등) 제공
  - 출력: DataFrame with 최소 컬럼  
    `Code, Name, Market, IndustryLarge, IndustryMid, IndustrySmall, SharesOutstanding`
- **`OHLCVProvider`**: 종목별 OHLCV(수정주가 포함 여부 옵션) 제공
  - 입력: `ticker, start_date, end_date, adjusted`
  - 출력(표준화 전): source-specific DataFrame  
  - 출력(표준화 후): 표준 스키마 DataFrame
- (선택) **`CalendarProvider`**: 거래일 캘린더/휴장일 (필요 시)
- (선택) **`RateLimitPolicy` / `RetryPolicy`**: 소스별 제한/재시도 정책

#### 2) Adapter Implementations (구현체)
- **`PykrxOHLCVProvider`**: 현재 `pykrx.stock.get_market_ohlcv` 래핑
- **`LocalSeibroStockMasterProvider`**: 현재 `data/krx_stock_master.json` 로드 래핑
- **`MasterDerivedUniverseProvider`**: StockMaster를 기반으로 tickers를 구성(현재 방식 유지)
- (향후) **`BrokerApiStockMasterProvider` / `BrokerApiOHLCVProvider`**: 증권사 API 어댑터

#### 3) Composition / Fallback
소스 장애/누락에 대비해 Provider를 합성합니다.
- **`FallbackProvider`(권장)**: 우선순위 리스트를 두고, 실패 시 다음 provider로 폴백
  - 예: Universe는 `BrokerApi` → `LocalSeibro` 순, OHLCV는 `BrokerApi` → `pykrx` 순
- 폴백 발생/실패 이유는 메타에 기록:
  - `meta["providers"]["ohlcv"]["selected"]="broker_api"`, `meta["providers"]["ohlcv"]["fallbacks"]=[...]`

#### 4) Orchestrator (파이프라인)
- `generate_cache`는 Provider 계약만 의존:
  - 유니버스 로드 → 벤치마크 로드 → 종목별 OHLCV 로드 → 표준화 → 지표 계산 → 저장/메타
- “업종 강도(Industry Strength)”는 **StockMasterProvider** 의 업종 컬럼에만 의존  
  (즉, 업종 분류 소스가 바뀌어도 계산 로직은 그대로 유지)

### 표준 스키마(공통 데이터 모델)
소스가 바뀌어도 Feature 계산이 깨지지 않도록 “표준화된 OHLCV 스키마”를 고정합니다.
- **표준 OHLCV 컬럼**: `Date, Open, High, Low, Close, Volume, TradingValue, Change, Ticker`
- **표준 규칙**
  - `Date`: timezone-naive date or normalized timestamp(일 단위), 정렬 보장
  - `Ticker`: 6자리 문자열(zfill)
  - 수치 컬럼: `float64` 또는 nullable numeric(결측 허용), 계산 전 `pd.to_numeric` 수행
  - adjusted 정책(수정주가): provider가 지원하지 못하면 **명시적으로 메타에 기록**

### 설정/선택 방식(구현 시)
코드 변경 없이 소스 교체가 가능하도록 “실행 인자 기반 선택”을 권장합니다.
- 예시 플래그(안):
  - `--ohlcv-provider {pykrx,broker_api}`
  - `--master-provider {local_seibro,broker_api}`
  - `--universe-provider {from_master,broker_api}`
  - `--provider-config path/to/config.json` (API 키/엔드포인트/우선순위 등)

### 리팩토링 단계 계획(구현 전제, TODO 아님: 설계 확정용)
아래 단계는 “동작 유지”를 최우선으로, 위험을 최소화하는 순서입니다.

#### Phase 0: 문서/계약 확정 (현재 단계)
- Provider 계약(입출력/표준 스키마/메타 기록 구조) 문서화
- “폴백/장애 시나리오”를 정의(예: 유니버스 실패, 벤치마크 실패, 일부 티커 실패)

#### Phase 1: 코드 구조 분리(동작 동일)
- `scripts/generate_cache.py`에서 다음을 분리:
  - provider interface + 구현(현재 pykrx/로컬)
  - 표준화 모듈(컬럼/타입/날짜 정규화)
  - orchestrator(파이프라인)
- 외부 결과물(parquet/meta 스키마)은 **변경하지 않음**

#### Phase 2: Universe/Master 소스 교체 가능화
- `StockMasterProvider`를 “로컬 JSON” 외의 provider로 대체 가능한 형태로 확장
- 유니버스는 기본적으로 “master 기반”을 유지하되, `BrokerApiUniverseProvider`를 추가 가능하도록 설계

#### Phase 3: Fallback/관측 가능성 강화
- Provider별 health/latency/error를 메타에 기록
- 폴백 사용 시 “어떤 provider에서 어떤 이유로 실패했는지”가 남도록 구조화

#### Phase 4: 테스트 전략(계약 테스트)
- Provider contract test(공통): 표준 스키마 준수/날짜 정렬/결측 처리
- “외부 API 의존” 테스트는 VCR(HTTP 녹화) 또는 샘플 payload fixture로 재현 가능하게 구성

### 의사 코드(구현 방향; arch 확정용)

```text
interface UniverseProvider:
  list_tickers(asof_date=None) -> (tickers, market_by_ticker, diagnostics)

interface StockMasterProvider:
  load_master(asof_date=None) -> (master_df, diagnostics)

interface OHLCVProvider:
  fetch_ohlcv(ticker, start, end, adjusted=True) -> (raw_df, diagnostics)

function standardize_ohlcv(raw_df, source) -> std_df
  - rename columns to Open/High/Low/Close/Volume/TradingValue/Change
  - normalize Date, sort, enforce dtypes
  - return std_df

function build_feature_cache(orchestrator_config, providers):
  master_df = providers.master.load_master(...)
  (tickers, market_map) = providers.universe.list_tickers(...)
  bench = providers.ohlcv.fetch_ohlcv(benchmark_ticker, ...)
  for ticker in tickers in parallel:
    raw = providers.ohlcv.fetch_ohlcv(ticker, ...)
    std = standardize_ohlcv(raw)
    feat = compute_indicators(std, bench_close_by_date)
    collect feat
  save parquet + meta(provider diagnostics, fallbacks, partial failures)
  if industry enabled:
    industry = compute_industry_strength(all_features, master_df, industry_benchmark)
    save industry parquet + meta
```

### Short-term
1.  **Data Source Abstraction (1차)**: Provider 인터페이스/표준화 모듈/Orchestrator 분리(동작 동일 유지).
2.  **Full Universe in CI**: 전 종목 수집은 시간이 오래 걸릴 수 있으므로, CI에서는 `--test-limit` 등으로 수집 종목 수를 제한하고 전체 빌드는 별도 스케줄로 분리 검토.
3.  **Feature Logic Integration**: 단순 OHLCV 외에 기술적 지표(MA, RSI, Bollinger Bands 등) 계산 로직 추가.
4.  **Scheduler**: 주간/일간 자동 실행을 위한 `schedule` (cron) 트리거 활성화.

## 6. Release Artifacts

릴리즈에는 아래 파일들이 함께 올라갑니다.

- **Feature Data**: `cache/korea_universe_feature_frame.parquet`
  - 주요 컬럼: `Date`, `Open`, `High`, `Low`, `Close`, `Volume`, `TradingValue`, `Change`, `Ticker`
  - 지표 컬럼: `SMA_5`, `SMA_10`, `SMA_20`, `SMA_60`, `SMA_120`, `SMA_200`, `MansfieldRS`, `IsNewHigh1Y`
- **Metadata**: `cache/korea_universe_feature_frame.meta.json`
  - 날짜 범위, 티커 목록/개수, 실제 수집 성공 종목 수, row/column, 지표 설정(예: `MansfieldRS` 벤치마크/윈도우 및 fetch 성공 여부), 실행 인자/소요시간/환경 버전 등
- **Industry Strength Data (A안, 동일가중)**: `cache/korea_industry_feature_frame.parquet`
  - 기준: `KRX Stock Master`의 `IndustryLarge/IndustryMid/IndustrySmall`
  - 주요 컬럼: `Date`, `Level(L/LM/LMS)`, `IndustryLarge`, `IndustryMid`, `IndustrySmall`, `IndustryClose`, `IndustryReturn`, `ConstituentCount`, `MansfieldRS`
- **Industry Strength Metadata**: `cache/korea_industry_feature_frame.meta.json`
  - 업종 지수 생성 방식, RS 벤치마크(`--industry-benchmark`) 및 윈도우, 마스터 로드 상태, 벤치마크 fetch 결과, 산출물 row/column/크기 및 에러(실패 시에도 디버깅용으로 기록) 등
- **KRX Stock Master (DataFrame)**: `cache/krx_stock_master.parquet`
  - 원본: `data/코스피.xlsx`, `data/코스닥.xlsx` (Seibro 수집)
  - 컬럼: `Code`, `Name`, `Market`, `IndustryLarge`, `IndustryMid`, `IndustrySmall`

### Long-term
1.  **Incremental Update**: 전체 재수집 대신 최신 데이터만 추가하는 증분 업데이트 구현.
2.  **Partitioning**: 데이터 크기 증가 시 연도별 또는 종목별 파일 분할 저장 고려.
