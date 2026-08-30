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
      - 1년 신고가 여부(고가 기준): `IsNewHigh1Y` (최근 252 거래일 롤링, `min_periods=252`)
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
*   **Refactor (DataProvider + Fail-fast)**:
    - `capybara_fetcher/` 패키지로 Provider/표준화/지표/업종/오케스트레이터 모듈 분리
    - 단일 `DataProvider` 계약 + `PykrxProvider` 구현(티커/마스터=로컬 JSON, 가격=pykrx)
    - Fail-fast: 티커 처리 중 오류 발생 시 즉시 중단(폴백 없음) + 실패 메타 기록
*   **Unit Tests**:
    - `pytest` 기반 core 유닛테스트 추가(표준화/지표/오케스트레이터 FakeProvider)
    - 외부 소스 스모크 테스트는 `external` 마커 + `RUN_EXTERNAL_SMOKE=1` 조건으로 분리

### ⚠️ Temporary Limitations
*   **Runtime/Scale**: 전 종목 수집은 시간이 오래 걸 수 있으며, 네트워크/소스 상태에 영향을 받음. (소요시간은 meta에 기록)
*   **Upstream Data Quirks**: pykrx/원천 데이터에서 날짜 중복 등이 발생할 수 있어 표준화 단계에서 date dedupe(keep last)를 적용

### ⚠️ Known Data Provider Issues

#### pykrx API Stability
*   **Ticker List APIs**: pykrx의 코스피/코스닥 종목 리스트 조회 API가 일시적으로 동작하지 않는 경우가 있음
*   **ETF List API**: ETF 종목 리스트 조회 API도 불안정할 수 있음
*   **Workaround**: 로컬 JSON 파일(`data/krx_stock_master.json`, Seibro 엑셀 기반)을 사용하여 종목 유니버스를 구성

#### FinanceDataReader Multi-threading
*   **Thread-Safety Issue**: FinanceDataReader는 멀티스레드 환경에서 OHLCV 데이터를 조회할 때 **스레드 안전하지 않음**
*   **Symptoms**: `max_workers > 1` 설정 시 2년 제한 에러, 403 Forbidden, 기타 API 오류가 발생할 수 있음
*   **Solution**: **FdrProvider 사용 시 반드시 `max_workers=1`로 설정**하여 순차 처리해야 함
*   **Implementation**: Orchestrator는 `max_workers=1`일 때 ThreadPoolExecutor 대신 단순 반복문을 사용하여 tqdm 진행 상황 표시를 최적화함

### ⚠️ 수집기가 조용히 성공하는 자리

**이 저장소가 실제로 밟은 실패는 대부분 예외로 오지 않았다.** 응답이 조금 모자란 채로
돌아오고, 검사들이 각자 자기 몫만 보다 전부 통과시키고, **부분적으로 틀린 캐시가 정상
릴리즈처럼 배포된다.** 소비자 쪽에서는 아무 경고도 안 뜬다.

수집기를 손대기 전에 **이 표를 먼저 대 볼 것.**

| # | 실패 | 왜 통과하나 | 실제 사례 |
|---|---|---|---|
| 1 | **차단이 빈 결과로 온다** | KRX 차단은 예외가 아니라 빈 DataFrame 이다. 수집기가 그걸 "상장 전"으로 세면 실패율 가드가 `failed=0` 으로 무력화된다 | [feeder#13](https://github.com/capybara-dance/feeder/pull/13) — 9,196건 중 **9,099건(99%)이 빈 결과**인 채 3.77시간, workflow 는 `success` |
| 2 | **조회 실패를 빈 프레임으로 물러선다** | `warnings.warn` + `return pd.DataFrame()` 이면 `main()` 이 그대로 진행해 **exit 0** 이다. 빠진 것이 출력 어디에도 안 나온다 | PR #40 — 폐지 목록이 그랬다. 생존편향을 가진 마스터가 "폐지 포함"처럼 릴리즈될 뻔했다 |
| 3 | **계약을 어긴 직렬화** | pandas `NaN` 을 `json.dumps` 가 `NaN` 토큰으로 쓴다. **유효한 JSON 이 아니고** 엄격한 파서가 거부한다 | PR #40 — 커밋된 마스터에 **7,556개** |
| 4 | **마스터에 넣었는데 아무도 안 본다** | `run_cache_build()` 는 `load_stock_master()` 가 아니라 **`list_tickers()`** 를 순회한다. 거래소에 묻는 provider 는 생존자만 답한다 | PR #40 — 폐지 492건 중 목록에 든 것 **1건**. 마스터만 커지고 릴리즈는 그대로 |
| 5 | **입력 스냅샷이 낡는다** | Seibro 엑셀은 사람이 받아 커밋한다. 시간이 지나면 이미 폐지된 종목이 상장 중으로 남는다 | PR #40 — **61건**이 2026년에 이미 폐지됐는데 엑셀엔 상장 중 |

#### 규칙

1. **빈 응답을 기본적으로 오류로 다룬다.** 진짜로 비어도 되는 자리에만 예외를 두고,
   **왜 비어도 되는지 주석으로** 남긴다
2. **기본 동작이 데이터를 빠뜨리면 실패시킨다.** 생략은 `--no-...` 처럼 **사람이 명시**
   했을 때만 허용한다
3. **직렬화 계약을 코드로 못 박는다.** `allow_nan=False` 처럼 어기면 터지게 한다
4. **산출물 자체를 검사하는 테스트를 둔다.** 함수만 시험하면 **이미 잘못 쓰인 파일**은
   계속 남는다 — 실제로 그랬다
5. **배선을 검증한다.** "데이터를 받을 수 있다"와 "**요청된다**"는 다르다.
   PR #40 은 `fetch_ohlcv` 4/4 를 확인하고도 `list_tickers()` 를 안 봐서 no-op 이었다
6. **막아 준다고 주장하는 것은 주입해서 확인한다.** 일부러 되돌려 보고 테스트가
   **실제로 실패하는지** 본다. 실패하지 않으면 그 문장은 거짓이다

#### 알려진 부채

| | 패턴 | 이슈 |
|---|---|---|
| `_fetch_etf_data()` 가 실패해도 경고만 내고 빈 프레임을 돌려준다 — ETF 1,163건이 조용히 빠진 마스터가 만들어질 수 있다 | **2번** | [#42](https://github.com/capybara-dance/capybara_fetcher/issues/42) |
| `KoreaInvestmentProvider.fetch_ohlcv()` 가 이력을 100행에서 자른다 — `FHKST03010100` 은 호출당 100건인데 페이지네이션이 없다 | **3번** | [#41](https://github.com/capybara-dance/capybara_fetcher/issues/41) |

둘 다 예외를 내지 않는다. 폐지 종목 쪽 2번 패턴은 `DelistedFetchError` 로 고쳤다.

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
- 오류 발생 시 **즉시 실패(fail-fast)** 하도록 하고, 원인 파악을 위한 **관측 가능성(메타/로그)** 을 강화

### 핵심 설계 원칙
- **Contracts First**: Provider가 반드시 지켜야 할 입출력 계약(컬럼/타입/의미)을 문서/테스트로 고정
- **Orchestrator는 순수 파이프라인**: “무엇을 만들지”만 알고 “어디서 가져오는지”는 모름
- **표준화는 단일 지점**: 원천 데이터의 컬럼/타입/날짜 정규화는 공통 모듈에서 일관되게 수행
- **에러는 숨기지 않고 기록**: 실패/부분 성공/폴백 발생을 `meta.json`에 구조적으로 남김
- **Fail-fast by default**: 실행 중 오류 발생 시 폴백 없이 예외를 발생시키고 종료(비정상 종료 코드를 반환)
- **Readability First**: 파일/클래스/함수 역할을 단순하게 유지하고, “한 함수는 한 가지 일” 원칙을 지킴
- **Avoid over-catching**: 과도한 `try/except`로 에러를 숨기지 않고, “명확한 입력 검증 + 자연스러운 예외 전파”로 조기 발견

### 제안 아키텍처(계층/모듈)
아래는 “데이터 소스 교체”를 위한 최소 단위 인터페이스 분리안입니다.

#### 1) Provider Contract (단일 인터페이스)
Provider를 세분화하지 않고, “데이터 소스(예: pykrx, 증권사 API)” 단위로 **하나의 Provider가 필요한 데이터를 모두 제공**합니다.

- **`DataProvider`**: 유니버스/종목마스터/OHLCV를 제공하는 단일 계약
  - `list_tickers(asof_date=None, market=None) -> (tickers, market_by_ticker)`
  - `load_stock_master(asof_date=None) -> master_df`
  - `fetch_ohlcv(ticker, start_date, end_date, adjusted=True) -> raw_df`
  - (권장) `name`/`capabilities`/`diagnostics` 같은 식별/상태 정보 제공

중요: 예를 들어 **`PykrxProvider`는 내부적으로 로컬 `krx_stock_master.json`(Seibro) 데이터를 사용**하여 `list_tickers/load_stock_master`를 구현하고, 가격만 pykrx에서 가져오는 식으로 “내부 구현”을 캡슐화합니다.  
즉, Orchestrator는 “pykrx가 로컬 마스터를 쓰는지/증권사 API를 쓰는지”를 몰라도 됩니다.

#### 2) Adapter Implementations (구현체)
- **`PykrxProvider`**: 가격은 `pykrx`로 수집하고, 유니버스/마스터는 내부에서 로컬 데이터(`data/krx_stock_master.json`)로 제공
  - **⚠️ Known Issues**: pykrx의 일부 API(코스피/코스닥 종목 리스트, ETF 리스트 등)가 일시적으로 동작하지 않을 수 있음
  - 이러한 이유로 로컬 JSON 파일(Seibro 엑셀 기반)을 사용하여 종목 유니버스를 구성
- **`KoreaInvestmentProvider`**: 한국투자증권 Open Trading API를 사용하여 가격 데이터를 수집하고, 유니버스/마스터는 로컬 데이터(`data/krx_stock_master.json`)로 제공
  - API 인증: `appkey`(HT_KE), `appsecret`(HT_SE) 필요
  - API 엔드포인트: `/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice`
  - 특징: 일자별 OHLCV 데이터 조회 (최대 100일), 수정주가/원주가 선택 가능
- **`FdrProvider`**: FinanceDataReader 라이브러리를 사용하여 가격 데이터를 수집
  - **⚠️ Multi-threading Warning**: FinanceDataReader는 멀티스레드 환경에서 **스레드 안전하지 않음**
  - `max_workers > 1` 설정 시 2년 제한 에러, 403 Forbidden 등의 문제가 발생
  - **반드시 `max_workers=1`로 설정**하여 순차 처리해야 안정적으로 동작함

#### 3) Error Handling (Fail-fast)
본 리포지토리의 캐시 생성은 “부분 성공”보다 “정확한 실패 감지”가 중요하므로, **폴백을 사용하지 않습니다.**
- Provider 호출(유니버스/마스터/벤치마크/개별 티커 OHLCV)에서 **오류가 발생하면 즉시 예외를 발생**시키고 실행을 종료합니다.
- 종료 시에는 가능한 범위에서 `meta.json`에 **실패 원인/스택 요약/어떤 단계에서 실패했는지**를 기록합니다.
  - 예: `meta["run_status"]="failed"`, `meta["error"]["stage"]="ohlcv_fetch"`, `meta["error"]["ticker"]="005930"`, `meta["error"]["message"]=...`

추가 원칙(과도한 예외처리 지양):
- **Catch는 “경계(boundary)”에서만**: CLI 엔트리포인트/Orchestrator 최상단에서만 예외를 잡아 메타 기록 후 종료
- 내부 로직(표준화/지표 계산/Provider 내부)에서는:
  - 가능한 경우 **입력 검증(assertion/명시적 체크)** 으로 빠르게 실패
  - 그 외에는 예외를 **그대로 전파**하여 원인을 숨기지 않음
  - “계속 진행하기 위한 복구 로직”을 넣지 않음(폴백/부분 성공 금지)

#### 4) Orchestrator (파이프라인)
- `generate_cache`는 **단일 `DataProvider` 계약만 의존**:
  - 유니버스 로드 → 마스터 로드 → 벤치마크 로드 → 종목별 OHLCV 로드 → 표준화 → 지표 계산 → 저장/메타
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

### 유닛 테스트 전략(핵심 기능 + 외부 소스 안정성 체크)
목표는 “코드 안정성”과 “외부 소스(Provider) 상태”를 빠르게 감지하는 것입니다.

#### 1) Core Unit Tests (항상 실행, 외부 네트워크 불필요)
- **`OHLCVStandardizer`**
  - 입력 raw_df(샘플/fixture) → 출력 컬럼/타입/정렬/티커 zfill 검증
  - 결측/비정상 타입 입력 시 “조기 실패”가 발생하는지 확인
- **`IndicatorEngine`**
  - 이동평균/RS/신고가 계산이 기대값과 일치하는지 소형 데이터로 검증
  - 벤치마크가 없는 경우(또는 결측) RS가 `NA`로 처리되는지 검증
- **`CacheBuildOrchestrator`(얇은 통합/유닛 경계)**
  - `FakeProvider`(in-memory fixture)로 tickers/master/ohlcv를 제공
  - 1~2개 티커에 대해 end-to-end로 parquet/meta 생성이 되는지 검증
  - Provider가 예외를 던지면 **즉시 실패(fail-fast)** 하는지 검증

#### 2) Provider Contract Tests (항상 실행, 구현체별)
각 Provider 구현체가 최소 계약을 지키는지 검증합니다(네트워크 없이 가능하면 fixture/recording 사용).
- `list_tickers`는 6자리 문자열을 반환하는지
- `load_stock_master`는 최소 컬럼을 포함하는지
- `fetch_ohlcv`는 Date 범위/정렬/필수 컬럼 확보가 가능한 형태인지(표준화 이전이라도 최소한의 일관성)

#### 3) External Source Smoke Tests (선택 실행: “외부 소스 안정성 체크”)
외부 소스는 변동성이 크므로 CI의 기본 테스트와 분리합니다.
- 실행 조건: 예) `RUN_EXTERNAL_SMOKE=1` 환경변수 또는 스케줄 워크플로(주간/일간)
- 테스트 내용(짧게, 10~30초 수준 목표):
  - `provider.fetch_ohlcv`를 대표 티커 1~3개(예: `069500`, `005930`)로 호출해 “응답이 비어있지 않음”을 확인
  - `provider.list_tickers/load_stock_master`가 정상 동작하는지 확인
- 실패 시: **Provider 소스 장애/변경을 빠르게 감지**하고, 캐시 생성 파이프라인이 위험하다는 신호로 사용

#### 4) 테스트 도구/구성(구현 시)
- 테스트 프레임워크: `pytest`
- 외부 API를 HTTP로 호출하는 provider의 경우: `vcrpy`(record/replay) 또는 응답 fixture 파일
- pykrx처럼 HTTP 래핑이 불투명한 경우:
  - “네트워크 없는 core test”는 `FakeProvider`/fixture DF로 충족
  - “외부 안정성 체크”는 smoke 테스트에서만 최소 호출로 분리

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
  master_df = provider.load_stock_master(...)
  (tickers, market_map) = provider.list_tickers(...)
  bench = provider.fetch_ohlcv(benchmark_ticker, ...)
  for ticker in tickers in parallel:
    raw = provider.fetch_ohlcv(ticker, ...)
    if raw failed:
      raise error and abort run
    std = standardize_ohlcv(raw)
    feat = compute_indicators(std, bench_close_by_date)
    collect feat
  save parquet + meta(provider diagnostics)
  if industry enabled:
    industry = compute_industry_strength(all_features, master_df, industry_benchmark)
    save industry parquet + meta
```

### Short-term
1.  **Data Source Abstraction (1차)**: Provider 인터페이스/표준화 모듈/Orchestrator 분리(동작 동일 유지).
2.  **Full Universe in CI**: 전 종목 수집은 시간이 오래 걸릴 수 있으므로, CI에서는 `--test-limit` 등으로 수집 종목 수를 제한하고 전체 빌드는 별도 스케줄로 분리 검토.
3.  **Feature Logic Integration**: 단순 OHLCV 외에 기술적 지표(MA, RSI, Bollinger Bands 등) 계산 로직 추가.
4.  **Scheduler**: 주간/일간 자동 실행을 위한 `schedule` (cron) 트리거 활성화.
5.  **CI: Unit Tests**: push/PR 시 `pytest`를 실행하는 워크플로 추가 및 유지(기본은 `external` 제외)

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
