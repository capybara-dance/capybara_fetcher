# Capybara Fetcher - 릴리즈 데이터 가이드

## 개요

Capybara Fetcher는 한국 주식 시장(KOSPI, KOSDAQ, ETF)의 가격 데이터와 기술적 지표를 수집하여 Parquet 형식으로 제공하는 데이터 파이프라인입니다. 본 문서는 GitHub Releases를 통해 배포되는 데이터의 구조, 의미, 계산 방식을 설명합니다.

## 릴리즈 파일 구성

각 릴리즈에는 다음 파일들이 포함됩니다:

1. **종목 Feature 데이터**: `korea_universe_feature_frame.parquet`
2. **종목 Feature 메타데이터**: `korea_universe_feature_frame.meta.json`
3. **업종 Feature 데이터**: `korea_industry_feature_frame.parquet`
4. **업종 Feature 메타데이터**: `korea_industry_feature_frame.meta.json`
5. **KRX 종목 마스터**: `krx_stock_master.parquet`

---

## 1. 종목 Feature 데이터 (korea_universe_feature_frame.parquet)

### 데이터 구조

전체 종목의 일별 가격 데이터와 기술적 지표를 담은 멀티인덱스 시계열 데이터입니다.

### 컬럼 정의

#### 기본 정보
| 컬럼명 | 데이터 타입 | 설명 |
|--------|------------|------|
| `Date` | datetime64[ns] | 거래일자 (timezone-naive, 정규화된 날짜) |
| `Ticker` | string | 종목코드 (6자리, 예: "005930") |

#### 가격 데이터 (OHLCV)
| 컬럼명 | 데이터 타입 | 설명 |
|--------|------------|------|
| `Open` | Int32 | 시가 (원) |
| `High` | Int32 | 고가 (원) |
| `Low` | Int32 | 저가 (원) |
| `Close` | Int32 | 종가 (원) - **수정주가 기준** |
| `Volume` | float64 | 거래량 (주) |
| `TradingValue` | float64 | 거래대금 (원) |
| `Change` | float64 | 등락률 (%) |

**참고**: OHLC 가격은 한국 주식의 최고가(~250만원)가 Int32 범위(~21억) 내에 안전하게 들어가므로 저장 효율을 위해 Int32를 사용합니다.

#### 이동평균 지표
| 컬럼명 | 데이터 타입 | 설명 |
|--------|------------|------|
| `SMA_5` | float32 | 5일 단순 이동평균 (종가 기준) |
| `SMA_10` | float32 | 10일 단순 이동평균 (종가 기준) |
| `SMA_20` | float32 | 20일 단순 이동평균 (종가 기준) |
| `SMA_60` | float32 | 60일 단순 이동평균 (종가 기준) |
| `SMA_120` | float32 | 120일 단순 이동평균 (종가 기준) |
| `SMA_200` | float32 | 200일 단순 이동평균 (종가 기준) |

**계산 방식**:
```
SMA_n(t) = (Close(t) + Close(t-1) + ... + Close(t-n+1)) / n
```
- 최소 데이터 개수(`min_periods`): n일
- n일 미만 데이터가 있는 경우: NA

#### 상대 강도 지표
| 컬럼명 | 데이터 타입 | 설명 |
|--------|------------|------|
| `MansfieldRS` | float32 | Mansfield 상대 강도 (%) |

**계산 방식**:
```
RS_raw(t) = Close_ticker(t) / Close_benchmark(t)
RS_sma(t) = SMA_200(RS_raw(t))
MansfieldRS(t) = (RS_raw(t) / RS_sma(t) - 1) × 100
```

- **벤치마크**: `069500` (KODEX 200, 수정주가 기준)
- **윈도우**: 200 거래일 이동평균
- **최소 기간**: 200 거래일 미만인 경우 NA
- **의미**: 양수이면 벤치마크 대비 상대적으로 강세, 음수이면 약세

#### 신고가 여부
| 컬럼명 | 데이터 타입 | 설명 |
|--------|------------|------|
| `IsNewHigh1Y` | boolean | 1년(252 거래일) 신고가 여부 |

**계산 방식**:
```
IsNewHigh1Y(t) = Close(t) == max(Close(t-251), ..., Close(t))
```
- **윈도우**: 252 거래일 (약 1년)
- **최소 기간**: 252 거래일 미만인 경우 NA
- **의미**: True이면 해당 일자가 최근 1년 내 최고 종가

---

## 2. 업종 Feature 데이터 (korea_industry_feature_frame.parquet)

### 데이터 구조

업종별 동일가중 지수와 상대 강도를 담은 시계열 데이터입니다. 3가지 레벨(대/대중/대중소)로 구성됩니다.

### 컬럼 정의

| 컬럼명 | 데이터 타입 | 설명 |
|--------|------------|------|
| `Date` | datetime64[ns] | 거래일자 |
| `Level` | string | 업종 레벨 ("L": 대분류, "LM": 대중분류, "LMS": 대중소분류) |
| `IndustryLarge` | string | 대분류 업종명 |
| `IndustryMid` | string | 중분류 업종명 (Level이 "L"인 경우 빈 문자열) |
| `IndustrySmall` | string | 소분류 업종명 (Level이 "L", "LM"인 경우 빈 문자열) |
| `IndustryKey` | string | 업종 식별자 (내부용) |
| `IndustryClose` | float32 | 업종 지수 (기준값: 100) |
| `IndustryReturn` | float32 | 업종 일간 수익률 (%) |
| `ConstituentCount` | int16 | 해당 업종 구성 종목 수 |
| `MansfieldRS` | float32 | 업종 Mansfield 상대 강도 (%) |

### 업종 지수 계산 방식

**동일가중 방식**으로 계산됩니다:

```
# 1. 각 종목의 일간 수익률 계산
Return_ticker(t) = (Close(t) - Close(t-1)) / Close(t-1)

# 2. 업종 일간 수익률 = 해당 업종 구성 종목들의 평균 수익률
IndustryReturn(t) = mean(Return_ticker(t) for all tickers in industry)

# 3. 업종 지수 = 누적 수익률 × 100 (기준값)
IndustryClose(t) = 100 × ∏(1 + IndustryReturn(i)) for i from start to t
```

**특징**:
- ETF는 업종 지수 계산에서 제외됩니다
- 결측 데이터는 0% 수익률로 보정하여 날짜 연속성을 보장합니다
- 업종 분류는 KRX Stock Master의 업종 정보를 기준으로 합니다

### 업종 Mansfield RS 계산

종목 RS와 동일한 방식으로 계산되며, 벤치마크는 실행 인자로 선택 가능합니다:
- `universe`: 전체 유니버스 동일가중 지수 (기본값)
- `069500`: KODEX 200 ETF

---

## 3. KRX 종목 마스터 (krx_stock_master.parquet)

### 데이터 구조

한국 거래소 전체 종목(KOSPI, KOSDAQ, ETF)의 메타 정보입니다.

### 컬럼 정의

| 컬럼명 | 데이터 타입 | 설명 |
|--------|------------|------|
| `Code` | string | 종목코드 (6자리, 예: "005930") |
| `Name` | string | 종목명 (예: "삼성전자") |
| `Market` | string | 시장 구분 ("KOSPI", "KOSDAQ", "ETF") |
| `IndustryLarge` | string | 대분류 업종 (ETF는 null) |
| `IndustryMid` | string | 중분류 업종 (ETF는 null) |
| `IndustrySmall` | string | 소분류 업종 (ETF는 null) |
| `SharesOutstanding` | float64 | 상장주식수 (주) |

---

## 4. 메타데이터 파일

각 데이터 파일에는 대응하는 `.meta.json` 파일이 있습니다.

### 종목 Feature 메타데이터 예시

```json
{
  "run_status": "success",
  "start_date": "2015-01-01",
  "end_date": "2026-01-27",
  "tickers_requested": 1234,
  "tickers_succeeded": 1234,
  "row_count": 3456789,
  "column_count": 18,
  "file_size_bytes": 123456789,
  "indicators": {
    "moving_averages": [5, 10, 20, 60, 120, 200],
    "mansfield_rs": {
      "benchmark_ticker": "069500",
      "window": 200,
      "adjusted": true,
      "fetch_success": true
    },
    "new_high_1y": {
      "window": 252
    }
  },
  "runtime_seconds": 1234.56,
  "python_version": "3.12.0",
  "provider": "composite"
}
```

### 업종 Feature 메타데이터 예시

```json
{
  "run_status": "success",
  "industry_benchmark": "universe",
  "levels": ["L", "LM", "LMS"],
  "row_count": 123456,
  "column_count": 10,
  "file_size_bytes": 12345678,
  "runtime_seconds": 123.45
}
```

---

## 5. 데이터 출처

### 원천 데이터

1. **종목 리스트 및 업종 분류**
   - 출처: [한국예탁결제원 세이브로(Seibro)](https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/stock/BIP_CNTS02004V.xml&menuNo=41)
   - 파일: `data/kospi.xlsx`, `data/kosdaq.xlsx`
   - 처리: `scripts/build_krx_stock_master.py`로 JSON 변환 후 사용

2. **가격 데이터 (OHLCV)**
   - 기본 출처: `pykrx` 라이브러리 (한국거래소 데이터)
   - 대체 출처 (CompositeProvider 사용 시):
     - `KoreaInvestmentProvider`: 한국투자증권 Open Trading API
     - `FdrProvider`: FinanceDataReader (KRX, NAVER, YAHOO)

### 데이터 수집 프로세스

1. **병렬 수집**: ThreadPoolExecutor를 사용하여 다수의 종목 동시 수집
2. **표준화**: 컬럼명 영문 변환, 날짜 정규화, 데이터 타입 최적화
3. **지표 계산**: 이동평균, Mansfield RS, 신고가 여부 계산
4. **업종 강도 계산**: 동일가중 지수 및 업종별 RS 계산
5. **저장**: Parquet 형식 (zstd 압축)으로 저장
6. **릴리즈**: GitHub Actions를 통해 자동으로 GitHub Releases에 업로드

---

## 6. 데이터 사용 가이드

### 권장 사용 방법: On-demand Query (DuckDB)

대용량 Parquet 파일을 전체 다운로드하지 않고 필요한 부분만 쿼리하는 방식입니다.

```python
import duckdb

# GitHub Release URL (실제 릴리즈에서 확인)
url = "https://github.com/capybara-dance/capybara_fetcher/releases/download/v2024.01.27/korea_universe_feature_frame.parquet"

# DuckDB로 원격 Parquet 쿼리
con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

# 특정 종목, 기간만 조회 (메모리 효율적)
query = f"""
    SELECT * FROM read_parquet('{url}')
    WHERE Ticker = '005930'
    AND Date BETWEEN '2025-01-01' AND '2026-01-01'
"""
df = con.execute(query).df()
print(df)
```

### Pandas로 전체 로드 (주의)

메모리가 충분한 환경에서만 사용하세요:

```python
import pandas as pd

url = "https://github.com/capybara-dance/capybara_fetcher/releases/download/v2024.01.27/korea_universe_feature_frame.parquet"
df = pd.read_parquet(url)

# 특정 종목 필터링
samsung = df[df['Ticker'] == '005930']
```

### Streamlit 앱으로 데이터 확인

본 레포지토리의 `streamlit_app.py`를 실행하면 웹 브라우저에서 데이터를 탐색할 수 있습니다:

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

---

## 7. 주의사항

### 데이터 품질

1. **수정주가**: 모든 가격 데이터는 수정주가(adjusted price) 기준입니다
2. **결측치**: 상장 전 날짜, 거래정지 등으로 데이터가 없는 경우 NA 처리됩니다
3. **중복 제거**: 원천 데이터의 날짜 중복이 있는 경우 최신 값(keep='last')을 유지합니다
4. **지표 초기값**: 이동평균, RS 등은 충분한 과거 데이터가 있어야 계산되므로 초기 기간은 NA입니다

### 데이터 소스 제약

1. **pykrx API 불안정성**: pykrx의 일부 API(종목 리스트 등)가 간헐적으로 동작하지 않을 수 있습니다
2. **FDR 멀티스레딩 이슈**: FdrProvider는 스레드 안전하지 않으므로 `max_workers=1` 사용이 필수입니다
3. **API 인증**: KoreaInvestmentProvider 사용 시 API 키가 필요합니다

### 파일 크기

- 전체 종목 Feature 데이터는 수백 MB~수 GB까지 커질 수 있습니다
- Streamlit Cloud, Colab 등 메모리 제약 환경에서는 DuckDB 방식 사용을 권장합니다

---

## 8. 업데이트 주기

- **자동 업데이트**: 평일 오후 5시 (KST) GitHub Actions 스케줄 실행
- **수동 업데이트**: GitHub Actions의 "Update Feature Cache" 워크플로우를 수동으로 실행 가능

---

## 9. 문의 및 기여

- **이슈 제기**: [GitHub Issues](https://github.com/capybara-dance/capybara_fetcher/issues)
- **기여 가이드**: Pull Request 환영합니다
- **아키텍처 문서**: [arch.md](./arch.md) 참고

---

## 라이선스

본 프로젝트의 라이선스는 레포지토리의 LICENSE 파일을 참조하세요.
