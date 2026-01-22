# Mansfield RS (Relative Strength) 계산 방법

## 개요

Mansfield RS (Relative Strength)는 특정 주식이나 업종이 시장 또는 벤치마크 대비 얼마나 상대적으로 강한 성과를 보이는지를 측정하는 지표입니다. 이 프로젝트에서는 Stan Weinstein의 Mansfield Relative Strength 개념을 구현하고 있습니다.

## 계산 위치

Mansfield RS 계산은 두 곳에서 수행됩니다:

1. **개별 종목**: `capybara_fetcher/indicators.py` - `compute_features()` 함수
2. **업종 지수**: `capybara_fetcher/industry.py` - `build_industry_strength_frame()` 함수

## 기본 개념

Mansfield RS는 다음 3단계로 계산됩니다:

### 1단계: Raw RS 계산
```
RS_raw(t) = Close_ticker(t) / Close_benchmark(t)
```
- 특정 날짜(t)의 종목 종가를 벤치마크 종가로 나눈 값
- 종목이 벤치마크 대비 얼마나 강한지를 나타내는 비율

### 2단계: RS의 200일 이동평균 계산
```
RS_sma(t) = SMA_200(RS_raw(t))
```
- RS_raw의 200거래일 단순이동평균(Simple Moving Average)
- `min_periods=200` 설정으로 인해 초반 200거래일 미만 구간은 NA(결측값)

### 3단계: Mansfield RS 계산
```
MansfieldRS(t) = (RS_raw(t) / RS_sma(t) - 1) × 100
```
- RS_raw가 RS_sma 대비 얼마나 높거나 낮은지를 백분율로 표현
- **양수**: 종목이 최근 200일 평균보다 강한 상태 (상승 추세)
- **음수**: 종목이 최근 200일 평균보다 약한 상태 (하락 추세)
- **0 근처**: 종목이 평균적인 상태

## 벤치마크 설정

### 개별 종목의 벤치마크
- **티커**: `069500` (KODEX 200 ETF)
- **가격**: 수정주가 (`adjusted=True`)
- 코드 위치: `capybara_fetcher/orchestrator.py`의 `MANSFIELD_BENCHMARK_TICKER` 상수

### 업종 지수의 벤치마크
업종 Mansfield RS는 실행 인자로 선택 가능:
- `--industry-benchmark universe` (기본값): 전체 유니버스 동일가중 지수
- `--industry-benchmark 069500`: 개별 종목과 동일한 069500 벤치마크 사용

## 구현 코드

### 개별 종목 Mansfield RS (indicators.py)

```python
# 상수 정의
MANSFIELD_RS_SMA_WINDOW = 200

def compute_features(ohlcv_df: pd.DataFrame, *, benchmark_close_by_date: pd.Series | None) -> pd.DataFrame:
    """
    OHLCV 데이터에 Mansfield RS를 포함한 특성 컬럼 추가
    
    Args:
        ohlcv_df: 표준화된 OHLCV 데이터프레임 (Date, Close 등 포함)
        benchmark_close_by_date: 벤치마크의 날짜별 종가 Series (index=날짜, value=종가)
    
    Returns:
        특성 컬럼이 추가된 데이터프레임 (MansfieldRS 컬럼 포함)
    """
    # ... (생략) ...
    
    # Mansfield Relative Strength 계산
    if benchmark_close_by_date is not None and not benchmark_close_by_date.empty:
        # 벤치마크 중복 날짜 처리 (마지막 값 유지)
        if not benchmark_close_by_date.index.is_unique:
            benchmark_close_by_date = benchmark_close_by_date[
                ~benchmark_close_by_date.index.duplicated(keep="last")
            ]
        
        # 1단계: 날짜별 벤치마크 매핑 및 Raw RS 계산
        bench = df["Date"].dt.normalize().map(benchmark_close_by_date)
        bench = pd.to_numeric(bench, errors="coerce")
        rs_raw = close / bench
        
        # 2단계: RS의 200일 이동평균 계산
        rs_sma = rs_raw.rolling(
            window=MANSFIELD_RS_SMA_WINDOW, 
            min_periods=MANSFIELD_RS_SMA_WINDOW
        ).mean()
        
        # 3단계: Mansfield RS 계산 (백분율)
        df["MansfieldRS"] = (rs_raw / rs_sma - 1.0) * 100.0
    else:
        # 벤치마크가 없으면 NA 설정
        df["MansfieldRS"] = pd.NA
    
    return df
```

### 업종 지수 Mansfield RS (industry.py)

```python
def build_industry_strength_frame(...):
    """
    업종별 강도 프레임 생성 (동일가중 업종 지수 + Mansfield RS)
    
    Returns:
        업종별 데이터프레임 (IndustryClose, IndustryReturn, MansfieldRS 등 포함)
    """
    # ... (업종 지수 계산 생략) ...
    
    # 업종별 Mansfield RS 계산
    if benchmark_close_by_date is not None and not benchmark_close_by_date.empty:
        # 날짜별 벤치마크 매핑
        b = out["Date"].map(benchmark_close_by_date)
        b = pd.to_numeric(b, errors="coerce")
        
        # Raw RS 계산
        rs_raw = out["IndustryClose"] / b
        
        # 업종별 그룹핑하여 RS SMA 계산
        rs_sma = rs_raw.groupby(out["IndustryKey"]).transform(
            lambda s: s.rolling(
                window=MANSFIELD_RS_SMA_WINDOW, 
                min_periods=MANSFIELD_RS_SMA_WINDOW
            ).mean()
        )
        
        # Mansfield RS 계산
        out["MansfieldRS"] = (rs_raw / rs_sma - 1.0) * 100.0
    else:
        out["MansfieldRS"] = pd.NA
    
    return out
```

## 계산 예제

실제 데이터로 계산 과정을 설명합니다:

### 가정
- 날짜: 2025-01-15
- 종목 종가: 50,000원
- 벤치마크 (069500) 종가: 10,000원
- 과거 200일간 RS_raw의 평균: 4.8

### 계산 과정

1. **Raw RS 계산**:
   ```
   RS_raw = 50,000 / 10,000 = 5.0
   ```

2. **RS SMA** (이미 계산됨):
   ```
   RS_sma = 4.8  (과거 200일 평균)
   ```

3. **Mansfield RS 계산**:
   ```
   MansfieldRS = (5.0 / 4.8 - 1) × 100
               = (1.0417 - 1) × 100
               = 0.0417 × 100
               = 4.17
   ```

### 해석
- MansfieldRS = +4.17
- 이 종목은 최근 200일 평균 대비 4.17% 더 강한 상태
- 벤치마크 대비 상승 추세에 있음을 의미

## Mansfield RS 값의 의미

| 값 범위 | 의미 | 투자 신호 |
|---------|------|-----------|
| MansfieldRS > +10 | 매우 강한 상승 추세 | 강한 매수 신호 |
| 0 < MansfieldRS < +10 | 상승 추세 | 매수 고려 |
| -10 < MansfieldRS < 0 | 하락 추세 | 매도 고려 |
| MansfieldRS < -10 | 매우 약한 하락 추세 | 강한 매도 신호 |

> **참고**: 위 기준은 Stan Weinstein의 일반적인 가이드라인이며, 실제 투자 결정 시에는 다른 지표들과 함께 종합적으로 판단해야 합니다.

## 주요 특징

### 1. 초기 구간 NA 처리
- 상장 후 200거래일 미만인 종목은 MansfieldRS가 NA
- 충분한 데이터가 축적되어야 신뢰성 있는 값 산출 가능

### 2. 벤치마크 정규화
- 날짜를 정규화(normalize)하여 시간 정보 제거
- 중복 날짜는 마지막 값(keep="last")을 유지

### 3. 업종별 독립 계산
- 업종 지수의 경우 `groupby`로 업종별 독립적으로 RS SMA 계산
- 각 업종이 자체 200일 이동평균 기준 보유

## 데이터 흐름

```
1. 벤치마크 데이터 수집 (069500)
   ↓
2. 개별 종목/업종 OHLCV 수집
   ↓
3. 날짜별 벤치마크 매핑
   ↓
4. RS_raw 계산 (Close/Benchmark)
   ↓
5. RS_sma 계산 (200일 롤링)
   ↓
6. MansfieldRS 계산 ((raw/sma - 1) × 100)
   ↓
7. Parquet 파일로 저장
```

## 사용 예시

### Streamlit 앱에서 활용
```python
# MansfieldRS 상위 종목 조회
query = """
SELECT Ticker, Name, MansfieldRS, Date
FROM feature_data
WHERE Date = (SELECT MAX(Date) FROM feature_data)
  AND MansfieldRS IS NOT NULL
ORDER BY MansfieldRS DESC
LIMIT 10
"""
```

### 업종 강도 분석
```python
# MansfieldRS 상위 업종 조회
query = """
SELECT IndustryLarge, IndustryMid, MansfieldRS, Date
FROM industry_data
WHERE Date = (SELECT MAX(Date) FROM industry_data)
  AND MansfieldRS IS NOT NULL
ORDER BY MansfieldRS DESC
LIMIT 5
"""
```

## 참고 자료

- Stan Weinstein, "Secrets for Profiting in Bull and Bear Markets"
- 구현 코드:
  - `capybara_fetcher/indicators.py` - 개별 종목 계산
  - `capybara_fetcher/industry.py` - 업종 지수 계산
  - `capybara_fetcher/orchestrator.py` - 벤치마크 설정
- 아키텍처 문서: `arch.md`

## 문의 및 개선 사항

Mansfield RS 계산 방법에 대한 질문이나 개선 제안은 GitHub Issues를 통해 제출해 주세요.
