# Feature Cache Repository Architecture

## 1. Project Overview
이 레포지토리는 KOSPI 및 KOSDAQ 시장의 주가 데이터를 미리 수집하고 가공하여 **Feature Cache (Parquet)** 형태로 제공하는 역할을 합니다.  
백테스트 및 분석 시스템(`capybara_share` 등)은 이 레포지토리의 **GitHub Releases**에서 최신 캐시 파일을 다운로드하여 사용함으로써 데이터 수집 시간을 단축합니다.

## 2. Architecture

### Data Flow
1.  **Universe Construction**: KOSPI, KOSDAQ 전 종목 티커 리스트 확보 (현재 검증을 위해 5+5종목 제한 중).
2.  **Data Fetching**: `pykrx`를 통해 각 종목의 OHLCV 데이터 병렬 수집.
3.  **Standardization**: 컬럼명 영문 변환 (`시가` -> `Open` 등) 및 날짜 인덱스 처리.
4.  **Serialization**: 수집된 전체 데이터를 단일 `Parquet` 파일로 저장 (zstd 압축).
5.  **Distribution**: GitHub Actions를 통해 Parquet 파일을 **GitHub Releases**에 자동 업로드.

## 3. Directory Structure

```
/workspace/
├── .github/
│   └── workflows/
│       └── update_feature_cache.yml  # 캐시 생성 및 릴리스 자동화 워크플로우
├── scripts/
│   └── generate_cache.py             # 데이터 수집 및 Parquet 생성 스크립트
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
*   **Quick Verification Mode**: 빠른 테스트를 위해 KOSPI 5종목, KOSDAQ 5종목으로 수집 범위 제한.

### ⚠️ Temporary Limitations
*   **Limited Universe**: 검증 속도를 위해 수집 대상을 상위 10개 종목으로 하드코딩/제한함.
*   **Basic Data Only**: 현재는 OHLCV(가격/거래량) 데이터만 포함하며, 이동평균선 등 파생 변수(Feature)는 계산하지 않음.
*   **PyKrx Ticker Issue**: `pykrx`의 티커 리스트 조회 불안정 시 Fallback 리스트 사용 로직 포함.

## 5. Remaining Tasks

### Short-term
1.  **Remove Verification Limit**: 검증 완료 후 `scripts/generate_cache.py`의 10종목 제한 해제하여 전 종목 수집으로 전환.
2.  **Feature Logic Integration**: 단순 OHLCV 외에 기술적 지표(MA, RSI, Bollinger Bands 등) 계산 로직 추가.
3.  **Scheduler**: 주간/일간 자동 실행을 위한 `schedule` (cron) 트리거 활성화.

### Long-term
1.  **Incremental Update**: 전체 재수집 대신 최신 데이터만 추가하는 증분 업데이트 구현.
2.  **Partitioning**: 데이터 크기 증가 시 연도별 또는 종목별 파일 분할 저장 고려.
