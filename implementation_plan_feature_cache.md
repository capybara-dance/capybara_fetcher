# KOSPI+KOSDAQ 유니버스 FeatureFrame 캐시 구현 계획

## 1. 개요

### 목적
백테스트 수행 속도를 크게 개선하기 위해, KOSPI+KOSDAQ 전체 유니버스의 주가 정보 및 각종 지표를 미리 계산하여 파일로 저장하고 재사용한다.

### 배경
- 현재 백테스트는 매번 네트워크에서 OHLCV 데이터를 가져오고 지표를 재계산하여 느림
- 삼성전자 10년치 기준: pickle 0.25MB, parquet 0.16MB (약 36% 감소)
- 전체 유니버스(약 2,600 종목) 예상 용량: **parquet 기준 약 400MB**

### 목표
- KOSPI+KOSDAQ 전체 유니버스의 FeatureFrame을 Parquet 포맷으로 사전 계산
- **별도 레포지토리에서 캐시 관리** (코드 레포와 분리)
- GitHub Releases를 통한 안정적인 배포 및 버전 관리
- 백테스트 워크플로우에서 캐시 자동 다운로드/사용

---

## 2. 아키텍처

### 2.1 레포지토리 구조

**캐시 전용 레포지토리** (예: `your-org/korea-stock-cache`):
- 역할: FeatureFrame Parquet 파일만 저장/관리
- 워크플로우: 주간 자동 갱신 → Releases에 업로드
- 태그: `data-YYYYMMDD` 또는 `v1.0.0-data`

**코드 레포지토리** (현재 레포, 예: `your-org/capybara_share`):
- 역할: 캐시 다운로드 및 사용만
- 워크플로우: 캐시 전용 레포에서 다운로드 → 백테스트 실행

**장점**:
- 레포지토리 분리: 코드 레포는 작게 유지, 캐시는 별도 관리
- 독립적 업데이트: 캐시 갱신이 코드 변경과 분리
- 공유 가능: 여러 프로젝트에서 동일한 캐시 레포 사용 가능
- 권한 분리: 캐시 레포만 별도 권한 관리

### 2.2 데이터 흐름

```
[캐시 전용 레포지토리]
[1] 유니버스 구성
    └─> KOSPI 전체 + KOSDAQ 전체 (약 2,600 종목)
    
[2] FeatureFrame Precompute
    └─> 각 종목별로 compute_feature_df(spec) 실행
    └─> 병렬 처리 (ThreadPoolExecutor)
    └─> MultiIndex(date, ticker) 형태로 병합
    
[3] Parquet 저장
    └─> 단일 파일 또는 티커별 분할 저장
    └─> 압축: snappy 또는 zstd
    
[4] GitHub Releases 업로드
    └─> 태그: data-YYYYMMDD
    └─> Asset: korea_universe_feature_frame.parquet

[코드 레포지토리]
[5] 백테스트에서 사용
    └─> 캐시 전용 레포의 Releases에서 최신 캐시 다운로드
    └─> FeatureFrameDataProvider로 로드
    └─> 네트워크 조회 없이 즉시 실행
```

### 2.2 저장 포맷

**FeatureFrame 구조** (현재 `core2/stock.py`의 `compute_feature_df` 기준):
- **Index**: `date` (normalized datetime)
- **Columns**:
  - 가격: `Open`, `High`, `Low`, `Close`, `Adj Close`, `Volume`, `TradingValue`
  - 지표: `sma`, `rolling_max_prev`, `new_high`, `recent_new_high_count`, `ret_N`, `tv_ma_N`, `ready`
  - 옵션: `MarketCap` (가중치 설정에 따라)

**Parquet 저장 옵션**:
- `index=True`: date 인덱스 포함
- `compression='snappy'` 또는 `'zstd'`: 압축 (zstd가 더 작지만 느림)
- `engine='pyarrow'`: 기본 엔진

---

## 3. 구현 단계

### Phase 1: 캐시 전용 레포지토리 생성 및 캐시 생성 스크립트

**캐시 전용 레포지토리 생성**:
- 새 레포지토리 생성: `your-org/korea-stock-cache` (또는 원하는 이름)
- Public 또는 Private 설정 (Public이면 Token 불필요, Private이면 Token 필요)

#### 3.1 `scripts/generate_korea_universe_feature_cache.py` (캐시 전용 레포에 구현)

**기능**:
- KOSPI+KOSDAQ 전체 유니버스 구성 (기존 `build_korea_universe` 확장 또는 별도 함수)
- FeatureSpec 기반으로 각 종목의 feature_df 계산
- 병렬 처리로 속도 최적화
- 단일 Parquet 파일로 저장

**입력 파라미터**:
- `--start-date`: 데이터 시작일 (YYYY-MM-DD, 기본: 10년 전)
- `--end-date`: 데이터 종료일 (YYYY-MM-DD, 기본: 오늘)
- `--output`: 출력 파일 경로 (기본: `cache/korea_universe_feature_frame.parquet`)
- `--max-workers`: 병렬 작업 수 (기본: 8)
- `--spec-config`: FeatureSpec 설정 JSON (또는 전략 기본값 사용)

**출력**:
- Parquet 파일
- 메타데이터 JSON (생성일시, 종목 수, 기간, spec 해시 등)

#### 3.2 유니버스 확장

**현재**: `build_korea_universe()` → KOSPI200 + KOSDAQ150 (약 350 종목)

**확장 필요**: KOSPI 전체 + KOSDAQ 전체 (약 2,600 종목)

**방법**:
- `pykrx.stock.get_market_ticker_list()` 사용
- 또는 KRX API로 전체 종목 리스트 조회

```python
# 예시 (구현 필요)
def build_korea_full_universe(
    universe_date: Optional[str] = None,
    lookback_days: int = 21,
) -> List[str]:
    """KOSPI 전체 + KOSDAQ 전체 유니버스 구성."""
    from pykrx import stock as pykrx_stock
    
    # KOSPI 전체
    kospi_tickers = pykrx_stock.get_market_ticker_list(
        market="KOSPI",
        date=universe_date or datetime.now().strftime("%Y%m%d")
    )
    kospi = [f"{t}.KS" for t in kospi_tickers]
    
    # KOSDAQ 전체
    kosdaq_tickers = pykrx_stock.get_market_ticker_list(
        market="KOSDAQ",
        date=universe_date or datetime.now().strftime("%Y%m%d")
    )
    kosdaq = [f"{t}.KQ" for t in kosdaq_tickers]
    
    return sorted({*kospi, *kosdaq})
```

---

### Phase 2: 캐시 전용 레포지토리 - GitHub Releases 업로드

#### 3.3 `scripts/upload_feature_cache_to_releases.py` (캐시 전용 레포에 구현)

**기능**:
- 로컬 Parquet 파일을 **캐시 전용 레포지토리**의 GitHub Releases에 업로드
- 태그 자동 생성: `data-YYYYMMDD`
- 기존 릴리스가 있으면 덮어쓰기 또는 새 버전 생성

**입력 파라미터**:
- `--parquet-file`: 업로드할 Parquet 파일 경로
- `--github-repo`: 캐시 전용 레포지토리 (예: `your-org/korea-stock-cache`)
- `--tag`: 릴리스 태그 (기본: `data-YYYYMMDD`)
- `--release-name`: 릴리스 제목 (기본: "KOSPI+KOSDAQ FeatureFrame Cache (YYYY-MM-DD)")
- `--github-token`: GitHub Personal Access Token (환경변수 `GITHUB_TOKEN` 사용 가능)

**구현 방법**:
- `gh` CLI 또는 `PyGithub` 라이브러리 사용
- 또는 GitHub Actions에서 `softprops/action-gh-release` 사용

---

### Phase 3: 캐시 전용 레포지토리 - GitHub Actions 워크플로우

#### 3.4 `.github/workflows/update_feature_cache.yml` (캐시 전용 레포에 생성)

**트리거**:
- `workflow_dispatch`: 수동 실행
- `schedule`: 주간 자동 갱신 (예: 매주 월요일 새벽 2시)

**작업**:
1. Python 환경 설정
2. 의존성 설치 (`pyarrow` 포함)
3. 유니버스 캐시 생성 (`scripts/generate_korea_universe_feature_cache.py`)
4. **캐시 전용 레포지토리**의 GitHub Releases에 업로드 (`softprops/action-gh-release`)

**예시**:
```yaml
name: Update Feature Cache

on:
  workflow_dispatch:
  schedule:
    - cron: '0 2 * * 1'  # 매주 월요일 새벽 2시

jobs:
  build-and-upload:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pyarrow
      
      - name: Generate feature cache
        run: |
          python scripts/generate_korea_universe_feature_cache.py \
            --output cache/korea_universe_feature_frame.parquet
      
      - name: Create Release and Upload Asset
        uses: softprops/action-gh-release@v1
        with:
          tag_name: data-${{ github.run_number }}
          name: Feature Cache ${{ github.run_number }}
          files: cache/korea_universe_feature_frame.parquet
          draft: false
          prerelease: false
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

---

### Phase 4: 코드 레포지토리 - 캐시 다운로드 및 사용

#### 3.5 `core2/feature_cache_loader.py` (코드 레포에 신규 구현)

**기능**:
- **캐시 전용 레포지토리**의 GitHub Releases에서 최신 Parquet 캐시 다운로드
- 로컬 캐시 디렉토리에 저장
- 캐시가 최신이면 재다운로드 스킵
- `FeatureFrameDataProvider`에 통합

**API**:
```python
def download_latest_feature_cache(
    cache_dir: str = "cache",
    github_repo: str = "your-org/korea-stock-cache",  # 캐시 전용 레포
    asset_name: str = "korea_universe_feature_frame.parquet",
    force_download: bool = False,
    github_token: Optional[str] = None,  # Private 레포인 경우 필요
) -> str:
    """
    캐시 전용 레포지토리의 GitHub Releases에서 최신 FeatureFrame 캐시를 다운로드.
    
    Args:
        github_repo: 캐시 전용 레포지토리 (예: "your-org/korea-stock-cache")
        github_token: Private 레포인 경우 필요 (Public 레포는 None 가능)
    
    Returns:
        로컬 Parquet 파일 경로
    """
    pass

def load_feature_frame_from_cache(
    parquet_path: str,
    spec: FeatureSpec,
) -> pd.DataFrame:
    """
    Parquet 파일에서 FeatureFrame을 로드하고 spec에 맞게 필터링.
    """
    pass
```

**다운로드 URL 예시**:
- Public 레포: `https://github.com/your-org/korea-stock-cache/releases/download/data-20250102/korea_universe_feature_frame.parquet`
- Private 레포: GitHub API를 통해 토큰 인증 필요

#### 3.6 백테스트 워크플로우 수정 (코드 레포)

**`.github/workflows/backtest_52_weeks_high.yml` 수정**:

```yaml
- name: Download feature cache from cache repo
  continue-on-error: true
  env:
    CACHE_REPO: "your-org/korea-stock-cache"  # 캐시 전용 레포지토리
    # Private 레포인 경우:
    # GITHUB_TOKEN: ${{ secrets.CACHE_REPO_TOKEN }}  # 별도 토큰 필요
  run: |
    python scripts/download_feature_cache.py \
      --cache-dir cache \
      --github-repo ${{ env.CACHE_REPO }} \
      --asset-name korea_universe_feature_frame.parquet
      # Private 레포인 경우:
      # --github-token ${{ env.GITHUB_TOKEN }}

- name: Run 52 Weeks High backtest
  env:
    BT_USE_FEATURE_CACHE: "true"
    BT_FEATURE_CACHE_PATH: "cache/korea_universe_feature_frame.parquet"
  run: |
    python - <<'PY'
    # ... 기존 백테스트 코드 ...
    # FeatureFrameDataProvider가 캐시 경로를 자동으로 사용하도록 수정
    PY
```

#### 3.7 로컬 PC에서 사용

**로컬에서도 동일한 방식으로 사용 가능**:

```powershell
# 캐시 다운로드 (캐시 전용 레포에서)
python scripts/download_feature_cache.py \
  --cache-dir cache \
  --github-repo your-org/korea-stock-cache \
  --asset-name korea_universe_feature_frame.parquet

# 백테스트 실행 (캐시 자동 사용)
python go.py
```

**Public 레포지토리인 경우**: Token 없이 다운로드 가능  
**Private 레포지토리인 경우**: Personal Access Token 필요

---

## 4. 기술 스택

### 필수 의존성
- `pandas`: DataFrame 처리
- `pyarrow`: Parquet 읽기/쓰기
- `pykrx`: 한국 주식 데이터 수집

### 선택적 의존성
- `PyGithub`: GitHub API 클라이언트 (Releases 업로드용)
- `gh`: GitHub CLI (대안)

---

## 5. 예상 파일 크기

### 삼성전자 기준 (10년치)
- **pickle**: 0.25 MB
- **csv.gz**: 0.11 MB
- **parquet**: 0.16 MB

### 전체 유니버스 추정 (2,600 종목, 10년치)
- **pickle**: 약 650 MB
- **parquet**: 약 400 MB (36% 감소)
- **parquet (zstd 압축)**: 약 250-300 MB (추가 25-30% 감소 가능)

---

## 6. 성능 개선 예상

### 현재 (네트워크 조회 + 계산)
- 유니버스 2,600 종목 × 평균 2초/종목 = **약 1.4시간** (병렬 처리 시 약 20-30분)

### 캐시 사용 시
- Parquet 로드: **약 10-30초**
- 백테스트 실행: 네트워크 조회 없이 즉시 시작

**예상 개선율**: **약 40-100배 속도 향상**

---

## 7. 주의사항 및 제약

### 7.1 데이터 갱신
- 주가 데이터는 매일 갱신되므로, 캐시도 주기적으로 업데이트 필요
- GitHub Actions 스케줄러로 주간 자동 갱신 권장

### 7.2 캐시 무효화
- FeatureSpec이 변경되면 (지표 파라미터 변경 등) 캐시를 재생성해야 함
- 해시 기반 캐시 키로 자동 무효화 가능

### 7.3 GitHub Releases 제한
- 파일당 최대 2GB (현재 예상 400MB로 충분)
- 레이트리밋: 시간당 약 5,000 요청 (다운로드는 문제 없음)

### 7.5 레포지토리 분리 관련
- **Public 레포지토리**: Token 없이 다운로드 가능, 누구나 접근 가능
- **Private 레포지토리**: GitHub Token 필요 (Personal Access Token 또는 GitHub Actions Secrets)
- 캐시 전용 레포와 코드 레포의 권한을 독립적으로 관리 가능

### 7.4 메모리 사용량
- 전체 FeatureFrame을 메모리에 로드하면 약 400MB-1GB 필요
- 필요시 티커별/기간별 분할 저장으로 부분 로드 가능

---

## 8. 향후 개선 방향

### 8.1 분할 저장
- 티커별 또는 연도별로 Parquet 파일 분할
- 부분 업데이트 가능 (변경된 종목만 재계산)

### 8.2 증분 업데이트
- 최신 N일만 추가 계산하여 기존 캐시에 병합
- 전체 재계산보다 훨씬 빠름

### 8.3 압축 최적화
- `zstd` 압축으로 용량 추가 감소 (단, 로딩 시간 약간 증가)

### 8.4 클라우드 스토리지
- GitHub Releases 대신 S3/GCS/R2 사용 (더 큰 용량, 더 빠른 다운로드)

---

## 9. 구현 체크리스트

### Phase 1: 캐시 전용 레포지토리 설정
- [ ] 캐시 전용 레포지토리 생성 (`your-org/korea-stock-cache`)
- [ ] Public 또는 Private 설정 결정
- [ ] `scripts/generate_korea_universe_feature_cache.py` 구현 (캐시 레포에)
- [ ] KOSPI+KOSDAQ 전체 유니버스 구성 함수 구현
- [ ] FeatureSpec 기반 병렬 precompute
- [ ] Parquet 저장 및 메타데이터 생성

### Phase 2: 캐시 전용 레포 - Releases 업로드
- [ ] `scripts/upload_feature_cache_to_releases.py` 구현 (캐시 레포에)
- [ ] GitHub API 연동 (또는 `gh` CLI 사용)
- [ ] `.github/workflows/update_feature_cache.yml` 생성 (캐시 레포에)
- [ ] 주간 자동 갱신 스케줄 설정

### Phase 3: 코드 레포지토리 - 캐시 다운로드 및 사용
- [ ] `core2/feature_cache_loader.py` 구현 (코드 레포에)
- [ ] `scripts/download_feature_cache.py` 구현 (코드 레포에)
- [ ] `FeatureFrameDataProvider`에 캐시 로더 통합
- [ ] 백테스트 워크플로우에서 캐시 다운로드/사용

### Phase 4: 테스트 및 검증
- [ ] 로컬에서 캐시 생성/로드 테스트
- [ ] 캐시 전용 레포에서 GitHub Actions 자동 업로드 테스트
- [ ] 코드 레포에서 GitHub Actions 자동 다운로드 테스트
- [ ] 로컬 PC에서 캐시 다운로드/사용 테스트
- [ ] 백테스트 성능 개선 검증

---

## 10. 레포지토리 구조 요약

### 캐시 전용 레포지토리 (`your-org/korea-stock-cache`)
```
korea-stock-cache/
├── .github/
│   └── workflows/
│       └── update_feature_cache.yml  # 주간 자동 갱신
├── scripts/
│   ├── generate_korea_universe_feature_cache.py
│   └── upload_feature_cache_to_releases.py
├── requirements.txt
└── (Releases에 Parquet 파일만 저장)
```

### 코드 레포지토리 (`your-org/capybara_share`)
```
capybara_share/
├── .github/
│   └── workflows/
│       └── backtest_52_weeks_high.yml  # 캐시 다운로드 후 백테스트
├── core2/
│   └── feature_cache_loader.py  # 캐시 로더
├── scripts/
│   └── download_feature_cache.py  # 캐시 다운로드
└── (기존 코드)
```

---

## 11. 참고 자료

- [Pandas Parquet 문서](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html)
- [PyArrow 문서](https://arrow.apache.org/docs/python/)
- [GitHub Releases API](https://docs.github.com/en/rest/releases/releases)
- [softprops/action-gh-release](https://github.com/softprops/action-gh-release)
- [GitHub API - Download Release Asset](https://docs.github.com/en/rest/releases/assets#get-a-release-asset)

---

**작성일**: 2025-01-02  
**작성자**: AI Assistant  
**버전**: 2.0 (별도 레포지토리 구조로 업데이트)

