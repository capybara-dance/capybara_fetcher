# GitHub API Rate Limit 해결 가이드

## 문제 상황

`play.py` 또는 다른 스크립트에서 GitHub Releases를 통해 데이터를 다운로드할 때 다음과 같은 에러가 간헐적으로 발생할 수 있습니다:

```
Error: Failed to fetch latest release: 403 Client Error: rate limit exceeded 
for url: https://api.github.com/repos/capybara-dance/capybara_fetcher/releases/latest
```

## 원인

GitHub API는 인증 없이 사용할 경우 **시간당 60회**로 요청 횟수가 제한됩니다. 
여러 사용자가 같은 IP에서 접근하거나, 스크립트가 자주 실행되면 이 제한에 도달하게 됩니다.

## 해결 방법

### 1. GitHub Personal Access Token 사용 (권장)

Personal Access Token을 사용하면 **시간당 5,000회**로 제한이 대폭 증가합니다.

#### 1.1 Token 생성

1. GitHub에 로그인 후 [Settings > Developer settings > Personal access tokens > Tokens (classic)](https://github.com/settings/tokens)으로 이동
2. "Generate new token" > "Generate new token (classic)" 클릭
3. Token 이름 입력 (예: "capybara_fetcher_api")
4. **Expiration**: 원하는 만료 기간 선택 (예: 90 days)
5. **Scopes**: Public 레포지토리만 접근한다면 아무 scope도 선택하지 않아도 됩니다
   - Private 레포지토리 접근이 필요하면 `repo` scope 선택
6. "Generate token" 클릭
7. **생성된 토큰을 안전한 곳에 복사** (다시 볼 수 없습니다!)

#### 1.2 Token 사용 방법

**환경 변수로 설정 (권장):**

```bash
# Linux/Mac
export GITHUB_TOKEN="your_token_here"

# Windows (CMD)
set GITHUB_TOKEN=your_token_here

# Windows (PowerShell)
$env:GITHUB_TOKEN="your_token_here"
```

**Python 코드에서 사용:**

```python
import os
import requests

# 환경 변수에서 토큰 읽기
github_token = os.environ.get('GITHUB_TOKEN')

headers = {}
if github_token:
    headers['Authorization'] = f'token {github_token}'

# GitHub API 요청
url = "https://api.github.com/repos/capybara-dance/capybara_fetcher/releases/latest"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    release = response.json()
    print(f"Latest release: {release['tag_name']}")
elif response.status_code == 403:
    print("Rate limit exceeded!")
    # 남은 제한 확인
    if 'X-RateLimit-Remaining' in response.headers:
        print(f"Remaining: {response.headers['X-RateLimit-Remaining']}")
        print(f"Reset at: {response.headers['X-RateLimit-Reset']}")
else:
    print(f"Error: {response.status_code}")
```

### 2. 캐싱 전략

API 호출을 최소화하기 위해 캐싱을 적극 활용하세요.

#### 2.1 로컬 캐시 활용

```python
import os
import json
import time
from pathlib import Path

CACHE_DIR = Path.home() / ".capybara_fetcher" / "cache"
CACHE_DURATION = 3600  # 1시간 (초 단위)

def get_latest_release_cached():
    """캐시된 릴리즈 정보를 반환하거나 새로 가져옴"""
    cache_file = CACHE_DIR / "latest_release.json"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    # 캐시 파일이 있고 유효한 경우
    if cache_file.exists():
        cache_age = time.time() - cache_file.stat().st_mtime
        if cache_age < CACHE_DURATION:
            with open(cache_file, 'r') as f:
                return json.load(f)
    
    # 캐시가 없거나 만료된 경우 API 호출
    try:
        release = fetch_latest_release()  # API 호출 함수
        
        # 캐시에 저장
        with open(cache_file, 'w') as f:
            json.dump(release, f)
        
        return release
    except Exception as e:
        # API 호출 실패 시 만료된 캐시라도 사용
        if cache_file.exists():
            print(f"Using expired cache due to error: {e}")
            with open(cache_file, 'r') as f:
                return json.load(f)
        raise
```

#### 2.2 조건부 요청 (If-None-Match)

ETag를 사용하여 변경되지 않은 경우 전송량을 절약할 수 있습니다:

```python
def fetch_with_etag(url, etag=None):
    headers = {}
    if etag:
        headers['If-None-Match'] = etag
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 304:
        # 변경 없음 - 캐시 사용
        return None, etag
    elif response.status_code == 200:
        # 새 데이터
        new_etag = response.headers.get('ETag')
        return response.json(), new_etag
    else:
        response.raise_for_status()
```

### 3. Retry with Exponential Backoff

Rate limit에 도달했을 때 재시도 로직을 구현하세요:

```python
import os
import time
import requests
from datetime import datetime

def fetch_with_retry(url, max_retries=3, initial_delay=1):
    """지수 백오프를 사용한 재시도 로직"""
    headers = {}
    github_token = os.environ.get('GITHUB_TOKEN')
    if github_token:
        headers['Authorization'] = f'token {github_token}'
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 403:
                # Rate limit 확인
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining = int(response.headers['X-RateLimit-Remaining'])
                    
                    if remaining == 0:
                        # Rate limit 리셋 시간까지 대기
                        reset_time = int(response.headers['X-RateLimit-Reset'])
                        wait_time = reset_time - time.time()
                        
                        if wait_time > 0 and attempt < max_retries - 1:
                            print(f"Rate limit exceeded. Waiting {wait_time:.0f}s until reset...")
                            time.sleep(wait_time + 1)
                            continue
                
                raise Exception(f"Rate limit exceeded: {response.text}")
            
            else:
                response.raise_for_status()
        
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)  # 지수 백오프
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise
    
    raise Exception(f"Failed after {max_retries} attempts")
```

### 4. 직접 다운로드 URL 사용

릴리즈 정보를 미리 알고 있다면, GitHub API를 거치지 않고 직접 파일을 다운로드할 수 있습니다:

```python
# API를 사용하는 대신 (rate limit 소비):
# https://api.github.com/repos/capybara-dance/capybara_fetcher/releases/latest

# 직접 다운로드 URL 사용 (rate limit 소비 안함):
download_url = "https://github.com/capybara-dance/capybara_fetcher/releases/download/data-20260128-1734/korea_universe_feature_frame.parquet"

# 단, 최신 릴리즈 태그를 알아야 함
# 태그 정보는 로컬에 캐싱하거나, 설정 파일로 관리
```

### 5. 로컬 데이터 우선 사용

데이터를 이미 다운로드한 경우, 로컬 파일을 우선 사용하도록 구현:

```python
from pathlib import Path

def get_feature_data(force_download=False):
    """로컬 캐시를 우선 사용하고, 없을 때만 다운로드"""
    local_path = Path("cache/korea_universe_feature_frame.parquet")
    
    # 로컬 파일이 있고 강제 다운로드가 아닌 경우
    if local_path.exists() and not force_download:
        # 파일 생성 시간 확인 (옵션)
        age_hours = (time.time() - local_path.stat().st_mtime) / 3600
        
        # 24시간 이내면 로컬 파일 사용
        if age_hours < 24:
            print(f"Using local cache (age: {age_hours:.1f}h)")
            return pd.read_parquet(local_path)
    
    # 새로 다운로드
    print("Downloading latest release...")
    try:
        release = get_latest_release_cached()  # 캐싱된 API 호출
        download_url = next(
            asset['browser_download_url'] 
            for asset in release['assets'] 
            if asset['name'] == 'korea_universe_feature_frame.parquet'
        )
        
        df = pd.read_parquet(download_url)
        
        # 로컬에 저장
        local_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(local_path)
        
        return df
    except Exception as e:
        # 다운로드 실패 시 오래된 로컬 파일이라도 사용
        if local_path.exists():
            print(f"Download failed, using old cache: {e}")
            return pd.read_parquet(local_path)
        raise
```

## 종합 예제: 견고한 릴리즈 가져오기

```python
import os
import json
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

class ReleaseDataFetcher:
    """GitHub API rate limit을 고려한 릴리즈 데이터 가져오기"""
    
    def __init__(self, cache_dir=None):
        self.cache_dir = Path(cache_dir or Path.home() / ".capybara_fetcher" / "cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.repo = "capybara-dance/capybara_fetcher"
        
        # GitHub token 확인
        self.token = os.environ.get('GITHUB_TOKEN')
        if not self.token:
            print("Warning: GITHUB_TOKEN not set. Rate limit will be restricted to 60/hour.")
    
    def get_headers(self):
        """API 요청 헤더 생성"""
        headers = {}
        if self.token:
            headers['Authorization'] = f'token {self.token}'
        return headers
    
    def check_rate_limit(self):
        """현재 rate limit 상태 확인"""
        url = "https://api.github.com/rate_limit"
        response = requests.get(url, headers=self.get_headers())
        
        if response.status_code == 200:
            data = response.json()
            core = data['rate']
            remaining = core['remaining']
            limit = core['limit']
            reset_time = datetime.fromtimestamp(core['reset'])
            
            print(f"Rate limit: {remaining}/{limit}")
            print(f"Reset at: {reset_time.strftime('%Y-%m-%d %H:%M:%S')}")
            return remaining, reset_time
        
        return None, None
    
    def get_latest_release(self, use_cache=True, cache_duration=3600):
        """최신 릴리즈 정보 가져오기 (캐싱 지원)"""
        cache_file = self.cache_dir / "latest_release.json"
        
        # 캐시 확인
        if use_cache and cache_file.exists():
            cache_age = time.time() - cache_file.stat().st_mtime
            if cache_age < cache_duration:
                print(f"Using cached release info (age: {cache_age/60:.1f}min)")
                with open(cache_file, 'r') as f:
                    return json.load(f)
        
        # API 호출
        url = f"https://api.github.com/repos/{self.repo}/releases/latest"
        
        try:
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            
            if response.status_code == 200:
                release = response.json()
                
                # 캐시에 저장
                with open(cache_file, 'w') as f:
                    json.dump(release, f, indent=2)
                
                return release
            
            elif response.status_code == 403:
                # Rate limit 에러 처리
                remaining = response.headers.get('X-RateLimit-Remaining', '?')
                reset_time = response.headers.get('X-RateLimit-Reset', '?')
                
                if remaining == '0':
                    reset_dt = datetime.fromtimestamp(int(reset_time))
                    raise Exception(
                        f"GitHub API rate limit exceeded. "
                        f"Reset at: {reset_dt.strftime('%Y-%m-%d %H:%M:%S')}. "
                        f"Consider setting GITHUB_TOKEN environment variable."
                    )
                else:
                    raise Exception(f"API error 403: {response.text}")
            
            else:
                response.raise_for_status()
        
        except Exception as e:
            # 에러 발생 시 만료된 캐시라도 사용
            if cache_file.exists():
                print(f"API call failed: {e}")
                print(f"Using expired cache from {cache_file}")
                with open(cache_file, 'r') as f:
                    return json.load(f)
            raise RuntimeError(f"Failed to fetch release and no cache available: {e}")
    
    def download_parquet(self, asset_name, force=False):
        """Parquet 파일 다운로드 (로컬 캐시 우선)"""
        local_path = self.cache_dir / asset_name
        
        # 로컬 파일 확인
        if not force and local_path.exists():
            age_hours = (time.time() - local_path.stat().st_mtime) / 3600
            if age_hours < 24:
                print(f"Using local {asset_name} (age: {age_hours:.1f}h)")
                return pd.read_parquet(local_path)
        
        # 최신 릴리즈 정보 가져오기
        print("Fetching latest release info...")
        release = self.get_latest_release()
        
        # 다운로드 URL 찾기
        download_url = None
        for asset in release.get('assets', []):
            if asset['name'] == asset_name:
                download_url = asset['browser_download_url']
                break
        
        if not download_url:
            raise ValueError(f"Asset '{asset_name}' not found in release {release['tag_name']}")
        
        print(f"Downloading {asset_name} from {release['tag_name']}...")
        
        # 다운로드 (browser_download_url은 rate limit 적용 안됨)
        df = pd.read_parquet(download_url)
        
        # 로컬에 저장
        df.to_parquet(local_path)
        print(f"Saved to {local_path}")
        
        return df

# 사용 예제
if __name__ == "__main__":
    fetcher = ReleaseDataFetcher()
    
    # Rate limit 상태 확인
    fetcher.check_rate_limit()
    
    # 데이터 가져오기
    try:
        df = fetcher.download_parquet("korea_universe_feature_frame.parquet")
        print(f"Loaded dataframe with shape: {df.shape}")
    except Exception as e:
        print(f"Error: {e}")
```

## 추가 팁

1. **CI/CD 환경**: GitHub Actions 등에서는 `${{ secrets.GITHUB_TOKEN }}`을 자동으로 사용할 수 있습니다.

2. **Rate limit 모니터링**: 주기적으로 rate limit 상태를 확인하고 로그를 남기세요.

3. **다중 토큰 사용**: 여러 사용자가 있다면 각자 토큰을 발급받아 사용하세요.

4. **GraphQL API 사용**: REST API 대신 GraphQL API를 사용하면 더 효율적으로 데이터를 가져올 수 있습니다.

5. **최신 태그를 설정 파일로 관리**: 자주 변하지 않는다면 최신 릴리즈 태그를 설정 파일에 하드코딩하고, 주기적으로만 업데이트하세요.

## 참고 자료

- [GitHub API Rate Limiting](https://docs.github.com/en/rest/overview/resources-in-the-rest-api#rate-limiting)
- [Creating a personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
- [Best practices for integrators](https://docs.github.com/en/rest/guides/best-practices-for-integrators)
