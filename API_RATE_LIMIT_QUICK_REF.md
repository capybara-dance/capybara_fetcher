# GitHub API Rate Limit - Quick Reference

## 문제
```
403 Client Error: rate limit exceeded for url: https://api.github.com/repos/.../releases/latest
```

## 빠른 해결 방법

### 방법 1: GitHub Token 사용 (가장 효과적)

```bash
# 1. Token 생성: https://github.com/settings/tokens
# 2. 환경 변수 설정
export GITHUB_TOKEN="ghp_your_token_here"

# 3. 스크립트 실행
python play.py
```

**효과**: 시간당 60회 → 5,000회로 증가

### 방법 2: 로컬 캐시 확인

```bash
# 캐시 디렉토리 확인
ls -la ~/.capybara_fetcher/cache/
ls -la cache/

# 캐시가 있으면 API 호출 없이 사용됨
```

### 방법 3: 직접 URL 사용

릴리즈 태그를 알고 있다면:

```python
# API 호출 없이 직접 다운로드
url = "https://github.com/capybara-dance/capybara_fetcher/releases/download/data-20260128-1734/korea_universe_feature_frame.parquet"
df = pd.read_parquet(url)
```

## Rate Limit 확인

```bash
# 현재 상태 확인
curl https://api.github.com/rate_limit

# Token 사용 시
curl -H "Authorization: token YOUR_TOKEN" https://api.github.com/rate_limit
```

## 상세 가이드

전체 해결 방법은 [API_RATE_LIMIT_GUIDE.md](./API_RATE_LIMIT_GUIDE.md)를 참고하세요.

## 요약

| 방법 | 시간당 제한 | 구현 난이도 | 권장도 |
|------|------------|-----------|--------|
| Token 사용 | 5,000회 | 쉬움 | ⭐⭐⭐⭐⭐ |
| 로컬 캐시 | N/A | 중간 | ⭐⭐⭐⭐ |
| 직접 URL | N/A | 쉬움 | ⭐⭐⭐ |
| Retry 로직 | 60회 | 어려움 | ⭐⭐ |
