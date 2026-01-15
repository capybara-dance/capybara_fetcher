"""
Korea Investment API authentication and base functionality.
Simplified version based on https://github.com/koreainvestment/open-trading-api
"""
from __future__ import annotations

import json
from datetime import datetime
import random
import threading
import time

import requests


class _RateLimiter:
    """
    Thread-safe minimum-interval rate limiter.

    Ensures consecutive requests are spaced by at least `min_interval_seconds`
    across all threads sharing the instance.
    """

    def __init__(self, *, min_interval_seconds: float):
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be >= 0")
        self._min_interval_seconds = float(min_interval_seconds)
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0

    def wait(self) -> None:
        if self._min_interval_seconds <= 0:
            return
        sleep_for = 0.0
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed_at:
                sleep_for = self._next_allowed_at - now
            # Reserve the next slot (based on current schedule, not wall time)
            base = self._next_allowed_at if self._next_allowed_at > now else now
            self._next_allowed_at = base + self._min_interval_seconds
        if sleep_for > 0:
            time.sleep(sleep_for)


def _json_or_none(res: requests.Response) -> dict | None:
    try:
        return res.json()
    except Exception:
        return None


def _is_kis_rate_limit_error(*, http_status: int, json_data: dict | None, text: str) -> bool:
    # KIS sometimes returns HTTP 500 with JSON body:
    # {"rt_cd":"1","msg_cd":"EGW00201","msg1":"초당 거래건수를 초과하였습니다."}
    msg_cd = (json_data or {}).get("msg_cd")
    msg1 = (json_data or {}).get("msg1")
    if msg_cd == "EGW00201":
        return True
    combined = f"{msg1 or ''} {text or ''}"
    return "초당 거래건수" in combined or "거래건수를 초과" in combined


class KISAuth:
    """Handle Korea Investment Securities API authentication."""
    
    def __init__(
        self,
        appkey: str,
        appsecret: str,
        base_url: str = "https://openapi.koreainvestment.com:9443",
        *,
        max_requests_per_second: float = 5.0,
        max_retries: int = 8,
        backoff_base_seconds: float = 0.5,
        backoff_max_seconds: float = 10.0,
    ):
        self.appkey = appkey
        self.appsecret = appsecret
        self.base_url = base_url
        self.token = None
        self.token_expire = None
        self._token_lock = threading.Lock()
        self._session = requests.Session()

        # Conservative defaults; KIS returns EGW00201 when per-second limit is exceeded.
        rps = float(max_requests_per_second)
        min_interval = 0.0 if rps <= 0 else (1.0 / rps)
        self._rate_limiter = _RateLimiter(min_interval_seconds=min_interval)

        self._max_retries = int(max_retries)
        self._backoff_base_seconds = float(backoff_base_seconds)
        self._backoff_max_seconds = float(backoff_max_seconds)
        
        self.base_headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8",
        }
    
    def authenticate(self) -> str:
        """Get access token from Korea Investment API."""
        with self._token_lock:
            # Check if existing token is still valid
            if self.token and self.token_expire:
                now = datetime.now()
                if self.token_expire > now:
                    return self.token

            # Request new token
            url = f"{self.base_url}/oauth2/tokenP"
            params = {
                "grant_type": "client_credentials",
                "appkey": self.appkey,
                "appsecret": self.appsecret,
            }

            headers = self.base_headers.copy()
            self._rate_limiter.wait()
            res = self._session.post(url, data=json.dumps(params), headers=headers)

            if res.status_code == 200:
                data = res.json()
                self.token = data["access_token"]
                expire_str = data["access_token_token_expired"]
                self.token_expire = datetime.strptime(expire_str, "%Y-%m-%d %H:%M:%S")
                return self.token
            raise RuntimeError(f"Authentication failed: {res.status_code} {res.text}")
    
    def get_headers(self, tr_id: str) -> dict:
        """Get request headers with authentication."""
        token = self.authenticate()
        headers = self.base_headers.copy()
        headers["authorization"] = f"Bearer {token}"
        headers["appkey"] = self.appkey
        headers["appsecret"] = self.appsecret
        headers["tr_id"] = tr_id
        headers["custtype"] = "P"
        return headers
    
    def fetch_api(self, api_path: str, tr_id: str, params: dict) -> dict:
        """Fetch data from Korea Investment API."""
        url = f"{self.base_url}{api_path}"
        last_err: Exception | None = None
        for attempt in range(self._max_retries + 1):
            headers = self.get_headers(tr_id)
            headers["tr_cont"] = ""

            try:
                self._rate_limiter.wait()
                res = self._session.get(url, headers=headers, params=params)
            except requests.RequestException as e:
                last_err = e
                # transient network error -> retry with backoff
                if attempt >= self._max_retries:
                    break
                delay = min(self._backoff_max_seconds, self._backoff_base_seconds * (2**attempt))
                delay = delay * (0.8 + 0.4 * random.random())
                time.sleep(delay)
                continue

            data = _json_or_none(res)
            if res.status_code == 200:
                if isinstance(data, dict) and data.get("rt_cd") == "0":
                    return data
                # 200 but API-level error
                if _is_kis_rate_limit_error(http_status=res.status_code, json_data=data, text=res.text):
                    if attempt >= self._max_retries:
                        last_err = RuntimeError(f"HTTP/API rate limit: {res.status_code} {res.text}")
                        break
                    delay = min(self._backoff_max_seconds, self._backoff_base_seconds * (2**attempt))
                    delay = delay * (0.8 + 0.4 * random.random())
                    time.sleep(delay)
                    continue
                msg_cd = (data or {}).get("msg_cd")
                msg1 = (data or {}).get("msg1")
                raise RuntimeError(f"API error: {msg_cd} - {msg1}")

            # Non-200 HTTP
            if _is_kis_rate_limit_error(http_status=res.status_code, json_data=data, text=res.text):
                if attempt >= self._max_retries:
                    last_err = RuntimeError(f"HTTP rate limit: {res.status_code} {res.text}")
                    break
                delay = min(self._backoff_max_seconds, self._backoff_base_seconds * (2**attempt))
                delay = delay * (0.8 + 0.4 * random.random())
                time.sleep(delay)
                continue

            raise RuntimeError(f"HTTP error: {res.status_code} {res.text}")

        raise RuntimeError(f"KIS API request failed after retries: {last_err}") from last_err
