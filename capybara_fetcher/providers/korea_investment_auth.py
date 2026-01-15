"""
Korea Investment API authentication and base functionality.
Simplified version based on https://github.com/koreainvestment/open-trading-api
"""
from __future__ import annotations

import json
import time
from datetime import datetime

import requests


class KISAuth:
    """Handle Korea Investment Securities API authentication."""
    
    def __init__(self, appkey: str, appsecret: str, base_url: str = "https://openapi.koreainvestment.com:9443"):
        self.appkey = appkey
        self.appsecret = appsecret
        self.base_url = base_url
        self.token = None
        self.token_expire = None
        
        self.base_headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8",
        }
    
    def authenticate(self) -> str:
        """Get access token from Korea Investment API."""
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
        res = requests.post(url, data=json.dumps(params), headers=headers)
        
        if res.status_code == 200:
            data = res.json()
            self.token = data["access_token"]
            expire_str = data["access_token_token_expired"]
            self.token_expire = datetime.strptime(expire_str, "%Y-%m-%d %H:%M:%S")
            return self.token
        else:
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
    
    def fetch_api(self, api_path: str, tr_id: str, params: dict, max_retries: int = 3, initial_delay: float = 1.0) -> dict:
        """
        Fetch data from Korea Investment API with retry logic for rate limiting.
        
        Args:
            api_path: API endpoint path
            tr_id: Transaction ID
            params: Request parameters
            max_retries: Maximum number of retry attempts (default: 3)
            initial_delay: Initial delay in seconds for exponential backoff (default: 1.0)
        
        Returns:
            dict: API response data
            
        Raises:
            RuntimeError: If all retries are exhausted or non-retryable error occurs
        """
        url = f"{self.base_url}{api_path}"
        
        for attempt in range(max_retries + 1):
            headers = self.get_headers(tr_id)
            headers["tr_cont"] = ""
            
            res = requests.get(url, headers=headers, params=params)
            
            if res.status_code == 200:
                data = res.json()
                if data.get("rt_cd") == "0":
                    return data
                else:
                    # Check if it's a rate limit error
                    msg_cd = data.get('msg_cd', '')
                    if msg_cd == "EGW00201":
                        if attempt < max_retries:
                            # Rate limit error - retry with exponential backoff
                            delay = initial_delay * (2 ** attempt)
                            time.sleep(delay)
                            continue
                        else:
                            # All retries exhausted on rate limit error
                            raise RuntimeError(f"API request failed after {max_retries + 1} attempts (rate limit exceeded): {data.get('msg1')}")
                    else:
                        # Non-rate-limit API error
                        raise RuntimeError(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")
            elif res.status_code == 500:
                # Check if response body contains rate limit error
                try:
                    data = res.json()
                    msg_cd = data.get('msg_cd', '')
                    if msg_cd == "EGW00201":
                        if attempt < max_retries:
                            # Rate limit error - retry with exponential backoff
                            delay = initial_delay * (2 ** attempt)
                            time.sleep(delay)
                            continue
                        else:
                            # All retries exhausted on rate limit error
                            raise RuntimeError(f"API request failed after {max_retries + 1} attempts (rate limit exceeded): {data.get('msg1')}")
                except (json.JSONDecodeError, ValueError):
                    pass
                # For other 500 errors, raise immediately
                raise RuntimeError(f"HTTP error: {res.status_code} {res.text}")
            else:
                raise RuntimeError(f"HTTP error: {res.status_code} {res.text}")
        
        # This should never be reached but included for completeness
        raise RuntimeError(f"API request failed after {max_retries + 1} attempts")
