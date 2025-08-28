import os
import time
import random
from threading import Lock

from gspread import http_client as _http_mod
from gspread.exceptions import APIError

_PATCHED = False

_last_request_ts = 0.0
_lock = Lock()

def _min_interval() -> float:
    return float(os.getenv("SHEETS_MIN_INTERVAL", "1.0"))

def _pre_request_throttle():
    global _last_request_ts
    with _lock:
        now = time.time()
        wait = _min_interval() - (now - _last_request_ts)
        if wait > 0:
            time.sleep(wait)
        _last_request_ts = time.time()

def _parse_retry_after(value: str) -> float:
    if not value:
        return 0.0
    try:
        return float(value.strip())
    except Exception:
        return 0.0

def _should_retry(exc: Exception) -> bool:
    if isinstance(exc, APIError):
        try:
            resp = exc.response
            code = getattr(resp, "status_code", None)
            if code in (429, 500, 502, 503, 504):
                return True
        except Exception:
            pass
        msg = str(exc).lower()
        if "quota exceeded" in msg or "rate limit" in msg or " 429" in msg:
            return True
    return False

def _retry_after_seconds(exc: Exception) -> float:
    if isinstance(exc, APIError):
        resp = getattr(exc, "response", None)
        if resp is not None:
            try:
                ra = resp.headers.get("Retry-After")
                return _parse_retry_after(ra)
            except Exception:
                return 0.0
    return 0.0

class _BackoffHTTPClient(_http_mod.HTTPClient):
    def request(self, method, url, **kwargs):
        max_retries = int(os.getenv("SHEETS_MAX_RETRIES", "7"))
        base = float(os.getenv("SHEETS_BACKOFF_BASE", "0.8"))
        jitter = float(os.getenv("SHEETS_BACKOFF_JITTER", "0.25"))
        cap = float(os.getenv("SHEETS_BACKOFF_CAP", "16"))

        attempt = 0
        while True:
            _pre_request_throttle()
            try:
                return super().request(method, url, **kwargs)
            except Exception as e:
                if attempt >= max_retries or not _should_retry(e):
                    raise
                ra = _retry_after_seconds(e)
                exp = min(cap, base * (2 ** attempt))
                exp = max(0.0, exp + random.uniform(-jitter / 2, jitter / 2))
                time.sleep(max(ra, exp))
                attempt += 1

def install_gspread_backoff():
    global _PATCHED
    if _PATCHED:
        return

    _http_mod.HTTPClient = _BackoffHTTPClient

    BatchHTTPClient = getattr(_http_mod, "BatchHTTPClient", None)
    if BatchHTTPClient is not None:
        class _BackoffBatchHTTPClient(BatchHTTPClient):
            def request(self, method, url, **kwargs):
                max_retries = int(os.getenv("SHEETS_MAX_RETRIES", "7"))
                base = float(os.getenv("SHEETS_BACKOFF_BASE", "0.8"))
                jitter = float(os.getenv("SHEETS_BACKOFF_JITTER", "0.25"))
                cap = float(os.getenv("SHEETS_BACKOFF_CAP", "16"))

                attempt = 0
                while True:
                    _pre_request_throttle()
                    try:
                        return super().request(method, url, **kwargs)
                    except Exception as e:
                        if attempt >= max_retries or not _should_retry(e):
                            raise
                        ra = _retry_after_seconds(e)
                        exp = min(cap, base * (2 ** attempt))
                        exp = max(0.0, exp + random.uniform(-jitter / 2, jitter / 2))
                        time.sleep(max(ra, exp))
                        attempt += 1

        _http_mod.BatchHTTPClient = _BackoffBatchHTTPClient

    _PATCHED = True
