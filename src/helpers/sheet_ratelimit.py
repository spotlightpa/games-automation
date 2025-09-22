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
    # 6 seconds minimum between any API calls
    return float(os.getenv("SHEETS_MIN_INTERVAL", "6.0"))

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
    # Always retry API errors - we never give up
    if isinstance(exc, APIError):
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

class _NeverFailHTTPClient(_http_mod.HTTPClient):
    def request(self, method, url, **kwargs):
        base = float(os.getenv("SHEETS_BACKOFF_BASE", "10.0"))   # Longer base delay
        jitter = float(os.getenv("SHEETS_BACKOFF_JITTER", "2.0")) # More jitter
        max_delay = float(os.getenv("SHEETS_BACKOFF_CAP", "1800"))  # 30 minute max

        attempt = 0
        while True:  # Infinite retry loop
            _pre_request_throttle()
            try:
                return super().request(method, url, **kwargs)
            except Exception as e:
                attempt += 1
                
                if not _should_retry(e):
                    # Even for non-API errors, wait and try again
                    wait_time = min(max_delay, 30 * attempt)
                    print(f"Non-API error (attempt {attempt}), waiting {wait_time}s: {e}")
                    time.sleep(wait_time)
                    continue
                
                # Calculate wait time based on error type
                ra = _retry_after_seconds(e)
                error_msg = str(e).lower()
                
                if "quota exceeded" in error_msg or "429" in str(e):
                    # For quota errors, use very long delays
                    if "per minute" in error_msg:
                        wait_time = min(max_delay, 120 + (attempt * 60))  # 2min, 3min, 4min
                    elif "per day" in error_msg:
                        wait_time = max_delay  # Full 30 minutes
                    else:
                        wait_time = min(max_delay, 60 + (attempt * 30))   # 1min, 1.5min, 2min
                else:
                    # For other errors, use exponential backoff
                    exp = min(max_delay, base * (2 ** min(attempt - 1, 10)))
                    wait_time = max(ra, exp + random.uniform(-jitter / 2, jitter / 2))
                
                print(f"API error (attempt {attempt}), waiting {wait_time:.1f}s: {e}")
                
                # Break long waits into chunks so we can show progress
                total_wait = wait_time
                while total_wait > 0:
                    chunk = min(30, total_wait)
                    time.sleep(chunk)
                    total_wait -= chunk
                    if total_wait > 30:
                        print(f"   Still waiting... {total_wait:.0f}s remaining")

def install_gspread_backoff():
    global _PATCHED
    if _PATCHED:
        return

    _http_mod.HTTPClient = _NeverFailHTTPClient

    # Also patch BatchHTTPClient if it exists
    BatchHTTPClient = getattr(_http_mod, "BatchHTTPClient", None)
    if BatchHTTPClient is not None:
        class _NeverFailBatchHTTPClient(BatchHTTPClient):
            def request(self, method, url, **kwargs):
                base = float(os.getenv("SHEETS_BACKOFF_BASE", "10.0"))
                jitter = float(os.getenv("SHEETS_BACKOFF_JITTER", "2.0"))
                max_delay = float(os.getenv("SHEETS_BACKOFF_CAP", "1800"))

                attempt = 0
                while True:  # Infinite retry loop
                    _pre_request_throttle()
                    try:
                        return super().request(method, url, **kwargs)
                    except Exception as e:
                        attempt += 1
                        
                        ra = _retry_after_seconds(e)
                        error_msg = str(e).lower()
                        
                        if "quota exceeded" in error_msg or "429" in str(e):
                            if "per minute" in error_msg:
                                wait_time = min(max_delay, 120 + (attempt * 60))
                            elif "per day" in error_msg:
                                wait_time = max_delay
                            else:
                                wait_time = min(max_delay, 60 + (attempt * 30))
                        else:
                            exp = min(max_delay, base * (2 ** min(attempt - 1, 10)))
                            wait_time = max(ra, exp + random.uniform(-jitter / 2, jitter / 2))
                        
                        print(f"Batch API error (attempt {attempt}), waiting {wait_time:.1f}s: {e}")
                        
                        total_wait = wait_time
                        while total_wait > 0:
                            chunk = min(30, total_wait)
                            time.sleep(chunk)
                            total_wait -= chunk
                            if total_wait > 30:
                                print(f"   Still waiting... {total_wait:.0f}s remaining")

        _http_mod.BatchHTTPClient = _NeverFailBatchHTTPClient

    _PATCHED = True