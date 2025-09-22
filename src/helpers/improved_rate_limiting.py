import time
import os
from threading import Lock
from functools import wraps
from gspread.exceptions import APIError
from modules.logging_utils import log

# Global rate limiting
_GLOBAL_LOCK = Lock()
_LAST_API_CALL = 0.0
_API_CALL_COUNT = 0
_CALL_WINDOW_START = 0.0

def global_rate_limit(min_interval=5.0, calls_per_minute=15):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _LAST_API_CALL, _API_CALL_COUNT, _CALL_WINDOW_START
            
            with _GLOBAL_LOCK:
                now = time.time()
                
                # Reset call count every minute
                if now - _CALL_WINDOW_START >= 60:
                    _API_CALL_COUNT = 0
                    _CALL_WINDOW_START = now
                
                # If we're at limit, wait for the full minute to reset
                if _API_CALL_COUNT >= calls_per_minute:
                    wait_time = 60 - (now - _CALL_WINDOW_START) + 5  # +5 second buffer
                    if wait_time > 0:
                        log(f"Rate limit reached, waiting {wait_time:.1f} seconds for reset...")
                        time.sleep(wait_time)
                        _API_CALL_COUNT = 0
                        _CALL_WINDOW_START = time.time()
                
                # Minimum interval between calls
                since_last = now - _LAST_API_CALL
                if since_last < min_interval:
                    wait = min_interval - since_last
                    time.sleep(wait)
                
                _LAST_API_CALL = time.time()
                _API_CALL_COUNT += 1
                
            return func(*args, **kwargs)
        return wrapper
    return decorator

def never_fail_api_call(func, operation_name="API operation"):
    attempt = 0
    base_delay = 10.0
    max_delay = 1800  # 30 minutes max delay between retries
    
    while True:
        attempt += 1
        try:
            return func()
        except APIError as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            error_msg = str(e).lower()
            
            # Handle quota exceeded errors
            if status_code == 429 or "quota exceeded" in error_msg or "rate limit" in error_msg:
                # Progressive delays: 1min, 2min, 5min, 10min, 15min, 30min, then stay at 30min
                if "per minute" in error_msg:
                    wait_time = min(max_delay, 60 + (attempt * 60))  # 1, 2, 3... up to 30 min
                elif "per day" in error_msg:
                    wait_time = max_delay  # Wait 30 minutes for daily quota
                else:
                    wait_time = min(max_delay, base_delay * (2 ** min(attempt - 1, 8)))
                
                log(f"{operation_name} quota exceeded (attempt {attempt}). Waiting {wait_time//60:.0f}m {wait_time%60:.0f}s...")
                
                # Break long waits into 30-second chunks so we can log progress
                total_wait = wait_time
                while total_wait > 0:
                    chunk = min(30, total_wait)
                    time.sleep(chunk)
                    total_wait -= chunk
                    if total_wait > 30:  # Only log if significant time remaining
                        log(f"Still waiting... {total_wait//60:.0f}m {total_wait%60:.0f}s remaining for {operation_name}")
                
                continue
                
            # Handle server errors
            elif status_code in (500, 502, 503, 504):
                wait_time = min(60, 5 * attempt)  # Up to 1 minute for server errors
                log(f"{operation_name} server error (attempt {attempt}), waiting {wait_time}s: {e}")
                time.sleep(wait_time)
                continue
            else:
                # Unknown API error
                wait_time = min(120, 10 * attempt)  # Up to 2 minutes for unknown errors
                log(f"{operation_name} unknown error (attempt {attempt}), waiting {wait_time}s: {e}")
                time.sleep(wait_time)
                continue
                
        except Exception as e:
            # Non-API errors
            wait_time = min(60, 5 * attempt)
            log(f"{operation_name} unexpected error (attempt {attempt}), waiting {wait_time}s: {e}")
            time.sleep(wait_time)
            continue

# Ultra-safe wrapper functions
@global_rate_limit(min_interval=6.0, calls_per_minute=12)
def safe_get_all_values(worksheet, operation_name="reading data"):
    return never_fail_api_call(
        lambda: worksheet.get_all_values(),
        f"{operation_name} from {worksheet.title}"
    )

@global_rate_limit(min_interval=8.0, calls_per_minute=10)
def safe_update_range(worksheet, range_name, values, value_input_option="USER_ENTERED"):
    return never_fail_api_call(
        lambda: worksheet.update(range_name, values, value_input_option=value_input_option),
        f"updating range {range_name} in {worksheet.title}"
    )

@global_rate_limit(min_interval=10.0, calls_per_minute=8)
def safe_batch_update(worksheet, data, value_input_option="USER_ENTERED"):
    return never_fail_api_call(
        lambda: worksheet.batch_update(data, value_input_option=value_input_option),
        f"batch updating {len(data)} ranges in {worksheet.title}"
    )

@global_rate_limit(min_interval=8.0, calls_per_minute=10)
def safe_append_rows(worksheet, values, value_input_option="USER_ENTERED"):
    return never_fail_api_call(
        lambda: worksheet.append_rows(values, value_input_option=value_input_option),
        f"appending {len(values)} rows to {worksheet.title}"
    )

@global_rate_limit(min_interval=5.0, calls_per_minute=15)
def safe_get_worksheet(spreadsheet, name):
    return never_fail_api_call(
        lambda: spreadsheet.worksheet(name),
        f"accessing worksheet {name}"
    )

@global_rate_limit(min_interval=5.0, calls_per_minute=15)
def safe_row_values(worksheet, row_num):
    return never_fail_api_call(
        lambda: worksheet.row_values(row_num),
        f"reading row {row_num} from {worksheet.title}"
    )

@global_rate_limit(min_interval=6.0, calls_per_minute=12)
def safe_get_range(worksheet, range_name):
    return never_fail_api_call(
        lambda: worksheet.get(range_name),
        f"reading range {range_name} from {worksheet.title}"
    )

@global_rate_limit(min_interval=8.0, calls_per_minute=10)
def safe_update_cell(worksheet, row, col, value):
    return never_fail_api_call(
        lambda: worksheet.update_cell(row, col, value),
        f"updating cell ({row},{col}) in {worksheet.title}"
    )

def log_api_stats():
    global _API_CALL_COUNT, _CALL_WINDOW_START
    with _GLOBAL_LOCK:
        elapsed = time.time() - _CALL_WINDOW_START
        if elapsed > 0:
            rate = _API_CALL_COUNT / (elapsed / 60)
            log(f"API stats: {_API_CALL_COUNT} calls in {elapsed:.1f}s ({rate:.1f} calls/minute)")

def wait_for_quota_reset():
    log("Waiting 2 minutes for quota reset...")
    for i in range(120):
        time.sleep(1)
        if i % 30 == 29:  # Log every 30 seconds
            remaining = 120 - i - 1
            log(f"Quota reset wait: {remaining} seconds remaining...")