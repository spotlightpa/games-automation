import time
import re
from datetime import datetime
from dateutil import parser

from gspread.exceptions import APIError
from modules.first_names import normalize_first_name
from modules.last_names import normalize_last_initial
from modules.logging_utils import log


def _sleep_backoff(attempt, base=0.8, cap=16.0):
    delay = min(cap, base * (2 ** attempt))
    time.sleep(delay)

def _safe_get_all_values(ws, tries=7):
    from modules.logging_utils import log_quota_issue, log_error_with_fix
    
    for attempt in range(tries):
        try:
            return ws.get_all_values()
        except APIError as e:
            msg = str(e).lower()
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            
            if status_code == 429 or "quota exceeded" in msg or "rate limit" in msg:
                if attempt < tries - 1:
                    # Progressive delays: 30s, 90s, 180s, 300s (5min), 600s (10min)
                    if "per minute" in msg:
                        wait_time = 90 + (attempt * 60)  # 90s, 150s, 210s, 270s, 330s
                    else:
                        wait_time = 30 + (attempt * 30)  # 30s, 60s, 90s, 120s, 150s
                    
                    log_quota_issue("reading spreadsheet", wait_time, attempt + 1, tries)
                    time.sleep(wait_time)
                    continue
                else:
                    log_error_with_fix(
                        f"Google Sheets API quota exhausted after {tries} attempts",
                        "Wait 10-15 minutes before running again, or contact admin to check Google Cloud quotas"
                    )
            elif status_code in (500, 502, 503, 504) and attempt < tries - 1:
                wait_time = min(16.0, 2 ** attempt)
                log_quota_issue("server error recovery", int(wait_time), attempt + 1, tries)
                time.sleep(wait_time)
                continue
            
            raise

def _safe_update_range(ws, range_a1, values, value_input_option="USER_ENTERED", tries=7):
    from modules.logging_utils import log_quota_issue, log_error_with_fix
    
    for attempt in range(tries):
        try:
            return ws.update(range_a1, values, value_input_option=value_input_option)
        except APIError as e:
            msg = str(e).lower()
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            
            if status_code == 429 or "quota exceeded" in msg or "rate limit" in msg:
                if attempt < tries - 1:
                    if "per minute" in msg:
                        wait_time = 120 + (attempt * 60)  # 120s, 180s, 240s, 300s, 360s
                    else:
                        wait_time = 45 + (attempt * 30)   # 45s, 75s, 105s, 135s, 165s
                    
                    log_quota_issue("updating spreadsheet", wait_time, attempt + 1, tries)
                    time.sleep(wait_time)
                    continue
                else:
                    log_error_with_fix(
                        f"Google Sheets write quota exhausted after {tries} attempts",
                        "Wait 15-20 minutes before running again, or process data in smaller batches"
                    )
            elif status_code in (500, 502, 503, 504) and attempt < tries - 1:
                wait_time = min(16.0, 2 ** attempt)
                log_quota_issue("server error recovery", int(wait_time), attempt + 1, tries)
                time.sleep(wait_time)
                continue
            
            raise

def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _reformat_entire_column(ws, header_name, transform_fn, *, skip_row_2=True):
    """
    Reads the sheet once, transforms a single column, and writes that column back
    in one range update. Retries with backoff on 429/5xx.

    transform_fn(raw_value) -> new_value
    """
    rows = _safe_get_all_values(ws)
    if not rows:
        log("ℹ️ Sheet is empty; nothing to reformat.")
        return

    headers = rows[0]
    if header_name not in headers:
        log(f"❌ Column '{header_name}' not found.")
        return

    col_idx_0 = headers.index(header_name)
    first_data_row = 3 if skip_row_2 else 2
    last_row_idx_1based = len(rows)

    out_col = []
    changed = 0
    for r_1based in range(first_data_row, last_row_idx_1based + 1):
        row_0 = r_1based - 1
        raw = ""
        if row_0 < len(rows) and col_idx_0 < len(rows[row_0]):
            raw = (rows[row_0][col_idx_0] or "").strip()

        new_val = transform_fn(raw)
        if new_val != raw:
            changed += 1

        out_col.append([new_val])

    if not out_col:
        log(f"ℹ️ '{header_name}': no rows to update.")
        return

    col_letter = _col_letter(col_idx_0 + 1)
    range_a1 = f"{col_letter}{first_data_row}:{col_letter}{last_row_idx_1based}"

    _safe_update_range(ws, range_a1, out_col, value_input_option="USER_ENTERED")
    log(f"🎉 '{header_name}' cleanup complete. {changed} rows updated (single write).")


def reformat_first_names(sheet):
    log("✏️ Starting first name cleanup in Submissions tab...")
    ws = sheet.worksheet("Submissions")
    _reformat_entire_column(ws, "First Name", normalize_first_name)


def reformat_last_initials(sheet):
    log("✏️ Starting Last Name Initial cleanup...")
    ws = sheet.worksheet("Submissions")
    _reformat_entire_column(ws, "Last Name Initial", normalize_last_initial)


def _format_ts_cell(raw_ts: str) -> str:
    """
    Normalize to 'MM/DD/YYYY HH:MM AM/PM' or pass through if unparseable or instructional.
    """
    if not raw_ts:
        return ""
    if "(Autopopulated)" in raw_ts or "(Required)" in raw_ts:
        return raw_ts
    try:
        dt = parser.parse(raw_ts)
        s = dt.strftime("%m/%d/%Y %I:%M %p")
        return s
    except Exception:
        return raw_ts


def reformat_submission_timestamps(sheet):
    """
    Normalize Submissions!Timestamp to 'MM/DD/YYYY HH:MM AM/PM'.
    Single read, single write, with backoff.
    """
    log("🕒 Starting timestamp formatting for Submissions tab...")
    ws = sheet.worksheet("Submissions")
    _reformat_entire_column(ws, "Timestamp", _format_ts_cell)
    log("🎉 Timestamp formatting complete.")
