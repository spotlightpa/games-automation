import time
import re
from datetime import datetime
from dateutil import parser

from modules.first_names import normalize_first_name
from modules.last_names import normalize_last_initial
from modules.logging_utils import log
from helpers.improved_rate_limiting import (
    safe_get_all_values, safe_update_range
)

def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _reformat_entire_column(ws, header_name, transform_fn, *, skip_row_2=True):
    log(f"Starting formatting of '{header_name}' column...")
    
    rows = safe_get_all_values(ws, f"reading {header_name} data for formatting")
    
    if not rows:
        log("Sheet is empty; nothing to reformat.")
        return

    headers = rows[0]
    if header_name not in headers:
        log(f"Column '{header_name}' not found in headers: {headers}")
        return

    col_idx_0 = headers.index(header_name)
    first_data_row = 3 if skip_row_2 else 2
    last_row_idx_1based = len(rows)

    if last_row_idx_1based < first_data_row:
        log(f"No data rows to process for '{header_name}'")
        return

    out_col = []
    changed = 0
    processed = 0
    
    for r_1based in range(first_data_row, last_row_idx_1based + 1):
        row_0 = r_1based - 1
        raw = ""
        if row_0 < len(rows) and col_idx_0 < len(rows[row_0]):
            raw = (rows[row_0][col_idx_0] or "").strip()

        new_val = transform_fn(raw)
        if new_val != raw:
            changed += 1

        out_col.append([new_val])
        processed += 1
        
        # Log progress for large datasets
        if processed % 100 == 0:
            log(f"Processed {processed} rows for '{header_name}' formatting...")

    if not out_col:
        log(f"'{header_name}': no rows to update.")
        return

    col_letter = _col_letter(col_idx_0 + 1)
    range_a1 = f"{col_letter}{first_data_row}:{col_letter}{last_row_idx_1based}"

    safe_update_range(
        ws, 
        range_a1, 
        out_col, 
        value_input_option="USER_ENTERED"
    )
    
    log(f"'{header_name}' formatting completed successfully! {changed} of {processed} rows updated.")

def reformat_first_names(sheet):
    log("Starting first name cleanup...")
    ws = sheet.worksheet("Submissions")
    _reformat_entire_column(ws, "First Name", normalize_first_name)
    log("First name cleanup completed!")

def reformat_last_initials(sheet):
    log("Starting last name initial cleanup...")
    ws = sheet.worksheet("Submissions")
    _reformat_entire_column(ws, "Last Name Initial", normalize_last_initial)
    log("Last name initial cleanup completed!")

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
    log("Starting timestamp formatting...")
    ws = sheet.worksheet("Submissions")
    _reformat_entire_column(ws, "Timestamp", _format_ts_cell)
    log("Timestamp formatting completed successfully!")