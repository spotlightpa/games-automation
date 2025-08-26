from dateutil import parser
from modules.first_names import normalize_first_name
from modules.last_names import normalize_last_initial
from modules.logging_utils import log


def reformat_column_with_function(ws, header_name, normalize_fn):
    rows = ws.get_all_values()
    headers = rows[0]

    if header_name not in headers:
        log(f"❌ Column '{header_name}' not found.")
        return

    col_idx = headers.index(header_name) + 1  # Sheets API is 1-based
    updated_count = 0

    for i in range(2, len(rows)):
        row = rows[i]
        if len(row) < col_idx:
            continue

        raw_value = row[col_idx - 1].strip()
        normalized = normalize_fn(raw_value)

        if raw_value != normalized:
            ws.update_cell(i + 1, col_idx, normalized)
            log(f"✅ Row {i+1}: '{raw_value}' → '{normalized}'")
            updated_count += 1

    log(f"🎉 '{header_name}' cleanup complete. {updated_count} rows updated.")


def reformat_first_names(sheet):
    log("✏️ Starting first name cleanup in Submissions tab...")
    ws = sheet.worksheet("Submissions")
    rows = ws.get_all_values()
    headers = rows[0]

    if "First Name" not in headers:
        log("❌ 'First Name' column not found.")
        return

    col_idx = headers.index("First Name") + 1
    updated_count = 0

    for i in range(2, len(rows)):
        row = rows[i]
        if len(row) < col_idx:
            continue

        raw_name = row[col_idx - 1].strip()
        normalized = normalize_first_name(raw_name)

        if raw_name != normalized:
            ws.update_cell(i + 1, col_idx, normalized)
            log(f"✅ Row {i+1}: '{raw_name}' → '{normalized}'")
            updated_count += 1

    log(f"🎉 First name cleanup complete. {updated_count} rows updated.")


def reformat_last_initials(sheet):
    log("✏️ Starting Last Name Initial cleanup...")
    ws = sheet.worksheet("Submissions")
    reformat_column_with_function(ws, "Last Name Initial", normalize_last_initial)


def reformat_submission_timestamps(sheet):
    """
    Normalize Submissions!Timestamp to 'MM/DD/YYYY HH:MM AM/PM'.

    - Skips row 2 (the template/instructions row).
    - Skips any cells that include '(Autopopulated)' or '(Required)'.
    """
    log("🕒 Starting timestamp formatting for Submissions tab...")

    ws = sheet.worksheet("Submissions")
    rows = ws.get_all_values()
    headers = rows[0]

    if "Timestamp" not in headers:
        log("❌ 'Timestamp' column not found in Submissions.")
        return

    timestamp_col_idx = headers.index("Timestamp")

    # Start at sheet row 3 → rows[2] to avoid the template/instructions row
    for sheet_row_num, row in enumerate(rows[2:], start=3):
        if len(row) <= timestamp_col_idx:
            continue

        raw_ts = row[timestamp_col_idx].strip()
        if not raw_ts:
            continue

        # Ignore instruction/template text
        if "(Autopopulated)" in raw_ts or "(Required)" in raw_ts:
            log(f"⏭️ Row {sheet_row_num}: Skipping instructional timestamp cell.")
            continue

        try:
            dt = parser.parse(raw_ts)
            formatted_ts = dt.strftime("%m/%d/%Y %I:%M %p")
            if formatted_ts != raw_ts:
                ws.update_cell(sheet_row_num, timestamp_col_idx + 1, formatted_ts)
                log(f"✅ Row {sheet_row_num}: Fixed timestamp → {formatted_ts}")
        except Exception as e:
            # Keep quiet for other weird non-date cells; just note and continue
            log(f"⏭️ Row {sheet_row_num}: Unparseable timestamp '{raw_ts}'. Skipping.")

    log("🎉 Timestamp formatting complete.")
