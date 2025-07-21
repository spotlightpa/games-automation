from dateutil import parser
from modules.logging_utils import log


def reformat_submission_timestamps(sheet):
    log("🕒 Starting timestamp formatting for Submissions tab...")

    ws = sheet.worksheet("Submissions")
    rows = ws.get_all_values()
    headers = rows[0]
    timestamp_col_idx = headers.index("Timestamp")

    updated_rows = []
    for i, row in enumerate(rows[1:], start=2):
        if len(row) <= timestamp_col_idx:
            continue

        raw_ts = row[timestamp_col_idx].strip()
        if not raw_ts:
            continue

        try:
            # Try parsing and formatting
            dt = parser.parse(raw_ts)
            formatted_ts = dt.strftime("%m/%d/%Y %H:%M")

            # Only update if different
            if formatted_ts != raw_ts:
                ws.update_cell(i, timestamp_col_idx + 1, formatted_ts)
                log(f"✅ Row {i}: Fixed timestamp → {formatted_ts}")
        except Exception as e:
            log(f"⚠️ Row {i}: Could not parse timestamp '{raw_ts}': {e}")

    log("🎉 Timestamp formatting complete.")

# Rich-text formatting for riddles
def write_riddle_with_formatting(sheet, ws, row: int):
    # Get the header row to map column names to indexes
    headers = ws.row_values(1)
    header_map = {h.strip(): i for i, h in enumerate(headers)}

    # Ensure all required columns are present before proceeding
    required_cols = ["Case Number", "Teaser", "Question", "Newsletter Text"]
    if not all(col in header_map for col in required_cols):
        log("Missing one or more required columns for formatting.")
        return

    # Retrieve the target row's values and pad if necessary
    row_values = ws.row_values(row)
    while len(row_values) < len(headers):
        row_values.append("")

    # Extract the needed fields and strip whitespace
    case_no = row_values[header_map["Case Number"]].strip()
    teaser = row_values[header_map["Teaser"]].strip()
    question = row_values[header_map["Question"]].strip()

    # Skip formatting if essential fields are missing
    if not case_no or not question:
        log(f"Skipping row {row}, missing Case Number or Question.")
        return

    # Format teaser in uppercase, and build the full riddle prompt
    teaser_upper = teaser.upper()
    case_text = f"(Case No. {case_no}):"
    full_text = f"{teaser_upper} {case_text} {question}"

    # Determine the start and end positions of the "Case No." portion for styling
    start_case = len(teaser_upper) + 1
    end_case = start_case + len(case_text)

    # Get the column index where the formatted string will be written
    col_index = header_map["Newsletter Text"]

    # Create a batchUpdate request to write and format the target cell
    requests = [
        {
            "updateCells": {
                "range": {
                    "sheetId": ws._properties["sheetId"],
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1,
                },
                "rows": [
                    {
                        "values": [
                            {
                                "userEnteredValue": {"stringValue": full_text},
                                "textFormatRuns": [
                                    {
                                        "startIndex": start_case,
                                        "format": {"bold": True, "italic": True},
                                    },
                                    {
                                        "startIndex": end_case,
                                        "format": {"bold": False, "italic": False},
                                    },
                                ],
                            }
                        ]
                    }
                ],
                "fields": "userEnteredValue,textFormatRuns",
            }
        }
    ]

    # Submit the formatting request to the Google Sheets API
    sheet.batch_update({"requests": requests})
    log(f"Formatted row {row} successfully.")