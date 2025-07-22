import os
import re

import gspread
from dotenv import load_dotenv
from googleapiclient.discovery import build
from helpers.utils import rows_to_dicts
from modules.formatting import reformat_first_names
from modules.formatting import reformat_last_initials

from modules import config

from modules import grading
from modules import mail
from modules.formatting import write_riddle_with_formatting
from modules.formatting import reformat_submission_timestamps
from modules.winners import populate_winners_tab
from modules.logging_utils import log
from helpers.sheets_client import get_sheet_and_ws
from modules.auth import (
    get_credentials,
    get_openai_client,
    get_gspread_client,
)


# Initialize API clients and worksheet references
client, sheet, ws = get_sheet_and_ws()

# Backfill Game column using Case Number
def backfill_game_column(target_ws_name):
    # Log the start of the backfill process for the given worksheet
    log(f"Backfilling Game column in sheet: {target_ws_name}")

    # Access the target worksheet by name
    target_ws = sheet.worksheet(target_ws_name)

    # Retrieve all records from the "Games" worksheet
    riddles_data = ws.get_all_records()

    # Build a lookup dictionary mapping Case Number to Game
    game_map = {
        str(r.get("Case Number")): r.get("Game", "") for r in riddles_data if r.get("Case Number")
    }

    # Fetch all rows from the target worksheet, including headers
    rows = target_ws.get_all_values()
    headers = rows[0]

    # Create a map from header names to their column indexes
    header_map = {h.strip(): i for i, h in enumerate(headers)}

    # If Game column is missing, insert it as the second column and update headers
    if "Game" not in header_map:
        headers.insert(1, "Game")
        target_ws.update("A1", [headers])
        header_map = {h.strip(): i for i, h in enumerate(headers)}

    # Determine the column index for the "Game" column
    game_col_index = header_map["Game"] + 1

    # Start from row 3 since row 1 is headers and row 2 is template
    for i in range(2, len(rows)):
        row = rows[i]

        # Pad row to match header length to avoid index errors
        while len(row) < len(headers):
            row.append("")

        # Extract the Case Number from the row
        case = row[header_map["Case Number"]].strip()

        # If there's a Case Number and no Game value yet, try to backfill it
        if case and not row[header_map["Game"]].strip():
            game = game_map.get(case, "")
            if game:
                # Update the cell in the Game column with the matched value
                target_ws.update_cell(i + 1, game_col_index, game)
                log(f"Filled Game for row {i+1}: {game}")

# Run all steps in sequence
def format_and_populate_all():
    # Fill in missing Game column values using Case Number
    backfill_game_column("Submissions")
    backfill_game_column("Winners")

    # Standardize timestamp format before any logic depends on it
    reformat_submission_timestamps(sheet)

    # Generate AI grading logic where missing
    grading.populate_ai_grading_prompts()

    # Format each row's newsletter riddle entry with stylized text
    total_rows = len(ws.get_all_values())
    for row in range(3, total_rows + 1):
        write_riddle_with_formatting(sheet, ws, row)

    # Grade all ungraded submissions using AI
    grading.grade_submissions_for_sheet("Submissions")

    # Build and populate the Winners tab with correct respondents
    populate_winners_tab(sheet)

    # Log total estimated OpenAI token cost
    log(f"💰 Total estimated OpenAI token cost: ${grading.total_token_cost:.6f}")

if __name__ == "__main__":
    log("🚀 Starting full automation: formatting, grading, and winner population...")

    # Run the main processing pipeline
    format_and_populate_all()

    # Normalize First Name and Last Initial columns
    reformat_first_names(sheet)
    reformat_last_initials(sheet)

    # Normalize Timestamps
    reformat_submission_timestamps(sheet)

    # Log success
    log("✅ Automation completed successfully.")

    # Log Gmail label names
    mail.list_labels()

    # Fetch new submissions
    mail.fetch_riddler_emails()

    # Re-clean new data
    reformat_first_names(sheet)
    reformat_last_initials(sheet)
    reformat_submission_timestamps(sheet)

    # TK
    # mail.fetch_scrambler_emails()
