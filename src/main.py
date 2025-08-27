import os

import gspread
from dotenv import load_dotenv

from modules.formatting import reformat_first_names
from modules.formatting import reformat_last_initials
from modules.formatting import reformat_submission_timestamps

from modules import grading
from modules import mail
from modules.winners import populate_winners_tab
from modules.logging_utils import log
from helpers.sheets_client import get_sheet_and_ws


# Initialize API clients and worksheet references
client, sheet, ws = get_sheet_and_ws()


def format_and_populate_all():
    # Normalize timestamps in Submissions before we depend on them
    reformat_submission_timestamps(sheet)

    # Ensure AI Grading Prompts exist for all Games rows
    grading.populate_ai_grading_prompts()

    # Grade all ungraded submissions by matching to the correct Games window
    grading.grade_submissions_for_sheet("Submissions")

    # Build the Winners tab from time-window matches
    populate_winners_tab(sheet)

    # Token cost
    log(f"💰 Total estimated OpenAI token cost: ${grading.total_token_cost:.6f}")


if __name__ == "__main__":
    load_dotenv()
    log("🚀 Starting full automation: fetch, format, grade, and winner population...")

    # Light cleanup before fetching
    reformat_first_names(sheet)
    reformat_last_initials(sheet)
    reformat_submission_timestamps(sheet)

    mail.list_labels()

    mail.fetch_emails_for_label(label_id_env="RIDDLE_LABEL_ID", game_name="Riddler",  fetch_all=False, since_now=True)
    mail.fetch_emails_for_label(label_id_env="SCRAMBLER_LABEL_ID", game_name="Scrambler", fetch_all=False, since_now=True)

    # Clean the just-added rows
    reformat_first_names(sheet)
    reformat_last_initials(sheet)
    reformat_submission_timestamps(sheet)

    # Grade and populate Winners
    format_and_populate_all()

    log("✅ Automation completed successfully.")
