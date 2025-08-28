import time
from dotenv import load_dotenv
from helpers.sheet_ratelimit import install_gspread_backoff

load_dotenv()
install_gspread_backoff()

from modules.formatting import reformat_first_names, reformat_last_initials, reformat_submission_timestamps
from modules import grading, mail
from modules.winners import populate_winners_tab
from modules.logging_utils import log
from helpers.sheets_client import get_sheet_and_ws

client, sheet, ws = get_sheet_and_ws()

def format_and_populate_all():
    reformat_submission_timestamps(sheet)
    grading.populate_ai_grading_prompts()
    grading.grade_submissions_for_sheet("Submissions")
    populate_winners_tab(sheet)
    log(f"💰 Total estimated OpenAI token cost: ${grading.total_token_cost:.6f}")

if __name__ == "__main__":
    log("🚀 Starting full automation: fetch, format, grade, and winner population...")

    mail.fetch_emails_for_label(label_id_env="RIDDLE_LABEL_ID", game_name="Riddler", fetch_all=True)
    mail.fetch_emails_for_label(label_id_env="SCRAMBLER_LABEL_ID", game_name="Scrambler", fetch_all=True)

    time.sleep(0.5)

    reformat_first_names(sheet)
    reformat_last_initials(sheet)
    reformat_submission_timestamps(sheet)

    time.sleep(0.5)

    format_and_populate_all()
    log("✅ Automation completed successfully.")

