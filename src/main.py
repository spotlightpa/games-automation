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
    from modules.logging_utils import log_error_with_fix
    
    try:
        log("🚀 Starting full automation: fetch, format, grade, and winner population...")
        
        # Fetch emails
        log("📧 Phase 1: Fetching emails from Gmail...")
        mail.fetch_emails_for_label(label_id_env="RIDDLE_LABEL_ID", game_name="Riddler", fetch_all=True)
        mail.fetch_emails_for_label(label_id_env="SCRAMBLER_LABEL_ID", game_name="Scrambler", fetch_all=True)
        
        # Wait between Gmail and Sheets APIs to avoid quota conflicts
        log("⏸️ Waiting 2 minutes to avoid API quota conflicts between Gmail and Sheets...")
        time.sleep(120)
        
        # Format data
        log("✏️ Phase 2: Cleaning and formatting submission data...")
        reformat_first_names(sheet)
        time.sleep(5)  # Small delays between formatting operations
        
        reformat_last_initials(sheet)
        time.sleep(5)
        
        reformat_submission_timestamps(sheet)
        time.sleep(10)  # Longer delay before heavy operations
        
        # AI processing
        log("🤖 Phase 3: AI grading and winner selection...")
        format_and_populate_all()
        
        log("✅ Automation completed successfully! Check the spreadsheet for results.")
        
    except Exception as e:
        error_msg = str(e)
        if "quota exceeded" in error_msg.lower() or "429" in error_msg:
            log_error_with_fix(
                f"Google API limits reached: {error_msg}",
                "Wait 15-30 minutes before running again. This happens with large amounts of data and will resolve automatically."
            )
        elif "openai" in error_msg.lower():
            log_error_with_fix(
                f"OpenAI API error: {error_msg}",
                "Check your OpenAI API key and billing status. You may need to add credits or wait for rate limits to reset."
            )
        else:
            log_error_with_fix(
                f"Unexpected error: {error_msg}",
                "Check the error message above for clues. You may need to restart the script or check your connection."
            )
        raise

