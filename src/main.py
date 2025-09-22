import time
import sys
from dotenv import load_dotenv
from helpers.sheet_ratelimit import install_gspread_backoff

load_dotenv()
install_gspread_backoff()

from modules.logging_utils import log, log_error_with_fix
from modules.formatting import reformat_first_names, reformat_last_initials, reformat_submission_timestamps
from modules import grading, mail
from modules.winners import populate_winners_tab
from helpers.sheets_client import get_sheet_and_ws
from helpers.improved_rate_limiting import wait_for_quota_reset


client, sheet, ws = get_sheet_and_ws()

def never_fail_execute(operation_name, operation_func, *args, **kwargs):
    attempt = 0
    start_time = time.time()
    
    while True:
        attempt += 1
        try:
            log(f"Executing {operation_name} (attempt {attempt})...")
            result = operation_func(*args, **kwargs)
            elapsed = time.time() - start_time
            log(f"✅ {operation_name} completed successfully after {elapsed:.1f} seconds")
            return result
            
        except KeyboardInterrupt:
            log(f"🛑 {operation_name} interrupted by user")
            sys.exit(1)
            
        except Exception as e:
            error_msg = str(e).lower()
            elapsed = time.time() - start_time
            
            if "quota exceeded" in error_msg or "429" in error_msg:
                # For quota errors, use long waits
                if attempt <= 5:
                    wait_time = 300 * attempt  # 5min, 10min, 15min, 20min, 25min
                else:
                    wait_time = 1800  # Then 30 minutes for all subsequent attempts
                    
                log_error_with_fix(
                    f"{operation_name} quota exceeded (attempt {attempt} after {elapsed:.1f}s): {e}",
                    f"Waiting {wait_time//60} minutes for quota recovery, then trying again..."
                )
                
            elif "openai" in error_msg:
                # OpenAI errors - shorter waits
                wait_time = min(600, 60 * attempt)  # Up to 10 minutes
                log_error_with_fix(
                    f"{operation_name} OpenAI error (attempt {attempt}): {e}",
                    f"Waiting {wait_time} seconds for OpenAI recovery..."
                )
                
            elif any(code in error_msg for code in ["500", "502", "503", "504", "network", "timeout"]):
                # Server/network errors
                wait_time = min(180, 30 * attempt)  # Up to 3 minutes
                log(f"{operation_name} network error (attempt {attempt}), waiting {wait_time}s: {e}")
                
            else:
                # Unknown errors
                wait_time = min(300, 60 * attempt)  # Up to 5 minutes
                log_error_with_fix(
                    f"{operation_name} unknown error (attempt {attempt}): {e}",
                    f"Waiting {wait_time} seconds before retry..."
                )
            
            # Break long waits into chunks with progress
            total_wait = wait_time
            while total_wait > 0:
                chunk = min(60, total_wait)  # 1 minute chunks
                time.sleep(chunk)
                total_wait -= chunk
                if total_wait > 60:
                    log(f"   {operation_name}: Still waiting {total_wait//60}m {total_wait%60}s...")

def format_and_populate_all():

    never_fail_execute(
        "Timestamp Reformatting",
        reformat_submission_timestamps,
        sheet
    )
    
    log("Waiting 30 seconds between formatting operations...")
    time.sleep(30)
    
    never_fail_execute(
        "AI Grading Prompt Generation", 
        grading.populate_ai_grading_prompts
    )
    
    log("Waiting 60 seconds before grading operations...")
    time.sleep(60)
    
    never_fail_execute(
        "Submission Grading",
        grading.grade_submissions_for_sheet,
        "Submissions"
    )
    
    log("Waiting 30 seconds before winner processing...")
    time.sleep(30)
    
    never_fail_execute(
        "Winner Population",
        populate_winners_tab,
        sheet
    )
    
    log(f"💰 Total estimated OpenAI token cost: ${grading.total_token_cost:.6f}")

if __name__ == "__main__":
    script_start_time = time.time()
    
    try:
        log("🚀 Starting automation...")
        
        log("📧 Phase 1: Fetching emails from Gmail...")
        
        never_fail_execute(
            "Riddler Email Fetch",
            mail.fetch_emails_for_label,
            label_id_env="RIDDLE_LABEL_ID",
            game_name="Riddler", 
            fetch_all=True
        )
        
        log("Waiting 2 minutes between game email fetches...")
        time.sleep(120)
        
        never_fail_execute(
            "Scrambler Email Fetch",
            mail.fetch_emails_for_label,
            label_id_env="SCRAMBLER_LABEL_ID",
            game_name="Scrambler",
            fetch_all=True
        )
        
        log("Waiting 2 minutes between game email fetches...")
        time.sleep(120)
        
        never_fail_execute(
            "Puzzler Email Fetch", 
            mail.fetch_emails_for_label,
            label_id_env="PUZZLER_LABEL_ID",
            game_name="Puzzler",
            fetch_all=True
        )
        
        log("📊 Email fetching complete. Waiting 5 minutes before data processing...")
        for i in range(300):
            time.sleep(1)
            if i % 60 == 59:  # Log every minute
                remaining = 300 - i - 1
                log(f"   Phase transition wait: {remaining} seconds remaining...")
        
        log("✏️ Phase 2: Data cleaning and formatting...")
        
        never_fail_execute(
            "First Name Formatting",
            reformat_first_names,
            sheet
        )
        
        log("Waiting 60 seconds between formatting operations...")
        time.sleep(60)
        
        never_fail_execute(
            "Last Initial Formatting", 
            reformat_last_initials,
            sheet
        )
        
        log("Waiting 60 seconds before AI processing...")
        time.sleep(60)
        
        log("🤖 Phase 3: AI grading and winner selection...")
        format_and_populate_all()
        
        total_time = time.time() - script_start_time
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        
        if hours > 0:
            time_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            time_str = f"{minutes}m {seconds}s"
        else:
            time_str = f"{seconds}s"
            
        log(f"🎉 AUTOMATION COMPLETED SUCCESSFULLY! Total time: {time_str}")
        log("📋 Check the spreadsheet for all results.")
        
    except KeyboardInterrupt:
        log("🛑 Process interrupted by user")
        total_time = time.time() - script_start_time
        log(f"⏰ Process ran for {total_time:.1f} seconds before interruption")
        sys.exit(1)
        
    except Exception as e:
        total_time = time.time() - script_start_time
        log_error_with_fix(
            f"🚨 Unexpected failure after {total_time:.1f}s: {e}"
        )
        sys.exit(1)