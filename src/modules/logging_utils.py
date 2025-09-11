import os
import requests
from dotenv import load_dotenv

load_dotenv()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def slack_log(message: str, is_error: bool = False):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        if is_error:
            prefix = "🚨 ERROR: "
        elif "waiting" in message.lower() or "delay" in message.lower():
            prefix = "⏳ WAITING: "
        elif "quota" in message.lower():
            prefix = "📊 API LIMIT: "
        elif "completed" in message.lower() or "success" in message.lower():
            prefix = "✅ SUCCESS: "
        else:
            prefix = "ℹ️ STATUS: "
        
        formatted_message = prefix + message
        
        requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": formatted_message},
            timeout=5
        )
    except Exception as e:
        print(f"Slack logging failed: {e}")

def log(message: str, is_error: bool = False):
    print(message)
    slack_log(message, is_error)

def log_quota_issue(operation: str, wait_time: int, attempt: int, max_attempts: int):
    msg = f"Google API limit reached during {operation}. This is normal - waiting {wait_time} seconds for limits to reset (attempt {attempt}/{max_attempts}). No action needed."
    log(msg)
    slack_log(f"API limits hit during {operation}. Automatically waiting {wait_time}s. This happens when processing lots of data - just wait, the system will recover.", False)

def log_error_with_fix(error_msg: str, possible_fix: str = None):
    log(error_msg, is_error=True)
    if possible_fix:
        fix_msg = f"💡 POSSIBLE FIX: {possible_fix}"
        log(fix_msg)
        slack_log(fix_msg, False)

def log_progress(current: int, total: int, operation: str):
    if total > 0:
        percent = (current / total) * 100
        msg = f"{operation}: {current}/{total} ({percent:.1f}%)"
        log(msg)
        if current % 10 == 0 or current == total:
            slack_log(f"Progress update: {operation} is {percent:.1f}% complete ({current}/{total})", False)
