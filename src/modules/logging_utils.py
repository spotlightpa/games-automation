import os
import time
import threading
import requests
from dotenv import load_dotenv

load_dotenv()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Global variables to track progress updates
_progress_thread = None
_progress_stop_event = threading.Event()
_current_progress = {}

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

def _create_progress_bar(current: int, total: int, width: int = 20) -> str:
    """Create a visual progress bar"""
    if total <= 0:
        return "█" * width + " (unknown)"
    
    filled = int(width * current / total)
    bar = "█" * filled + "░" * (width - filled)
    percentage = (current / total) * 100
    return f"{bar} {percentage:.1f}% ({current}/{total})"

def _progress_updater():
    """Background thread that updates Slack with progress"""
    global _current_progress
    last_update = 0
    
    while not _progress_stop_event.is_set():
        current_time = time.time()
        
        # Update every 30 seconds
        if current_time - last_update >= 30 and _current_progress:
            try:
                operation = _current_progress.get('operation', 'Processing')
                current = _current_progress.get('current', 0)
                total = _current_progress.get('total', 0)
                start_time = _current_progress.get('start_time', current_time)
                
                elapsed = current_time - start_time
                elapsed_str = f"{int(elapsed)}s"
                
                if total > 0 and current > 0:
                    # Estimate remaining time
                    rate = current / elapsed
                    remaining = (total - current) / rate if rate > 0 else 0
                    remaining_str = f" (~{int(remaining)}s remaining)"
                else:
                    remaining_str = ""
                
                progress_bar = _create_progress_bar(current, total)
                message = f"⏳ {operation}: {progress_bar} | Time: {elapsed_str}{remaining_str}"
                
                requests.post(
                    SLACK_WEBHOOK_URL,
                    json={"text": message},
                    timeout=5
                )
                last_update = current_time
                
            except Exception as e:
                print(f"Progress update failed: {e}")
        
        time.sleep(5)  # Check every 5 seconds

def start_progress_tracking(operation: str, total: int = 0):
    """Start tracking progress for a long operation"""
    global _progress_thread, _current_progress, _progress_stop_event
    
    # Stop any existing progress tracking
    stop_progress_tracking()
    
    _current_progress = {
        'operation': operation,
        'current': 0,
        'total': total,
        'start_time': time.time()
    }
    
    _progress_stop_event = threading.Event()
    _progress_thread = threading.Thread(target=_progress_updater, daemon=True)
    _progress_thread.start()
    
    # Send initial message
    if total > 0:
        slack_log(f"Starting {operation} - this may take several minutes...")
    else:
        slack_log(f"Starting {operation}...")

def update_progress(current: int):
    """Update the current progress"""
    global _current_progress
    if _current_progress:
        _current_progress['current'] = current

def stop_progress_tracking():
    """Stop progress tracking and send completion message"""
    global _progress_thread, _progress_stop_event, _current_progress
    
    if _progress_thread and _progress_thread.is_alive():
        _progress_stop_event.set()
        _progress_thread.join(timeout=2)
    
    if _current_progress:
        operation = _current_progress.get('operation', 'Operation')
        current = _current_progress.get('current', 0)
        total = _current_progress.get('total', 0)
        start_time = _current_progress.get('start_time', time.time())
        
        elapsed = time.time() - start_time
        elapsed_str = f"{int(elapsed)}s"
        
        if total > 0 and current >= total:
            slack_log(f"✅ {operation} completed! Processed {current} items in {elapsed_str}")
        else:
            slack_log(f"✅ {operation} completed in {elapsed_str}")
    
    _current_progress = {}
    _progress_thread = None

def log_quota_wait_with_progress(operation: str, wait_time: int, attempt: int, max_attempts: int):
    """Log quota issues with a countdown progress bar"""
    msg = f"Google API limit reached during {operation}. Waiting {wait_time} seconds for limits to reset (attempt {attempt}/{max_attempts})"
    log(msg)
    
    # Start progress tracking for the wait
    start_progress_tracking(f"waiting for API limits to reset ({operation})", wait_time)
    
    # Update progress every second during wait
    for i in range(wait_time):
        update_progress(i + 1)
        time.sleep(1)
        
        # Print countdown every 30 seconds to console
        if (i + 1) % 30 == 0:
            remaining = wait_time - (i + 1)
            print(f"   Still waiting... {remaining} seconds remaining")
    
    stop_progress_tracking()

def log_quota_issue(operation: str, wait_time: int, attempt: int, max_attempts: int):
    """Log quota issues and wait with progress tracking"""
    log_quota_wait_with_progress(operation, wait_time, attempt, max_attempts)

def log_error_with_fix(error_msg: str, possible_fix: str = None):
    log(error_msg, is_error=True)
    if possible_fix:
        fix_msg = f"💡 POSSIBLE FIX: {possible_fix}"
        log(fix_msg)
        slack_log(fix_msg, False)

def log_progress(current: int, total: int, operation: str):
    """Log progress updates - uses background thread for Slack"""
    if total > 0:
        percent = (current / total) * 100
        msg = f"{operation}: {current}/{total} ({percent:.1f}%)"
        print(msg)  # Always print to console
        
        # Update background progress tracker if it exists
        if _current_progress.get('operation') == operation:
            update_progress(current)
        elif current == 1:  # Start tracking on first update
            start_progress_tracking(operation, total)
            update_progress(current)
        elif current >= total:  # Stop tracking when complete
            update_progress(current)
            stop_progress_tracking()

def log_with_progress_start(message: str, operation: str, total: int = 0):
    """Log a message and start progress tracking"""
    log(message)
    start_progress_tracking(operation, total)

def log_with_progress_end(message: str):
    """Log a message and stop progress tracking"""
    stop_progress_tracking()
    log(message)