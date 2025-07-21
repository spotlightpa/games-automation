import re
import base64

from googleapiclient.discovery import build
from modules.auth import get_credentials, get_gspread_client
from modules.logging_utils import log
from modules import config
from helpers.sheets_client import get_sheet_and_ws

client, sheet, ws = get_sheet_and_ws()

def list_labels():
    
    # Lists Gmail labels relevant to riddle games
    creds = get_credentials()
    service = build("gmail", "v1", credentials=creds)

    # Get the labels from Gmail
    results = service.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])

    # Fetch labels defined in .env
    public_label_ids = {
        config.RIDDLE_LABEL_ID: "Riddler",
        config.SCRAMBLER_LABEL_ID: "Scrambler"
    }

    for label in labels:
        label_id = label.get("id")
        if label_id in public_label_ids:
            label_name = public_label_ids[label_id]
            log(f"✔️ {label_name} Label — Name: {label['name']} | ID: {label_id}")


def fetch_riddler_emails(max_results=10):
    """
    Fetches emails labeled as riddle submissions from Gmail inbox.
    Parses, deduplicates, and appends valid answers into the 'Submissions' sheet.
    Accepts a limit on number of messages to fetch (default: testing 10).
    """
    creds = get_credentials()
    service = build("gmail", "v1", credentials=creds)
    ws_sub = sheet.worksheet("Submissions")

    try:
        # Gmail label ID for filtering relevant riddle emails
        results = service.users().messages().list(
            userId="me",
            labelIds=[config.RIDDLE_LABEL_ID],
            maxResults=max_results,
        ).execute()

        messages = results.get("messages", [])
        if not messages:
            log("📭 No messages found in 'Riddler' label.")
            return

        # Read the headers and get the current game name
        headers = ws_sub.row_values(1)
        game_name = ws.row_values(3)[1]

        # Load existing submission data to avoid duplicates
        existing_rows = ws_sub.get_all_records()
        existing_keys = set(
            (row.get("Email", "").strip().lower(), row.get("Answer", "").strip())
            for row in existing_rows if row.get("Email") and row.get("Answer")
        )

        # Iterate over each email message
        for msg in messages:
            msg_id = msg["id"]
            message = service.users().messages().get(
                userId="me", id=msg_id, format="full"
            ).execute()

            payload = message["payload"]
            headers_data = payload.get("headers", [])

            # Extract email metadata (subject, sender, timestamp)
            subject = next((h["value"] for h in headers_data if h["name"] == "Subject"), "(No Subject)")
            from_email = next((h["value"] for h in headers_data if h["name"] == "From"), "(No From)")
            date = next((h["value"] for h in headers_data if h["name"] == "Date"), "")
            timestamp = date

            # Try to parse sender's name and extract first name + last initial
            name_match = re.match(r"(.*?)(<|via)", from_email)
            name = name_match.group(1).strip().strip("'\"") if name_match else from_email
            name_parts = name.split()
            first_name = name_parts[0] if name_parts else ""
            last_initial = name_parts[1][0] if len(name_parts) > 1 else ""

            # Look for text/plain body (most reliable), fallback to root body
            parts = payload.get("parts", [])
            body_data = ""
            for part in parts:
                if part["mimeType"] == "text/plain":
                    body_data = part["body"].get("data", "")
                    break

            if not body_data:
                body_data = payload.get("body", {}).get("data", "")

            # Decode the body from base64 into readable text
            if body_data:
                body_text = base64.urlsafe_b64decode(body_data).decode("utf-8").strip()
            else:
                body_text = "(No body found)"

            # Check for duplicate submission using (email, answer) pair
            key = (from_email.strip().lower(), body_text)
            if key in existing_keys:
                log(f"⚠️ Skipping duplicate submission from {from_email}")
                continue

            log(f"📧 Parsed answer: {body_text[:50]}...")

            # Prepare row for appending to Submissions sheet
            new_row = [
                "",                    # Case Number — left blank for now
                game_name,             # Current game name from Games sheet
                timestamp,             # Timestamp from the email
                first_name,            # Extracted first name
                last_initial,          # First character of last name (if available)
                from_email,            # Full sender email
                body_text,             # The decoded user submission
                "",                    # AI Grade — filled later by model
                "",                    # AI Confidence — filled later
                ""                     # Override — human override for grading
            ]

            # Append the cleaned submission row to the Google Sheet
            ws_sub.append_row(new_row, value_input_option="USER_ENTERED")
            log(f"✅ Added submission from {from_email}")

    except Exception as e:
        log(f"⚠️ Error fetching riddler emails: {e}")


# TK add Scrambler emails, we need to set up case numbers and separate grading logic for unscrambling words with various answers accepted