import os
import re
import time
import base64
import gspread
from email.utils import parseaddr
from dateutil import parser as dtparser

from googleapiclient.discovery import build
from modules.first_names import normalize_first_name
from modules.last_names import normalize_last_initial
from modules.logging_utils import log
from modules import config
from helpers.sheets_client import get_sheet_and_ws
from modules.auth import get_credentials

client, sheet, ws = get_sheet_and_ws()


def _extract_plaintext(payload):
    """
    Recursively extract the best text/plain body from a Gmail payload.
    Fallbacks: text/html stripped tags to plain text; final fallback: empty string.
    """
    def html_to_text(html):
        # minimal HTML to text
        text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html)
        text = re.sub(r"(?is)<br\s*/?>", "\n", text)
        text = re.sub(r"(?is)</p>", "\n", text)
        text = re.sub(r"(?is)<.*?>", "", text)
        return re.sub(r"\n{3,}", "\n\n", text).strip()

    # If body is directly on payload
    if payload.get("mimeType") == "text/plain":
        data = payload.get("body", {}).get("data", "")
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore").strip() if data else ""

    if payload.get("mimeType") == "text/html":
        data = payload.get("body", {}).get("data", "")
        html = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore").strip() if data else ""
        return html_to_text(html)

    # Walk parts recursively
    for part in (payload.get("parts", []) or []):
        txt = _extract_plaintext(part)
        if txt:
            return txt

    data = payload.get("body", {}).get("data", "")
    if data:
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore").strip()
    return ""


def _parse_date_to_string(date_header: str) -> str:
    dt = dtparser.parse(date_header)
    return dt.strftime("%m/%d/%Y %I:%M %p")


def _parse_sender(from_header: str):
    name, email = parseaddr(from_header)
    email = email or from_header.strip()

    # Derive names
    name_clean = re.sub(r"[\"']", "", name).strip()
    parts = [p for p in re.split(r"\s+", name_clean) if p]

    first = normalize_first_name(parts[0]) if parts else ""
    last_initial = normalize_last_initial(parts[1] if len(parts) > 1 else "")

    return first, last_initial, email


def _clean_answer(body_text: str) -> str:
    """
    Trim out common footers/quotes to keep the answer concise.
    Keep this conservative so we don't delete valid content.
    """
    if not body_text:
        return ""

    # Drop everything after common mobile signatures or reply quotes
    cut_patterns = [
        r"^Sent from my iPhone.*$",
        r"^Sent from Yahoo Mail.*$",
        r"^Get Outlook for.*$",
        r"^On .+ wrote:$",
        r"^From: .+$",
        r"^—+$",
        r"^-- $",
        r"^This message is being sent to you because you are a moderator of the group.*$",
    ]
    lines = body_text.splitlines()
    out = []
    for ln in lines:
        if any(re.search(pat, ln.strip(), flags=re.IGNORECASE) for pat in cut_patterns):
            break
        out.append(ln)
    cleaned = "\n".join(out).strip()

    # If it's overly long, truncate to first 3000 chars to be safe
    if len(cleaned) > 3000:
        cleaned = cleaned[:3000].rstrip() + "…"
    return cleaned


def _looks_like_digest_or_moderator(subject: str, from_email: str, body_text: str) -> bool:
    subject = (subject or "").lower()
    f = (from_email or "").lower()
    if "moderator's spam report" in subject or "digest" in subject:
        return True
    if "noreply-spamdigest" in f or "via riddler" in f or "no-reply" in f or "noreply" in f:
        return True
    if body_text and body_text.lower().count("approve: https://groups.google.com") >= 1:
        return True
    return False


def _ensure_submission_headers(ws_sub):
    headers = ws_sub.row_values(1)
    needed = [
        "Game", "Timestamp", "First Name", "Last Name Initial",
        "Email", "Answer", "AI Grade", "AI Confidence", "Override"
    ]
    changed = False
    for col in needed:
        if col not in headers:
            headers.append(col)
            changed = True
    if changed:
        ws_sub.update("A1", [headers])
    return headers


def _load_existing_keys(ws_sub, headers):
    header_idx = {h: i for i, h in enumerate(headers)}
    all_rows = ws_sub.get_all_values()
    keys = set()
    for r in all_rows[2:]:
        if len(r) < len(headers):
            r = r + [""] * (len(headers) - len(r))
        game = (r[header_idx["Game"]] or "").strip()
        ts = (r[header_idx["Timestamp"]] or "").strip()
        email = (r[header_idx["Email"]] or "").strip().lower()
        ans = (r[header_idx["Answer"]] or "").strip()
        if game and ts and email and ans:
            keys.add((game, email, ts, ans))
    return keys, header_idx


def _hardcoded_cutoff_dt_for_game(game_name: str):
    g = (game_name or "").strip().lower()
    if g == "riddler":
        return dtparser.parse("2025-08-26 6:49:00")
    if g == "scrambler":
        return dtparser.parse("2025-08-26 6:49:04")
    return None


def _max_timestamp_for_game(ws_sub, headers, game_name):
    """
    Scan Submissions for the latest Timestamp for this Game.
    Returns (max_dt, raw_string_from_sheet) or (None, None) if not found.
    """
    idx = {h: i for i, h in enumerate(headers)}
    if "Game" not in idx or "Timestamp" not in idx:
        return (None, None)

    max_dt = None
    max_raw = None
    rows = ws_sub.get_all_values()
    for r in rows[2:]:
        if len(r) <= idx["Timestamp"]:
            continue
        g = (r[idx["Game"]] or "").strip().lower()
        if g != (game_name or "").strip().lower():
            continue
        raw = (r[idx["Timestamp"]] or "").strip()
        if not raw:
            continue
        try:
            dt = dtparser.parse(raw)
        except Exception:
            continue
        if (max_dt is None) or (dt > max_dt):
            max_dt = dt
            max_raw = raw
    return (max_dt, max_raw)


def _append_rows_with_backoff(ws_sub, rows, retries=6, initial_delay=2):
    if not rows:
        return True
    delay = initial_delay
    for attempt in range(retries):
        try:
            ws_sub.append_rows(rows, value_input_option="USER_ENTERED")
            return True
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota hit, retrying in {delay}s (attempt {attempt+1}/{retries})...")
                time.sleep(delay)
                delay *= 2
                continue
            raise
    log("❌ Failed to append rows after multiple retries due to quota.")
    return False


def list_labels():
    creds = get_credentials()
    service = build("gmail", "v1", credentials=creds)

    results = service.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])

    public_label_ids = {
        config.RIDDLE_LABEL_ID: "Riddler",
        config.SCRAMBLER_LABEL_ID: "Scrambler"
    }

    for label in labels:
        label_id = label.get("id")
        if label_id in public_label_ids:
            label_name = public_label_ids[label_id]
            log(f"✔️ {label_name} Label — Name: {label['name']} | ID: {label_id}")


def fetch_emails_for_label(label_id_env: str, game_name: str, fetch_all: bool = False, since_now: bool = False):
    """
    Pull emails from a Gmail label into the Submissions sheet.

    Modes:
      • Default (fetch_all=False):
          Only pulls messages newer than the latest Timestamp already in Submissions for this Game, and also newer than the hardcoded cutoff below.
      • Historical backfill (fetch_all=True):
          Replays all messages in the label (subject to duplicate key filtering).
      • From-now-on (since_now=True):
          Same behavior as default; flag is accepted for API compatibility but not required.
    """
    env_label_id = os.getenv(label_id_env)
    default_label_id = config.RIDDLE_LABEL_ID if game_name.lower() == "riddler" else config.SCRAMBLER_LABEL_ID
    label_id = env_label_id or default_label_id

    if not label_id:
        log(f"❌ Missing Gmail label id for {game_name}. Set {label_id_env} or update config.")
        return

    creds = get_credentials()
    service = build("gmail", "v1", credentials=creds)
    ws_sub = sheet.worksheet("Submissions")

    headers = _ensure_submission_headers(ws_sub)
    existing_keys, header_idx = _load_existing_keys(ws_sub, headers)

    last_seen_dt, last_seen_str = _max_timestamp_for_game(ws_sub, headers, game_name)
    hardcoded_dt = _hardcoded_cutoff_dt_for_game(game_name)

    boundary_dt = None
    sources = []
    if last_seen_dt:
        boundary_dt = last_seen_dt
        sources.append(f"latest sheet: {last_seen_str}")
    if hardcoded_dt:
        boundary_dt = max(boundary_dt, hardcoded_dt) if boundary_dt else hardcoded_dt
        sources.append("hardcoded cutoff")

    if not fetch_all and boundary_dt:
        log(f"🧭 Only-new boundary for {game_name}: {', '.join(sources)}")

    page_token = None
    pulled_total = 0
    page_num = 0
    stop_paging = False

    while True:
        page_num += 1
        req = {
            "userId": "me",
            "labelIds": [label_id],
            "maxResults": 100,
        }
        if page_token:
            req["pageToken"] = page_token

        results = service.users().messages().list(**req).execute()
        messages = results.get("messages", [])
        page_token = results.get("nextPageToken")

        if not messages:
            break

        rows_to_append = []

        for msg in messages:
            if stop_paging:
                break

            msg_id = msg.get("id")
            if not msg_id:
                continue

            message = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
            payload = message.get("payload", {})
            headers_data = payload.get("headers", [])

            def hget(name, default=""):
                return next((h["value"] for h in headers_data if h["name"].lower() == name.lower()), default)

            subject = hget("Subject", "")
            from_header = hget("From", "")
            date_header = hget("Date", "")

            try:
                ts_str = _parse_date_to_string(date_header) if date_header else ""
                ts_dt = dtparser.parse(ts_str) if ts_str else None  # naive
            except Exception:
                log(f"⏭️ Skipping message {msg_id}: unparseable Date header '{date_header}'")
                continue

            if not fetch_all and boundary_dt and ts_dt and ts_dt <= boundary_dt:
                log(f"🛑 Reached time boundary at {ts_str}. Stopping pagination.")
                stop_paging = True
                break

            first_name, last_initial, email_addr = _parse_sender(from_header)
            body_text = _extract_plaintext(payload)
            body_text = _clean_answer(body_text)

            if _looks_like_digest_or_moderator(subject, email_addr, body_text):
                log(f"⏭️ Skipping moderator/digest: {subject[:80]}")
                continue

            game = game_name

            key = (game, (email_addr or "").lower(), ts_str, body_text)
            if not body_text:
                log(f"⏭️ Skipping empty-body email from {email_addr} ({subject[:80]})")
                continue

            if key in existing_keys:
                continue

            row_map = {h: "" for h in headers}
            row_map["Game"] = game
            row_map["Timestamp"] = ts_str
            row_map["First Name"] = first_name
            row_map["Last Name Initial"] = last_initial
            row_map["Email"] = email_addr
            row_map["Answer"] = body_text
            row_map["AI Grade"] = ""
            row_map["AI Confidence"] = ""
            row_map["Override"] = ""

            new_row = [row_map.get(h, "") for h in headers]
            rows_to_append.append(new_row)
            existing_keys.add(key)

        if rows_to_append:
            ok = _append_rows_with_backoff(ws_sub, rows_to_append, retries=6, initial_delay=2)
            if ok:
                pulled_total += len(rows_to_append)
                log(f"✅ Appended {len(rows_to_append)} {game_name} rows (page {page_num}).")

        if stop_paging:
            break
        if not page_token:
            break

        # Small pause between pages to reduce quota pressure
        time.sleep(0.5)

    if fetch_all:
        log(f"📥 Pulled {pulled_total} {game_name} submission(s) (historical).")
    else:
        log(f"📥 Pulled {pulled_total} new {game_name} submission(s).")
