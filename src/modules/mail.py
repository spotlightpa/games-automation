import os
import re
import time
import base64
import gspread
from email.utils import parseaddr
from dateutil import parser as dtparser
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from googleapiclient.discovery import build
from modules.first_names import normalize_first_name
from modules.last_names import normalize_last_initial
from modules.logging_utils import log
from modules import config
from helpers.sheets_client import get_sheet_and_ws
from modules.auth import get_credentials

client, sheet, ws = get_sheet_and_ws()

_TS_FMT = "%m/%d/%Y %I:%M %p"


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

    mt = payload.get("mimeType")

    if mt == "text/plain":
        data = payload.get("body", {}).get("data", "")
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore").strip() if data else ""

    if mt == "text/html":
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
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_local = dt.astimezone(ZoneInfo("America/New_York"))
    return dt_local.strftime(_TS_FMT)


def _normalize_ts_str(s: str) -> str:
    if not s:
        return ""
    try:
        dt = dtparser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("America/New_York"))
        return dt.astimezone(ZoneInfo("America/New_York")).strftime(_TS_FMT)
    except Exception:
        return (s or "").strip()


def _normalize_answer_for_key(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.lower()


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
    if body_text and "approve: https://groups.google.com" in body_text.lower():
        return True
    return False


def _ensure_submission_headers(ws_sub):
    headers = ws_sub.row_values(1)
    needed = [
        "Game", "Timestamp", "First Name", "Last Name Initial",
        "Email", "Answer", "AI Grade", "AI Confidence", "Override"
    ]
    changed = False
    norm_existing = {re.sub(r"\s+", " ", (h or "").strip().lower()) for h in headers}
    for col in needed:
        if re.sub(r"\s+", " ", col.lower()) not in norm_existing:
            headers.append(col)
            changed = True
    if changed:
        ws_sub.update("A1", [headers])
    return headers


def _load_existing_keys(ws_sub, headers):
    all_rows = ws_sub.get_all_values()
    keys = set()
    for r in all_rows[2:]:
        game = (r[0] if len(r) > 0 else "").strip().lower()
        ts_raw = (r[1] if len(r) > 1 else "").strip()
        email = (r[4] if len(r) > 4 else "").strip().lower()
        ans_raw = (r[5] if len(r) > 5 else "").strip()

        ts_norm = _normalize_ts_str(ts_raw)
        ans_norm = _normalize_answer_for_key(ans_raw)

        if game and ts_norm and email and ans_norm:
            keys.add((game, email, ts_norm, ans_norm))
    return keys


def _next_empty_row_in_A_to_I(ws_sub):
    data = ws_sub.get('A:I')
    last = 0
    for i, row in enumerate(data, start=1):
        if any((cell or "").strip() for cell in row):
            last = i
    return max(last + 1, 3)


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


def _extract_answer_from_subject(subject: str, game_name: str) -> str:
    if not subject:
        return ""
    s = subject.strip()
    s = re.sub(r"(?i)^(re|fw|fwd):\s*", "", s).strip()

    g = (game_name or "").strip()
    if g:
        m = re.search(rf"(?i)\b{re.escape(g)}\s*answer[:\-\s]*(.+)$", s)
        if m:
            return m.group(1).strip(" '\"–—-").strip()

    m = re.search(r"(?i)\banswer[:\-\s]+(.+)$", s)
    if m:
        return m.group(1).strip(" '\"–—-").strip()

    if len(s) <= 120:
        return s.strip(" '\"–—-").strip()

    return ""


def fetch_emails_for_label(label_id_env: str, game_name: str, fetch_all: bool = True):
    env_label_id = os.getenv(label_id_env)
    default_label_id = config.RIDDLE_LABEL_ID if game_name.lower() == "riddler" else config.SCRAMBLER_LABEL_ID
    label_id = env_label_id or default_label_id

    if not label_id:
        log(f"❌ Missing Gmail label id for {game_name}. Set {label_id_env} or update config.")
        return

    creds = get_credentials()
    service = build("gmail", "v1", credentials=creds)
    ws_sub = sheet.worksheet("Submissions")

    try:
        log(f"🗂️ Writing to worksheet: {ws_sub.title} (rows before: {len(ws_sub.get_all_values())})")
    except Exception:
        pass

    headers = _ensure_submission_headers(ws_sub)
    existing_keys = _load_existing_keys(ws_sub, headers)

    page_token = None
    pulled_total = 0
    page_num = 0

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
            internal_ms = message.get("internalDate")

            try:
                if internal_ms:
                    dt = datetime.fromtimestamp(int(internal_ms) / 1000, tz=timezone.utc)
                    ts_str = dt.astimezone(ZoneInfo("America/New_York")).strftime(_TS_FMT)
                else:
                    ts_str = _parse_date_to_string(date_header) if date_header else ""
            except Exception:
                log(f"⏭️ Skipping message {msg_id}: unparseable Date/internalDate")
                continue

            first_name, last_initial, email_addr = _parse_sender(from_header)
            body_text = _extract_plaintext(payload)
            body_text = _clean_answer(body_text)

            if _looks_like_digest_or_moderator(subject, email_addr, body_text):
                log(f"⏭️ Skipping moderator/digest: {subject[:80]}")
                continue

            if not body_text and subject:
                guess = _extract_answer_from_subject(subject, game_name)
                if guess:
                    body_text = guess
                    log(f"✳️ Used subject as answer for {email_addr}: {subject[:80]}")

            # normalized answer for dedupe key
            ans_for_key = _normalize_answer_for_key(body_text)
            if not ans_for_key:
                log(f"⏭️ Skipping empty-body email from {email_addr} ({subject[:80]})")
                continue

            # KEY uses normalized values (game, email, timestamp, answer)
            key = (game_name.strip().lower(), (email_addr or "").strip().lower(), ts_str, ans_for_key)
            if key in existing_keys:
                continue

            new_row = [
                game_name,             # Game
                ts_str,                # Timestamp (canonical NY-local)
                first_name,            # First Name
                last_initial,          # Last Name Initial
                email_addr,            # Email
                body_text,             # Answer (human-readable cleaned text)
                "",                    # AI Grade
                "",                    # AI Confidence
                "",                    # Override
            ]

            rows_to_append.append(new_row)
            existing_keys.add(key)

        if rows_to_append:
            rows_A_to_I = []
            for r in rows_to_append:
                row9 = (r + [""] * 9)[:9]
                rows_A_to_I.append(row9)

            try:
                start_row = _next_empty_row_in_A_to_I(ws_sub)
                end_row = start_row + len(rows_A_to_I) - 1

                ws_sub.update(
                    f"A{start_row}:I{end_row}",
                    rows_A_to_I,
                    value_input_option="USER_ENTERED"
                )

                pulled_total += len(rows_A_to_I)
                log(f"✅ Wrote {len(rows_A_to_I)} {game_name} rows to A{start_row}:I{end_row}.")

                try:
                    preview_start = max(3, end_row - min(10, len(rows_A_to_I)) + 1)
                    preview = ws_sub.get(f"A{preview_start}:I{end_row}")
                    log(f"🔎 Confirm A{preview_start}:I{end_row} (showing last 3):")
                    for row in (preview or [])[-3:]:
                        pad = (row + ['']*9)[:9]
                        log(f"   · {pad}")
                except Exception as e:
                    log(f"⚠️ Post-write preview failed: {e}")

            except Exception as e:
                log(f"❌ Failed A:I write: {e}")

        if not fetch_all or not page_token:
            break

        time.sleep(0.4)

    log(f"📥 Pulled {pulled_total} {game_name} submission(s).")
