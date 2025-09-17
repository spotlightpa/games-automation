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

def _safe_row_values(ws, row, tries=7):
    """Safe wrapper for getting row values with quota retry logic"""
    import time
    from gspread.exceptions import APIError
    from modules.logging_utils import log_quota_wait_with_progress, log_error_with_fix
    
    for attempt in range(tries):
        try:
            return ws.row_values(row)
        except APIError as e:
            msg = str(e).lower()
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            
            if status_code == 429 or "quota exceeded" in msg or "rate limit" in msg:
                if attempt < tries - 1:
                    if "per minute" in msg:
                        wait_time = 90 + (attempt * 60)
                    else:
                        wait_time = 30 + (attempt * 30)
                    
                    log_quota_wait_with_progress("reading worksheet headers", wait_time, attempt + 1, tries)
                    continue
                else:
                    log_error_with_fix(
                        f"Google Sheets quota exhausted in mail module after {tries} attempts",
                        "Wait 5-10 minutes before running again. The system hit Google's API limits."
                    )
            elif status_code in (500, 502, 503, 504) and attempt < tries - 1:
                wait_time = min(16.0, 2 ** attempt)
                time.sleep(wait_time)
                continue
            
            raise

def _safe_get_all_values_mail(ws, tries=7):
    """Safe wrapper for getting all values with quota retry logic"""
    import time
    from gspread.exceptions import APIError
    from modules.logging_utils import log_quota_wait_with_progress, log_error_with_fix
    
    for attempt in range(tries):
        try:
            return ws.get_all_values()
        except APIError as e:
            msg = str(e).lower()
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            
            if status_code == 429 or "quota exceeded" in msg or "rate limit" in msg:
                if attempt < tries - 1:
                    if "per minute" in msg:
                        wait_time = 90 + (attempt * 60)
                    else:
                        wait_time = 30 + (attempt * 30)
                    
                    log_quota_wait_with_progress("reading worksheet data", wait_time, attempt + 1, tries)
                    continue
                else:
                    log_error_with_fix(
                        f"Google Sheets quota exhausted in mail module after {tries} attempts",
                        "Wait 5-10 minutes before running again. The system hit Google's API limits."
                    )
            elif status_code in (500, 502, 503, 504) and attempt < tries - 1:
                wait_time = min(16.0, 2 ** attempt)
                time.sleep(wait_time)
                continue
            
            raise


_TS_FMT = "%m/%d/%Y %I:%M %p"


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


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
    # drop leading "Subject: ..." line, if present, for stable dedupe
    lines = s.splitlines()
    if lines and re.match(r"(?i)^\s*subject\s*:", lines[0]):
        lines = lines[1:]
    s = "\n".join(lines)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def _pick_personal_sender(from_header: str, reply_to_header: str, list_aliases=None):
    """
    Prefer the personal sender in Reply-To when the message arrived via a
    list alias like riddler@spotlightpa.org or scrambler@spotlightpa.org.
    Falls back to From if Reply-To is unusable.
    """
    if list_aliases is None:
        list_aliases = {"riddler@spotlightpa.org", "scrambler@spotlightpa.org"}

    from_name, from_email = parseaddr(from_header or "")
    rt_name, rt_email = parseaddr(reply_to_header or "")

    from_email_l = (from_email or "").strip().lower()
    rt_email_l = (rt_email or "").strip().lower()
    from_name_l = (from_name or "").strip().lower()

    #  If From email is a known list alias -> prefer Reply-To (if present)
    #  If From name contains " via " -> prefer Reply-To (if present)
    #  If Reply-To exists and is not a list alias -> prefer Reply-To
    use_reply_to = False
    if rt_email_l:
        if from_email_l in list_aliases:
            use_reply_to = True
        elif " via " in from_name_l:
            use_reply_to = True
        elif rt_email_l not in list_aliases:
            # Often true when list forwards to a real user
            use_reply_to = True

    # Choose
    if use_reply_to:
        return rt_name or rt_email, rt_email or from_email
    return from_name or from_email, from_email or rt_email


def _parse_sender(from_header: str, reply_to_header: str = ""):
    # Prefer a real person over a list alias when possible
    chosen_name, chosen_email = _pick_personal_sender(from_header, reply_to_header)

    # Derive display names
    name_clean = re.sub(r"[\"']", "", (chosen_name or "")).strip()
    parts = [p for p in re.split(r"\s+", name_clean) if p]

    first = normalize_first_name(parts[0]) if parts else ""
    last_initial = normalize_last_initial(parts[1] if len(parts) > 1 else "")

    return first, last_initial, (chosen_email or "").strip()


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
    headers = _safe_row_values(ws_sub, 1)
    needed = [
        "Game", "Timestamp", "First Name", "Last Name Initial",
        "Email", "Answer", "AI Grade", "AI Confidence", "Override", "Link"
    ]
    norm_existing = {re.sub(r"\s+", " ", (h or "").strip().lower()): i for i, h in enumerate(headers)}
    changed = False

    for col in needed[:-1]:
        if re.sub(r"\s+", " ", col.lower()) not in norm_existing:
            headers.append(col)
            norm_existing[re.sub(r"\s+", " ", col.lower())] = len(headers) - 1
            changed = True

    norm_override = re.sub(r"\s+", " ", "Override".lower())
    norm_link = re.sub(r"\s+", " ", "Link".lower())
    if norm_link not in norm_existing:
        override_idx = norm_existing.get(norm_override, len(headers) - 1)
        insert_at = override_idx + 1
        headers.insert(insert_at, "Link")
        norm_existing = {re.sub(r"\s+", " ", (h or "").strip().lower()): i for i, h in enumerate(headers)}
        changed = True

    if changed:
        ws_sub.update("A1", [headers])
    return headers


def _load_existing_keys(ws_sub, headers):
    all_rows = _safe_get_all_values_mail(ws_sub)
    keys = set()
    key_to_row = {}
    for row_idx, r in enumerate(all_rows[2:], start=3):
        game = (r[0] if len(r) > 0 else "").strip().lower()
        ts_raw = (r[1] if len(r) > 1 else "").strip()
        email = (r[4] if len(r) > 4 else "").strip().lower()
        ans_raw = (r[5] if len(r) > 5 else "").strip()

        ts_norm = _normalize_ts_str(ts_raw)
        ans_norm = _normalize_answer_for_key(ans_raw)

        if game and ts_norm and email and ans_norm:
            k = (game, email, ts_norm, ans_norm)
            keys.add(k)
            key_to_row[k] = row_idx
    return keys, key_to_row


def _next_empty_row(ws_sub, num_cols: int):
    last = 0
    last_col_letter = _col_letter(num_cols)
    for attempt in range(7):
        try:
            data = ws_sub.get(f"A:{last_col_letter}")
            break
        except APIError as e:
            from modules.logging_utils import log_quota_wait_with_progress
            if attempt < 6 and ("429" in str(e) or "quota exceeded" in str(e).lower()):
                wait_time = 90 + (attempt * 60)
                log_quota_wait_with_progress("checking worksheet size", wait_time, attempt + 1, 7)
                continue
            raise
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
    existing_keys, key_to_row = _load_existing_keys(ws_sub, headers)

    header_map = {h.strip(): i for i, h in enumerate(headers)}
    link_col_idx = header_map.get("Link", len(headers) - 1)

    page_token = None
    pulled_total = 0
    page_num = 0

    while True:
        page_num += 1
        req = {"userId": "me", "labelIds": [label_id], "maxResults": 100}
        if page_token:
            req["pageToken"] = page_token

        results = service.users().messages().list(**req).execute()
        messages = results.get("messages", [])
        page_token = results.get("nextPageToken")

        if not messages:
            break

        rows_to_append = []
        link_updates = []  # (row_idx, link)

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
            reply_to_header = hget("Reply-To", "")
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

            first_name, last_initial, email_addr = _parse_sender(from_header, reply_to_header)
            body_text = _extract_plaintext(payload)
            body_text = _clean_answer(body_text)

            if _looks_like_digest_or_moderator(subject, email_addr, body_text):
                continue

            subj_clean = (subject or "").strip()
            # If the email has no body, try to treat the subject as the answer.
            if not body_text and subj_clean:
                guess = _extract_answer_from_subject(subj_clean, game_name)
                if guess:
                    body_text = f"Subject: {subj_clean}\n\n{guess}"
                    log(f"✳️ Used subject as answer for {email_addr}: {subj_clean[:80]}")
                else:
                    body_text = f"Subject: {subj_clean}"
            elif subj_clean and body_text:
                body_text = f"Subject: {subj_clean}\n\n{body_text}"

            # normalized answer for dedupe key
            ans_for_key = _normalize_answer_for_key(body_text)
            if not ans_for_key:
                log(f"⏭️ Skipping empty-body email from {email_addr} ({subject[:80]})")
                continue

            msg_link = f"https://mail.google.com/mail/u/0/#all/{msg_id}"

            key = (game_name.strip().lower(), (email_addr or "").strip().lower(), ts_str, ans_for_key)
            if key in existing_keys:
                # backfill Link if empty
                row_idx = key_to_row.get(key)
                if row_idx:
                    try:
                        curr = ws_sub.get(f"{_col_letter(link_col_idx+1)}{row_idx}:{_col_letter(link_col_idx+1)}{row_idx}")
                        curr_val = curr[0][0] if curr and curr[0] else ""
                    except Exception:
                        curr_val = ""
                    if not curr_val:
                        link_updates.append((row_idx, msg_link))
                continue

            new_row_values = {
                "Game": game_name,
                "Timestamp": ts_str,
                "First Name": first_name,
                "Last Name Initial": last_initial,
                "Email": email_addr,
                "Answer": body_text,
                "AI Grade": "",
                "AI Confidence": "",
                "Override": "",
                "Link": msg_link,
            }
            row = [new_row_values.get(h, "") for h in headers]
            rows_to_append.append(row)

            existing_keys.add(key)

        if rows_to_append:
            try:
                ws_sub.append_rows(rows_to_append, value_input_option="USER_ENTERED")
                pulled_total += len(rows_to_append)
                log(f"✅ Appended {len(rows_to_append)} {game_name} rows.")
            except Exception as e:
                log(f"❌ Append failed: {e}")

        if link_updates:
            try:
                requests = []
                link_col_letter = _col_letter(link_col_idx + 1)
                for row_idx, link in link_updates:
                    requests.append({
                        "range": f"{link_col_letter}{row_idx}:{link_col_letter}{row_idx}",
                        "values": [[link]]
                    })
                if requests:
                    ws_sub.batch_update([{"range": r["range"], "values": r["values"]} for r in requests], value_input_option="USER_ENTERED")
                    log(f"🔗 Backfilled {len(requests)} link(s) in one batch.")
            except Exception as e:
                log(f"⚠️ Link backfill batch failed: {e}")


        if not fetch_all or not page_token:
            break

        time.sleep(0.4)

    log(f"📥 Pulled {pulled_total} {game_name} submission(s).")
