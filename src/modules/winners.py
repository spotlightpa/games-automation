import random
from dateutil import parser

from modules import grading
from modules.logging_utils import log


def _parse_dt_safe(s: str):
    try:
        return parser.parse(s) if s else None
    except Exception:
        return None


def _fmt_dt(dt):
    s = dt.strftime("%m/%d/%Y %I:%M %p")
    return s.replace(" 0", " ", 1)


def _load_previous_winner_emails(sheet):
    try:
        prev_ws = sheet.worksheet("Previous Winners")
    except Exception:
        return set()

    rows = prev_ws.get_all_values()
    if not rows:
        return set()

    headers = rows[0]
    if "Email" not in headers:
        return set()

    idx = headers.index("Email")
    emails = set()
    for r in rows[1:]:
        if len(r) > idx:
            e = (r[idx] or "").strip().lower()
            if e and e != "na" and e != "declined":
                emails.add(e)
    return emails


def populate_winners_tab(sheet):
    games_ws = sheet.worksheet("Games")
    subs_ws = sheet.worksheet("Submissions")
    winners_ws = sheet.worksheet("Winners")

    games_raw = games_ws.get_all_values()
    headers = games_raw[0]
    h = {name.strip(): idx for idx, name in enumerate(headers)}

    required = ["Start Time", "End Time", "Game", "Question", "Answer"]
    if not all(col in h for col in required):
        log("❌ Games sheet is missing required columns.")
        return

    games = []
    for row_num in range(3, len(games_raw) + 1):
        row = games_raw[row_num - 1]
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        gtype = row[h["Game"]].strip()
        start_dt = _parse_dt_safe(row[h["Start Time"]].strip())
        end_dt = _parse_dt_safe(row[h["End Time"]].strip())
        question = row[h["Question"]].strip()
        answer = row[h["Answer"]].strip()

        if not gtype or not start_dt or not end_dt:
            continue

        games.append({
            "row": row_num,
            "game": gtype,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "question": question,
            "answer": answer,
        })

    games.sort(key=lambda r: (r["game"].lower(), r["start_dt"]))

    subs_raw = subs_ws.get_all_values()
    sub_headers = subs_raw[0] if subs_raw else []
    sh = {name.strip(): idx for idx, name in enumerate(sub_headers)}
    needed_sub_cols = ["Game", "Timestamp", "First Name", "Last Name Initial", "Email", "AI Grade", "Override"]
    if not all(col in sh for col in needed_sub_cols):
        log("❌ Submissions sheet missing required columns.")
        return

    submissions = []
    for i, row in enumerate(subs_raw[2:], start=3):
        if len(row) < len(sub_headers):
            row = row + [""] * (len(sub_headers) - len(row))
        gtype = (row[sh["Game"]] or "").strip()
        ts = _parse_dt_safe((row[sh["Timestamp"]] or "").strip())
        first_name = (row[sh["First Name"]] or "").strip()
        last_initial = (row[sh["Last Name Initial"]] or "").strip()
        email = (row[sh["Email"]] or "").strip()
        ai_grade = (row[sh["AI Grade"]] or "").strip()
        override = (row[sh["Override"]] or "").strip()
        submissions.append({
            "row": i,
            "game": gtype,
            "dt": ts,
            "First Name": first_name,
            "Last Name Initial": last_initial,
            "Email": email,
            "AI Grade": ai_grade,
            "Override": override
        })

    previous_winner_emails = _load_previous_winner_emails(sheet)

    winner_headers = winners_ws.row_values(1)
    num_columns = len(winner_headers)

    # capture existing rows BEFORE clearing to preserve swag winner if still valid
    existing_rows = winners_ws.get_all_values()
    existing_map = {}
    if existing_rows and len(existing_rows) >= 3:
        wh_existing = {name.strip(): idx for idx, name in enumerate(existing_rows[0])}
        for r in existing_rows[2:]:
            if not r:
                continue
            def gv(col):
                idx = wh_existing.get(col)
                return (r[idx] if idx is not None and len(r) > idx else "").strip()
            key = (gv("Start Time"), gv("End Time"), gv("Game"))
            if any(key):
                existing_map[key] = (gv("Swag Winner"), gv("Swag Winner Email"))

    last_row = len(existing_rows)
    if last_row >= 3:
        last_col_letter = chr(64 + num_columns)
        winners_ws.update(
            range_name=f"A3:{last_col_letter}{last_row}",
            values=[["" for _ in range(num_columns)] for _ in range(last_row - 2)]
        )

    rows_out = []

    for g in games:
        correct_entries = []
        for s in submissions:
            if (s["game"] or "").strip().lower() != g["game"].strip().lower():
                continue
            if not s["dt"]:
                continue
            if g["start_dt"] <= s["dt"] <= g["end_dt"]:
                if grading.is_marked_correct(s):
                    if s["First Name"] and s["Last Name Initial"] and s["Email"]:
                        correct_entries.append(s)

        def display_name(e):
            return f"{e['First Name']} {e['Last Name Initial']}."

        correct_entries_sorted = sorted(correct_entries, key=lambda e: display_name(e))

        # Sorted, de-duped winners by display name
        winners_names = [display_name(e) for e in correct_entries_sorted]
        winners_emails = [e["Email"] for e in correct_entries_sorted]
        combined = sorted(zip(winners_names, winners_emails), key=lambda x: x[0].lower())
        seen_names = set()
        winners_names_sorted = []
        winners_emails_sorted = []
        for n, em in combined:
            if n not in seen_names:
                winners_names_sorted.append(n)
                winners_emails_sorted.append(em)
                seen_names.add(n)

        # Winner emails: alphabetize and dedupe case-insensitively
        email_map = {}
        for em in winners_emails_sorted:
            em_clean = (em or "").strip()
            if not em_clean:
                continue
            k = em_clean.lower()
            if k not in email_map:
                email_map[k] = em_clean
        winner_emails_alpha = [email_map[k] for k in sorted(email_map.keys())]

        # Preserve swag winner if still valid; otherwise choose anew
        swag_name = ""
        swag_email = ""
        pairs_all = {(display_name(e), (e["Email"] or "").strip()) for e in correct_entries_sorted}
        key = (_fmt_dt(g["start_dt"]), _fmt_dt(g["end_dt"]), g["game"])
        prior_swag_name, prior_swag_email = existing_map.get(key, ("", ""))

        if prior_swag_name and prior_swag_email:
            if (prior_swag_name, prior_swag_email) in pairs_all and (prior_swag_email or "").strip().lower() not in previous_winner_emails:
                swag_name, swag_email = prior_swag_name, prior_swag_email

        if not swag_name:
            eligible = [e for e in correct_entries_sorted if (e["Email"] or "").strip().lower() not in previous_winner_emails]
            pool = eligible if eligible else correct_entries_sorted
            if pool:
                choice = random.choice(pool)
                swag_name = display_name(choice)
                swag_email = choice["Email"]

        others_names = [n for n in winners_names_sorted if n != swag_name]

        if swag_name:
            full_text = f"Congrats to {swag_name}, who will receive Spotlight PA swag."
            if others_names:
                full_text += f" Others who answered correctly: {', '.join(others_names)}."
        else:
            full_text = ""

        # collapse accidental double period at the end
        if full_text.endswith(".."):
            full_text = full_text[:-1]

        values = {
            "Start Time": _fmt_dt(g["start_dt"]),
            "End Time": _fmt_dt(g["end_dt"]),
            "Game": g["game"],
            "Swag Winner": swag_name,
            "Swag Winner Email": swag_email,
            "Winners": ", ".join(winners_names_sorted),
            "Winner Emails": ", ".join(winner_emails_alpha),
            "Full Text": full_text,
        }
        row_out = [values.get(col, "") for col in winner_headers]
        rows_out.append(row_out)

    if rows_out:
        last_col_letter = chr(64 + num_columns)
        winners_ws.update(
            range_name=f"A3:{last_col_letter}{2 + len(rows_out)}",
            values=rows_out,
            value_input_option="USER_ENTERED"
        )

    log(f"✅ Populated {len(rows_out)} winner rows using time windows (with previous-winner exclusion and Full Text).")
