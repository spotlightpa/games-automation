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
    last_row = len(winners_ws.get_all_values())
    if last_row >= 3:
        winners_ws.update(
            range_name=f"A3:{chr(64 + num_columns)}{last_row}",
            values=[["" for _ in range(num_columns)] for _ in range(last_row - 2)]
        )

    rows_out = []

    for g in games:
        same_type = [x for x in games if x["game"].lower() == g["game"].lower()]
        idx = next((i for i, x in enumerate(same_type) if x["row"] == g["row"]), None)

        prev_question = ""
        prev_answer = ""
        if idx is not None and idx > 0:
            prev_question = same_type[idx - 1]["question"]
            prev_answer = same_type[idx - 1]["answer"]

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

        winners_names = [display_name(e) for e in correct_entries_sorted]
        winners_emails = [e["Email"] for e in correct_entries_sorted]

        swag_name = ""
        swag_email = ""

        if correct_entries_sorted:
            eligible = [e for e in correct_entries_sorted if (e["Email"] or "").strip().lower() not in previous_winner_emails]
            pool = eligible if eligible else correct_entries_sorted
            choice = random.choice(pool)
            swag_name = display_name(choice)
            swag_email = choice["Email"]

        others_names = [n for n in winners_names if n != swag_name]

        prefix = ""
        if prev_question and prev_answer:
            prefix = f"Last week's riddle: {prev_question} — Answer: {prev_answer}. "

        if swag_name:
            full_text = f"{prefix}Congrats to {swag_name}, who will receive Spotlight PA swag."
            if others_names:
                full_text += f" Others who answered correctly: {', '.join(others_names)}."
        else:
            if prefix:
                full_text = prefix.rstrip()
            else:
                full_text = ""

        row_out = [
            _fmt_dt(g["start_dt"]),
            _fmt_dt(g["end_dt"]),
            g["game"],
            swag_name,
            swag_email,
            ", ".join(winners_names),
            ", ".join(winners_emails),
            prev_question,
            prev_answer,
            full_text
        ]
        rows_out.append(row_out)

    if rows_out:
        winners_ws.update(
            range_name=f"A3:J{2 + len(rows_out)}",
            values=rows_out,
            value_input_option="USER_ENTERED"
        )

    log(f"✅ Populated {len(rows_out)} winner rows using time windows (with previous-winner exclusion and Full Text).")
