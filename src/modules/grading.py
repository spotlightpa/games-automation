import re
from dateutil import parser

import gspread

from modules import config
from modules.logging_utils import log
from modules.auth import get_openai_client, GENERIC_GRADING_INSTRUCTIONS
from modules.auth import get_credentials
from helpers.tokens import log_token_usage


openai_client = get_openai_client()
client = gspread.authorize(get_credentials())
sheet = client.open_by_key(config.SPREADSHEET_ID)
ws = sheet.worksheet("Games")
total_token_cost = 0.0


def _parse_dt_safe(s: str):
    try:
        return parser.parse(s) if s else None
    except Exception:
        return None


def _header_map_loose(headers):
    def norm(s): return re.sub(r"\s+", " ", (s or "").strip().lower())
    hm = {norm(h): i for i, h in enumerate(headers)}
    def idx_of(*candidates):
        for c in candidates:
            i = hm.get(norm(c))
            if i is not None:
                return i
        return None
    return hm, idx_of


def _normalize_letters(s: str) -> str:
    """letters-only, lowercase"""
    return re.sub(r"[^A-Za-z]+", "", (s or "")).lower()


def _parse_scrambler_answer_list(answer: str):
    """
    Parse the Scrambler 'Answer' cell into:
      - display: ordered, human-readable list (de-duped by letters-only)
      - targets: set of normalized (letters-only) answers for comparison

    Be flexible about separators. We treat separators as things such as:
    commas, semicolons, slashes, pipes, bullets, newlines, the words 'or'/'and',
    and even runs of extra spaces. We still keep each candidate's internal spaces
    or hyphens for display, but the match is letters-only.
    """
    if not (answer or "").strip():
        return [], set()

    s = answer

    # Normalize common textual separators to commas
    s = re.sub(r"\b(?:or|and)\b", ",", s, flags=re.IGNORECASE)
    s = s.replace("•", ",").replace("·", ",").replace("|", ",").replace("•", ",")
    s = re.sub(r"[;/\n]+", ",", s)

    # Collapse repeated commas/spaces
    s = re.sub(r"\s*,\s*", ",", s)
    s = re.sub(r",\s*,+", ",", s).strip(" ,")

    parts = [p.strip() for p in s.split(",") if p.strip()]

    # If it still looks like a single blob, try a very gentle split on big gaps
    if len(parts) <= 1:
        parts = [p for p in re.split(r"\s{2,}", s) if p.strip()]

    display = []
    seen_norm = set()
    for p in parts:
        # Keep nice display (trim), but dedupe by letters-only
        disp = p.strip()
        norm = _normalize_letters(disp)
        if not norm:
            continue
        if norm not in seen_norm:
            seen_norm.add(norm)
            display.append(disp)

    return display, seen_norm


def build_games_index():
    """
    Returns a list of dicts for each playable row in Games with parsed datetimes.
    Keys: game, start_dt, end_dt, question, answer, grading, row_number
    """
    all_rows = ws.get_all_values()
    if not all_rows:
        return []
    headers = all_rows[0]
    hm, idx_of = _header_map_loose(headers)

    col_game = idx_of("Game")
    col_start = idx_of("Start Time", "Start")
    col_end = idx_of("End Time", "End")
    col_question = idx_of("Question")
    col_answer = idx_of("Answer", "Accepted Answer(s)", "Answers")
    col_grading = idx_of("AI Grading Prompt", "AI Grading Instructions", "Grading Prompt")

    required_idx = [col_game, col_start, col_end, col_question, col_answer, col_grading]
    if any(i is None for i in required_idx):
        log("❌ Missing one or more required columns in Games.")
        return []

    index = []
    for row_num in range(3, len(all_rows) + 1):
        row = all_rows[row_num - 1]
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        start_dt = _parse_dt_safe((row[col_start] or "").strip())
        end_dt = _parse_dt_safe((row[col_end] or "").strip())
        game = (row[col_game] or "").strip()
        question = (row[col_question] or "").strip()
        answer = (row[col_answer] or "").strip()
        grading = (row[col_grading] or "").strip()

        if not game or not start_dt or not end_dt:
            continue

        index.append({
            "game": game,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "question": question,
            "answer": answer,
            "grading": grading,
            "row_number": row_num,
            "grading_col_1based": col_grading + 1,
        })

    index.sort(key=lambda r: (r["game"].lower(), r["start_dt"]))
    return index


def find_game_for_submission(game_type: str, submission_dt, games_index):
    if not submission_dt or not game_type:
        return None

    gtype = game_type.strip().lower()

    for r in games_index:
        if r["game"].strip().lower() != gtype:
            continue
        if r["start_dt"] <= submission_dt <= r["end_dt"]:
            return r
    return None


def _openai_chat_safe(messages, model="gpt-4o", temperature=0, max_tokens=250):
    try:
        resp = openai_client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        text = resp.choices[0].message.content.strip()
        usage = getattr(resp, "usage", None)
        return text, usage
    except Exception as e1:
        try:
            resp = openai_client.responses.create(
                model="gpt-4o-mini",
                input=messages[-1]["content"],
                temperature=temperature,
                max_output_tokens=max_tokens,
            )
            text = ""
            if hasattr(resp, "output") and resp.output:
                text = "".join(
                    getattr(item, "content", "")
                    for item in resp.output
                    if getattr(item, "type", "") == "output_text"
                ).strip()
            if not text and hasattr(resp, "output_text"):
                text = resp.output_text.strip()
            usage = getattr(resp, "usage", None)
            return text, usage
        except Exception as e2:
            raise RuntimeError(f"OpenAI call failed: {e1} // {e2}")


def generate_grading_logic(game_type: str, question: str, answer: str, existing_guidance: str = "") -> str:
    """
    Returns the short grading logic string that gets pasted into the sheet.
    (We keep the long, explicit instructions internally in grade_submission_entry.)
    """
    if (game_type or "").strip().lower() == "scrambler":
        display_list, _ = _parse_scrambler_answer_list(answer)
        if not display_list:
            return f"Accepted answer: must use all and only the letters from {question}."
        if len(display_list) == 1:
            return f"Accepted answer: {display_list[0]}"
        return f"Accepted answers: {', '.join(display_list)}"

    prompt = f"""{GENERIC_GRADING_INSTRUCTIONS}

{(existing_guidance or '').strip()}

Riddle Question: {question}
Correct Answer: {answer}

Provide grading logic:""".strip()

    text, usage = _openai_chat_safe(
        messages=[{"role": "user", "content": prompt}],
        model="gpt-4o",
        temperature=0,
        max_tokens=250
    )
    if usage:
        global total_token_cost
        total_token_cost += log_token_usage(usage)
    return text


def _parse_grade_confidence(text: str):
    m_grade = re.search(r"Correctness:\s*(Correct|Incorrect)", text, flags=re.IGNORECASE)
    grade = m_grade.group(1).capitalize() if m_grade else "Uncertain"

    m_conf = re.search(r"Confidence:\s*([0-9]+(?:\.[0-9]+)?)\s*%?", text, flags=re.IGNORECASE)
    if m_conf:
        val = float(m_conf.group(1))
        if val <= 1.0:
            val *= 100.0
        confidence = f"{int(round(val))}%"
    else:
        confidence = "N/A"
    return grade, confidence


def grade_submission_entry(grading_prompt, user_answer):
    global total_token_cost

    prompt = f"""{grading_prompt.strip()}

You are grading a riddle submission that was copied from an email. The raw text may contain the user's answer plus non-answer content (e.g., signatures, legal disclaimers, quotes, addresses/phone numbers, URLs, reply headers, or other footers).

INSTRUCTIONS:
1) Identify the candidate answer: the earliest, shortest span that clearly attempts to answer the puzzle.
   • Prefer the first non-empty line(s) that read as an answer.
   • Stop when you reach common signature/disclaimer markers (mobile signatures, lines like "Sent from", "Regards", "Thank you", dash separators, quoted-reply markers such as "On ... wrote:"), or when content shifts to contact info, legal notices, or unrelated quotations.
   • If multiple guesses appear, grade the first clear answer.
2) Judge correctness ONLY using the candidate answer — ignore any trailing non-answer content.
3) Ignore case, punctuation, filler words, and trivial formatting differences. If the grading logic says to treat the answer in letters-only form, remove ALL non-letters (e.g., *, spaces, punctuation, markdown) before comparing.
4) Be faithful to the riddle’s intended meaning per the grading logic.

Respond in this format exactly:
Correctness: Correct or Incorrect
Confidence: [number from 0 to 100]

User's raw message:
{user_answer}""".strip()

    text, usage = _openai_chat_safe(
        messages=[{"role": "user", "content": prompt}],
        model="gpt-4o",
        temperature=0,
        max_tokens=200
    )
    if usage:
        total_token_cost += log_token_usage(usage)

    grade, confidence = _parse_grade_confidence(text)
    if grade == "Uncertain":
        log(f"⚠️ AI response could not be parsed:\n{text}")
    return grade, confidence


def populate_ai_grading_prompts():
    """
    Ensure every playable Games row has an AI Grading Prompt.
    Batched updates to avoid per-cell writes.
    """
    all_rows = ws.get_all_values()
    if not all_rows:
        log("ℹ️ No rows in Games.")
        return
    headers = all_rows[0]
    hm, idx_of = _header_map_loose(headers)
    total_rows = len(all_rows)

    game_idx = idx_of("Game")
    question_idx = idx_of("Question")
    answer_idx = idx_of("Answer", "Accepted Answer(s)", "Answers")
    grading_idx = idx_of("AI Grading Prompt", "AI Grading Instructions", "Grading Prompt")

    if grading_idx is None:
        log("❌ 'AI Grading Prompt' column not found.")
        return

    pending = []  # list of (row_number, final_output)

    for row_number in range(3, total_rows + 1):
        row = (all_rows[row_number - 1] + [""] * (grading_idx + 1))[:max(len(all_rows[0]), grading_idx + 1)]

        game = row[game_idx].strip() if game_idx is not None else ""
        question = row[question_idx].strip() if question_idx is not None else ""
        answer = row[answer_idx].strip() if answer_idx is not None else ""
        existing_guidance = row[grading_idx].strip() if grading_idx is not None else ""

        if not question or not answer:
            continue

        if existing_guidance.lower().startswith("ai:"):
            continue

        log(f"⚙️ Generating grading logic for row {row_number} ({game})...")
        grading_logic = generate_grading_logic(
            game_type=game,
            question=question,
            answer=answer,
            existing_guidance=existing_guidance
        )
        if not grading_logic:
            log(f"⚠️ Row {row_number}: No grading logic generated.")
            continue

        final_output = f"{existing_guidance.strip()}\nAI: {grading_logic}" if existing_guidance else f"AI: {grading_logic}"
        pending.append((row_number, final_output.strip()))

    if not pending:
        log("✅ All Games rows already have AI grading prompts.")
        return

    # Coalesce into contiguous blocks in the grading column
    pending.sort(key=lambda x: x[0])
    blocks = []
    start = end = pending[0][0]
    buf = [pending[0][1]]
    for r, val in pending[1:]:
        if r == end + 1:
            end = r
            buf.append(val)
        else:
            blocks.append((start, end, buf))
            start = end = r
            buf = [val]
    blocks.append((start, end, buf))

    col_letter = chr(65 + grading_idx)  # 0-based -> letter
    ranges = []
    for start, end, values in blocks:
        ranges.append({
            "range": f"{col_letter}{start}:{col_letter}{end}",
            "values": [[v] for v in values]
        })

    ws.batch_update(ranges, value_input_option="USER_ENTERED")
    log(f"📝 Updated grading logic for {sum(len(b[2]) for b in blocks)} row(s) in Games.")


# Determines if a submission is marked correct, considering override first
def is_marked_correct(entry):
    """
    A submission is considered correct if:
    - Override is "correct", OR
    - Override is blank AND AI Grade is "correct"
    """
    override = entry.get("Override", "").strip().lower() if "Override" in entry else ""
    grade = entry.get("AI Grade", "").strip().lower() if "AI Grade" in entry else ""
    return override == "correct" or (not override and grade == "correct")


def grade_submissions_for_sheet(sheet_name: str):
    """
    Grade submissions by matching each submission's (Game, Timestamp) to a Games window,
    retrieving the AI Grading Prompt from that Games row (or generating it), and scoring.
    """
    log(f"Grading submissions in sheet: {sheet_name}")
    ws_sub = sheet.worksheet(sheet_name)

    all_rows = ws_sub.get_all_values()
    if not all_rows:
        log("ℹ️ No rows to grade.")
        return
    headers = all_rows[0]

    header_set = {(h or "").strip() for h in headers}
    changed = False
    for col in ["AI Grade", "AI Confidence", "Override"]:
        if col not in header_set:
            headers.append(col)
            changed = True
    if changed:
        ws_sub.update("A1", [headers])
        all_rows = ws_sub.get_all_values()
        headers = all_rows[0]

    _, idx_of = _header_map_loose(headers)
    game_idx = idx_of("Game")
    ts_idx = idx_of("Timestamp")
    answer_idx = idx_of("Answer")
    grade_idx = idx_of("AI Grade")
    conf_idx = idx_of("AI Confidence")

    if None in (game_idx, ts_idx, answer_idx, grade_idx, conf_idx):
        log("❌ Submissions must have 'Game', 'Timestamp', and 'Answer' columns.")
        return

    games_index = build_games_index()
    if not games_index:
        log("❌ No playable Games rows indexed — nothing to grade.")
        return

    by_game = {}
    for r in games_index:
        by_game.setdefault(r["game"].strip().lower(), []).append(r)

    data_rows = all_rows[2:]
    out_updates = []

    for i, row in enumerate(data_rows, start=3):
        row = (row + [""] * len(headers))[:len(headers)]

        if (row[grade_idx] or "").strip():
            continue

        game_type = (row[game_idx] or "").strip()
        ts_raw = (row[ts_idx] or "").strip()
        user_answer = (row[answer_idx] or "").strip()

        if not game_type or not ts_raw or not user_answer:
            continue

        sub_dt = _parse_dt_safe(ts_raw)
        if not sub_dt:
            log(f"⚠️ Row {i}: Unparseable timestamp '{ts_raw}'")
            continue

        candidates = by_game.get(game_type.lower(), [])
        match = next((r for r in candidates if r["start_dt"] <= sub_dt <= r["end_dt"]), None)
        if not match:
            # Only log first few missing windows to avoid spam
            if not hasattr(log, '_missing_window_count'):
                log._missing_window_count = {}
            if game_type not in log._missing_window_count:
                log._missing_window_count[game_type] = 0

            if log._missing_window_count[game_type] < 3:
                log(f"⏭️ Row {i}: No matching {game_type} window for {ts_raw}")
                log._missing_window_count[game_type] += 1
            elif log._missing_window_count[game_type] == 3:
                log(f"⏭️ ... and more rows missing {game_type} windows (suppressing further messages)")
                log._missing_window_count[game_type] += 1
            continue

        grading_prompt = match["grading"]
        if not grading_prompt or not grading_prompt.lower().startswith("ai:"):
            # Build if empty; store back into Games
            log(f"⚙️ Missing AI grading prompt in Games row {match['row_number']}, generating...")
            logic = generate_grading_logic(match["game"], match["question"], match["answer"], grading_prompt or "")
            final_output = f"{grading_prompt.strip()}\nAI: {logic}" if grading_prompt else f"AI: {logic}"
            ws.update_cell(match["row_number"], match["grading_col_1based"], final_output.strip())
            grading_prompt = final_output

        try:
            grade, confidence = grade_submission_entry(grading_prompt, user_answer)
        except Exception as e:
            log(f"❌ Row {i}: OpenAI grading failed: {e}")
            grade, confidence = "Uncertain", "N/A"

        out_updates.append((i, grade, confidence))

    if out_updates:
        def col_letter(idx): return chr(65 + idx)
        min_row = min(r for (r, _, _) in out_updates)
        max_row = max(r for (r, _, _) in out_updates)
        num_rows = max_row - min_row + 1
        block = [["", ""] for _ in range(num_rows)]
        for r, g, c in out_updates:
            block[r - min_row][0] = g
            block[r - min_row][1] = c
        ws_sub.update(
            f"{col_letter(grade_idx)}{min_row}:{col_letter(conf_idx)}{max_row}",
            block,
            value_input_option="USER_ENTERED"
        )
        log(f"✅ Graded {len(out_updates)} rows in one batch.")
    else:
        log("ℹ️ No ungraded rows found.")
