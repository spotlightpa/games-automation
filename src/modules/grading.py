import re
from dateutil import parser

import gspread

from modules import config
from modules.logging_utils import log
from modules.auth import get_openai_client, GENERIC_GRADING_INSTRUCTIONS
from modules.auth import get_credentials
from helpers.utils import rows_to_dicts
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


def build_games_index():
    """
    Returns a list of dicts for each playable row in Games with parsed datetimes.
    Keys: game, start_dt, end_dt, question, answer, grading, row_number
    """
    all_rows = ws.get_all_values()
    headers = all_rows[0]
    h = {name.strip(): idx for idx, name in enumerate(headers)}

    required = ["Start Time", "End Time", "Game", "Question", "Answer", "AI Grading Prompt"]
    if not all(col in h for col in required):
        log("❌ Missing one or more required columns in Games.")
        return []

    index = []
    for row_num in range(3, len(all_rows) + 1):
        row = all_rows[row_num - 1]
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        start_dt = _parse_dt_safe(row[h["Start Time"]].strip())
        end_dt = _parse_dt_safe(row[h["End Time"]].strip())
        game = row[h["Game"]].strip()
        question = row[h["Question"]].strip()
        answer = row[h["Answer"]].strip()
        grading = row[h["AI Grading Prompt"]].strip()

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


def generate_grading_logic(game_type: str, question: str, answer: str, existing_guidance: str = "") -> str:
    global total_token_cost

    if game_type.lower() == "scrambler":
        prompt = f"""
You are grading a word scramble.

Scrambled letters: {question}
Expected answer(s): {answer}

{existing_guidance.strip() if existing_guidance else ""}

Instructions:
- Assume the system already knows how to grade a Scrambler.
- Do NOT repeat grading rules or letter counts.
- Just provide any additional accepted answers, or state clearly that the only accepted answer is the one listed.
- Be brief and specific.

Your response will be used to assist human editors and should be direct.
"""
    else:
        prompt = f"""{GENERIC_GRADING_INSTRUCTIONS}

{existing_guidance.strip() if existing_guidance else ""}

Riddle Question: {question}
Correct Answer: {answer}

Provide grading logic:"""

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=250
    )

    total_token_cost += log_token_usage(response.usage)
    return response.choices[0].message.content.strip()


def grade_submission_entry(grading_prompt, user_answer):
    """
    Use the grading prompt + user's raw answer text to determine correctness.

    The raw text may include non-answer content (signatures, confidentiality notices,
    inspirational quotes, addresses/phone numbers, URLs, reply headers, or other footers).
    Evaluate only the actual answer.
    """
    global total_token_cost

    prompt = f"""{grading_prompt.strip()}

You are grading a riddle submission that was copied from an email. The raw text may contain the user's answer plus
non-answer content (e.g., signatures, legal disclaimers, quotes, addresses/phone numbers, URLs, reply headers, or
other footers).

INSTRUCTIONS:
1) Identify the candidate answer: the earliest, shortest span that clearly attempts to answer the puzzle.
   • Prefer the first non-empty line(s) that read as an answer.
   • Stop when you reach common signature/disclaimer markers (mobile signatures, lines like "Sent from", "Regards",
     "Thank you", dash separators, quoted-reply markers such as "On ... wrote:"), or when content shifts to contact info,
     legal notices, or unrelated quotations.
   • If multiple guesses appear, grade the first clear answer.
2) Judge correctness ONLY using the candidate answer — ignore any trailing non-answer content.
3) Ignore case, punctuation, filler words, and trivial formatting differences.
4) Be faithful to the riddle’s intended meaning per the grading logic.

Respond in this format exactly:
Correctness: Correct or Incorrect
Confidence: [number from 0 to 100]

User's raw message:
{user_answer}"""

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=200
    )

    total_token_cost += log_token_usage(response.usage)

    content = response.choices[0].message.content.strip()

    match_grade = re.search(r"Correctness:\s*(Correct|Incorrect)", content, re.IGNORECASE)
    match_conf = re.search(r"Confidence:\s*(\d+)", content)

    if not match_grade or not match_conf:
        log(f"⚠️ AI response could not be parsed:\n{content}")
        return "Uncertain", "N/A"

    grade = match_grade.group(1).capitalize()
    confidence = f"{match_conf.group(1)}%"
    return grade, confidence


def populate_ai_grading_prompts():
    """
    Ensure every playable Games row has an AI Grading Prompt.
    """
    all_rows = ws.get_all_values()
    headers = all_rows[0]
    header_map = {h.strip(): i for i, h in enumerate(headers)}
    total_rows = len(all_rows)

    game_idx = header_map.get("Game")
    question_idx = header_map.get("Question")
    answer_idx = header_map.get("Answer")
    grading_idx = header_map.get("AI Grading Prompt")

    if grading_idx is None:
        log("❌ 'AI Grading Prompt' column not found.")
        return

    for row_number in range(3, total_rows + 1):
        row = all_rows[row_number - 1]

        game = row[game_idx].strip() if game_idx is not None and len(row) > game_idx else ""
        question = row[question_idx].strip() if question_idx is not None and len(row) > question_idx else ""
        answer = row[answer_idx].strip() if answer_idx is not None and len(row) > answer_idx else ""
        existing_guidance = row[grading_idx].strip() if grading_idx is not None and len(row) > grading_idx else ""

        if not question or not answer:
            log(f"⏭️ Skipping row {row_number}, missing question or answer.")
            continue

        # Skip if already has AI-generated content
        if existing_guidance.lower().startswith("ai:"):
            log(f"✅ Row {row_number} already has AI grading prompt, skipping.")
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
        ws.update_cell(row_number, grading_idx + 1, final_output.strip())
        log(f"📝 Updated grading logic for row {row_number}.")


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
    headers = all_rows[0]
    header_map = {h.strip(): i for i, h in enumerate(headers)}

    # Ensure result columns exist
    required = ["AI Grade", "AI Confidence", "Override"]
    modified = False
    for col in required:
        if col not in header_map:
            headers.append(col)
            modified = True

    if modified:
        ws_sub.update("A1", [headers])
        header_map = {h.strip(): i for i, h in enumerate(headers)}

    if "Game" not in header_map or "Timestamp" not in header_map or "Answer" not in header_map:
        log("❌ Submissions must have 'Game', 'Timestamp', and 'Answer' columns.")
        return

    game_idx = header_map["Game"]
    ts_idx = header_map["Timestamp"]
    answer_idx = header_map["Answer"]
    grade_idx = header_map["AI Grade"]
    conf_idx = header_map["AI Confidence"]

    games_index = build_games_index()

    # Iterate through Submissions rows (starting row 3)
    for i, row in enumerate(ws_sub.get_all_values()[2:], start=3):
        # pad to header length
        while len(row) < len(headers):
            row.append("")

        # Skip already-graded rows
        if row[grade_idx].strip():
            continue

        game_type = row[game_idx].strip()
        ts_raw = row[ts_idx].strip()
        user_answer = row[answer_idx].strip()

        if not game_type or not ts_raw or not user_answer:
            continue

        sub_dt = _parse_dt_safe(ts_raw)
        if not sub_dt:
            log(f"⚠️ Row {i}: Unparseable timestamp '{ts_raw}'")
            continue

        match = find_game_for_submission(game_type, sub_dt, games_index)
        if not match:
            log(f"⏭️ Row {i}: No matching {game_type} window for {ts_raw}")
            continue

        grading_prompt = match["grading"]
        if not grading_prompt or not grading_prompt.lower().startswith("ai:"):
            # Build if empty; store back into Games
            log(f"⚙️ Missing AI grading prompt in Games row {match['row_number']}, generating...")
            logic = generate_grading_logic(match["game"], match["question"], match["answer"], grading_prompt or "")
            final_output = f"{grading_prompt.strip()}\nAI: {logic}" if grading_prompt else f"AI: {logic}"
            ws.update_cell(match["row_number"], ws.row_values(1).index("AI Grading Prompt") + 1, final_output.strip())
            grading_prompt = final_output

        grade, confidence = grade_submission_entry(grading_prompt, user_answer)

        ws_sub.update_cell(i, grade_idx + 1, grade)
        ws_sub.update_cell(i, conf_idx + 1, confidence)
        log(f"✅ Row {i} graded: {grade}, {confidence}")
