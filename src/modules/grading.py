import re
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

# OpenAI API call to generate grading logic
def generate_grading_logic(question: str, answer: str) -> str:
  
    global total_token_cost

    prompt = f"""{GENERIC_GRADING_INSTRUCTIONS}

    Riddle Question: {question}
    Correct Answer: {answer}

    Provide grading logic:"""

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=150
    )

    # Extract token usage stats from the API response
    total_token_cost += log_token_usage(response.usage)

    return response.choices[0].message.content.strip()


# OpenAI grading using per-riddle grading prompt
# TK update this so it ignores email signatures and stuff
def grade_submission_entry(grading_prompt, user_answer):
    global total_token_cost

    prompt = f"""{grading_prompt.strip()}

    You are grading a riddle submission. Use the information above to determine if the user's answer is correct. Then estimate how confident you are in your judgment — not based on surface similarity, but on how well the answer logically matches the riddle's requirements.

    Use your full reasoning ability and the grading logic to determine confidence, just as a human editor would. Avoid mechanical scoring — your confidence should reflect your actual certainty in the answer being right or wrong.

    Respond in this format exactly:
    Correctness: Correct or Incorrect
    Confidence: [number from 0 to 100]

    User's Answer: {user_answer}"""

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=150
    )

    total_token_cost += log_token_usage(response.usage)

    content = response.choices[0].message.content.strip()

    match_grade = re.search(
        r"Correctness:\s*(Correct|Incorrect)", content, re.IGNORECASE
    )
    match_conf = re.search(r"Confidence:\s*(\d+)", content)

    if not match_grade or not match_conf:
        log(f"⚠️ AI response could not be parsed:\n{content}")
        return "Uncertain", "N/A"

    grade = match_grade.group(1).capitalize()
    confidence = f"{match_conf.group(1)}%"
    return grade, confidence

def populate_ai_grading_prompts():
    all_rows = ws.get_all_values()
    total_rows = len(all_rows)

    for row_number in range(3, total_rows + 1):
        row = all_rows[row_number - 1]

        question = row[2] if len(row) > 2 else ""
        answer = row[3] if len(row) > 3 else ""
        grading_prompt = row[5] if len(row) > 5 else ""

        if not question or not answer:
            log(f"Skipping row {row_number}, incomplete question or answer.")
            continue

        if grading_prompt.strip():
            log(f"Row {row_number} already has grading prompt, skipping.")
            continue

        log(f"Generating grading logic for row {row_number}...")
        grading_logic = generate_grading_logic(question, answer)

        ws.update_cell(row_number, 6, f"AI: {grading_logic}")
        log(f"Updated grading logic for row {row_number}.")

# Determines if a submission is marked correct, considering override first
def is_marked_correct(entry):
    override = entry.get("Override", "").strip().lower() if "Override" in entry else ""

    grade = entry.get("AI Grade", "").strip().lower() if "AI Grade" in entry else ""

    # A submission is considered correct if:
    # It has an explicit override marked "correct"
    # OR the override is blank and the AI Grade is "correct"
    return override == "correct" or (not override and grade == "correct")

# Grade only blank submissions in a sheet
def grade_submissions_for_sheet(sheet_name):
    log(f"Grading submissions in sheet: {sheet_name}")

    ws_sub = sheet.worksheet(sheet_name)

    all_rows = ws_sub.get_all_values()
    headers = all_rows[0]

    header_map = {h.strip(): i for i, h in enumerate(headers)}

    required = ["AI Grade", "AI Confidence", "Override"]
    modified = False
    for col in required:
        if col not in header_map:
            headers.append(col)
            modified = True

    if modified:
        ws_sub.update("A1", [headers])
        header_map = {h.strip(): i for i, h in enumerate(headers)}

    riddles_data = rows_to_dicts(ws.get_all_values()[2:], ws.get_all_values()[0])

    grading_map = {
        str(r.get("Case Number")): r.get("AI Grading Prompt")
        or generate_grading_logic(r.get("Question", ""), r.get("Answer", ""))
        for r in riddles_data
        if r.get("Case Number")
    }

    all_rows = ws_sub.get_all_values()

    for i, row in enumerate(all_rows[2:], start=3):
        while len(row) < len(headers):
            row.append("")

        if row[header_map["AI Grade"]].strip():
            continue

        case = row[header_map["Case Number"]].strip()
        user_answer = row[header_map["Answer"]].strip()

        if not case or not user_answer or case not in grading_map:
            continue

        grading_prompt = grading_map[case]
        grade, confidence = grade_submission_entry(grading_prompt, user_answer)

        ws_sub.update_cell(i, header_map["AI Grade"] + 1, grade)
        ws_sub.update_cell(i, header_map["AI Confidence"] + 1, confidence)
        log(f"✅ Row {i} graded: {grade}, {confidence}")