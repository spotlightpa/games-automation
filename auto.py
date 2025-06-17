import os
import yaml
import gspread
import re
import requests

from openai import OpenAI
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv


load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def slack_log(message: str):
    try:
        requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=5
        )
    except Exception as e:
        print(f"Slack logging failed: {e}")

def log(message: str):
    print(message)
    slack_log(message)



# OAuth scopes for Sheets and Gmail
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.readonly",
]

# Directory paths
BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, "config", "sheet_ids.yaml")
TOKEN_PATH = os.path.join(BASE_DIR, "config", "token.json")
CREDS_PATH = os.path.join(BASE_DIR, "config", "credentials-oauth.json")

# Load spreadsheet ID
with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)
SPREADSHEET_ID = CONFIG["games_admin"]


# OpenAI API key retrieval
def get_openai_key():
    key = os.getenv("OPENAI_API_KEY")
    if key:
        return key
    key_path = os.path.join(BASE_DIR, "config", "openai_key.txt")
    if os.path.exists(key_path):
        with open(key_path, "r") as f:
            return f.read().strip()
    raise ValueError(
        "OpenAI API key not found. Set OPENAI_API_KEY env variable or add to config/openai_key.txt"
    )


openai_client = OpenAI(api_key=get_openai_key())

# Default grading instructions
GENERIC_GRADING_INSTRUCTIONS = """
You are tasked with creating concise grading logic to evaluate if a user's answer to a riddle is correct. 
Do not restate these general rules. Assume trivial differences like punctuation, capitalization, or filler words are ignored. 
Be specific about variations or synonyms that should be accepted or rejected. Provide guidance in 1-3 sentences.
"""


# Google OAuth credential management
def get_credentials():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, "w") as token_file:
                token_file.write(creds.to_json())
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w") as token_file:
            token_file.write(creds.to_json())
    return creds


# Initialize Google Sheets client
creds = get_credentials()
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID)
ws = sheet.worksheet("Games")

# Token tracking
total_token_cost = 0.0


# Rich-text formatting for riddles
def write_riddle_with_formatting(row: int):
    cells = ws.row_values(row)
    if len(cells) < 4 or not all(cells[:4]):
        log(f"Skipping row {row}, incomplete data.")
        return
    case_no, teaser, question, _ = cells[:4]
    teaser_upper = teaser.upper().strip()
    case_text = f"(Case No. {case_no}):"
    full_text = f"{teaser_upper} {case_text} {question}"
    start_case = len(teaser_upper) + 1
    end_case = start_case + len(case_text)
    requests = [
        {
            "updateCells": {
                "range": {
                    "sheetId": ws._properties["sheetId"],
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": 4,
                    "endColumnIndex": 5,
                },
                "rows": [
                    {
                        "values": [
                            {
                                "userEnteredValue": {"stringValue": full_text},
                                "textFormatRuns": [
                                    {
                                        "startIndex": start_case,
                                        "format": {"bold": True, "italic": True},
                                    },
                                    {
                                        "startIndex": end_case,
                                        "format": {"bold": False, "italic": False},
                                    },
                                ],
                            }
                        ]
                    }
                ],
                "fields": "userEnteredValue,textFormatRuns",
            }
        }
    ]
    sheet.batch_update({"requests": requests})
    log(f"Formatted row {row} successfully.")

# Backfill Game column using Case Number
def backfill_game_column(target_ws_name):
    log(f"Backfilling Game column in sheet: {target_ws_name}")
    target_ws = sheet.worksheet(target_ws_name)

    riddles_data = ws.get_all_records()
    game_map = {
        str(r.get("Case Number")): r.get("Game", "") for r in riddles_data if r.get("Case Number")
    }

    rows = target_ws.get_all_values()
    headers = rows[0]
    header_map = {h.strip(): i for i, h in enumerate(headers)}

    if "Game" not in header_map:
        headers.insert(1, "Game")
        target_ws.update("A1", [headers])
        header_map = {h.strip(): i for i, h in enumerate(headers)}

    game_col_index = header_map["Game"] + 1

    for i in range(2, len(rows)):
        row = rows[i]
        while len(row) < len(headers):
            row.append("")
        case = row[header_map["Case Number"]].strip()
        if case and not row[header_map["Game"]].strip():
            game = game_map.get(case, "")
            if game:
                target_ws.update_cell(i + 1, game_col_index, game)
                log(f"Filled Game for row {i+1}: {game}")

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
        max_tokens=150,
    )

    usage = response.usage
    input_tokens = usage.prompt_tokens
    output_tokens = usage.completion_tokens
    total_tokens = usage.total_tokens

    input_cost = input_tokens * 0.005 / 1000
    output_cost = output_tokens * 0.015 / 1000
    total_cost = input_cost + output_cost
    total_token_cost += total_cost

    log(
        f"🔎 Tokens used — Prompt: {input_tokens}, Completion: {output_tokens}, Total: {total_tokens}"
    )
    log(f"💵 Estimated cost: ${total_cost:.6f}")

    return response.choices[0].message.content.strip()


# Populate empty AI Grading Prompt cells
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


# Helpers
def rows_to_dicts(data_rows, header):
    return [
        dict(zip(header, row)) for row in data_rows if any(cell.strip() for cell in row)
    ]


# Determines if a submission is marked correct (considering override first)
def is_marked_correct(entry):
    override = entry.get("Override", "").strip().lower() if "Override" in entry else ""
    grade = entry.get("AI Grade", "").strip().lower() if "AI Grade" in entry else ""
    return override == "correct" or (not override and grade == "correct")


# OpenAI-based grading using per-riddle grading prompt
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
        max_tokens=150,
    )

    usage = response.usage
    input_tokens = usage.prompt_tokens
    output_tokens = usage.completion_tokens
    total_tokens = usage.total_tokens

    input_cost = input_tokens * 0.005 / 1000
    output_cost = output_tokens * 0.015 / 1000
    total_cost = input_cost + output_cost
    total_token_cost += total_cost

    log(
        f"🔎 Tokens used — Prompt: {input_tokens}, Completion: {output_tokens}, Total: {total_tokens}"
    )
    log(f"💵 Estimated cost: ${total_cost:.6f}")

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

    for i in range(3, len(all_rows) + 1):
        row = ws_sub.row_values(i)
        while len(row) < len(headers):
            row.append("")

        if row[header_map["AI Grade"]].strip():
            continue  # already graded

        case = row[header_map["Case Number"]].strip()
        user_answer = row[header_map["Answer"]].strip()

        if not case or not user_answer or case not in grading_map:
            continue

        grading_prompt = grading_map[case]
        grade, confidence = grade_submission_entry(grading_prompt, user_answer)

        ws_sub.update_cell(i, header_map["AI Grade"] + 1, grade)
        ws_sub.update_cell(i, header_map["AI Confidence"] + 1, confidence)
        log(f"✅ Row {i} graded: {grade}, {confidence}")


# Populate winners
def populate_winners_tab():
    riddles_ws = sheet.worksheet("Games")
    submissions_ws = sheet.worksheet("Submissions")
    historical_ws = sheet.worksheet("Historical Submissions")
    winners_ws = sheet.worksheet("Winners")

    # Load Riddles
    riddles_raw = riddles_ws.get_all_values()
    riddles_header = riddles_raw[0]
    riddles_data = rows_to_dicts(riddles_raw[2:], riddles_header)

    # Load Submissions
    sub_header = submissions_ws.get_all_values()[0]
    submissions_data = rows_to_dicts(submissions_ws.get_all_values()[2:], sub_header)
    historical_data = rows_to_dicts(historical_ws.get_all_values()[2:], sub_header)
    all_submissions = submissions_data + historical_data

    # Clear data in rows 3 and below
    last_row = len(winners_ws.get_all_values())
    if last_row >= 3:
        winners_ws.update([[""] * 6] * (last_row - 2), f"A3:F{last_row}")

    all_rows = []

    for riddle in riddles_data:
        case_number = riddle.get("Case Number")
        if not case_number or not str(case_number).isdigit():
            continue
        case_number = int(case_number)
        prev_case_number = case_number - 1

        # Previous clue/answer
        prev_riddle = next(
            (
                r
                for r in riddles_data
                if str(r.get("Case Number")) == str(prev_case_number)
            ),
            None,
        )
        prev_clue = prev_riddle.get("Question", "").strip() if prev_riddle else ""
        prev_answer = prev_riddle.get("Answer", "").strip() if prev_riddle else ""

        # Correct entries
        correct_entries = [
            e
            for e in all_submissions
            if str(e.get("Case Number")).isdigit()
            and int(e["Case Number"]) == case_number
            and is_marked_correct(e)
        ]

        winner_names = sorted(
            {
                f"{e['First Name']} {e['Last Name Initial']}."
                for e in correct_entries
                if e.get("First Name") and e.get("Last Name Initial")
            }
        )

        winners_str = ", ".join(winner_names)
        full_text = ""
        if winner_names:
            full_text = (
                f"Congrats to (FIRST LAST INITIAL., skip for now), who will receive Spotlight PA swag."
                f" Others who answered correctly: {winners_str}"
            )

        row = [
            case_number,
            "",  # Swag Winner TK
            winners_str,
            prev_clue,
            prev_answer,
            full_text,
        ]

        all_rows.append(row)

    # ✅ Write to row 3 and down only
    if all_rows:
        winners_ws.update(
            all_rows, f"A3:F{2 + len(all_rows)}", value_input_option="USER_ENTERED"
        )

    log(f"✅ Populated {len(all_rows)} winner rows.")


# Run all steps
def format_and_populate_all():
    backfill_game_column("Submissions")
    backfill_game_column("Historical Submissions")
    backfill_game_column("Winners")
    populate_ai_grading_prompts()
    total_rows = len(ws.get_all_values())
    for row in range(3, total_rows + 1):
        write_riddle_with_formatting(row)
    grade_submissions_for_sheet("Submissions")
    grade_submissions_for_sheet("Historical Submissions")
    populate_winners_tab()
    log(f"💰 Total estimated OpenAI token cost: ${total_token_cost:.6f}")


# Main entry point
if __name__ == "__main__":
    log("🚀 Starting full automation: formatting, grading, and winner population...")
    format_and_populate_all()
    log("✅ Automation completed successfully.")

