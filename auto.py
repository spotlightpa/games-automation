import os
import yaml
import gspread
from openai import OpenAI
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# OAuth scopes for Sheets and Gmail
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/gmail.readonly'
]

# Directory paths
BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'sheet_ids.yaml')
TOKEN_PATH = os.path.join(BASE_DIR, 'config', 'token.json')
CREDS_PATH = os.path.join(BASE_DIR, 'config', 'credentials-oauth.json')

# Load spreadsheet ID
with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)
SPREADSHEET_ID = CONFIG['games_admin']

# OpenAI API key retrieval
def get_openai_key():
    key = os.getenv("OPENAI_API_KEY")
    if key:
        return key
    key_path = os.path.join(BASE_DIR, 'config', 'openai_key.txt')
    if os.path.exists(key_path):
        with open(key_path, 'r') as f:
            return f.read().strip()
    raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY env variable or add to config/openai_key.txt")

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
            with open(TOKEN_PATH, 'w') as token_file:
                token_file.write(creds.to_json())
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())
    return creds

# Initialize Google Sheets client
creds = get_credentials()
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID)
ws = sheet.worksheet('Riddles')

# Rich-text formatting for riddles
def write_riddle_with_formatting(row: int):
    cells = ws.row_values(row)
    if len(cells) < 4 or not all(cells[:4]):
        print(f"Skipping row {row}, incomplete data.")
        return
    case_no, teaser, question, _ = cells[:4]
    teaser_upper = teaser.upper().strip()
    case_text = f"(Case No. {case_no}):"
    full_text = f"{teaser_upper} {case_text} {question}"
    start_case = len(teaser_upper) + 1
    end_case = start_case + len(case_text)
    requests = [{
        'updateCells': {
            'range': {
                'sheetId': ws._properties['sheetId'],
                'startRowIndex': row - 1,
                'endRowIndex': row,
                'startColumnIndex': 4,
                'endColumnIndex': 5
            },
            'rows': [{
                'values': [{
                    'userEnteredValue': {'stringValue': full_text},
                    'textFormatRuns': [
                        {'startIndex': start_case, 'format': {'bold': True, 'italic': True}},
                        {'startIndex': end_case, 'format': {'bold': False, 'italic': False}}
                    ]
                }]
            }],
            'fields': 'userEnteredValue,textFormatRuns'
        }
    }]
    sheet.batch_update({'requests': requests})
    print(f"Formatted row {row} successfully.")

# OpenAI API call to generate grading logic
def generate_grading_logic(question: str, answer: str) -> str:
    prompt = f"""{GENERIC_GRADING_INSTRUCTIONS}

Riddle Question: {question}
Correct Answer: {answer}

Provide grading logic:"""
    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=150
    )
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
            print(f"Skipping row {row_number}, incomplete question or answer.")
            continue

        if grading_prompt.strip():
            print(f"Row {row_number} already has grading prompt, skipping.")
            continue

        print(f"Generating grading logic for row {row_number}...")
        grading_logic = generate_grading_logic(question, answer)

        ws.update_cell(row_number, 6, f"AI: {grading_logic}")
        print(f"Updated grading logic for row {row_number}.")

# Formatting and populating AI logic
def format_and_populate_all():
    populate_ai_grading_prompts()
    total_rows = len(ws.get_all_values())
    for row in range(3, total_rows + 1):
        write_riddle_with_formatting(row)

# Main entry point
if __name__ == '__main__':
    print("Starting full automation: formatting and AI grading logic...")
    format_and_populate_all()
    print("Automation completed successfully.")
