import os
import json
import base64
import gspread
from openai import OpenAI
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from modules import config

# Normalize curly quotes
SMART_QUOTES = {
    ord("“"): '"', ord("”"): '"', ord("„"): '"', ord("‟"): '"',
    ord("’"): "'", ord("‘"): "'", ord("‚"): "'", ord("‛"): "'",
}


def _load_token_from_env():
    tj_b64 = (os.getenv("TOKEN_JSON_BASE64") or "").strip()
    if tj_b64:
        try:
            data = base64.b64decode(tj_b64)
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"TOKEN_JSON_BASE64 could not be decoded/parsed: {e}")

    tj = (os.getenv("TOKEN_JSON") or "").strip()
    if tj:
        normalized = tj.translate(SMART_QUOTES)
        try:
            return json.loads(normalized)
        except json.JSONDecodeError as e:
            hint = (
                "Your TOKEN_JSON secret isn't valid JSON. "
                "Use straight quotes (\") and include fields like "
                '"client_id", "client_secret", "refresh_token", "token_uri", "type":"authorized_user". '
                "Better yet, use TOKEN_JSON_BASE64."
            )
            raise ValueError(f"Invalid TOKEN_JSON JSON: {e}\n{hint}")

    return None


def _load_token_from_file():
    path = config.TOKEN_PATH
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().translate(SMART_QUOTES)
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"{path} exists but is not valid JSON: {e}\n"
            "It must be an 'authorized user' token (not client secrets)."
        )


def get_credentials():
    info = _load_token_from_env()
    if info:
        return Credentials.from_authorized_user_info(info, scopes=config.SCOPES)

    info = _load_token_from_file()
    if info:
        return Credentials.from_authorized_user_info(info, scopes=config.SCOPES)

    client_secrets = config.CREDS_PATH
    if os.path.exists(client_secrets):
        flow = InstalledAppFlow.from_client_secrets_file(client_secrets, config.SCOPES)
        creds = flow.run_local_server(port=0)
        os.makedirs(os.path.dirname(config.TOKEN_PATH), exist_ok=True)
        with open(config.TOKEN_PATH, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
        return creds

    raise FileNotFoundError(
        "No Google OAuth token found.\n"
        "Provide one of:\n"
        "  • TOKEN_JSON_BASE64 (preferred in CI) — base64 of your token.json\n"
        "  • TOKEN_JSON — raw JSON of token.json (with straight quotes)\n"
        f"  • File at {config.TOKEN_PATH}\n"
        f"Or for local dev, add client secrets at {config.CREDS_PATH} to run an OAuth flow."
    )


def get_gspread_client():
    # Returns an authorized gspread client using Google OAuth credentials
    creds = get_credentials()
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return gspread.authorize(creds)


def get_openai_client():
    # Returns a configured OpenAI client instance
    key = os.getenv("OPENAI_API_KEY")
    if key:
        return OpenAI(api_key=key)

    if os.path.exists(config.OPENAI_KEY_PATH):
        with open(config.OPENAI_KEY_PATH, "r", encoding="utf-8") as f:
            return OpenAI(api_key=f.read().strip())

    raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY or add openai_key.txt")


# Default instruction prompt for generating AI grading logic
GENERIC_GRADING_INSTRUCTIONS = """
You are tasked with creating concise grading logic to evaluate if a user's answer to a riddle is correct.
Do not restate general rules. Assume trivial differences like punctuation, capitalization, or filler words are ignored.
Be specific about variations or synonyms that should be accepted or rejected. Provide guidance in 1–3 sentences.
"""
