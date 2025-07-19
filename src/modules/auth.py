import os

import gspread
from openai import OpenAI
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from modules import config


def get_credentials():
    # Handles OAuth authorization:
    creds = None

    # Load existing credentials if present
    if os.path.exists(config.TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(config.TOKEN_PATH, config.SCOPES)

        # Refresh credentials if expired
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(config.TOKEN_PATH, "w") as token_file:
                token_file.write(creds.to_json())

    # If no valid credentials, initiate a new login flow
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(config.CREDS_PATH, config.SCOPES)
        creds = flow.run_local_server(port=0)

        # Save the newly acquired credentials for future use
        with open(config.TOKEN_PATH, "w") as token_file:
            token_file.write(creds.to_json())

    return creds


def get_gspread_client():
    # Returns an authorized gspread client using Google OAuth credentials
    creds = get_credentials()
    return gspread.authorize(creds)


def get_openai_client():
    # Returns a configured OpenAI client instance
    key = os.getenv("OPENAI_API_KEY")
    if key:
        return OpenAI(api_key=key)

    if os.path.exists(config.OPENAI_KEY_PATH):
        with open(config.OPENAI_KEY_PATH, "r") as f:
            return OpenAI(api_key=f.read().strip())

    raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY or add openai_key.txt")


# Default instruction prompt for generating AI grading logic
GENERIC_GRADING_INSTRUCTIONS = """
You are tasked with creating concise grading logic to evaluate if a user's answer to a riddle is correct. 
Do not restate these general rules. Assume trivial differences like punctuation, capitalization, or filler words are ignored. 
Be specific about variations or synonyms that should be accepted or rejected. Provide guidance in 1-3 sentences.
"""

