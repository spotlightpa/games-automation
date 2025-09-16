import os
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Scopes required for Google Sheets and Gmail API access
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.readonly",
]

# Base directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Config file locations
CONFIG_DIR = os.path.join(BASE_DIR, "config")
CONFIG_PATH = os.path.join(CONFIG_DIR, "sheet_ids.yaml")
TOKEN_PATH = os.path.join(CONFIG_DIR, "token.json")
CREDS_PATH = os.path.join(CONFIG_DIR, "credentials-oauth.json")
OPENAI_KEY_PATH = os.path.join(CONFIG_DIR, "openai_key.txt")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()

if not SPREADSHEET_ID:
    # Try YAML only if it exists
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                _CONFIG = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML in {CONFIG_PATH}: {e}")
        except Exception as e:
            raise RuntimeError(f"Unable to read {CONFIG_PATH}: {e}")

        if "games_admin" not in _CONFIG or not str(_CONFIG["games_admin"]).strip():
            raise KeyError(
                f"Missing 'games_admin' key in {CONFIG_PATH}. "
                "Either add it, or set the SPREADSHEET_ID environment variable."
            )
        SPREADSHEET_ID = str(_CONFIG["games_admin"]).strip()
    else:
        # Neither env var nor YAML is available
        raise FileNotFoundError(
            "No spreadsheet configured. Set SPREADSHEET_ID as an environment variable "
            "OR add config/sheet_ids.yaml"
        )

# Gmail label IDs
RIDDLE_LABEL_ID = os.getenv("RIDDLE_LABEL_ID")
SCRAMBLER_LABEL_ID = os.getenv("SCRAMBLER_LABEL_ID")
PUZZLER_LABEL_ID = os.getenv("PUZZLER_LABEL_ID")