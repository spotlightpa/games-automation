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

# Paths to config files and tokens
CONFIG_PATH = os.path.join(BASE_DIR, "config", "sheet_ids.yaml")
TOKEN_PATH = os.path.join(BASE_DIR, "config", "token.json")
CREDS_PATH = os.path.join(BASE_DIR, "config", "credentials-oauth.json")

# Load config YAML and get the spreadsheet ID
try:
    with open(CONFIG_PATH) as f:
        CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")
except yaml.YAMLError as e:
    raise ValueError(f"Failed to parse YAML in {CONFIG_PATH}: {e}")

if "games_admin" not in CONFIG:
    raise KeyError("Missing 'games_admin' key in sheet_ids.yaml")

SPREADSHEET_ID = CONFIG["games_admin"]

# Make the label ID accessible everywhere
RIDDLE_LABEL_ID = os.getenv("RIDDLE_LABEL_ID")
SCRAMBLER_LABEL_ID = os.getenv("SCRAMBLER_LABEL_ID")