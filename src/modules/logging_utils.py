import os
import requests
from dotenv import load_dotenv

load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def slack_log(message: str):
    if not SLACK_WEBHOOK_URL:
        return
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
