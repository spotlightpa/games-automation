from modules.auth import get_gspread_client
from modules import config

def get_sheet_and_ws():
    client = get_gspread_client()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    ws = sheet.worksheet("Games")
    return client, sheet, ws