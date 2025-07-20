from modules import grading
from helpers.utils import rows_to_dicts
from modules.logging_utils import log


# Populate winners
def populate_winners_tab(sheet):
    # Access worksheets
    riddles_ws = sheet.worksheet("Games")
    submissions_ws = sheet.worksheet("Submissions")
    winners_ws = sheet.worksheet("Winners")

    # Load riddle data from the "Games" worksheet
    riddles_raw = riddles_ws.get_all_values()
    riddles_header = riddles_raw[0]
    riddles_data = rows_to_dicts(riddles_raw[2:], riddles_header)

    # Build a mapping of Case Number to riddle row dictionary
    riddles_by_case = {
        str(row.get("Case Number")): row for row in riddles_data if row.get("Case Number")
    }

    # Load all submissions from the "Submissions" sheet
    sub_header = submissions_ws.get_all_values()[0]
    submissions_data = rows_to_dicts(submissions_ws.get_all_values()[2:], sub_header)
    all_submissions = submissions_data

    # Clear previous winners data (starting from row 3) to allow fresh re-population
    winner_headers = winners_ws.row_values(1)
    num_columns = len(winner_headers)
    last_row = len(winners_ws.get_all_values())
    if last_row >= 3:
        winners_ws.update(
            range_name=f"A3:{chr(64 + num_columns)}{last_row}",
            values=[["" for _ in range(num_columns)] for _ in range(last_row - 2)]
        )

    all_rows = []  # Final list of rows to insert into the Winners sheet

    # Iterate through each case in order
    for case_str, riddle in riddles_by_case.items():
        if not case_str.isdigit():
            continue  # Skip non-numeric or malformed case numbers

        case_number = int(case_str)
        game = riddle.get("Game", "")
        prev_case_str = str(case_number - 1)

        # Lookup previous case's clue and answer for context
        prev_riddle = riddles_by_case.get(prev_case_str)
        prev_clue = prev_riddle.get("Question", "").strip() if prev_riddle else ""
        prev_answer = prev_riddle.get("Answer", "").strip() if prev_riddle else ""

        # Find all correct submissions for the current case
        correct_entries = [
            e for e in all_submissions
            if str(e.get("Case Number")) == case_str and grading.is_marked_correct(e)
        ]

        # Build a sorted dictionary of winner names to emails
        winner_names = sorted(
            {
                f"{e['First Name']} {e['Last Name Initial']}.": e.get("Email", "")
                for e in correct_entries
                if e.get("First Name") and e.get("Last Name Initial")
            }.items()
        )

        # Generate strings for display
        winners_str = ", ".join(name for name, _ in winner_names)
        emails_str = ", ".join(email for _, email in winner_names)

        # Prepare newsletter message if there are winners
        full_text = ""
        if winner_names:
            full_text = (
                f"Congrats to (FIRST LAST INITIAL., skip for now), who will receive Spotlight PA swag."
                f" Others who answered correctly: {winners_str}"
            )

        # Compose a row for the Winners tab
        row = [
            case_number,
            game,
            "",             # Swag Winner (TK)
            "",             # Swag Winner Email (TK)
            winners_str,    # All winner names
            emails_str,     # All winner emails
            prev_clue,      # Previous riddle's clue
            prev_answer,    # Previous riddle's answer
            full_text       # Newsletter text
        ]

        all_rows.append(row)

    # Write all winner rows
    if all_rows:
        winners_ws.update(
            range_name=f"A3:I{2 + len(all_rows)}",
            values=all_rows,
            value_input_option="USER_ENTERED"
        )

    log(f"✅ Populated {len(all_rows)} winner rows.")