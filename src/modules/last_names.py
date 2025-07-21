def normalize_last_initial(value: str) -> str:
    value = value.strip()
    if not value:
        return ""

    # Take only the first character
    initial = value[0]

    # If it's a letter, return as uppercase
    if initial.isalpha():
        return initial.upper()

    # If not valid (number or symbol), skip it
    return ""
