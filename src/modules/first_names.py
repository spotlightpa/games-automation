import re
from modules.logging_utils import log

def normalize_first_name(name: str) -> str:
    name = name.strip()

    # Handle random characters
    name = re.sub(r"[^\x00-\x7F]+", "", name)

    # Handle emails: take prefix before '@'
    if "@" in name:
        name = name.split("@")[0]

    # If all upper or all lower, capitalize first letter only
    if name.isupper() or name.islower():
        name = name.capitalize()

    # Leave mixed-case names (like MaryAnne) alone
    return name
