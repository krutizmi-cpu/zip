from thefuzz import process
import re

def normalize_name(value: str) -> str:
    value = str(value or "").lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value

def find_best_matches(name: str, choices: list[str], limit: int = 5):
    return process.extract(name, choices, limit=limit)
