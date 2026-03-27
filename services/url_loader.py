from pathlib import Path
import requests

DOWNLOAD_DIR = Path("data/downloads")

def normalize_google_sheet_url(url: str) -> str:
    if "docs.google.com/spreadsheets" in url:
        base = url.split("/edit")[0]
        return base + "/export?format=xlsx"
    return url

def download_supplier_source(url: str, supplier_key: str) -> Path:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    final_url = normalize_google_sheet_url(url)
    target = DOWNLOAD_DIR / f"{supplier_key}.xlsx"
    with requests.get(final_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(target, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
    return target
