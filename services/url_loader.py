from pathlib import Path
import requests

def download_by_url(url: str, target_path: Path, timeout: int = 60):
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(target_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
    return target_path

def google_sheet_to_export_xlsx(url: str) -> str:
    if "/edit" not in url:
        return url
    return url.split("/edit")[0] + "/export?format=xlsx"
