from pathlib import Path
import requests

def download_image(url: str, filename: str) -> str | None:
    if not url:
        return None
    path = Path("data/images") / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
        return str(path)
    except Exception:
        return None
