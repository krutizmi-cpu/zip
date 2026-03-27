from pathlib import Path
import requests
import hashlib

def download_images_for_df(df):
    df = df.copy()
    local_paths = []

    img_dir = Path("data/images")
    img_dir.mkdir(parents=True, exist_ok=True)

    for _, row in df.iterrows():
        url = row.get("image_url")
        if not url:
            local_paths.append("")
            continue

        try:
            ext = ".jpg"
            name_hash = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]
            file_path = img_dir / f"{name_hash}{ext}"
            if not file_path.exists():
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                with open(file_path, "wb") as f:
                    f.write(r.content)
            local_paths.append(str(file_path))
        except Exception:
            local_paths.append("")

    df["local_image"] = local_paths
    return df
