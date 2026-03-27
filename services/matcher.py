import re
import pandas as pd
from thefuzz import process

def normalize_name(value: str) -> str:
    value = str(value or "").lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value

def build_master_from_offers(offers_df: pd.DataFrame):
    if offers_df.empty:
        return [], []

    grouped = []
    mappings = []
    master_id = 1

    article_df = offers_df[offers_df["supplier_article"].fillna("") != ""].copy()
    no_article_df = offers_df[offers_df["supplier_article"].fillna("") == ""].copy()

    if not article_df.empty:
        for article, grp in article_df.groupby("supplier_article"):
            best_price = grp["base_price"].dropna().min() if "base_price" in grp.columns else None
            stock_sum = grp["stock"].dropna().sum() if "stock" in grp.columns else None
            image = next((x for x in grp["local_image"].fillna("").tolist() if x), "")
            name = grp.iloc[0]["name"]
            norm = grp.iloc[0]["normalized_name"]

            grouped.append((str(article), name, norm, best_price, int(stock_sum) if pd.notna(stock_sum) else None, image))
            for _, r in grp.iterrows():
                mappings.append((
                    r["supplier"], r.get("supplier_article"), r["name"], r["normalized_name"],
                    master_id, "article_exact", 100.0
                ))
            master_id += 1

    masters_names = [g[2] for g in grouped]
    for _, r in no_article_df.iterrows():
        norm = r["normalized_name"]
        if not masters_names:
            grouped.append((None, r["name"], norm, r.get("base_price"), r.get("stock"), r.get("local_image", "")))
            mappings.append((r["supplier"], None, r["name"], norm, master_id, "new_name", 100.0))
            masters_names.append(norm)
            master_id += 1
            continue

        match = process.extractOne(norm, masters_names)
        if match and match[1] >= 88:
            matched_norm = match[0]
            matched_master_index = masters_names.index(matched_norm) + 1
            mappings.append((r["supplier"], None, r["name"], norm, matched_master_index, "name_fuzzy", float(match[1])))
        else:
            grouped.append((None, r["name"], norm, r.get("base_price"), r.get("stock"), r.get("local_image", "")))
            mappings.append((r["supplier"], None, r["name"], norm, master_id, "new_name", 100.0))
            masters_names.append(norm)
            master_id += 1

    return grouped, mappings

def find_suggestions(target_name: str, master_names: list[str], limit: int = 5):
    if not master_names:
        return []
    return process.extract(target_name, master_names, limit=limit)
