import io
import re
import zipfile
import hashlib
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import requests
import streamlit as st
from rapidfuzz import fuzz
from openpyxl import load_workbook


st.set_page_config(page_title="Price Aggregator", layout="wide")

SUPPLIERS = {
    "s1": {
        "label": "1. Velozapchasti",
        "sheet_name": "Sheet1",
        "header_row": 7,
        "source_type": "file",
        "default_price_tier": "price",
        "allowed_price_tiers": ["price"],
    },
    "s2": {
        "label": "2. Форвард СПб",
        "sheet_name": "Лист_1",
        "header_row": 5,
        "source_type": "file",
        "default_price_tier": "price",
        "allowed_price_tiers": ["price", "price_opt2", "price_opt3", "price_rrc"],
    },
    "s3": {
        "label": "3. Колхозник / Монстр",
        "sheet_name": "Лист1",
        "header_row": 1,
        "source_type": "url",
        "default_price_tier": "price",
        "allowed_price_tiers": ["price", "price_opt10", "price_opt50"],
    },
    "s4": {
        "label": "4. BRATAN",
        "sheet_name": "Лена 25.05.24",
        "header_row": 0,
        "source_type": "url",
        "default_price_tier": "price",
        "allowed_price_tiers": ["price", "own_price", "price_rrc"],
    },
}

PRICE_TIER_LABELS = {
    "price": "Цена",
    "price_opt2": "Опт 2",
    "price_opt3": "Опт 3",
    "price_opt10": "От 10 шт",
    "price_opt50": "От 50 шт",
    "own_price": "Своим",
    "price_rrc": "РРЦ",
}

CATEGORY_RULES = [
    ("Велосипеды", ["велосипед", "электровелосипед", "байк", "bmx", "bike"]),
    ("Колёса и покрышки", ["колесо", "обод", "втулк", "спиц", "покрыш", "камера", "шина", "wheel", "rim"]),
    ("Тормоза", ["тормоз", "колодк", "диск тормоз", "ротор", "brake"]),
    ("Трансмиссия", ["цепь", "кассет", "трещот", "переключател", "звезд", "шатун", "каретк", "педал", "chain"]),
    ("Рули и управление", ["руль", "вынос", "грипс", "манетк", "ручк", "handlebar"]),
    ("Седла и подседелы", ["седл", "подседел", "хомут подсед", "seatpost"]),
    ("Вилки и амортизация", ["вилка", "амортиз", "fork", "shock"]),
    ("Освещение и электрика", ["фонарь", "фара", "свет", "контроллер", "дисплей", "мотор", "мотор-колес", "электро", "кабель"]),
    ("Аккумуляторы и зарядка", ["аккумулятор", "акб", "зарядн", "charger", "battery", "lifepo4"]),
    ("Запчасти и аксессуары", ["крыл", "багажник", "поднож", "зеркал", "крепеж", "замок", "болт", "гайк", "проставка"]),
]

DIAMETER_PATTERNS = [
    (r"\b(12|14|16|18|20|24|26|27\.5|28|29)\b", "Диаметр"),
    (r"\b(12|14|16|18|20|24|26|27\.5|28|29)''", "Диаметр"),
    (r"\b(12|14|16|18|20|24|26|27\.5|28|29)д", "Диаметр"),
]

VOLTAGE_RE = re.compile(r"\b(24|36|48|52|60|72)\s*v\b", re.I)
AH_RE = re.compile(r"\b(\d{1,2}(?:\.\d+)?)\s*ah\b", re.I)
WATT_RE = re.compile(r"\b(\d{2,5})\s*ват", re.I)


def init_state():
    defaults = {
        "offers_by_supplier": {},
        "images_by_supplier": {},
        "master_df": pd.DataFrame(),
        "mapping_df": pd.DataFrame(),
        "r2_exists_cache": {},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def normalize_name(value: str) -> str:
    value = str(value or "").lower().strip()
    value = re.sub(r"[\"'`]", "", value)
    value = re.sub(r"[^\w\s\.\-\/а-яА-Я]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def signature_name(value: str) -> str:
    value = normalize_name(value)
    tokens = [t for t in value.split() if t not in {
        "для", "и", "в", "на", "с", "под", "комплект", "шт", "шт.", "новый", "новая"
    }]
    return " ".join(tokens)


def extract_specs(text: str) -> dict:
    text = str(text or "")
    norm = normalize_name(text)
    voltage = None
    ah = None
    watt = None
    diameter = None

    m = VOLTAGE_RE.search(norm)
    if m:
        voltage = m.group(1)

    m = AH_RE.search(norm)
    if m:
        ah = m.group(1)

    m = WATT_RE.search(norm)
    if m:
        watt = m.group(1)

    for pattern, _ in DIAMETER_PATTERNS:
        m = re.search(pattern, norm)
        if m:
            diameter = m.group(1)
            break

    return {"voltage": voltage, "ah": ah, "watt": watt, "diameter": diameter}


def to_float(value):
    if pd.isna(value) or value == "":
        return None
    value = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(value)
    except Exception:
        return None


def normalize_stock(value):
    if pd.isna(value) or value == "":
        return None
    text = str(value).strip().lower()
    mapping = {
        "нет": 0, "мало": 1, "много": 10, "более 10": 10,
        "скоро будут": 0, "поз заказ": 0, "распродажа": 1,
    }
    if text in mapping:
        return mapping[text]
    try:
        return int(float(text))
    except Exception:
        return None


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def infer_categories(name: str):
    norm = normalize_name(name)
    category_l1 = "Прочее"
    category_l2 = ""

    for cat, keywords in CATEGORY_RULES:
        if any(keyword in norm for keyword in keywords):
            category_l1 = cat
            break

    if category_l1 == "Колёса и покрышки":
        for pattern, _ in DIAMETER_PATTERNS:
            m = re.search(pattern, norm)
            if m:
                category_l2 = f"Диаметр {m.group(1)}"
                break
        if not category_l2:
            if "покрыш" in norm:
                category_l2 = "Покрышки"
            elif "камера" in norm:
                category_l2 = "Камеры"
            elif "обод" in norm:
                category_l2 = "Обода"
            elif "втулк" in norm:
                category_l2 = "Втулки"
            else:
                category_l2 = "Разное"
    elif category_l1 == "Аккумуляторы и зарядка":
        specs = extract_specs(norm)
        if specs["voltage"] and specs["ah"]:
            category_l2 = f"{specs['voltage']}V {specs['ah']}Ah"
        elif "аккумулятор" in norm or "акб" in norm or "battery" in norm:
            category_l2 = "Аккумуляторы"
        elif "заряд" in norm or "charger" in norm:
            category_l2 = "Зарядные устройства"
    elif category_l1 == "Тормоза":
        if "колодк" in norm:
            category_l2 = "Колодки"
        elif "ротор" in norm or "диск тормоз" in norm:
            category_l2 = "Роторы"
        else:
            category_l2 = "Тормоза"
    elif category_l1 == "Трансмиссия":
        if "цепь" in norm:
            category_l2 = "Цепи"
        elif "кассет" in norm or "трещот" in norm:
            category_l2 = "Кассеты и трещотки"
        elif "звезд" in norm:
            category_l2 = "Звезды"
        else:
            category_l2 = "Разное"
    elif category_l1 == "Велосипеды":
        category_l2 = "Электровелосипеды" if "электровелосипед" in norm else "Велосипеды"

    return category_l1, category_l2


def normalize_google_sheet_url(url: str) -> str:
    if "docs.google.com/spreadsheets" in url and "/edit" in url:
        return url.split("/edit")[0] + "/export?format=xlsx"
    return url


def read_source_bytes(source_type, uploaded_file, source_url):
    if source_type == "file":
        if uploaded_file is None:
            raise ValueError("Нужно загрузить файл.")
        return uploaded_file.name, uploaded_file.getvalue()

    if not source_url.strip():
        raise ValueError("Нужно указать ссылку.")
    final_url = normalize_google_sheet_url(source_url.strip())
    response = requests.get(final_url, timeout=60)
    response.raise_for_status()
    filename = final_url.split("/")[-1] or "supplier.xlsx"
    if "format=xlsx" in final_url and not filename.endswith(".xlsx"):
        filename = "supplier.xlsx"
    return filename, response.content


def load_source_to_df(filename, file_bytes, sheet_name, header_row):
    suffix = Path(filename).suffix.lower()
    workbook = None

    if suffix == ".csv":
        df = pd.read_csv(io.BytesIO(file_bytes))
    elif suffix == ".xls":
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=header_row, engine="xlrd")
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=header_row)
        workbook = load_workbook(io.BytesIO(file_bytes))

    df = clean_columns(df)
    df["__excel_row__"] = range(header_row + 2, header_row + 2 + len(df))
    return df, workbook


def extract_hyperlinks_map(ws, header_row):
    mapping = {}
    excel_header_row = header_row + 1
    photo_col = None

    for col in range(1, ws.max_column + 1):
        if str(ws.cell(excel_header_row, col).value).strip() == "Товар на сайте":
            photo_col = col
            break

    if photo_col is None:
        return mapping

    for row in range(excel_header_row + 1, ws.max_row + 1):
        cell = ws.cell(row, photo_col)
        if cell.hyperlink and cell.hyperlink.target:
            mapping[row] = cell.hyperlink.target
    return mapping


def safe_ext_from_url(url: str) -> str:
    for ext in [".jpg", ".jpeg", ".png", ".webp"]:
        if ext in url.lower():
            return ext
    return ".jpg"


def extract_images_map(ws):
    images = {}
    for img in getattr(ws, "_images", []):
        try:
            excel_row = img.anchor._from.row + 1
            data = img._data()
            images[excel_row] = data
        except Exception:
            continue
    return images


def download_image_bytes(url: str):
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.content
    except Exception:
        return None


def build_photo_ref(prefix: str, seed: str, ext: str):
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()[:16]
    return f"images/{prefix}_{digest}{ext}"


def attach_images(parsed_df, supplier_key, workbook, header_row):
    parsed_df = parsed_df.copy()
    image_store = {}

    if supplier_key == "s2" and workbook is not None:
        ws = workbook[SUPPLIERS[supplier_key]["sheet_name"]]
        links_map = extract_hyperlinks_map(ws, header_row)
        photo_refs = []
        source_urls = []

        for _, row in parsed_df.iterrows():
            excel_row = int(row.get("__excel_row__", 0))
            url = links_map.get(excel_row, "")
            article = str(row.get("supplier_article", "") or excel_row)

            source_urls.append(url)

            if url:
                ext = safe_ext_from_url(url)
                photo_ref = build_photo_ref("s2", article, ext)
                photo_refs.append(photo_ref)
            else:
                photo_refs.append("")

        parsed_df["source_image_url"] = source_urls
        parsed_df["photo_ref"] = photo_refs
        return parsed_df, {}

    if supplier_key in ["s3", "s4"] and workbook is not None:
        ws = workbook[SUPPLIERS[supplier_key]["sheet_name"]]
        img_map = extract_images_map(ws)
        photo_refs = []

        for _, row in parsed_df.iterrows():
            excel_row = int(row.get("__excel_row__", 0))
            img_bytes = img_map.get(excel_row)
            if img_bytes:
                seed = f"{supplier_key}_{excel_row}_{row.get('name', '')}"
                photo_ref = build_photo_ref(supplier_key, seed, ".png")
                image_store[photo_ref] = img_bytes
                photo_refs.append(photo_ref)
            else:
                photo_refs.append("")

        parsed_df["source_image_url"] = ""
        parsed_df["photo_ref"] = photo_refs
        return parsed_df, image_store

    parsed_df["source_image_url"] = parsed_df.get("image_url", "")
    parsed_df["photo_ref"] = ""
    return parsed_df, image_store


def apply_selected_price_tier(df, selected_tier: str):
    df = df.copy()
    if selected_tier in df.columns:
        df["base_price"] = df[selected_tier]
        df["price_tier"] = selected_tier
    else:
        df["base_price"] = df.get("price")
        df["price_tier"] = "price"
    return df


def enrich_categories(df):
    df = df.copy()
    cats = df["name"].apply(infer_categories)
    df["category_l1"] = [x[0] for x in cats]
    df["category_l2"] = [x[1] for x in cats]
    return df


def parse_supplier1(df):
    df = df.rename(columns={
        "Артикул": "supplier_article",
        "Наименование товара": "name",
        "Ед.": "unit",
        "в уп": "pack_qty",
        "цена": "price",
        "фото": "photo",
    }).copy()

    if "supplier_article" not in df.columns or "name" not in df.columns:
        raise ValueError("Не найдены нужные колонки для поставщика 1")

    df["supplier_article"] = df["supplier_article"].fillna("").astype(str).str.strip()
    df["name"] = df["name"].fillna("").astype(str).str.strip()
    df = df[(df["supplier_article"] != "") & (df["name"] != "")]
    df = df[~df["name"].str.startswith("(")]
    df["price"] = df["price"].apply(to_float)
    df["stock"] = None
    df["image_url"] = ""
    df["supplier"] = "s1"
    return df[["supplier", "supplier_article", "name", "price", "stock", "image_url", "__excel_row__"]]


def parse_supplier2(df):
    df = df.rename(columns={
        "Артикул": "supplier_article",
        "Номенклатура": "name",
        "Остаток, шт.": "stock",
        "Цена Опт 1, руб.": "price",
        "Цена Опт 2, руб.": "price_opt2",
        "Цена Опт 3, руб.": "price_opt3",
        "Цена РРЦ, руб.": "price_rrc",
    }).copy()

    if "supplier_article" not in df.columns or "name" not in df.columns:
        raise ValueError("Не найдены нужные колонки для поставщика 2")

    df["supplier_article"] = df["supplier_article"].fillna("").astype(str).str.strip()
    df["name"] = df["name"].fillna("").astype(str).str.strip()
    df = df[(df["supplier_article"] != "") & (df["name"] != "")]
    df["stock"] = df["stock"].apply(normalize_stock)

    for col in ["price", "price_opt2", "price_opt3", "price_rrc"]:
        if col in df.columns:
            df[col] = df[col].apply(to_float)

    df["image_url"] = ""
    df["supplier"] = "s2"
    return df[["supplier", "supplier_article", "name", "price", "price_opt2", "price_opt3", "price_rrc", "stock", "image_url", "__excel_row__"]]


def parse_supplier3(df):
    df = df.rename(columns={
        "Наименование товара": "name",
        "Фото": "photo",
        "Цена, ₽": "price",
        "От 10шт": "price_opt10",
        "От 50шт": "price_opt50",
        "Безнал": "payment_note",
    }).copy()

    if "name" not in df.columns:
        raise ValueError("Не найдена колонка 'Наименование товара' для поставщика 3")

    df["name"] = df["name"].fillna("").astype(str).str.strip()
    df = df[df["name"] != ""]
    df = df[~df["name"].str.contains("WhatsApp|Наличие и актуальные цены", case=False, na=False)]

    for col in ["price", "price_opt10", "price_opt50"]:
        if col in df.columns:
            df[col] = df[col].apply(to_float)

    df["supplier_article"] = ""
    df["stock"] = None
    df["image_url"] = ""
    df["supplier"] = "s3"
    return df[["supplier", "supplier_article", "name", "price", "price_opt10", "price_opt50", "stock", "image_url", "__excel_row__"]]


def parse_supplier4(df):
    first_col = df.columns[0]
    df = df.rename(columns={
        first_col: "row_num",
        "Название": "name",
        "Наличие": "stock",
        "упаковка (шт)": "pack_qty",
        "вес 1 шт (кг)": "weight_kg",
        "РРЦ": "price_rrc",
        "от 1 уп": "price",
        "Своим": "own_price",
        "Изображение": "photo",
    }).copy()

    if "name" not in df.columns:
        raise ValueError("Не найдена колонка 'Название' для поставщика 4")

    df["name"] = df["name"].fillna("").astype(str).str.strip()
    df = df[df["name"] != ""]
    df["stock"] = df["stock"].apply(normalize_stock)

    for col in ["price", "price_rrc", "own_price", "pack_qty", "weight_kg"]:
        if col in df.columns:
            df[col] = df[col].apply(to_float)

    df["supplier_article"] = ""
    df["image_url"] = ""
    df["supplier"] = "s4"
    return df[["supplier", "supplier_article", "name", "price", "price_rrc", "own_price", "pack_qty", "weight_kg", "stock", "image_url", "__excel_row__"]]


def parse_supplier(supplier_key, df):
    if supplier_key == "s1":
        return parse_supplier1(df)
    if supplier_key == "s2":
        return parse_supplier2(df)
    if supplier_key == "s3":
        return parse_supplier3(df)
    if supplier_key == "s4":
        return parse_supplier4(df)
    raise ValueError("Неизвестный поставщик")


def add_normalized_columns(df):
    df = df.copy()
    df["normalized_name"] = df["name"].apply(normalize_name)
    df["name_signature"] = df["name"].apply(signature_name)
    df["specs"] = df["name"].apply(extract_specs)
    return df


def duplicate_score(row_a: dict, row_b: dict) -> float:
    a = row_a["name_signature"]
    b = row_b["name_signature"]

    score = max(
        fuzz.token_sort_ratio(a, b),
        fuzz.token_set_ratio(a, b),
        fuzz.partial_ratio(a, b),
    )

    specs_a = row_a.get("specs", {}) or {}
    specs_b = row_b.get("specs", {}) or {}

    for key in ["voltage", "ah", "watt", "diameter"]:
        va = specs_a.get(key)
        vb = specs_b.get(key)
        if va and vb:
            if va == vb:
                score += 4
            else:
                score -= 10

    if row_a.get("category_l1") == row_b.get("category_l1"):
        score += 2
    if row_a.get("category_l2") and row_a.get("category_l2") == row_b.get("category_l2"):
        score += 2

    return min(score, 100)


def build_master(offers_df):
    if offers_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    offers_df = offers_df.copy()
    offers_df["normalized_name"] = offers_df["name"].apply(normalize_name)
    offers_df["name_signature"] = offers_df["name"].apply(signature_name)
    offers_df["specs"] = offers_df["name"].apply(extract_specs)
    offers_df = enrich_categories(offers_df)

    master_rows = []
    mapping_rows = []
    master_id = 1
    masters = []

    def create_master_from_group(grp: pd.DataFrame, method: str, confidence: float):
        nonlocal master_id, master_rows, mapping_rows, masters

        best_price = grp["base_price"].dropna().min() if "base_price" in grp.columns else None
        stock_sum = grp["stock"].dropna().sum() if "stock" in grp.columns else None
        photo_ref = next((x for x in grp["photo_ref"].fillna("").tolist() if x), "")
        representative_idx = grp["base_price"].fillna(float("inf")).idxmin() if grp["base_price"].notna().any() else grp.index[0]
        representative = grp.loc[representative_idx]

        row = {
            "master_id": master_id,
            "article": str(representative.get("supplier_article", "") or ""),
            "name": representative["name"],
            "normalized_name": representative["normalized_name"],
            "category_l1": representative.get("category_l1", "Прочее"),
            "category_l2": representative.get("category_l2", ""),
            "final_price": best_price,
            "final_stock": int(stock_sum) if pd.notna(stock_sum) else None,
            "final_image": photo_ref,
        }
        master_rows.append(row)
        masters.append({
            "master_id": master_id,
            "normalized_name": representative["normalized_name"],
            "name_signature": representative["name_signature"],
            "specs": representative["specs"],
            "category_l1": representative.get("category_l1", ""),
            "category_l2": representative.get("category_l2", ""),
        })

        for _, item in grp.iterrows():
            mapping_rows.append({
                "supplier": item["supplier"],
                "supplier_article": item.get("supplier_article", ""),
                "supplier_name": item["name"],
                "normalized_name": item["normalized_name"],
                "category_l1": item.get("category_l1", ""),
                "category_l2": item.get("category_l2", ""),
                "master_id": master_id,
                "match_method": method,
                "confidence": confidence,
            })

        master_id += 1

    article_df = offers_df[offers_df["supplier_article"].fillna("") != ""].copy()
    no_article_df = offers_df[offers_df["supplier_article"].fillna("") == ""].copy()

    if not article_df.empty:
        for article, grp in article_df.groupby("supplier_article"):
            create_master_from_group(grp, "article_exact", 100.0)

    exact_name_groups = {}
    if not no_article_df.empty:
        for idx, row in no_article_df.iterrows():
            sig = row["name_signature"]
            exact_name_groups.setdefault(sig, []).append(idx)

        used = set()
        for sig, idxs in exact_name_groups.items():
            if len(idxs) >= 2:
                grp = no_article_df.loc[idxs]
                create_master_from_group(grp, "name_exact_signature", 99.0)
                used.update(idxs)

        no_article_df = no_article_df.drop(index=list(used), errors="ignore")

    for _, row in no_article_df.iterrows():
        best_master_id = None
        best_score = -1

        row_payload = {
            "name_signature": row["name_signature"],
            "specs": row["specs"],
            "category_l1": row.get("category_l1", ""),
            "category_l2": row.get("category_l2", ""),
        }

        for master in masters:
            score = duplicate_score(row_payload, master)
            if score > best_score:
                best_score = score
                best_master_id = master["master_id"]

        if best_score >= 90 and best_master_id is not None:
            mapping_rows.append({
                "supplier": row["supplier"],
                "supplier_article": row.get("supplier_article", ""),
                "supplier_name": row["name"],
                "normalized_name": row["normalized_name"],
                "category_l1": row.get("category_l1", ""),
                "category_l2": row.get("category_l2", ""),
                "master_id": best_master_id,
                "match_method": "name_fuzzy_strong",
                "confidence": float(best_score),
            })

            for mr in master_rows:
                if mr["master_id"] == best_master_id:
                    if pd.notna(row.get("base_price")):
                        if pd.isna(mr["final_price"]) or float(row["base_price"]) < float(mr["final_price"]):
                            mr["final_price"] = float(row["base_price"])
                            mr["name"] = row["name"]
                            mr["normalized_name"] = row["normalized_name"]
                            mr["category_l1"] = row.get("category_l1", mr["category_l1"])
                            mr["category_l2"] = row.get("category_l2", mr["category_l2"])
                    if pd.notna(row.get("stock")):
                        existing = mr.get("final_stock")
                        mr["final_stock"] = (existing or 0) + int(row["stock"])
                    if not mr.get("final_image") and row.get("photo_ref"):
                        mr["final_image"] = row.get("photo_ref")
                    break
        else:
            create_master_from_group(pd.DataFrame([row]), "new_name", 100.0)

    master_df = pd.DataFrame(master_rows)
    mapping_df = pd.DataFrame(mapping_rows)

    if not master_df.empty:
        master_df = master_df.sort_values(["category_l1", "category_l2", "name"]).reset_index(drop=True)
    if not mapping_df.empty:
        mapping_df = mapping_df.sort_values(["match_method", "confidence"], ascending=[True, False]).reset_index(drop=True)

    return master_df, mapping_df


def build_excel_bytes(df: pd.DataFrame, sheet_name: str):
    output = io.BytesIO()
    export_df = df.copy()
    for col in export_df.columns:
        if export_df[col].dtype == "object":
            export_df[col] = export_df[col].fillna("")
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()


def has_r2_config():
    required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME", "R2_PUBLIC_BASE_URL"]
    return all(key in st.secrets for key in required)


def get_r2_client():
    return boto3.client(
        service_name="s3",
        endpoint_url=f"https://{st.secrets['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=st.secrets["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def r2_public_url_for_key(key: str) -> str:
    return f"{st.secrets['R2_PUBLIC_BASE_URL'].rstrip('/')}/{key}"


def r2_object_exists(key: str) -> bool:
    cache = st.session_state.r2_exists_cache
    if key in cache:
        return cache[key]

    s3 = get_r2_client()
    try:
        s3.head_object(Bucket=st.secrets["R2_BUCKET_NAME"], Key=key)
        cache[key] = True
        return True
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in ["404", "NoSuchKey", "NotFound"]:
            cache[key] = False
            return False
        raise


def upload_bytes_to_r2_if_needed(key: str, data: bytes, content_type: str = "application/octet-stream"):
    if r2_object_exists(key):
        return r2_public_url_for_key(key), "cached"

    s3 = get_r2_client()
    s3.put_object(
        Bucket=st.secrets["R2_BUCKET_NAME"],
        Key=key,
        Body=data,
        ContentType=content_type,
    )
    st.session_state.r2_exists_cache[key] = True
    return r2_public_url_for_key(key), "uploaded"


def guess_content_type(key: str) -> str:
    key = key.lower()
    if key.endswith(".png"):
        return "image/png"
    if key.endswith(".webp"):
        return "image/webp"
    if key.endswith(".jpeg") or key.endswith(".jpg"):
        return "image/jpeg"
    return "application/octet-stream"


def upload_final_images_to_r2(export_df: pd.DataFrame, offers_by_supplier: dict, images_by_supplier: dict):
    export_df = export_df.copy()
    uploaded_map = {}
    stats = {"cached": 0, "uploaded": 0, "missed": 0}

    if not has_r2_config():
        raise ValueError("R2 secrets не настроены в Streamlit.")

    in_memory = {}
    for supplier_images in images_by_supplier.values():
        in_memory.update(supplier_images)

    source_lookup = {}
    for _, df in offers_by_supplier.items():
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            photo_ref = str(row.get("photo_ref", "") or "")
            source_url = str(row.get("source_image_url", "") or "")
            if photo_ref and source_url and photo_ref not in source_lookup:
                source_lookup[photo_ref] = source_url

    final_urls = []
    progress = st.progress(0, text="Проверка и загрузка фото в R2...")
    total = max(len(export_df), 1)

    for idx, (_, row) in enumerate(export_df.iterrows(), start=1):
        key = str(row.get("final_image", "") or "").strip()
        if not key:
            final_urls.append("")
            stats["missed"] += 1
            progress.progress(idx / total, text=f"Фото {idx}/{total}")
            continue

        if key in uploaded_map:
            final_urls.append(uploaded_map[key])
            progress.progress(idx / total, text=f"Фото {idx}/{total}")
            continue

        if r2_object_exists(key):
            public_url = r2_public_url_for_key(key)
            uploaded_map[key] = public_url
            final_urls.append(public_url)
            stats["cached"] += 1
            progress.progress(idx / total, text=f"Фото {idx}/{total}")
            continue

        data = in_memory.get(key)
        if data:
            public_url, mode = upload_bytes_to_r2_if_needed(key, data, guess_content_type(key))
            uploaded_map[key] = public_url
            final_urls.append(public_url)
            stats[mode] += 1
            progress.progress(idx / total, text=f"Фото {idx}/{total}")
            continue

        source_url = source_lookup.get(key, "")
        if source_url:
            data = download_image_bytes(source_url)
            if data:
                public_url, mode = upload_bytes_to_r2_if_needed(key, data, guess_content_type(key))
                uploaded_map[key] = public_url
                final_urls.append(public_url)
                stats[mode] += 1
                progress.progress(idx / total, text=f"Фото {idx}/{total}")
                continue

        final_urls.append("")
        stats["missed"] += 1
        progress.progress(idx / total, text=f"Фото {idx}/{total}")

    progress.empty()
    export_df["final_image_public_url"] = final_urls
    return export_df, stats


init_state()

with st.sidebar:
    st.title("Price Aggregator")
    page = st.radio("Раздел", ["Дашборд", "Загрузка прайсов", "Дубли и склейка", "Итоговый прайс", "R2"])

if page == "Дашборд":
    st.title("🏠 Дашборд")
    supplier_stats = []
    total_rows = 0

    for key, cfg in SUPPLIERS.items():
        df = st.session_state.offers_by_supplier.get(key, pd.DataFrame())
        count = len(df) if not df.empty else 0
        total_rows += count
        supplier_stats.append({
            "Поставщик": cfg["label"],
            "Код": key,
            "Строк загружено": count,
            "Тип источника": cfg["source_type"],
        })

    c1, c2, c3 = st.columns(3)
    c1.metric("Всего строк поставщиков", total_rows)
    c2.metric("Карточек в итоге", len(st.session_state.master_df))
    c3.metric("Связей", len(st.session_state.mapping_df))

    st.dataframe(pd.DataFrame(supplier_stats), use_container_width=True)
    st.info("Теперь перед загрузкой фото в R2 идет проверка: если файл уже есть в R2, повторной загрузки не будет.")

elif page == "Загрузка прайсов":
    st.title("📥 Загрузка прайсов")

    supplier = st.selectbox("Выберите поставщика", list(SUPPLIERS.keys()), format_func=lambda x: SUPPLIERS[x]["label"])
    cfg = SUPPLIERS[supplier]

    col1, col2 = st.columns(2)
    with col1:
        source_type = st.radio("Источник", ["file", "url"], horizontal=True)
    with col2:
        allowed_price_tiers = cfg.get("allowed_price_tiers", ["price"])
        default_price_tier = cfg.get("default_price_tier", "price")
        default_index = allowed_price_tiers.index(default_price_tier) if default_price_tier in allowed_price_tiers else 0
        selected_price_tier = st.selectbox("Основная цена", allowed_price_tiers, index=default_index, format_func=lambda x: PRICE_TIER_LABELS[x])

    uploaded_file = None
    source_url = ""
    if source_type == "file":
        uploaded_file = st.file_uploader("Загрузите файл", type=["xls", "xlsx", "csv"])
    else:
        source_url = st.text_input("Ссылка на прайс / Google Sheets")

    if st.button("Обработать прайс"):
        try:
            filename, file_bytes = read_source_bytes(source_type, uploaded_file, source_url)
            raw_df, workbook = load_source_to_df(filename, file_bytes, cfg["sheet_name"], cfg["header_row"])

            parsed = parse_supplier(supplier, raw_df)
            parsed = add_normalized_columns(parsed)
            parsed = enrich_categories(parsed)
            parsed = apply_selected_price_tier(parsed, selected_price_tier)
            parsed, image_store = attach_images(parsed, supplier, workbook, cfg["header_row"])

            st.session_state.offers_by_supplier[supplier] = parsed
            st.session_state.images_by_supplier[supplier] = image_store

            st.success("Прайс обработан.")
            st.dataframe(parsed.head(100), use_container_width=True)

            normalized_preview = parsed.drop(columns=["__excel_row__"], errors="ignore")
            excel_bytes = build_excel_bytes(normalized_preview, "normalized")
            st.download_button(
                "Скачать нормализованный Excel",
                data=excel_bytes,
                file_name=f"{supplier}_normalized.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            if supplier == "s2":
                st.info("Для Форвард СПб фото не скачиваются во время импорта. Они загрузятся в R2 только для финальных товаров и только если их еще нет в R2.")
        except Exception as e:
            st.error(f"Ошибка: {e}")

elif page == "Дубли и склейка":
    st.title("🔎 Дубли и склейка")
    dfs = [df for df in st.session_state.offers_by_supplier.values() if not df.empty]
    if not dfs:
        st.info("Сначала загрузите хотя бы один прайс.")
    else:
        all_offers = pd.concat(dfs, ignore_index=True)
        if st.button("Запустить склейку"):
            master_df, mapping_df = build_master(all_offers)
            st.session_state.master_df = master_df
            st.session_state.mapping_df = mapping_df
            st.success("Склейка выполнена.")

        if not st.session_state.master_df.empty:
            st.subheader("Итоговые карточки по категориям")
            st.dataframe(st.session_state.master_df, use_container_width=True)

        if not st.session_state.mapping_df.empty:
            strong_df = st.session_state.mapping_df[st.session_state.mapping_df["match_method"].isin(["name_fuzzy_strong"])]
            st.subheader("Сильные fuzzy-связки")
            if strong_df.empty:
                st.info("Сильных fuzzy-связок пока нет.")
            else:
                st.dataframe(strong_df, use_container_width=True)

            weak_df = st.session_state.mapping_df[
                (st.session_state.mapping_df["match_method"].str.contains("fuzzy", na=False)) &
                (st.session_state.mapping_df["confidence"] < 93)
            ]
            st.subheader("Сомнительные fuzzy-совпадения")
            if weak_df.empty:
                st.info("Сомнительных совпадений пока нет.")
            else:
                st.dataframe(weak_df, use_container_width=True)
                fuzzy_excel = build_excel_bytes(weak_df, "fuzzy_matches")
                st.download_button(
                    "Скачать сомнительные совпадения Excel",
                    data=fuzzy_excel,
                    file_name="fuzzy_matches.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

elif page == "Итоговый прайс":
    st.title("📤 Итоговый прайс")
    master_df = st.session_state.master_df.copy()

    if master_df.empty:
        st.info("Сначала загрузите прайсы и выполните склейку.")
    else:
        markup_percent = st.number_input("Наценка, %", min_value=0.0, max_value=1000.0, value=30.0, step=1.0)

        category_l1_options = sorted([x for x in master_df["category_l1"].dropna().unique().tolist() if str(x).strip()])
        selected_l1 = st.multiselect("Какие группы выгружать", category_l1_options, default=category_l1_options)

        filtered_df = master_df.copy()
        if selected_l1:
            filtered_df = filtered_df[filtered_df["category_l1"].isin(selected_l1)]

        category_l2_options = sorted([x for x in filtered_df["category_l2"].dropna().unique().tolist() if str(x).strip()])
        selected_l2 = st.multiselect("Подгруппы (необязательно)", category_l2_options, default=[])

        if selected_l2:
            filtered_df = filtered_df[filtered_df["category_l2"].isin(selected_l2)]

        export_df = filtered_df.copy()
        export_df["price_with_markup"] = export_df["final_price"].apply(
            lambda x: round(float(x) * (1 + markup_percent / 100), 2) if pd.notna(x) else None
        )

        auto_upload = st.checkbox("Автоматически загрузить фото в R2 перед выгрузкой", value=False)

        c1, c2, c3 = st.columns(3)
        c1.metric("Карточек", len(export_df))
        c2.metric("С фото", int((export_df["final_image"].fillna("") != "").sum()))
        c3.metric("Наценка", f"{markup_percent:.0f}%")

        if auto_upload:
            if has_r2_config():
                try:
                    export_df, stats = upload_final_images_to_r2(
                        export_df,
                        st.session_state.offers_by_supplier,
                        st.session_state.images_by_supplier,
                    )
                    st.success(f"R2: уже были {stats['cached']}, загружены новые {stats['uploaded']}, без фото {stats['missed']}.")
                except Exception as e:
                    st.error(f"Ошибка загрузки фото в R2: {e}")
            else:
                st.warning("R2 secrets не настроены. Открой вкладку R2.")

        st.dataframe(export_df, use_container_width=True)

        final_excel = build_excel_bytes(export_df, "final_price")
        st.download_button(
            "Скачать итоговый Excel",
            data=final_excel,
            file_name="final_price_list.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

elif page == "R2":
    st.title("☁️ R2")
    st.write("Приложение теперь проверяет существование файла в R2 до загрузки. Если объект уже есть, повторной загрузки не будет.")

    st.code(
"""R2_ACCOUNT_ID = "ваш_account_id"
R2_ACCESS_KEY_ID = "ваш_access_key_id"
R2_SECRET_ACCESS_KEY = "ваш_secret_access_key"
R2_BUCKET_NAME = "images"
R2_PUBLIC_BASE_URL = "https://pub-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.r2.dev" """,
        language="toml"
    )

    if has_r2_config():
        st.success("R2 secrets настроены.")
        st.write("Bucket:", st.secrets["R2_BUCKET_NAME"])
        st.write("Public URL:", st.secrets["R2_PUBLIC_BASE_URL"])
        st.write("Кэш проверенных R2-ключей в текущей сессии:", len(st.session_state.r2_exists_cache))
    else:
        st.warning("R2 secrets пока не заданы.")
