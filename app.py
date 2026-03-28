import io
import zipfile
import hashlib
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from rapidfuzz import fuzz, process
from openpyxl import load_workbook


st.set_page_config(page_title="Price Aggregator", layout="wide")

SUPPLIERS = {
    "supplier1": {
        "label": "1. Velozapchasti",
        "sheet_name": "Sheet1",
        "header_row": 7,
        "source_type": "file",
        "default_price_tier": "price",
        "allowed_price_tiers": ["price"],
    },
    "supplier2": {
        "label": "2. Форвард СПб",
        "sheet_name": "Лист_1",
        "header_row": 5,
        "source_type": "file",
        "default_price_tier": "price",
        "allowed_price_tiers": ["price", "price_opt2", "price_opt3", "price_rrc"],
    },
    "supplier3": {
        "label": "3. Колхозник / Монстр",
        "sheet_name": "Лист1",
        "header_row": 1,
        "source_type": "url",
        "default_price_tier": "price",
        "allowed_price_tiers": ["price", "price_opt10", "price_opt50"],
    },
    "supplier4": {
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


def init_state():
    defaults = {
        "offers_by_supplier": {},
        "images_by_supplier": {},
        "master_df": pd.DataFrame(),
        "mapping_df": pd.DataFrame(),
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def normalize_name(value: str) -> str:
    value = str(value or "").lower().strip()
    return " ".join(value.split())


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
        "нет": 0,
        "мало": 1,
        "много": 10,
        "более 10": 10,
        "скоро будут": 0,
        "поз заказ": 0,
        "распродажа": 1,
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

    # Supplier 2: do NOT download thousands of images on import.
    # We only save hidden internal refs and original URLs for later use.
    if supplier_key == "supplier2" and workbook is not None:
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
                photo_ref = build_photo_ref("supplier2", article, ext)
                photo_refs.append(photo_ref)
            else:
                photo_refs.append("")

        parsed_df["source_image_url"] = source_urls
        parsed_df["photo_ref"] = photo_refs
        return parsed_df, {}

    # Suppliers 3 and 4: embedded Excel images can be extracted immediately.
    if supplier_key in ["supplier3", "supplier4"] and workbook is not None:
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

    # Supplier 1 and fallback.
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
    df["supplier"] = "supplier1"
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
    df["supplier"] = "supplier2"
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
    df["supplier"] = "supplier3"
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
    df["supplier"] = "supplier4"
    return df[["supplier", "supplier_article", "name", "price", "price_rrc", "own_price", "pack_qty", "weight_kg", "stock", "image_url", "__excel_row__"]]


def parse_supplier(supplier_key, df):
    if supplier_key == "supplier1":
        return parse_supplier1(df)
    if supplier_key == "supplier2":
        return parse_supplier2(df)
    if supplier_key == "supplier3":
        return parse_supplier3(df)
    if supplier_key == "supplier4":
        return parse_supplier4(df)
    raise ValueError("Неизвестный поставщик")


def add_normalized_columns(df):
    df = df.copy()
    df["normalized_name"] = df["name"].apply(normalize_name)
    return df


def build_master(offers_df):
    if offers_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    offers_df = offers_df.copy()
    offers_df["normalized_name"] = offers_df["name"].apply(normalize_name)

    master_rows = []
    mapping_rows = []
    master_id = 1

    article_df = offers_df[offers_df["supplier_article"].fillna("") != ""].copy()
    no_article_df = offers_df[offers_df["supplier_article"].fillna("") == ""].copy()

    if not article_df.empty:
        for article, grp in article_df.groupby("supplier_article"):
            best_price = grp["base_price"].dropna().min() if "base_price" in grp.columns else None
            stock_sum = grp["stock"].dropna().sum() if "stock" in grp.columns else None
            photo_ref = next((x for x in grp["photo_ref"].fillna("").tolist() if x), "")
            name = grp.iloc[0]["name"]
            norm = grp.iloc[0]["normalized_name"]

            master_rows.append({
                "master_id": master_id,
                "article": str(article),
                "name": name,
                "normalized_name": norm,
                "final_price": best_price,
                "final_stock": int(stock_sum) if pd.notna(stock_sum) else None,
                "final_image": photo_ref,
            })

            for _, row in grp.iterrows():
                mapping_rows.append({
                    "supplier": row["supplier"],
                    "supplier_article": row.get("supplier_article"),
                    "supplier_name": row["name"],
                    "normalized_name": row["normalized_name"],
                    "master_id": master_id,
                    "match_method": "article_exact",
                    "confidence": 100,
                })
            master_id += 1

    master_names = [row["normalized_name"] for row in master_rows]

    for _, row in no_article_df.iterrows():
        norm = row["normalized_name"]

        if not master_names:
            master_rows.append({
                "master_id": master_id,
                "article": "",
                "name": row["name"],
                "normalized_name": norm,
                "final_price": row.get("base_price"),
                "final_stock": row.get("stock"),
                "final_image": row.get("photo_ref", ""),
            })
            mapping_rows.append({
                "supplier": row["supplier"],
                "supplier_article": "",
                "supplier_name": row["name"],
                "normalized_name": norm,
                "master_id": master_id,
                "match_method": "new_name",
                "confidence": 100,
            })
            master_names.append(norm)
            master_id += 1
            continue

        match = process.extractOne(norm, master_names, scorer=fuzz.ratio)
        if match and match[1] >= 88:
            matched_norm = match[0]
            matched_master_id = next(r["master_id"] for r in master_rows if r["normalized_name"] == matched_norm)
            mapping_rows.append({
                "supplier": row["supplier"],
                "supplier_article": "",
                "supplier_name": row["name"],
                "normalized_name": norm,
                "master_id": matched_master_id,
                "match_method": "name_fuzzy",
                "confidence": float(match[1]),
            })
        else:
            master_rows.append({
                "master_id": master_id,
                "article": "",
                "name": row["name"],
                "normalized_name": norm,
                "final_price": row.get("base_price"),
                "final_stock": row.get("stock"),
                "final_image": row.get("photo_ref", ""),
            })
            mapping_rows.append({
                "supplier": row["supplier"],
                "supplier_article": "",
                "supplier_name": row["name"],
                "normalized_name": norm,
                "master_id": master_id,
                "match_method": "new_name",
                "confidence": 100,
            })
            master_names.append(norm)
            master_id += 1

    return pd.DataFrame(master_rows), pd.DataFrame(mapping_rows)


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


def build_images_zip_bytes(images_dict: dict):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for photo_ref, data in images_dict.items():
            if data:
                zf.writestr(photo_ref, data)
    output.seek(0)
    return output.getvalue()


init_state()

with st.sidebar:
    st.title("Price Aggregator")
    page = st.radio(
        "Раздел",
        ["Дашборд", "Загрузка прайсов", "Дубли и склейка", "Итоговый прайс"]
    )

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
            "Строк загружено": count,
            "Тип источника": cfg["source_type"],
        })

    c1, c2, c3 = st.columns(3)
    c1.metric("Всего строк поставщиков", total_rows)
    c2.metric("Карточек в итоге", len(st.session_state.master_df))
    c3.metric("Связей", len(st.session_state.mapping_df))

    st.dataframe(pd.DataFrame(supplier_stats), use_container_width=True)
    st.info("Фото в финале теперь скрываются: в выгрузке будет внутренний путь вида images/...., а не ссылка на поставщика.")

elif page == "Загрузка прайсов":
    st.title("📥 Загрузка прайсов")

    supplier = st.selectbox(
        "Выберите поставщика",
        list(SUPPLIERS.keys()),
        format_func=lambda x: SUPPLIERS[x]["label"]
    )
    cfg = SUPPLIERS[supplier]

    col1, col2 = st.columns(2)
    with col1:
        source_type = st.radio("Источник", ["file", "url"], horizontal=True)
    with col2:
        allowed_price_tiers = cfg.get("allowed_price_tiers", ["price"])
        default_price_tier = cfg.get("default_price_tier", "price")
        default_index = allowed_price_tiers.index(default_price_tier) if default_price_tier in allowed_price_tiers else 0
        selected_price_tier = st.selectbox(
            "Основная цена",
            allowed_price_tiers,
            index=default_index,
            format_func=lambda x: PRICE_TIER_LABELS[x]
        )

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

            if image_store:
                img_zip = build_images_zip_bytes(image_store)
                st.download_button(
                    "Скачать архив фото этого поставщика",
                    data=img_zip,
                    file_name=f"{supplier}_images.zip",
                    mime="application/zip"
                )

            if supplier == "supplier1":
                st.warning("У поставщика 1 прямые ссылки на файлы изображений не найдены. В финальном прайсе ссылки поставщика скрываются, но фото для него автоматически не забираются.")
            if supplier == "supplier2":
                st.info("Для Форвард СПб фото не скачиваются во время импорта, чтобы приложение не зависало. Внутренние скрытые пути создаются, а сами изображения лучше забирать отдельно по необходимости.")

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
            st.subheader("Итоговые карточки")
            st.dataframe(st.session_state.master_df, use_container_width=True)

        if not st.session_state.mapping_df.empty:
            fuzzy_df = st.session_state.mapping_df[
                st.session_state.mapping_df["match_method"] == "name_fuzzy"
            ]
            st.subheader("Сомнительные fuzzy-совпадения")
            if fuzzy_df.empty:
                st.info("Сомнительных совпадений пока нет.")
            else:
                st.dataframe(fuzzy_df, use_container_width=True)
                fuzzy_excel = build_excel_bytes(fuzzy_df, "fuzzy_matches")
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
        export_df = master_df.copy()
        export_df["price_with_markup"] = export_df["final_price"].apply(
            lambda x: round(float(x) * (1 + markup_percent / 100), 2) if pd.notna(x) else None
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Карточек", len(export_df))
        c2.metric("С фото", int((export_df["final_image"].fillna("") != "").sum()))
        c3.metric("Наценка", f"{markup_percent:.0f}%")

        st.dataframe(export_df, use_container_width=True)

        final_excel = build_excel_bytes(export_df, "final_price")
        st.download_button(
            "Скачать итоговый Excel",
            data=final_excel,
            file_name="final_price_list.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        all_images = {}
        for supplier_images in st.session_state.images_by_supplier.values():
            all_images.update(supplier_images)

        if all_images:
            final_img_zip = build_images_zip_bytes(all_images)
            st.download_button(
                "Скачать архив всех скрытых фото",
                data=final_img_zip,
                file_name="all_images.zip",
                mime="application/zip"
            )

        st.caption("В столбце final_image выгружается скрытый внутренний путь вида images/.... Его можно потом раздавать уже со своего домена или из S3/Cloudflare R2.")
