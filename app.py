import io
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from rapidfuzz import fuzz, process


st.set_page_config(page_title="Price Aggregator", layout="wide")


SUPPLIERS = {
    "supplier1": {
        "label": "1. Velozapchasti",
        "sheet_name": "Sheet1",
        "header_row": 7,
        "source_type": "file",
        "default_price_tier": "price",
    },
    "supplier2": {
        "label": "2. Форвард СПб",
        "sheet_name": "Лист_1",
        "header_row": 5,
        "source_type": "file",
        "default_price_tier": "price",
    },
    "supplier3": {
        "label": "3. Колхозник / Монстр",
        "sheet_name": "Лист1",
        "header_row": 1,
        "source_type": "url",
        "default_price_tier": "price",
    },
    "supplier4": {
        "label": "4. BRATAN",
        "sheet_name": "Лена 25.05.24",
        "header_row": 0,
        "source_type": "url",
        "default_price_tier": "price",
    },
}

PRICE_TIER_LABELS = {
    "price": "Основная оптовая",
    "price_opt2": "Опт 2",
    "price_opt3": "Опт 3",
    "price_opt10": "От 10 шт",
    "price_opt50": "От 50 шт",
    "own_price": "Своим",
    "price_rrc": "РРЦ",
}


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


def normalize_google_sheet_url(url: str) -> str:
    if "docs.google.com/spreadsheets" in url and "/edit" in url:
        return url.split("/edit")[0] + "/export?format=xlsx"
    return url


def load_from_url(url: str, sheet_name, header_row):
    final_url = normalize_google_sheet_url(url)
    response = requests.get(final_url, timeout=60)
    response.raise_for_status()
    content = io.BytesIO(response.content)
    return pd.read_excel(content, sheet_name=sheet_name, header=header_row)


def load_uploaded_file(uploaded_file, sheet_name, header_row):
    suffix = Path(uploaded_file.name).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file, sheet_name=sheet_name, header=header_row)


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

    df["supplier_article"] = df["supplier_article"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["supplier_article"] != ""]
    df = df[~df["name"].str.startswith("(")]
    df["price"] = df["price"].apply(to_float)
    df["stock"] = None
    df["image_url"] = df["supplier_article"].apply(
        lambda x: f"https://velozapchasti-optom.ru/?product={x}" if x else ""
    )
    df["supplier"] = "supplier1"
    return df[["supplier", "supplier_article", "name", "price", "stock", "image_url"]]


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

    df["supplier_article"] = df["supplier_article"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[(df["supplier_article"] != "") & (df["name"] != "")]
    df["stock"] = df["stock"].apply(normalize_stock)

    for col in ["price", "price_opt2", "price_opt3", "price_rrc"]:
        if col in df.columns:
            df[col] = df[col].apply(to_float)

    df["image_url"] = ""
    df["supplier"] = "supplier2"
    return df[["supplier", "supplier_article", "name", "price", "price_opt2", "price_opt3", "price_rrc", "stock", "image_url"]]


def parse_supplier3(df):
    df = df.rename(columns={
        "Наименование товара": "name",
        "Фото": "photo",
        "Цена, ₽": "price",
        "От 10шт": "price_opt10",
        "От 50шт": "price_opt50",
        "Безнал": "payment_note",
    }).copy()

    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["name"] != ""]
    df = df[~df["name"].str.contains("WhatsApp|Наличие и актуальные цены", case=False, na=False)]

    for col in ["price", "price_opt10", "price_opt50"]:
        if col in df.columns:
            df[col] = df[col].apply(to_float)

    df["supplier_article"] = ""
    df["stock"] = None
    df["image_url"] = df["photo"].astype(str).str.strip() if "photo" in df.columns else ""
    df["supplier"] = "supplier3"
    return df[["supplier", "supplier_article", "name", "price", "price_opt10", "price_opt50", "stock", "image_url"]]


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

    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["name"] != ""]
    df["stock"] = df["stock"].apply(normalize_stock)

    for col in ["price", "price_rrc", "own_price", "pack_qty", "weight_kg"]:
        if col in df.columns:
            df[col] = df[col].apply(to_float)

    df["supplier_article"] = ""
    df["image_url"] = df["photo"].astype(str).str.strip() if "photo" in df.columns else ""
    df["supplier"] = "supplier4"
    return df[["supplier", "supplier_article", "name", "price", "price_rrc", "own_price", "pack_qty", "weight_kg", "stock", "image_url"]]


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
            image = next((x for x in grp["image_url"].fillna("").tolist() if x), "")
            name = grp.iloc[0]["name"]
            norm = grp.iloc[0]["normalized_name"]

            master_rows.append({
                "master_id": master_id,
                "article": str(article),
                "name": name,
                "normalized_name": norm,
                "final_price": best_price,
                "final_stock": int(stock_sum) if pd.notna(stock_sum) else None,
                "final_image": image,
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
                "final_image": row.get("image_url", ""),
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
                "confidence": match[1],
            })
        else:
            master_rows.append({
                "master_id": master_id,
                "article": "",
                "name": row["name"],
                "normalized_name": norm,
                "final_price": row.get("base_price"),
                "final_stock": row.get("stock"),
                "final_image": row.get("image_url", ""),
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


if "offers_by_supplier" not in st.session_state:
    st.session_state.offers_by_supplier = {}

if "master_df" not in st.session_state:
    st.session_state.master_df = pd.DataFrame()

if "mapping_df" not in st.session_state:
    st.session_state.mapping_df = pd.DataFrame()

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
        selected_price_tier = st.selectbox(
            "Основная цена",
            list(PRICE_TIER_LABELS.keys()),
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
            if source_type == "file":
                if uploaded_file is None:
                    st.warning("Нужно загрузить файл.")
                    st.stop()
                raw_df = load_uploaded_file(uploaded_file, cfg["sheet_name"], cfg["header_row"])
            else:
                if not source_url.strip():
                    st.warning("Нужно указать ссылку.")
                    st.stop()
                raw_df = load_from_url(source_url.strip(), cfg["sheet_name"], cfg["header_row"])

            parsed = parse_supplier(supplier, raw_df)
            parsed = add_normalized_columns(parsed)
            parsed = apply_selected_price_tier(parsed, selected_price_tier)

            st.session_state.offers_by_supplier[supplier] = parsed

            st.success("Прайс обработан.")
            st.dataframe(parsed.head(100), use_container_width=True)

            csv_bytes = parsed.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "Скачать нормализованный CSV",
                data=csv_bytes,
                file_name=f"{supplier}_normalized.csv",
                mime="text/csv"
            )
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

elif page == "Итоговый прайс":
    st.title("📤 Итоговый прайс")

    master_df = st.session_state.master_df
    if master_df.empty:
        st.info("Сначала загрузите прайсы и выполните склейку.")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Карточек", len(master_df))
        c2.metric("С фото", int((master_df["final_image"].fillna("") != "").sum()))

        st.dataframe(master_df, use_container_width=True)

        csv_bytes = master_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "Скачать итоговый прайс CSV",
            data=csv_bytes,
            file_name="final_price_list.csv",
            mime="text/csv"
        )
