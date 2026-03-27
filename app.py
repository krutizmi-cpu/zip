from pathlib import Path
import pandas as pd
import streamlit as st

from config import SUPPLIERS, PRICE_TIER_LABELS
from database import (
    init_db, save_supplier_setting, get_supplier_setting,
    save_supplier_offers, get_supplier_offers, get_all_offers,
    replace_master_catalog, get_master_catalog, replace_mappings, get_mappings
)
from utils.file_loader import load_local_file
from services.url_loader import download_supplier_source
from parsers import get_parser
from services.price_selector import apply_selected_price_tier
from services.image_loader import download_images_for_df
from services.matcher import build_master_from_offers, find_suggestions

init_db()
st.set_page_config(page_title="Price Aggregator v5", layout="wide")

if "page" not in st.session_state:
    st.session_state.page = "dashboard"

def goto(page):
    st.session_state.page = page

with st.sidebar:
    st.markdown("## Price Aggregator")
    st.button("🏠 Дашборд", use_container_width=True, on_click=goto, args=("dashboard",))
    st.button("📥 Загрузка прайсов", use_container_width=True, on_click=goto, args=("upload",))
    st.button("🔎 Дубли и склейка", use_container_width=True, on_click=goto, args=("dedupe",))
    st.button("📤 Итоговый прайс", use_container_width=True, on_click=goto, args=("final",))
    st.button("❓ Инструкция", use_container_width=True, on_click=goto, args=("help",))

page = st.session_state.page

def render_supplier_stats():
    cards = []
    for s in SUPPLIERS.keys():
        rows = get_supplier_offers(s)
        cards.append({
            "Поставщик": SUPPLIERS[s]["label"],
            "Строк загружено": len(rows),
            "Источник по умолчанию": SUPPLIERS[s]["source_type"],
        })
    return pd.DataFrame(cards)

if page == "dashboard":
    st.title("🏠 Дашборд")
    st.write("Здесь команда работает по понятным шагам: загрузить прайсы → проверить дубли → скачать итоговый прайс.")

    c1, c2, c3 = st.columns(3)
    all_offers = get_all_offers()
    master = get_master_catalog()
    mappings = get_mappings()

    c1.metric("Всего строк поставщиков", len(all_offers))
    c2.metric("Карточек в итоговом прайсе", len(master))
    c3.metric("Связей товаров", len(mappings))

    st.subheader("Статус поставщиков")
    st.dataframe(render_supplier_stats(), use_container_width=True)

    st.info("Для старта нажмите слева «Загрузка прайсов».")

elif page == "upload":
    st.title("📥 Загрузка прайсов")
    supplier = st.selectbox(
        "Выберите поставщика",
        list(SUPPLIERS.keys()),
        format_func=lambda x: SUPPLIERS[x]["label"]
    )
    cfg = SUPPLIERS[supplier]
    saved = get_supplier_setting(supplier) or {}

    col1, col2 = st.columns(2)
    with col1:
        source_type = st.radio(
            "Источник прайса",
            ["file", "url"],
            index=0 if saved.get("source_type", cfg.get("source_type", "file")) == "file" else 1,
            horizontal=True
        )
    with col2:
        available_tiers = list(PRICE_TIER_LABELS.keys())
        default_tier = saved.get("selected_price_tier", cfg.get("default_price_tier", "price"))
        selected_price_tier = st.selectbox(
            "Какая цена будет основной",
            available_tiers,
            index=available_tiers.index(default_tier) if default_tier in available_tiers else 0,
            format_func=lambda x: PRICE_TIER_LABELS.get(x, x)
        )

    uploaded = None
    source_url = saved.get("source_value", "")

    if source_type == "file":
        uploaded = st.file_uploader("Загрузить прайс-файл", type=["xls", "xlsx", "csv"], key=f"file_{supplier}")
    else:
        source_url = st.text_input("Ссылка на прайс / Google Sheets", value=source_url, key=f"url_{supplier}")

    download_images = st.checkbox("Скачать картинки локально", value=False)

    if st.button("Обработать прайс", type="primary"):
        try:
            if source_type == "file":
                if not uploaded:
                    st.warning("Нужно выбрать файл.")
                    st.stop()
                df, meta = load_local_file(uploaded, cfg)
            else:
                if not source_url.strip():
                    st.warning("Нужно указать ссылку.")
                    st.stop()
                local_path = download_supplier_source(source_url.strip(), supplier)
                df, meta = load_local_file(local_path, cfg, is_path=True)

            parser = get_parser(supplier)
            parsed = parser(df, meta).parse()
            parsed = apply_selected_price_tier(parsed, selected_price_tier)

            if download_images:
                parsed = download_images_for_df(parsed)
            else:
                parsed["local_image"] = ""

            save_supplier_offers(supplier, parsed)
            save_supplier_setting(supplier, source_type, source_url, selected_price_tier)

            st.success("Прайс успешно обработан.")
            st.dataframe(parsed.head(100), use_container_width=True)

            csv_data = parsed.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "Скачать нормализованный CSV",
                data=csv_data,
                file_name=f"{supplier}_normalized.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.error(f"Ошибка при обработке: {e}")

elif page == "dedupe":
    st.title("🔎 Дубли и склейка")
    offers = pd.DataFrame(get_all_offers())
    if offers.empty:
        st.info("Сначала загрузите хотя бы один прайс.")
    else:
        if st.button("Запустить склейку", type="primary"):
            grouped, mappings = build_master_from_offers(offers)
            replace_master_catalog(grouped)
            replace_mappings(mappings)
            st.success("Склейка выполнена.")

        master_df = pd.DataFrame(get_master_catalog())
        map_df = pd.DataFrame(get_mappings())

        st.subheader("Итоговые карточки")
        st.dataframe(master_df, use_container_width=True)

        st.subheader("Сомнительные fuzzy-совпадения")
        if not map_df.empty:
            fuzzy_df = map_df[map_df["match_method"] == "name_fuzzy"]
            if fuzzy_df.empty:
                st.info("Сомнительных совпадений пока нет.")
            else:
                st.dataframe(fuzzy_df, use_container_width=True)
        else:
            st.info("Сначала запустите склейку.")

        st.subheader("Поиск похожего товара вручную")
        target_name = st.text_input("Введите название")
        if target_name:
            suggestions = find_suggestions(
                target_name,
                master_df["normalized_name"].dropna().tolist() if not master_df.empty else [],
                limit=5
            )
            sug_df = pd.DataFrame(suggestions, columns=["Похожее название", "Сходство"])
            st.dataframe(sug_df, use_container_width=True)

elif page == "final":
    st.title("📤 Итоговый прайс")
    master_df = pd.DataFrame(get_master_catalog())
    if master_df.empty:
        st.info("Сначала загрузите прайсы и выполните склейку.")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Карточек", len(master_df))
        c2.metric("С фото", int((master_df["final_image"].fillna("") != "").sum()) if "final_image" in master_df.columns else 0)

        st.dataframe(master_df, use_container_width=True)

        csv_data = master_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "Скачать итоговый прайс CSV",
            data=csv_data,
            file_name="final_price_list.csv",
            mime="text/csv"
        )

elif page == "help":
    st.title("❓ Инструкция")
    st.markdown("""
### Как пользоваться
1. Откройте раздел **Загрузка прайсов**
2. Выберите поставщика
3. Вставьте ссылку или загрузите файл
4. Нажмите **Обработать прайс**
5. Перейдите в раздел **Дубли и склейка**
6. Нажмите **Запустить склейку**
7. Перейдите в раздел **Итоговый прайс**
8. Скачайте готовый файл

### Для команды
- программист не нужен для ежедневной работы
- сотрудники работают только в браузере
- можно запускать локально или на сервере

### Что лучше сделать следующим этапом
- авторизация пользователей
- сохранение подтвержденных дублей навсегда
- отдельная страница карточки товара
- серверный запуск, чтобы входить по одной ссылке
""")
