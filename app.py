import streamlit as st
import pandas as pd

from database import init_db
from config import SUPPLIERS
from utils.file_loader import load_file
from parsers import get_parser

init_db()
st.set_page_config(page_title="Price Aggregator", layout="wide")
st.title("📦 Агрегатор прайсов")

supplier = st.selectbox(
    "Поставщик",
    list(SUPPLIERS.keys()),
    format_func=lambda x: SUPPLIERS[x]["label"]
)

uploaded = st.file_uploader("Загрузить прайс", type=["xls", "xlsx", "csv"])

if uploaded:
    df, meta = load_file(uploaded, SUPPLIERS[supplier])
    parser = get_parser(supplier)
    parsed = parser(df, meta).parse()

    st.subheader("Нормализованные данные")
    st.dataframe(parsed.head(50), use_container_width=True)
    st.caption(f"Строк после нормализации: {len(parsed)}")
