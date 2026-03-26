import streamlit as st
from utils.file_loader import load_file
from database import init_db
from parsers.base_parser import BaseParser
from config import SUPPLIERS

init_db()

st.title("📦 Прайс агрегатор")

supplier = st.selectbox("Выбери поставщика", list(SUPPLIERS.keys()))

file = st.file_uploader("Загрузи прайс")

if file:
    df = load_file(file)

    parser = BaseParser(df, SUPPLIERS[supplier])
    parsed = parser.parse()

    st.write("Результат:")
    st.dataframe(parsed.head())
