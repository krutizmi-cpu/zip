import pandas as pd
from services.matcher import normalize_name

class BaseParser:
    def __init__(self, df: pd.DataFrame, meta: dict | None = None):
        self.df = df.copy()
        self.meta = meta or {}

    def clean_text(self, value):
        if pd.isna(value):
            return ""
        return str(value).strip()

    def to_float(self, value):
        if pd.isna(value) or value == "":
            return None
        value = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".")
        try:
            return float(value)
        except Exception:
            return None

    def normalize_stock(self, value):
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

    def finish(self, df: pd.DataFrame) -> pd.DataFrame:
        if "name" in df.columns:
            df["normalized_name"] = df["name"].apply(normalize_name)
        return df
