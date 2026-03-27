import pandas as pd
from .base_parser import BaseParser

class Supplier3Parser(BaseParser):
    def parse(self):
        df = self.df.copy()
        df = df.rename(columns={
            "Наименование товара": "name",
            "Фото": "photo",
            "Цена, ₽": "price",
            "От 10шт": "price_opt10",
            "От 50шт": "price_opt50",
            "Безнал": "payment_note",
        })

        df["name"] = df["name"].apply(self.clean_text)
        df = df[df["name"] != ""]
        df = df[~df["name"].str.contains("WhatsApp|Наличие и актуальные цены", case=False, na=False)]
        df["price"] = df["price"].apply(self.to_float)
        df["price_opt10"] = df["price_opt10"].apply(self.to_float)
        df["price_opt50"] = df["price_opt50"].apply(self.to_float)
        df["supplier"] = "supplier3"
        df["supplier_article"] = None
        df["stock"] = None
        df["image_url"] = df["photo"].apply(self.clean_text)
        return self.finish(df[["supplier", "supplier_article", "name", "price", "stock", "image_url", "price_opt10", "price_opt50", "payment_note"]])
