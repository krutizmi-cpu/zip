import pandas as pd
from .base_parser import BaseParser

class Supplier1Parser(BaseParser):
    def parse(self):
        df = self.df.copy()
        df = df.rename(columns={
            "Артикул": "supplier_article",
            "Наименование товара": "name",
            "Ед.": "unit",
            "в уп": "pack_qty",
            "цена": "price",
            "фото": "photo",
        })

        df["supplier_article"] = df["supplier_article"].apply(self.clean_text)
        df["name"] = df["name"].apply(self.clean_text)
        df = df[df["supplier_article"] != ""]
        df = df[~df["name"].str.startswith("(")]
        df["price"] = df["price"].apply(self.to_float)
        df["image_url"] = df["supplier_article"].apply(
            lambda x: f"https://velozapchasti-optom.ru/?product={x}" if x else None
        )
        df["supplier"] = "supplier1"
        df["stock"] = None
        return self.finish(df[["supplier", "supplier_article", "name", "price", "stock", "image_url"]])
