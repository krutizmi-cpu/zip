from .base_parser import BaseParser

class Supplier2Parser(BaseParser):
    def parse(self):
        df = self.df.copy()
        df = df.rename(columns={
            "Артикул": "supplier_article",
            "Номенклатура": "name",
            "Остаток, шт.": "stock",
            "Цена Опт 1, руб.": "price",
            "Цена Опт 2, руб.": "price_opt2",
            "Цена Опт 3, руб.": "price_opt3",
            "Цена РРЦ, руб.": "price_rrc",
        })
        df["supplier_article"] = df["supplier_article"].apply(self.clean_text)
        df["name"] = df["name"].apply(self.clean_text)
        df = df[(df["supplier_article"] != "") & (df["name"] != "")]
        df = df[~df["name"].str.match(r"^\d+\.|^[A-Z ]+$", na=False)]
        df["stock"] = df["stock"].apply(self.normalize_stock)
        for c in ["price", "price_opt2", "price_opt3", "price_rrc"]:
            if c in df.columns:
                df[c] = df[c].apply(self.to_float)
        df["image_url"] = ""
        df["supplier"] = "supplier2"
        return self.finish(df[["supplier", "supplier_article", "name", "price", "stock", "image_url", "price_opt2", "price_opt3", "price_rrc"]])
