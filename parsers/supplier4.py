from .base_parser import BaseParser

class Supplier4Parser(BaseParser):
    def parse(self):
        df = self.df.copy()
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
        })
        df["name"] = df["name"].apply(self.clean_text)
        df = df[df["name"] != ""]
        df["stock"] = df["stock"].apply(self.normalize_stock)
        for c in ["price", "price_rrc", "own_price", "pack_qty", "weight_kg"]:
            if c in df.columns:
                df[c] = df[c].apply(self.to_float)
        df["supplier_article"] = ""
        df["image_url"] = df["photo"].apply(self.clean_text) if "photo" in df.columns else ""
        df["supplier"] = "supplier4"
        return self.finish(df[["supplier", "supplier_article", "name", "price", "stock", "image_url", "price_rrc", "own_price", "pack_qty", "weight_kg"]])
