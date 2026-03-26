class BaseParser:
    def __init__(self, df, config):
        self.df = df
        self.config = config

    def parse(self):
        return self.df.rename(columns={
            self.config["name_col"]: "name",
            self.config["price_col"]: "price",
            self.config["article_col"]: "article",
            self.config["stock_col"]: "stock",
        })
