SUPPLIERS = {
    "supplier1": {
        "label": "1. Velozapchasti",
        "parser": "supplier1",
        "sheet_name": "Sheet1",
        "header_row": 7,
        "source_type": "file",
        "default_price_tier": "price",
    },
    "supplier2": {
        "label": "2. Форвард СПб",
        "parser": "supplier2",
        "sheet_name": "Лист_1",
        "header_row": 5,
        "source_type": "file",
        "default_price_tier": "price",
    },
    "supplier3": {
        "label": "3. Колхозник / Монстр",
        "parser": "supplier3",
        "sheet_name": "Лист1",
        "header_row": 1,
        "source_type": "url",
        "default_price_tier": "price",
    },
    "supplier4": {
        "label": "4. BRATAN",
        "parser": "supplier4",
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
