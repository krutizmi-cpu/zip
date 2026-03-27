from pathlib import Path
import pandas as pd

def load_local_file(file_or_path, supplier_config, is_path=False):
    if is_path:
        path = Path(file_or_path)
        suffix = path.suffix.lower()
        filename = path.name
    else:
        path = None
        suffix = Path(file_or_path.name).suffix.lower()
        filename = file_or_path.name

    meta = {"filename": filename, "suffix": suffix}

    if suffix == ".csv":
        df = pd.read_csv(path if is_path else file_or_path)
    elif suffix in [".xlsx", ".xlsm", ".xls"]:
        sheet = supplier_config.get("sheet_name", 0)
        header_row = supplier_config.get("header_row", 0)
        engine = "xlrd" if suffix == ".xls" else None
        df = pd.read_excel(path if is_path else file_or_path, sheet_name=sheet, header=header_row, engine=engine)
    else:
        raise ValueError(f"Неподдерживаемый формат: {suffix}")

    return df, meta
