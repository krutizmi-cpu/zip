from pathlib import Path
import pandas as pd

def load_file(file, supplier_config):
    suffix = Path(file.name).suffix.lower()
    meta = {"filename": file.name, "suffix": suffix}

    if suffix == ".csv":
        df = pd.read_csv(file)
    elif suffix in [".xlsx", ".xlsm", ".xls"]:
        sheet = supplier_config.get("sheet_name", 0)
        header_row = supplier_config.get("header_row", 0)
        engine = "xlrd" if suffix == ".xls" else None
        df = pd.read_excel(file, sheet_name=sheet, header=header_row, engine=engine)
    else:
        raise ValueError(f"Unsupported format: {suffix}")

    return df, meta
