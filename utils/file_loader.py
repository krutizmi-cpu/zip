import pandas as pd

def load_file(file):
    if file.name.endswith(".xlsx"):
        return pd.read_excel(file)
    elif file.name.endswith(".csv"):
        return pd.read_csv(file)
    else:
        raise ValueError("Unsupported format")
