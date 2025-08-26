import pandas as pd
import awswrangler as wr
from pathlib import Path
import yaml

def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def read_excel(file_path: str, sheet_name = None): 
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    if sheet_name:
        return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    else:
        return pd.read_excel(path, sheet_name=None, engine="openpyxl")
    
if __name__ == "__main__":
    config = load_config()
    
    df = read_excel(config["input"]["excel_file_path"])
    for sheet_name, df in df.items():
        print(f"\nAba: {sheet_name}")
        print(df.head(5))
        print(f"Linhas: {len(df)}, Colunas: {len(df.columns)}")