import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Union
import yaml
import re

def load_config(config_path: str) -> Dict[str, Any]:
    
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(path, 'r', encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def read_excel_from_config(config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    
    excel_cfg = config.get("excel_reader", {})
    input_file = excel_cfg.get("input_file")
    sheet_name = excel_cfg.get("sheet_name", None)
    skip_empty =  bool(excel_cfg.get("skip_empty_sheets", True))
    encoding = excel_cfg.get("encoding", None)
    
    if not input_file:
        raise ValueError("Config key 'excel_reader.input_file' is required but missing.")
    
    path = Path(input_file)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {input_file}")
    
    try:
        xls = pd.ExcelFile(path)
        # Decide which sheets to parse:
        # - If sheet_name is None: read all sheets
        # - If it's a string: read that named sheet
        # - If it's an int: treat as positional index (0-based) into xls.sheet_names
        
        if sheet_name is None:
            target_sheets: List[Union[str, int]] = xls.sheet_names
        else:
            target_sheets = [sheet_name]
        
        sheets: Dict[str, pd.DataFrame] = {}
        
        for sh in target_sheets:
            if isinstance(sh, int):
                if sh < 0 or sh >= len(xls.sheet_names):
                    raise IndexError(f"sheet_name index out of range: {sh}")
                canonical_name = xls.sheet_names[sh]
            else:
                canonical_name = sh
            
            df = xls.parse(sh)
            
            if skip_empty and df.empty:
                print(f"Warning: sheet '{canonical_name}' is empty and will be skipped")
                continue
            
            sheets[canonical_name] = df
            
        if not sheets:
            raise ValueError("No non-empty sheets found in the Excel file.")
        
        return sheets
    
    except ImportError as ie:
        raise ImportError("Missing Excel dependency. Install 'openpyxl' for .xlsx/.xlsm and 'xlrd' for legacy .xls files.") from ie
    except Exception as e:
        raise ValueError(e)
    
def read_excel(config_path: str = "config/settings.yaml") -> Dict[str, pd.DataFrame]:
    
    config = load_config(config_path)
    return read_excel_from_config(config)


def save_sheets_to_parquet(
    sheets: Dict[str, pd.DataFrame],
    config: Dict[str, Any]
) -> None:
    
    pq_config = config.get("parquet_writer", {})
    output_dir = pq_config.get("output_dir")
    compression = pq_config.get("compressio", "snappy") # default: snappy (fast & widely supported)
    overwrite = bool(pq_config.get("overwrite", True))
    safe_names = bool(pq_config.get("sanitize_names", True))
    
    if not output_dir:
        raise ValueError("Config key 'parquet_writer.output_dir' is required but missing.")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
        
    
    for sheet_name, df in sheets.items():
        if safe_names:
            safe_sheet_name = re.sub(r'[^a-zA-Z0-9_]', '', sheet_name.replace(" ", "_").lower())    
        else:
            safe_sheet_name = sheet_name
                    
        file_name = f"{safe_sheet_name}.parquet"
        file_path = output_path / file_name
        
        if file_path.exists() and not overwrite:
            print(f"Skipping sheet '{sheet_name}' → {file_path} already exists (overwrite disabled).")
            continue
        
        try:
            df.to_parquet(file_path, compression=compression, index=False)
            print(f"Saved sheet '{sheet_name}' to {file_path}")
        except Exception as e:
            raise RuntimeError(
                f"Failed to save sheet '{sheet_name}' to Parquet at {file_path}: {e}"
            ) from e

def validate_parquet_schema(file_path: str) -> pd.DataFrame:


    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Parquet file not found: {file_path}")
    
    df = pd.read_parquet(file_path)  
    
    print(f"\nPreview of the data from {file_path}:")
    print(df.head())

    print("\nSchema of the DataFrame:")
    print(df.dtypes)

    return df

    
if __name__ == "__main__":
    
    try:
        config = load_config("config/settings.yaml")
        sheets = read_excel_from_config(config)
        
        for name, df in sheets.items():
            print(f"\nSheet: {name} | Rows: {len(df)} | Columns: {list(df.columns)}")
            print(df.head())
        
        save_sheets_to_parquet(sheets, config)
        
        pq_config = config.get("parquet_writer", {})
        output_dir = pq_config.get("output_dir")
        
        for parquet_file in Path(output_dir).glob("*parquet"):
            validate_parquet_schema(parquet_file)
            
    except Exception as e:
        print(f"Error reading Excel file: {e}")
