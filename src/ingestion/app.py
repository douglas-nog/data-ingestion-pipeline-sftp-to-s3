import pandas as pd
from pathlib import Path
from typing import Optional
from typing import Dict

def read_excel(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        # Read all sheets
        sheets_dict = pd.read_excel(file_path, sheet_name=None)
        
        # Validate each sheet
        valid_sheets = {}
        for sheet_name, df in sheets_dict.items():
            if df.empty:
                print(f"Warning: Sheet '{sheet_name}' is empty and will be skipped.")
            else:
                valid_sheets[sheet_name] = df

        if not valid_sheets:
            raise ValueError("No non-empty sheets found in the Excel file.")

        return valid_sheets
    
    except Exception as e:
        raise ValueError(e)
    


def save_sheets_to_parquet(
    sheets: Dict[str, pd.DataFrame],
    output_dir: str,
    compression: str = "snappy"
) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for sheet_name, df in sheets.items():
        file_name = f"{sheet_name.replace(' ', '_').lower()}.parquet"
        file_path = output_path / file_name
        
        try:
            df.to_parquet(file_path, compression=compression, index=False)
            print(f"Saved sheet '{sheet_name}' to {file_path}")
        except Exception as e:
            print(e)
    
if __name__ == "__main__":
    excel_file_path = "C:/dev/sample_data/ncr_ride_bookings.xlsx"
    
    output_dir = "C:/dev/sample_data/processed"
    
    try:
        sheets = read_excel(excel_file_path)
        
        for sheet_name, df in sheets.items():
            print(f"\nSheet: {sheet_name}, Rows: {len(df)}, Columns: {df.columns.tolist()}")
            print(df.head())
        
        save_sheets_to_parquet(sheets, output_dir)
    
    except Exception as e:
        print(f"Error reading Excel file: {e}")
