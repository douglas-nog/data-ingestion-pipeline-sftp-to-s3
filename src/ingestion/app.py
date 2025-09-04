import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Union
import yaml
import re
from logging import Logger

from src.logger import get_logger

logger = get_logger(__name__)

def load_config(config_path: str) -> Dict[str, Any]:
    
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(path, 'r', encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def read_excel_from_config(config: Dict[str, Any], logger: Logger) -> Dict[str, pd.DataFrame]:
    
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
    
    logger.info("Opening Excel file", extra={"input_file": str(path)})
    
    try:
        xls = pd.ExcelFile(path)
        # Decide which sheets to parse:
        # - If sheet_name is None: read all sheets
        # - If it's a string: read that named sheet
        # - If it's an int: treat as positional index (0-based) into xls.sheet_names
        logger.debug("Excel file opened", extra={"sheets": xls.sheet_names})
        
        if sheet_name is None:
            target_sheets: List[Union[str, int]] = xls.sheet_names
        else:
            target_sheets = [sheet_name]
        
        sheets: Dict[str, pd.DataFrame] = {}
        
        for sh in target_sheets:
            if isinstance(sh, int):
                if sh < 0 or sh >= len(xls.sheet_names):
                    raise IndexError(f"sheet_name index out of range: {sh}")
                canonical_name: str = xls.sheet_names[sh]
            else:
                canonical_name: str = str(sh)
                
            logger.info("Parsing sheet", extra={"sheet": canonical_name})
            
            df = xls.parse(sh)
            
            if skip_empty and df.empty:
                logger.warning(
                    "Skipping empty sheet",
                    extra={"sheet": canonical_name}
                )
                continue
            
            logger.info(
                "Sheet loaded",
                extra={"sheet": canonical_name, "rows": len(df), "cols": len(df.columns)}
            )
            sheets[canonical_name] = df
            
        if not sheets:
            raise ValueError("No non-empty sheets found in the Excel file.")
        
        return sheets
    
    except ImportError as ie:
        logger.error(
            "Missing Excel dependency: install openpyxl for .xlsx/.xlsm and xlrd for legacy .xls",
            exc_info=True
        )
        raise ImportError("Missing Excel dependency. Install 'openpyxl' for .xlsx/.xlsm and 'xlrd' for legacy .xls files.") from ie
    except Exception as e:
        logger.error("Failed to read Excel file", extra={"input_file": str(path)}, exc_info=True)
        raise 


def save_sheets_to_parquet(
    sheets: Dict[str, pd.DataFrame],
    config: Dict[str, Any],
    logger
) -> None:
    
    pq_config = config.get("parquet_writer", {})
    output_dir = pq_config.get("output_dir")
    compression = pq_config.get("compression", "snappy") # default: snappy (fast & widely supported)
    overwrite = bool(pq_config.get("overwrite", True))
    safe_names = bool(pq_config.get("sanitize_names", True))
    
    if not output_dir:
        raise ValueError("Config key 'parquet_writer.output_dir' is required but missing.")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(
        "Saving sheets to Parquet",
        extra={"output_dir": str(output_path), "compression": compression, "overwrite": overwrite}
    )
    
    for sheet_name, df in sheets.items():
        if safe_names:
            safe_sheet_name = re.sub(r'[^a-zA-Z0-9_]', '', sheet_name.replace(" ", "_").lower())    
        else:
            safe_sheet_name = sheet_name
                    
        file_name = f"{safe_sheet_name}.parquet"
        file_path = output_path / file_name
        
        if file_path.exists() and not overwrite:
            logger.warning(
                "File exists and overwrite disabled; skipping",
                extra={"sheet": sheet_name, "file_path": str(file_path)}
            )
            continue
        
        try:
            df.to_parquet(file_path, compression=compression, index=False)
            logger.info(
                "Parquet written",
                extra={"sheet": sheet_name, "file_path": str(file_path), "rows": len(df)}
            )
        except Exception as e:
            logger.error(
                "Failed to write Parquet",
                extra={"sheet": sheet_name, "file_path": str(file_path)},
                exc_info=True
            )
            raise RuntimeError(
                f"Failed to save sheet '{sheet_name}' to Parquet at {file_path}: {e}"
            ) from e

def validate_parquet_schema(file_path: str, logger) -> pd.DataFrame:


    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Parquet file not found: {file_path}")
    
    logger.info("Validating parquet schema", extra={"file_path": str(path)})
    
    df = pd.read_parquet(file_path)  
    
    logger.info(
        "Parquet loaded",
        extra={"rows": len(df), "cols": len(df.columns)}
    )

    logger.debug("Dtypes", extra={"dtypes": {c: str(t) for c, t in df.dtypes.items()}})

    return df

    
if __name__ == "__main__":
    
    try:
        config = load_config("config/settings.yaml")
        
        log_cfg = config.get("logging", {})
        logger_name = log_cfg.get("logger_name", "excel_pipeline")
        log_level = log_cfg.get("level", "INFO")
        logger = get_logger(name=logger_name, level=log_level)  # run_id gerado automaticamente

        logger.info("Pipeline start")
        
        
        sheets = read_excel_from_config(config, logger=logger)
        
        save_sheets_to_parquet(sheets, config, logger=logger)
        
        pq_config = config.get("parquet_writer", {})
        output_dir = pq_config.get("output_dir")
        
        for parquet_file in Path(output_dir).glob("*.parquet"):
            validate_parquet_schema(str(parquet_file), logger=logger)

        logger.info("Pipeline success")
            
    except Exception as e:
        logger = get_logger(name="excel_pipeline_fallback")
        logger.error("Pipeline failure", exc_info=True)
