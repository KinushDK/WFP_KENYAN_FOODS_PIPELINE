"""
Data extraction module for WFP Foods pipeline
"""
import logging
import pandas as pd
from typing import Optional
from pathlib import Path

# Configure logger
logger = logging.getLogger(__name__)

def extract_data(file_path: str, encoding: str = "latin1") -> Optional[pd.DataFrame]:
    """
    Extract data from CSV file
    
    Args:
        file_path: Path to the CSV file
        encoding: File encoding (default: latin1)
    
    Returns:
        DataFrame with extracted data or None if extraction fails
    
    Raises:
        FileNotFoundError: If the file doesn't exist
        pd.errors.EmptyDataError: If the file is empty
        pd.errors.ParserError: If the file cannot be parsed
    """
    try:
        # Check if file exists
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Starting data extraction from: {file_path}")
        
        # Extract data
        df = pd.read_csv(file_path, encoding=encoding)
        
        # Log extraction results
        logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
        logger.debug(f"Columns: {list(df.columns)}")
        
        return df
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
        
    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty CSV file: {e}")
        raise
        
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {e}")
        raise
        
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {e}")
        raise

def preview_data(df: pd.DataFrame, n_rows: int = 5) -> None:
    """
    Preview first few rows of the dataframe
    
    Args:
        df: Input DataFrame
        n_rows: Number of rows to preview (default: 5)
    """
    logger.info(f"Previewing first {n_rows} rows of data:")
    logger.debug(f"\n{df.head(n_rows).to_string()}")