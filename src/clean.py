"""
Data cleaning module for WFP Foods pipeline
"""
import logging
import pandas as pd
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

def clean_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Clean the extracted data
    
    Args:
        df: Raw DataFrame to clean
    
    Returns:
        Cleaned DataFrame or None if cleaning fails
    """
    try:
        logger.info("Starting data cleaning process")
        initial_rows = len(df)
        
        # Step 1: Standardize column names
        logger.info("Standardizing column names")
        df = standardize_columns(df)
        
        # Step 2: Remove duplicates
        logger.info("Removing duplicate rows")
        df = remove_duplicates(df)
        duplicates_removed = initial_rows - len(df)
        logger.debug(f"Removed {duplicates_removed} duplicate rows")
        
        # Step 3: Handle missing values
        logger.info("Handling missing values")
        df = handle_missing_values(df)
        
        # Step 4: Clean date column
        logger.info("Cleaning date column")
        df = clean_dates(df)
        
        # Step 5: Clean price column
        logger.info("Cleaning price column")
        df = clean_prices(df)
        
        # Log cleaning summary
        final_rows = len(df)
        logger.info(f"Data cleaning complete: {initial_rows} → {final_rows} rows "
                   f"({initial_rows - final_rows} rows removed)")
        
        return df
        
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}")
        raise

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to lowercase with underscores
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with standardized column names
    """
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    logger.debug(f"Standardized column names: {list(df.columns)}")
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from DataFrame
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with duplicates removed
    """
    before_count = len(df)
    df = df.drop_duplicates()
    after_count = len(df)
    
    if before_count != after_count:
        logger.debug(f"Removed {before_count - after_count} duplicate rows")
    
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in critical columns
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with missing values handled
    """
    critical_columns = ["commodity", "price"]
    
    # Log missing values before removal
    for col in critical_columns:
        missing_count = df[col].isnull().sum()
        if missing_count > 0:
            logger.warning(f"Found {missing_count} missing values in column: {col}")
    
    # Remove rows with missing critical data
    df = df.dropna(subset=critical_columns)
    logger.info(f"Removed rows with missing critical data")
    
    return df

def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert and validate date column
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with cleaned date column
    """
    # Convert to datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    # Log invalid dates
    invalid_dates = df["date"].isnull().sum()
    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} invalid date entries")
    
    # Remove rows with invalid dates
    df = df.dropna(subset=["date"])
    logger.debug(f"Removed {invalid_dates} rows with invalid dates")
    
    return df

def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate price column
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with cleaned price column
    """
    # Convert to numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    
    # Log invalid prices
    invalid_prices = df["price"].isnull().sum()
    if invalid_prices > 0:
        logger.warning(f"Found {invalid_prices} invalid price entries")
    
    # Remove rows with invalid prices
    df = df.dropna(subset=["price"])
    logger.debug(f"Removed {invalid_prices} rows with invalid prices")
    
    # Validate price range (non-negative)
    negative_prices = (df["price"] < 0).sum()
    if negative_prices > 0:
        logger.warning(f"Found {negative_prices} negative price values")
        df = df[df["price"] >= 0]
        logger.debug(f"Removed {negative_prices} rows with negative prices")
    
    return df