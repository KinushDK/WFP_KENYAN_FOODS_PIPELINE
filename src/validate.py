"""
Data validation module for WFP Foods pipeline
"""
import logging
import pandas as pd
from typing import Dict, Tuple, Optional

logger = logging.getLogger(__name__)

def validate_data(df: pd.DataFrame) -> Tuple[bool, Dict[str, any]]:
    """
    Validate cleaned data quality
    
    Args:
        df: Cleaned DataFrame to validate
    
    Returns:
        Tuple of (is_valid, validation_report)
    """
    logger.info("Starting data validation")
    
    validation_report = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "missing_values": {},
        "data_types": {},
        "unique_counts": {},
        "issues": []
    }
    
    try:
        # Check if DataFrame is empty
        if df.empty:
            validation_report["issues"].append("DataFrame is empty")
            logger.error("Validation failed: DataFrame is empty")
            return False, validation_report
        
        # Validate required columns
        required_columns = ["date", "commodity", "price", "market", "category"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validation_report["issues"].append(f"Missing required columns: {missing_columns}")
            logger.error(f"Missing required columns: {missing_columns}")
            return False, validation_report
        
        # Check missing values
        for col in required_columns:
            missing = df[col].isnull().sum()
            validation_report["missing_values"][col] = missing
            if missing > 0:
                validation_report["issues"].append(f"Column '{col}' has {missing} missing values")
                logger.warning(f"Column '{col}' has {missing} missing values")
        
        # Validate data types
        for col in df.columns:
            validation_report["data_types"][col] = str(df[col].dtype)
        
        # Validate date range
        date_range_valid = validate_date_range(df)
        if not date_range_valid:
            validation_report["issues"].append("Date range validation failed")
        
        # Validate price range
        price_range_valid = validate_price_range(df)
        if not price_range_valid:
            validation_report["issues"].append("Price range validation failed")
        
        # Get unique counts for categorical columns
        categorical_columns = ["commodity", "market", "category", "admin1", "admin2"]
        for col in categorical_columns:
            if col in df.columns:
                validation_report["unique_counts"][col] = df[col].nunique()
                logger.debug(f"Column '{col}' has {df[col].nunique()} unique values")
        
        # Determine if validation passed
        is_valid = len(validation_report["issues"]) == 0
        
        if is_valid:
            logger.info("Data validation passed successfully")
        else:
            logger.warning(f"Data validation failed with {len(validation_report['issues'])} issues")
        
        return is_valid, validation_report
        
    except Exception as e:
        logger.error(f"Error during data validation: {e}")
        validation_report["issues"].append(f"Validation error: {str(e)}")
        return False, validation_report

def validate_date_range(df: pd.DataFrame) -> bool:
    """
    Validate that dates are within expected range
    
    Args:
        df: DataFrame with date column
    
    Returns:
        True if date range is valid, False otherwise
    """
    try:
        min_date = df["date"].min()
        max_date = df["date"].max()
        current_date = pd.Timestamp.now()
        
        logger.info(f"Date range: {min_date.date()} to {max_date.date()}")
        
        # Check if dates are not in the future
        if max_date > current_date:
            logger.warning(f"Future dates found: {max_date.date()}")
            return False
        
        # Check if date range is reasonable (not too old)
        if min_date < pd.Timestamp("2000-01-01"):
            logger.warning(f"Very old dates found: {min_date.date()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating date range: {e}")
        return False

def validate_price_range(df: pd.DataFrame) -> bool:
    """
    Validate that prices are within reasonable range
    
    Args:
        df: DataFrame with price column
    
    Returns:
        True if price range is valid, False otherwise
    """
    try:
        min_price = df["price"].min()
        max_price = df["price"].max()
        
        logger.info(f"Price range: {min_price:.2f} to {max_price:.2f}")
        
        # Check for unreasonable prices
        if max_price > 1000000:  # 1 million KES seems unreasonable
            logger.warning(f"Extremely high prices found: {max_price:.2f}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating price range: {e}")
        return False

def print_validation_report(report: Dict[str, any]) -> None:
    """
    Print validation report in a readable format
    
    Args:
        report: Validation report dictionary
    """
    logger.info("=" * 60)
    logger.info("DATA VALIDATION REPORT")
    logger.info("=" * 60)
    logger.info(f"Total Rows: {report['total_rows']}")
    logger.info(f"Total Columns: {report['total_columns']}")
    
    if report['missing_values']:
        logger.info("\nMissing Values:")
        for col, count in report['missing_values'].items():
            if count > 0:
                logger.info(f"  {col}: {count}")
    
    if report['unique_counts']:
        logger.info("\nUnique Value Counts:")
        for col, count in report['unique_counts'].items():
            logger.info(f"  {col}: {count}")
    
    if report['issues']:
        logger.warning("\nIssues Found:")
        for issue in report['issues']:
            logger.warning(f"  - {issue}")
    else:
        logger.info("\n✓ No issues found - Data is valid!")
    
    logger.info("=" * 60)