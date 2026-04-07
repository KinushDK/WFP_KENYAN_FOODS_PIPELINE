"""
Data loading module for WFP Foods pipeline
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional
from config import DatabaseConfig

logger = logging.getLogger(__name__)

def load_data_to_postgres(
    df: pd.DataFrame, 
    table_name: str, 
    db_config: DatabaseConfig,
    if_exists: str = "append"
) -> bool:
    """
    Load cleaned data to PostgreSQL database
    
    Args:
        df: Cleaned DataFrame to load
        table_name: Target table name
        db_config: Database configuration
        if_exists: How to behave if table exists ('fail', 'replace', 'append')
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Starting data load to PostgreSQL table: {table_name}")
        logger.info(f"Loading {len(df)} rows and {len(df.columns)} columns")
        
        # Create database engine
        engine = create_engine(db_config.connection_string)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
        
        # Load data to PostgreSQL
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method='multi',  # Faster insertion for large datasets
            chunksize=1000   # Insert in batches
        )
        
        # Verify load
        with engine.connect() as conn:
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = count_result.scalar()
            logger.info(f"Verified {row_count} rows in table '{table_name}'")
        
        logger.info(f"Successfully loaded data to '{table_name}'")
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Database error during load: {e}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error during data load: {e}")
        return False

def create_table_if_not_exists(engine, table_name: str, df: pd.DataFrame) -> bool:
    """
    Create table if it doesn't exist
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        df: DataFrame with schema to create
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if table exists
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
            """))
            exists = result.scalar()
        
        if not exists:
            logger.info(f"Creating table '{table_name}'")
            df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f"Table '{table_name}' created successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False