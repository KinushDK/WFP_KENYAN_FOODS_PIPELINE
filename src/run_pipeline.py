"""
Main WFP Foods Data Pipeline Script
"""
import sys
import os
import logging
from pathlib import Path

# Add parent directory to path for imports (no longer needed if running from correct location)
# But keep for flexibility
sys.path.insert(0, str(Path(__file__).parent))

from config import load_config
from extract import extract_data, preview_data
from clean import clean_data
from validate import validate_data, print_validation_report
from load import load_data_to_postgres

def setup_logging():
    """Configure logging for the pipeline"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = logging.INFO
    
    # Create logs directory in project root (one level up from src)
    project_root = Path(__file__).parent.parent
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_dir / "pipeline.log"),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def run_pipeline():
    """
    Main pipeline execution function
    """
    # Setup logging
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("WFP FOODS DATA PIPELINE STARTING")
    logger.info("=" * 60)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Step 1: Extract data
        logger.info("\n[STEP 1] EXTRACTING DATA")
        df_raw = extract_data(config.source_file_path, config.file_encoding)
        
        if df_raw is None:
            logger.error("Extraction failed - pipeline aborted")
            return False
        
        preview_data(df_raw)
        
        # Step 2: Clean data
        logger.info("\n[STEP 2] CLEANING DATA")
        df_clean = clean_data(df_raw)
        
        if df_clean is None:
            logger.error("Cleaning failed - pipeline aborted")
            return False
        
        logger.info(f"Cleaned data shape: {df_clean.shape}")
        
        # Step 3: Validate data
        logger.info("\n[STEP 3] VALIDATING DATA")
        is_valid, validation_report = validate_data(df_clean)
        print_validation_report(validation_report)
        
        if not is_valid:
            logger.warning("Validation found issues - continuing with load anyway")
        
        # Step 4: Load data to PostgreSQL
        logger.info("\n[STEP 4] LOADING DATA TO POSTGRESQL")
        success = load_data_to_postgres(
            df_clean,
            config.table_name,
            config.db_config,
            if_exists="append"  # Change to "replace" if you want to overwrite
        )
        
        if success:
            logger.info("\n✓ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Data loaded to table: {config.table_name}")
            logger.info(f"Total rows processed: {len(df_clean)}")
            return True
        else:
            logger.error("\n✗ PIPELINE FAILED DURING LOAD")
            return False
            
    except Exception as e:
        logger.error(f"\n✗ PIPELINE FAILED WITH ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)