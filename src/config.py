"""
Configuration module for WFP Foods Pipeline
"""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    user: str
    password: str
    host: str
    port: str
    database: str
    
    @property
    def connection_string(self) -> str:
        """Build PostgreSQL connection string"""
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    db_config: DatabaseConfig
    table_name: str = "wfp_food_prices_clean"
    source_file_path: str = "D:\\Personnal\\WFP_FOODS_PIPELINE\\wfp_food_prices_ken.csv"
    file_encoding: str = "latin1"

def load_config() -> PipelineConfig:
    """
    Load configuration from environment variables or defaults
    
    Returns:
        PipelineConfig object with configuration settings
    """
    db_config = DatabaseConfig(
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "sekonda"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "wfp_foods")
    )
    
    return PipelineConfig(
        db_config=db_config,
        table_name=os.getenv("TABLE_NAME", "wfp_food_prices_clean"),
        source_file_path=os.getenv("SOURCE_FILE_PATH", 
                                   "D:\\Personnal\\WFP_FOODS_PIPELINE\\wfp_food_prices_ken.csv"),
        file_encoding=os.getenv("FILE_ENCODING", "latin1")
    )