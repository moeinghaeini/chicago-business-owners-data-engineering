"""
Configuration settings for Chicago Business Demographics Data Lake
"""
import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings"""
    
    # Database
    database_url: str = "sqlite:///./chicago_business.db"
    
    # AWS S3
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_s3_bucket: str = "chicago-business-data-lake"
    aws_region: str = "us-east-1"
    
    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = True
    
    # Data Processing
    batch_size: int = 10000
    max_workers: int = 4
    
    # Data Lake Paths
    raw_data_path: str = "data/raw"
    processed_data_path: str = "data/processed"
    analytics_data_path: str = "data/analytics"
    
    class Config:
        env_file = ".env"
        case_sensitive = False

# Global settings instance
settings = Settings()

# Create data directories
Path(settings.raw_data_path).mkdir(parents=True, exist_ok=True)
Path(settings.processed_data_path).mkdir(parents=True, exist_ok=True)
Path(settings.analytics_data_path).mkdir(parents=True, exist_ok=True)
