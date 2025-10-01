"""
Data Ingestion Pipeline for Chicago Business Demographics Data Lake
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import json

from config.settings import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BusinessDataIngestion:
    """Handles ingestion of Chicago Business Owners data"""
    
    def __init__(self):
        self.raw_data_path = Path(settings.raw_data_path)
        self.processed_data_path = Path(settings.processed_data_path)
        
    def load_raw_data(self, file_path: str) -> pd.DataFrame:
        """Load raw CSV data with optimized settings"""
        logger.info(f"Loading data from {file_path}")
        
        try:
            # Load with optimized settings for large CSV
            df = pd.read_csv(
                file_path,
                dtype={
                    'Account Number': 'Int64',
                    'Legal Name': 'string',
                    'Owner First Name': 'string',
                    'Owner Middle Initial': 'string',
                    'Owner Last Name': 'string',
                    'Suffix': 'string',
                    'Legal Entity Owner': 'string',
                    'Title': 'string'
                },
                na_values=['', ' ', 'N/A', 'NULL', 'null'],
                keep_default_na=True
            )
            
            logger.info(f"Loaded {len(df):,} records")
            return df
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, any]:
        """Validate data quality and return metrics"""
        logger.info("Validating data quality...")
        
        quality_metrics = {
            'total_records': len(df),
            'total_businesses': df['Account Number'].nunique(),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'data_types': df.dtypes.to_dict(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        # Calculate completeness percentages
        quality_metrics['completeness'] = {
            col: (1 - df[col].isnull().sum() / len(df)) * 100 
            for col in df.columns
        }
        
        logger.info(f"Data quality validation complete: {quality_metrics['total_records']:,} records")
        return quality_metrics
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the data"""
        logger.info("Cleaning data...")
        
        df_clean = df.copy()
        
        # Clean string columns
        string_columns = ['Legal Name', 'Owner First Name', 'Owner Last Name', 
                         'Legal Entity Owner', 'Title']
        
        for col in string_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].str.strip().str.upper()
                df_clean[col] = df_clean[col].replace('', np.nan)
        
        # Clean Owner Middle Initial
        if 'Owner Middle Initial' in df_clean.columns:
            df_clean['Owner Middle Initial'] = df_clean['Owner Middle Initial'].str.strip().str.upper()
            df_clean['Owner Middle Initial'] = df_clean['Owner Middle Initial'].replace('', np.nan)
        
        # Clean Suffix
        if 'Suffix' in df_clean.columns:
            df_clean['Suffix'] = df_clean['Suffix'].str.strip().str.upper()
            df_clean['Suffix'] = df_clean['Suffix'].replace('', np.nan)
        
        # Create derived fields
        df_clean['Owner Full Name'] = self._create_full_name(df_clean)
        df_clean['Is Individual Owner'] = df_clean['Legal Entity Owner'].isnull()
        df_clean['Has Multiple Owners'] = df_clean.groupby('Account Number')['Account Number'].transform('count') > 1
        
        logger.info("Data cleaning complete")
        return df_clean
    
    def _create_full_name(self, df: pd.DataFrame) -> pd.Series:
        """Create full name from individual components"""
        def combine_name(row):
            parts = []
            if pd.notna(row.get('Owner First Name')):
                parts.append(row['Owner First Name'])
            if pd.notna(row.get('Owner Middle Initial')):
                parts.append(row['Owner Middle Initial'])
            if pd.notna(row.get('Owner Last Name')):
                parts.append(row['Owner Last Name'])
            if pd.notna(row.get('Suffix')):
                parts.append(row['Suffix'])
            return ' '.join(parts) if parts else None
        
        return df.apply(combine_name, axis=1)
    
    def save_processed_data(self, df: pd.DataFrame, filename: str = None) -> str:
        """Save processed data to parquet format"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"business_owners_processed_{timestamp}.parquet"
        
        file_path = self.processed_data_path / filename
        
        logger.info(f"Saving processed data to {file_path}")
        df.to_parquet(file_path, index=False, compression='snappy')
        
        return str(file_path)
    
    def save_quality_report(self, quality_metrics: Dict, filename: str = None) -> str:
        """Save data quality report"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_quality_report_{timestamp}.json"
        
        file_path = self.processed_data_path / filename
        
        with open(file_path, 'w') as f:
            json.dump(quality_metrics, f, indent=2, default=str)
        
        logger.info(f"Quality report saved to {file_path}")
        return str(file_path)
    
    def run_ingestion_pipeline(self, source_file: str) -> Dict[str, str]:
        """Run the complete ingestion pipeline"""
        logger.info("Starting data ingestion pipeline...")
        
        # Load data
        df = self.load_raw_data(source_file)
        
        # Validate quality
        quality_metrics = self.validate_data_quality(df)
        
        # Clean data
        df_clean = self.clean_data(df)
        
        # Save processed data
        processed_file = self.save_processed_data(df_clean)
        
        # Save quality report
        quality_report = self.save_quality_report(quality_metrics)
        
        logger.info("Data ingestion pipeline completed successfully")
        
        return {
            'processed_data': processed_file,
            'quality_report': quality_report,
            'total_records': len(df_clean),
            'total_businesses': df_clean['Account Number'].nunique()
        }

if __name__ == "__main__":
    # Example usage
    ingestion = BusinessDataIngestion()
    
    # Run pipeline on the source CSV
    source_file = "../../Business_Owners.csv"
    results = ingestion.run_ingestion_pipeline(source_file)
    
    print("Ingestion Results:")
    for key, value in results.items():
        print(f"  {key}: {value}")
