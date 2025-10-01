#!/usr/bin/env python3
"""
Main pipeline script for Chicago Business Demographics Data Lake
"""
import sys
import os
from pathlib import Path
import logging
import argparse
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_ingestion.ingestion_pipeline import BusinessDataIngestion
from src.analytics.demographics_analyzer import DemographicsAnalyzer
from src.data_lake.storage_manager import DataLakeStorageManager
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_ingestion_pipeline(source_file: str):
    """Run the data ingestion pipeline"""
    logger.info("Starting data ingestion pipeline...")
    
    ingestion = BusinessDataIngestion()
    results = ingestion.run_ingestion_pipeline(source_file)
    
    logger.info("Ingestion pipeline completed successfully")
    return results

def run_analytics_pipeline(processed_file: str):
    """Run the analytics pipeline"""
    logger.info("Starting analytics pipeline...")
    
    # Load processed data
    import pandas as pd
    df = pd.read_parquet(processed_file)
    
    # Run demographics analysis
    analyzer = DemographicsAnalyzer(df)
    analytics_report = analyzer.generate_comprehensive_report()
    
    # Store analytics results
    storage_manager = DataLakeStorageManager()
    analytics_path = storage_manager.store_analytics_data(analytics_report)
    
    logger.info(f"Analytics pipeline completed. Results stored at: {analytics_path}")
    return analytics_path

def run_full_pipeline(source_file: str):
    """Run the complete data lake pipeline"""
    logger.info("Starting full data lake pipeline...")
    
    start_time = datetime.now()
    
    try:
        # Step 1: Data Ingestion
        logger.info("Step 1: Data Ingestion")
        ingestion_results = run_ingestion_pipeline(source_file)
        
        # Step 2: Analytics
        logger.info("Step 2: Analytics Processing")
        analytics_path = run_analytics_pipeline(ingestion_results['processed_data'])
        
        # Step 3: Store aggregated data
        logger.info("Step 3: Creating aggregated datasets")
        storage_manager = DataLakeStorageManager()
        
        # Load processed data for aggregation
        import pandas as pd
        df = pd.read_parquet(ingestion_results['processed_data'])
        
        # Create ownership aggregations
        ownership_agg = df.groupby('Account Number').agg({
            'Legal Name': 'first',
            'Owner Full Name': lambda x: list(x.dropna()),
            'Title': lambda x: list(x.dropna()),
            'Is Individual Owner': 'any',
            'Has Multiple Owners': 'first'
        }).reset_index()
        
        ownership_path = storage_manager.store_aggregated_data(
            ownership_agg, 
            "ownership_summary"
        )
        
        # Create role aggregations
        role_agg = df.groupby('Title').size().reset_index(name='count')
        role_agg = role_agg.sort_values('count', ascending=False)
        
        role_path = storage_manager.store_aggregated_data(
            role_agg,
            "role_distribution"
        )
        
        # Create name aggregations
        individual_owners = df[df['Is Individual Owner'] == True]
        name_agg = individual_owners.groupby('Owner First Name').size().reset_index(name='count')
        name_agg = name_agg.sort_values('count', ascending=False)
        
        name_path = storage_manager.store_aggregated_data(
            name_agg,
            "name_distribution"
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("Full pipeline completed successfully!")
        logger.info(f"Total duration: {duration}")
        
        return {
            'ingestion': ingestion_results,
            'analytics': analytics_path,
            'aggregations': {
                'ownership': ownership_path,
                'roles': role_path,
                'names': name_path
            },
            'duration': str(duration)
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Chicago Business Demographics Data Lake Pipeline")
    parser.add_argument(
        "--source-file",
        required=True,
        help="Path to source CSV file"
    )
    parser.add_argument(
        "--mode",
        choices=["ingestion", "analytics", "full"],
        default="full",
        help="Pipeline mode to run"
    )
    parser.add_argument(
        "--processed-file",
        help="Path to processed file (required for analytics mode)"
    )
    
    args = parser.parse_args()
    
    try:
        if args.mode == "ingestion":
            results = run_ingestion_pipeline(args.source_file)
            print(f"Ingestion completed: {results}")
            
        elif args.mode == "analytics":
            if not args.processed_file:
                raise ValueError("--processed-file is required for analytics mode")
            results = run_analytics_pipeline(args.processed_file)
            print(f"Analytics completed: {results}")
            
        elif args.mode == "full":
            results = run_full_pipeline(args.source_file)
            print(f"Full pipeline completed: {results}")
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
