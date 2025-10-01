"""
Data Lake Storage Manager for Chicago Business Demographics
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import json
import boto3
from botocore.exceptions import ClientError
import s3fs

from config.settings import settings

logger = logging.getLogger(__name__)

class DataLakeStorageManager:
    """Manages data lake storage operations"""
    
    def __init__(self):
        self.s3_bucket = settings.aws_s3_bucket
        self.aws_region = settings.aws_region
        self.local_data_path = Path(settings.processed_data_path)
        self.analytics_path = Path(settings.analytics_data_path)
        
        # Initialize S3 client if AWS credentials are available
        self.s3_client = None
        self.s3_fs = None
        if settings.aws_access_key_id and settings.aws_secret_access_key:
            self._initialize_s3()
    
    def _initialize_s3(self):
        """Initialize S3 client and filesystem"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=self.aws_region
            )
            
            self.s3_fs = s3fs.S3FileSystem(
                key=settings.aws_access_key_id,
                secret=settings.aws_secret_access_key,
                region_name=self.aws_region
            )
            
            logger.info("S3 client initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize S3 client: {str(e)}")
            self.s3_client = None
            self.s3_fs = None
    
    def store_raw_data(self, df: pd.DataFrame, partition_date: str = None) -> str:
        """Store raw data in data lake with partitioning"""
        if partition_date is None:
            partition_date = datetime.now().strftime("%Y%m%d")
        
        # Create partition path
        partition_path = f"raw/business_owners/date={partition_date}"
        
        # Store locally
        local_path = self._store_local_parquet(df, partition_path, "raw_data.parquet")
        
        # Store in S3 if available
        s3_path = None
        if self.s3_fs:
            s3_path = self._store_s3_parquet(df, partition_path, "raw_data.parquet")
        
        logger.info(f"Raw data stored - Local: {local_path}, S3: {s3_path}")
        return local_path
    
    def store_processed_data(self, df: pd.DataFrame, partition_date: str = None) -> str:
        """Store processed data in data lake"""
        if partition_date is None:
            partition_date = datetime.now().strftime("%Y%m%d")
        
        partition_path = f"processed/business_owners/date={partition_date}"
        
        # Store locally
        local_path = self._store_local_parquet(df, partition_path, "processed_data.parquet")
        
        # Store in S3 if available
        s3_path = None
        if self.s3_fs:
            s3_path = self._store_s3_parquet(df, partition_path, "processed_data.parquet")
        
        logger.info(f"Processed data stored - Local: {local_path}, S3: {s3_path}")
        return local_path
    
    def store_analytics_data(self, analytics_data: Dict, partition_date: str = None) -> str:
        """Store analytics results in data lake"""
        if partition_date is None:
            partition_date = datetime.now().strftime("%Y%m%d")
        
        partition_path = f"analytics/demographics/date={partition_date}"
        
        # Store as JSON
        local_path = self._store_local_json(analytics_data, partition_path, "demographics_analytics.json")
        
        # Store in S3 if available
        s3_path = None
        if self.s3_fs:
            s3_path = self._store_s3_json(analytics_data, partition_path, "demographics_analytics.json")
        
        logger.info(f"Analytics data stored - Local: {local_path}, S3: {s3_path}")
        return local_path
    
    def store_aggregated_data(self, df: pd.DataFrame, aggregation_type: str, partition_date: str = None) -> str:
        """Store aggregated data for specific analytics"""
        if partition_date is None:
            partition_date = datetime.now().strftime("%Y%m%d")
        
        partition_path = f"aggregated/{aggregation_type}/date={partition_date}"
        
        # Store locally
        local_path = self._store_local_parquet(df, partition_path, f"{aggregation_type}_aggregated.parquet")
        
        # Store in S3 if available
        s3_path = None
        if self.s3_fs:
            s3_path = self._store_s3_parquet(df, partition_path, f"{aggregation_type}_aggregated.parquet")
        
        logger.info(f"Aggregated data stored - Local: {local_path}, S3: {s3_path}")
        return local_path
    
    def _store_local_parquet(self, df: pd.DataFrame, partition_path: str, filename: str) -> str:
        """Store DataFrame as Parquet locally"""
        full_path = self.local_data_path / partition_path
        full_path.mkdir(parents=True, exist_ok=True)
        
        file_path = full_path / filename
        df.to_parquet(file_path, index=False, compression='snappy')
        
        return str(file_path)
    
    def _store_s3_parquet(self, df: pd.DataFrame, partition_path: str, filename: str) -> Optional[str]:
        """Store DataFrame as Parquet in S3"""
        if not self.s3_fs:
            return None
        
        try:
            s3_path = f"{self.s3_bucket}/{partition_path}/{filename}"
            
            # Convert to PyArrow table for better S3 compatibility
            table = pa.Table.from_pandas(df)
            
            # Write to S3
            with self.s3_fs.open(s3_path, 'wb') as f:
                pq.write_table(table, f, compression='snappy')
            
            return s3_path
        except Exception as e:
            logger.error(f"Failed to store data in S3: {str(e)}")
            return None
    
    def _store_local_json(self, data: Dict, partition_path: str, filename: str) -> str:
        """Store data as JSON locally"""
        full_path = self.analytics_path / partition_path
        full_path.mkdir(parents=True, exist_ok=True)
        
        file_path = full_path / filename
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        return str(file_path)
    
    def _store_s3_json(self, data: Dict, partition_path: str, filename: str) -> Optional[str]:
        """Store data as JSON in S3"""
        if not self.s3_fs:
            return None
        
        try:
            s3_path = f"{self.s3_bucket}/{partition_path}/{filename}"
            
            with self.s3_fs.open(s3_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            return s3_path
        except Exception as e:
            logger.error(f"Failed to store JSON in S3: {str(e)}")
            return None
    
    def read_data(self, partition_path: str, filename: str, from_s3: bool = False) -> Optional[pd.DataFrame]:
        """Read data from data lake"""
        if from_s3 and self.s3_fs:
            return self._read_s3_parquet(partition_path, filename)
        else:
            return self._read_local_parquet(partition_path, filename)
    
    def _read_local_parquet(self, partition_path: str, filename: str) -> Optional[pd.DataFrame]:
        """Read Parquet file from local storage"""
        file_path = self.local_data_path / partition_path / filename
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return None
        
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            logger.error(f"Failed to read local parquet: {str(e)}")
            return None
    
    def _read_s3_parquet(self, partition_path: str, filename: str) -> Optional[pd.DataFrame]:
        """Read Parquet file from S3"""
        if not self.s3_fs:
            return None
        
        try:
            s3_path = f"{self.s3_bucket}/{partition_path}/{filename}"
            return pd.read_parquet(f"s3://{s3_path}")
        except Exception as e:
            logger.error(f"Failed to read S3 parquet: {str(e)}")
            return None
    
    def list_partitions(self, data_type: str = "processed") -> List[str]:
        """List available partitions"""
        base_path = self.local_data_path if data_type == "processed" else self.analytics_path
        partitions = []
        
        for partition_dir in base_path.glob(f"{data_type}/*/date=*"):
            if partition_dir.is_dir():
                partitions.append(str(partition_dir.relative_to(base_path)))
        
        return sorted(partitions)
    
    def get_latest_partition(self, data_type: str = "processed") -> Optional[str]:
        """Get the latest partition date"""
        partitions = self.list_partitions(data_type)
        if not partitions:
            return None
        
        # Extract dates and find latest
        dates = []
        for partition in partitions:
            if "date=" in partition:
                date_str = partition.split("date=")[-1]
                dates.append(date_str)
        
        return max(dates) if dates else None
    
    def cleanup_old_partitions(self, data_type: str = "processed", days_to_keep: int = 30):
        """Clean up old partitions to manage storage"""
        logger.info(f"Cleaning up partitions older than {days_to_keep} days...")
        
        partitions = self.list_partitions(data_type)
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
        
        cleaned_count = 0
        for partition in partitions:
            partition_path = self.local_data_path / partition if data_type == "processed" else self.analytics_path / partition
            if partition_path.exists():
                # Check if partition is older than cutoff
                if partition_path.stat().st_mtime < cutoff_date:
                    import shutil
                    shutil.rmtree(partition_path)
                    cleaned_count += 1
                    logger.info(f"Removed old partition: {partition}")
        
        logger.info(f"Cleaned up {cleaned_count} old partitions")
        return cleaned_count
