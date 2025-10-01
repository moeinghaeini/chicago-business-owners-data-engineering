"""
Big Data Streaming Processor for Chicago Business Demographics
Real-time data processing with Apache Kafka, Apache Spark, and streaming analytics
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, AsyncGenerator
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import aioredis
import aiohttp
from dataclasses import dataclass
from enum import Enum
import uuid
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp

from config.settings import settings

logger = logging.getLogger(__name__)

class ProcessingStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class DataEvent:
    """Data event for streaming processing"""
    event_id: str
    event_type: str
    timestamp: datetime
    data: Dict[str, Any]
    source: str
    batch_id: Optional[str] = None
    status: ProcessingStatus = ProcessingStatus.PENDING

class BigDataStreamingProcessor:
    """Big data streaming processor with real-time analytics"""
    
    def __init__(self):
        self.engine = create_engine(settings.database_url)
        self.redis_client = None
        self.kafka_producer = None
        self.kafka_consumer = None
        self.processing_pool = ThreadPoolExecutor(max_workers=settings.max_workers)
        self.batch_size = settings.batch_size
        
    async def initialize(self):
        """Initialize streaming components"""
        try:
            # Initialize Redis for caching
            self.redis_client = aioredis.from_url("redis://localhost:6379")
            
            # Initialize Kafka producer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            # Initialize Kafka consumer
            self.kafka_consumer = KafkaConsumer(
                'business_events',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='business_analytics_group',
                auto_offset_reset='latest'
            )
            
            logger.info("Streaming components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize streaming components: {str(e)}")
            raise
    
    async def process_real_time_data(self, data_source: str) -> AsyncGenerator[DataEvent, None]:
        """Process real-time data from various sources"""
        logger.info(f"Starting real-time processing from {data_source}")
        
        try:
            if data_source == "api":
                async for event in self._process_api_stream():
                    yield event
            elif data_source == "database":
                async for event in self._process_database_stream():
                    yield event
            elif data_source == "file":
                async for event in self._process_file_stream():
                    yield event
            else:
                raise ValueError(f"Unknown data source: {data_source}")
                
        except Exception as e:
            logger.error(f"Error in real-time processing: {str(e)}")
            raise
    
    async def _process_api_stream(self) -> AsyncGenerator[DataEvent, None]:
        """Process data from API endpoints"""
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    # Simulate API data fetching
                    async with session.get('http://localhost:8000/businesses?limit=100') as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            for business in data.get('businesses', []):
                                event = DataEvent(
                                    event_id=str(uuid.uuid4()),
                                    event_type="business_update",
                                    timestamp=datetime.now(),
                                    data=business,
                                    source="api"
                                )
                                yield event
                    
                    await asyncio.sleep(30)  # Poll every 30 seconds
                    
                except Exception as e:
                    logger.error(f"Error processing API stream: {str(e)}")
                    await asyncio.sleep(60)  # Wait before retry
    
    async def _process_database_stream(self) -> AsyncGenerator[DataEvent, None]:
        """Process data from database changes"""
        last_processed = await self._get_last_processed_timestamp()
        
        while True:
            try:
                # Query for new or updated records
                with self.engine.connect() as conn:
                    query = text("""
                        SELECT * FROM staging_business_owners 
                        WHERE created_at > :last_processed
                        ORDER BY created_at
                        LIMIT :batch_size
                    """)
                    
                    result = conn.execute(query, {
                        'last_processed': last_processed,
                        'batch_size': self.batch_size
                    })
                    
                    records = result.fetchall()
                    
                    for record in records:
                        event = DataEvent(
                            event_id=str(uuid.uuid4()),
                            event_type="business_record",
                            timestamp=record.created_at,
                            data=dict(record._mapping),
                            source="database"
                        )
                        yield event
                        
                        last_processed = record.created_at
                    
                    await asyncio.sleep(10)  # Check every 10 seconds
                    
            except Exception as e:
                logger.error(f"Error processing database stream: {str(e)}")
                await asyncio.sleep(30)
    
    async def _process_file_stream(self) -> AsyncGenerator[DataEvent, None]:
        """Process data from file changes"""
        # Monitor file changes and process new data
        watched_files = ["data/raw/Business_Owners.csv"]
        
        for file_path in watched_files:
            try:
                if Path(file_path).exists():
                    # Process file in chunks
                    for chunk in pd.read_csv(file_path, chunksize=1000):
                        for _, row in chunk.iterrows():
                            event = DataEvent(
                                event_id=str(uuid.uuid4()),
                                event_type="file_record",
                                timestamp=datetime.now(),
                                data=row.to_dict(),
                                source="file"
                            )
                            yield event
                            
            except Exception as e:
                logger.error(f"Error processing file stream: {str(e)}")
    
    async def process_event(self, event: DataEvent) -> Dict[str, Any]:
        """Process a single data event"""
        try:
            event.status = ProcessingStatus.PROCESSING
            
            # Real-time analytics
            analytics_result = await self._perform_real_time_analytics(event)
            
            # Update cache
            await self._update_cache(event, analytics_result)
            
            # Publish to Kafka
            await self._publish_to_kafka(event, analytics_result)
            
            # Update database
            await self._update_database(event, analytics_result)
            
            event.status = ProcessingStatus.COMPLETED
            
            return {
                'event_id': event.event_id,
                'status': 'success',
                'analytics': analytics_result,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            event.status = ProcessingStatus.FAILED
            logger.error(f"Error processing event {event.event_id}: {str(e)}")
            return {
                'event_id': event.event_id,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    async def _perform_real_time_analytics(self, event: DataEvent) -> Dict[str, Any]:
        """Perform real-time analytics on event data"""
        analytics = {
            'event_type': event.event_type,
            'processing_time': datetime.now(),
            'metrics': {}
        }
        
        # Business metrics
        if event.event_type == "business_update":
            business_data = event.data
            analytics['metrics'] = {
                'business_id': business_data.get('account_number'),
                'owner_count': len(business_data.get('Owner Full Name', [])),
                'complexity_score': self._calculate_complexity_score(business_data),
                'diversity_score': self._calculate_diversity_score(business_data)
            }
        
        # Owner metrics
        elif event.event_type == "business_record":
            record_data = event.data
            analytics['metrics'] = {
                'owner_type': 'individual' if record_data.get('legal_entity_owner') is None else 'corporate',
                'name_length': len(record_data.get('owner_first_name', '')),
                'role_category': self._categorize_role(record_data.get('title', '')),
                'is_leadership': self._is_leadership_role(record_data.get('title', ''))
            }
        
        return analytics
    
    def _calculate_complexity_score(self, business_data: Dict) -> float:
        """Calculate business complexity score"""
        owner_count = len(business_data.get('Owner Full Name', []))
        name_lengths = [len(name) for name in business_data.get('Owner Full Name', [])]
        
        if owner_count == 0:
            return 0.0
        
        avg_name_length = np.mean(name_lengths) if name_lengths else 0
        complexity = (owner_count * 0.3) + (avg_name_length * 0.1)
        
        return min(complexity, 10.0)  # Cap at 10
    
    def _calculate_diversity_score(self, business_data: Dict) -> float:
        """Calculate ownership diversity score"""
        owners = business_data.get('Owner Full Name', [])
        if len(owners) <= 1:
            return 0.0
        
        # Simple diversity calculation based on name uniqueness
        unique_names = len(set(owners))
        total_names = len(owners)
        
        return (unique_names / total_names) * 10.0
    
    def _categorize_role(self, title: str) -> str:
        """Categorize business role"""
        if not title:
            return 'Unknown'
        
        title_lower = title.lower()
        
        if any(word in title_lower for word in ['ceo', 'president', 'chairman']):
            return 'Executive'
        elif any(word in title_lower for word in ['manager', 'director']):
            return 'Management'
        elif any(word in title_lower for word in ['owner', 'member', 'partner']):
            return 'Ownership'
        else:
            return 'Other'
    
    def _is_leadership_role(self, title: str) -> bool:
        """Check if role is leadership position"""
        if not title:
            return False
        
        leadership_keywords = ['ceo', 'president', 'chairman', 'director', 'manager']
        return any(keyword in title.lower() for keyword in leadership_keywords)
    
    async def _update_cache(self, event: DataEvent, analytics: Dict[str, Any]):
        """Update Redis cache with real-time data"""
        if not self.redis_client:
            return
        
        try:
            cache_key = f"business_analytics:{event.event_id}"
            cache_data = {
                'event': event.__dict__,
                'analytics': analytics,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.redis_client.setex(
                cache_key,
                3600,  # 1 hour TTL
                json.dumps(cache_data, default=str)
            )
            
        except Exception as e:
            logger.error(f"Error updating cache: {str(e)}")
    
    async def _publish_to_kafka(self, event: DataEvent, analytics: Dict[str, Any]):
        """Publish event to Kafka topic"""
        if not self.kafka_producer:
            return
        
        try:
            message = {
                'event_id': event.event_id,
                'event_type': event.event_type,
                'timestamp': event.timestamp.isoformat(),
                'data': event.data,
                'analytics': analytics
            }
            
            self.kafka_producer.send(
                'business_analytics',
                key=event.event_id,
                value=message
            )
            
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {str(e)}")
    
    async def _update_database(self, event: DataEvent, analytics: Dict[str, Any]):
        """Update database with real-time analytics"""
        try:
            with self.engine.connect() as conn:
                # Insert real-time analytics
                conn.execute(text("""
                    INSERT INTO real_time_analytics 
                    (event_id, event_type, timestamp, metrics, status)
                    VALUES (:event_id, :event_type, :timestamp, :metrics, :status)
                    ON CONFLICT (event_id) DO UPDATE SET
                        metrics = EXCLUDED.metrics,
                        status = EXCLUDED.status,
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'event_id': event.event_id,
                    'event_type': event.event_type,
                    'timestamp': event.timestamp,
                    'metrics': json.dumps(analytics['metrics']),
                    'status': event.status.value
                })
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating database: {str(e)}")
    
    async def _get_last_processed_timestamp(self) -> datetime:
        """Get last processed timestamp from cache"""
        if not self.redis_client:
            return datetime.now() - timedelta(hours=1)
        
        try:
            last_timestamp = await self.redis_client.get("last_processed_timestamp")
            if last_timestamp:
                return datetime.fromisoformat(last_timestamp.decode())
            else:
                return datetime.now() - timedelta(hours=1)
        except Exception as e:
            logger.error(f"Error getting last processed timestamp: {str(e)}")
            return datetime.now() - timedelta(hours=1)
    
    async def start_streaming_processor(self, data_source: str = "database"):
        """Start the streaming processor"""
        logger.info(f"Starting streaming processor for {data_source}")
        
        try:
            await self.initialize()
            
            async for event in self.process_real_time_data(data_source):
                # Process event asynchronously
                result = await self.process_event(event)
                logger.info(f"Processed event {event.event_id}: {result['status']}")
                
                # Update last processed timestamp
                if self.redis_client:
                    await self.redis_client.set(
                        "last_processed_timestamp",
                        event.timestamp.isoformat()
                    )
                
        except Exception as e:
            logger.error(f"Streaming processor failed: {str(e)}")
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup streaming resources"""
        try:
            if self.redis_client:
                await self.redis_client.close()
            
            if self.kafka_producer:
                self.kafka_producer.close()
            
            if self.kafka_consumer:
                self.kafka_consumer.close()
            
            self.processing_pool.shutdown(wait=True)
            
            logger.info("Streaming resources cleaned up")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

class BatchProcessor:
    """Batch processing for large datasets"""
    
    def __init__(self):
        self.engine = create_engine(settings.database_url)
        self.batch_size = settings.batch_size
        self.max_workers = settings.max_workers
    
    def process_large_dataset(self, source_file: str) -> Dict[str, Any]:
        """Process large dataset in batches"""
        logger.info(f"Starting batch processing of {source_file}")
        
        start_time = datetime.now()
        total_records = 0
        processed_batches = 0
        
        try:
            # Process file in chunks
            for chunk in pd.read_csv(source_file, chunksize=self.batch_size):
                # Process chunk
                processed_chunk = self._process_chunk(chunk)
                
                # Load to database
                self._load_chunk_to_database(processed_chunk)
                
                total_records += len(processed_chunk)
                processed_batches += 1
                
                if processed_batches % 10 == 0:
                    logger.info(f"Processed {processed_batches} batches, {total_records:,} records")
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info(f"Batch processing completed: {total_records:,} records in {duration}")
            
            return {
                'total_records': total_records,
                'processed_batches': processed_batches,
                'duration': str(duration),
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            raise
    
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a data chunk"""
        # Apply transformations
        chunk_clean = chunk.copy()
        
        # Data cleaning and standardization
        string_columns = ['Legal Name', 'Owner First Name', 'Owner Last Name', 'Title']
        for col in string_columns:
            if col in chunk_clean.columns:
                chunk_clean[col] = chunk_clean[col].str.strip().str.upper()
                chunk_clean[col] = chunk_clean[col].replace('', np.nan)
        
        # Create derived fields
        chunk_clean['Owner Full Name'] = self._create_full_name(chunk_clean)
        chunk_clean['Is Individual Owner'] = chunk_clean['Legal Entity Owner'].isnull()
        chunk_clean['Complexity Score'] = self._calculate_batch_complexity(chunk_clean)
        chunk_clean['Diversity Score'] = self._calculate_batch_diversity(chunk_clean)
        
        return chunk_clean
    
    def _create_full_name(self, df: pd.DataFrame) -> pd.Series:
        """Create full name from components"""
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
    
    def _calculate_batch_complexity(self, df: pd.DataFrame) -> pd.Series:
        """Calculate complexity score for batch"""
        return df.groupby('Account Number').transform(
            lambda x: min(len(x) * 0.5 + np.mean([len(str(name)) for name in x if pd.notna(name)]) * 0.1, 10.0)
        )
    
    def _calculate_batch_diversity(self, df: pd.DataFrame) -> pd.Series:
        """Calculate diversity score for batch"""
        return df.groupby('Account Number').transform(
            lambda x: (len(set(x)) / len(x)) * 10.0 if len(x) > 1 else 0.0
        )
    
    def _load_chunk_to_database(self, chunk: pd.DataFrame):
        """Load processed chunk to database"""
        try:
            with self.engine.connect() as conn:
                # Bulk insert to staging table
                chunk.to_sql(
                    'staging_business_owners',
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error loading chunk to database: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    async def main():
        processor = BigDataStreamingProcessor()
        await processor.start_streaming_processor("database")
    
    asyncio.run(main())
