"""
Big Data ETL Pipeline for Chicago Business Demographics
ETL pipeline with staging, processing, and analytics layers
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, date
import uuid
import json
from pathlib import Path
import asyncio
import aiofiles
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp

from config.settings import settings

logger = logging.getLogger(__name__)

class BigDataETLPipeline:
    """ETL pipeline for big data processing"""
    
    def __init__(self):
        self.engine = create_engine(settings.database_url)
        self.Session = sessionmaker(bind=self.engine)
        self.batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.max_workers = settings.max_workers
        
    def run_full_pipeline(self, source_file: str) -> Dict[str, Any]:
        """Run the ETL pipeline"""
        logger.info("Starting Big Data ETL Pipeline...")
        start_time = datetime.now()
        
        try:
            # Stage 1: Extract and Load to Staging
            logger.info("Stage 1: Extract and Load to Staging")
            staging_results = self.extract_and_stage(source_file)
            
            # Stage 2: Data Quality Assessment
            logger.info("Stage 2: Data Quality Assessment")
            quality_results = self.assess_data_quality()
            
            # Stage 3: Transform and Load to Dimensions
            logger.info("Stage 3: Transform and Load to Dimensions")
            dimension_results = self.load_dimensions()
            
            # Stage 4: Load Fact Tables
            logger.info("Stage 4: Load Fact Tables")
            fact_results = self.load_fact_tables()
            
            # Stage 5: Create Aggregations
            logger.info("Stage 5: Create Aggregations")
            aggregation_results = self.create_aggregations()
            
            # Stage 6: Data Quality Validation
            logger.info("Stage 6: Data Quality Validation")
            validation_results = self.validate_data_quality()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            results = {
                'batch_id': self.batch_id,
                'staging': staging_results,
                'quality': quality_results,
                'dimensions': dimension_results,
                'facts': fact_results,
                'aggregations': aggregation_results,
                'validation': validation_results,
                'duration': str(duration),
                'status': 'success'
            }
            
            logger.info(f"ETL Pipeline completed successfully in {duration}")
            return results
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
    
    def extract_and_stage(self, source_file: str) -> Dict[str, Any]:
        """Extract data from source and load to staging tables"""
        logger.info("Extracting data from source file...")
        
        # Read CSV in chunks for memory efficiency
        chunk_size = 10000
        total_records = 0
        processed_chunks = 0
        
        try:
            with self.Session() as session:
                for chunk in pd.read_csv(source_file, chunksize=chunk_size):
                    # Process chunk
                    chunk_processed = self._process_chunk(chunk)
                    
                    # Load to staging table
                    self._load_chunk_to_staging(session, chunk_processed)
                    
                    total_records += len(chunk_processed)
                    processed_chunks += 1
                    
                    if processed_chunks % 10 == 0:
                        logger.info(f"Processed {processed_chunks} chunks, {total_records:,} records")
                
                session.commit()
                logger.info(f"Staging complete: {total_records:,} records processed")
                
                return {
                    'total_records': total_records,
                    'processed_chunks': processed_chunks,
                    'batch_id': self.batch_id
                }
                
        except Exception as e:
            logger.error(f"Error in extract_and_stage: {str(e)}")
            raise
    
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a data chunk"""
        # Clean and standardize data
        chunk_clean = chunk.copy()
        
        # Standardize string columns
        string_columns = ['Legal Name', 'Owner First Name', 'Owner Last Name', 
                         'Legal Entity Owner', 'Title']
        
        for col in string_columns:
            if col in chunk_clean.columns:
                chunk_clean[col] = chunk_clean[col].str.strip().str.upper()
                chunk_clean[col] = chunk_clean[col].replace('', np.nan)
        
        # Create derived fields
        chunk_clean['Owner Full Name'] = self._create_full_name(chunk_clean)
        chunk_clean['Is Individual Owner'] = chunk_clean['Legal Entity Owner'].isnull()
        chunk_clean['batch_id'] = self.batch_id
        chunk_clean['file_name'] = 'Business_Owners.csv'
        chunk_clean['raw_data'] = chunk_clean.to_json(orient='records')
        
        return chunk_clean
    
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
    
    def _load_chunk_to_staging(self, session, chunk: pd.DataFrame):
        """Load processed chunk to staging table"""
        try:
            # Convert to records
            records = chunk.to_dict('records')
            
            # Insert in batches
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                # Prepare data for insertion
                staging_data = []
                for record in batch:
                    staging_data.append({
                        'account_number': record.get('Account Number'),
                        'legal_name': record.get('Legal Name'),
                        'owner_first_name': record.get('Owner First Name'),
                        'owner_middle_initial': record.get('Owner Middle Initial'),
                        'owner_last_name': record.get('Owner Last Name'),
                        'suffix': record.get('Suffix'),
                        'legal_entity_owner': record.get('Legal Entity Owner'),
                        'title': record.get('Title'),
                        'raw_data': record.get('raw_data'),
                        'file_name': record.get('file_name'),
                        'batch_id': record.get('batch_id'),
                        'status': 'processed'
                    })
                
                # Bulk insert
                session.execute(text("""
                    INSERT INTO staging_business_owners 
                    (account_number, legal_name, owner_first_name, owner_middle_initial,
                     owner_last_name, suffix, legal_entity_owner, title, raw_data,
                     file_name, batch_id, status, processed_at)
                    VALUES (:account_number, :legal_name, :owner_first_name, :owner_middle_initial,
                            :owner_last_name, :suffix, :legal_entity_owner, :title, :raw_data,
                            :file_name, :batch_id, :status, CURRENT_TIMESTAMP)
                """), staging_data)
                
        except Exception as e:
            logger.error(f"Error loading chunk to staging: {str(e)}")
            raise
    
    def assess_data_quality(self) -> Dict[str, Any]:
        """Assess data quality of staged data"""
        logger.info("Assessing data quality...")
        
        with self.Session() as session:
            # Get quality metrics
            quality_query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN account_number IS NOT NULL THEN 1 END) as valid_account_numbers,
                    COUNT(CASE WHEN legal_name IS NOT NULL THEN 1 END) as valid_legal_names,
                    COUNT(CASE WHEN owner_first_name IS NOT NULL THEN 1 END) as valid_first_names,
                    COUNT(CASE WHEN title IS NOT NULL THEN 1 END) as valid_titles,
                    COUNT(DISTINCT account_number) as unique_businesses,
                    COUNT(DISTINCT CONCAT(owner_first_name, ' ', owner_last_name)) as unique_owners
                FROM staging_business_owners 
                WHERE batch_id = :batch_id
            """)
            
            result = session.execute(quality_query, {'batch_id': self.batch_id}).fetchone()
            
            # Calculate completeness scores
            total_records = result.total_records
            completeness_scores = {
                'account_number': (result.valid_account_numbers / total_records) * 100,
                'legal_name': (result.valid_legal_names / total_records) * 100,
                'owner_first_name': (result.valid_first_names / total_records) * 100,
                'title': (result.valid_titles / total_records) * 100
            }
            
            overall_completeness = np.mean(list(completeness_scores.values()))
            
            # Store quality metrics
            quality_metrics = {
                'total_records': total_records,
                'valid_records': result.valid_account_numbers,
                'invalid_records': total_records - result.valid_account_numbers,
                'completeness_score': round(overall_completeness, 2),
                'unique_businesses': result.unique_businesses,
                'unique_owners': result.unique_owners,
                'field_completeness': completeness_scores
            }
            
            # Insert quality record
            session.execute(text("""
                INSERT INTO staging_data_quality 
                (batch_id, table_name, total_records, valid_records, invalid_records,
                 completeness_score, quality_metrics)
                VALUES (:batch_id, 'staging_business_owners', :total_records, :valid_records,
                        :invalid_records, :completeness_score, :quality_metrics)
            """), {
                'batch_id': self.batch_id,
                'total_records': total_records,
                'valid_records': result.valid_account_numbers,
                'invalid_records': total_records - result.valid_account_numbers,
                'completeness_score': overall_completeness,
                'quality_metrics': json.dumps(quality_metrics)
            })
            
            session.commit()
            
            logger.info(f"Data quality assessment complete: {overall_completeness:.2f}% completeness")
            return quality_metrics
    
    def load_dimensions(self) -> Dict[str, Any]:
        """Load dimension tables from staging"""
        logger.info("Loading dimension tables...")
        
        with self.Session() as session:
            # Load business dimension
            business_count = self._load_business_dimension(session)
            
            # Load owner dimension
            owner_count = self._load_owner_dimension(session)
            
            # Load role dimension (already populated in schema)
            role_count = session.execute(text("SELECT COUNT(*) FROM dim_role")).scalar()
            
            logger.info(f"Dimensions loaded: {business_count} businesses, {owner_count} owners, {role_count} roles")
            
            return {
                'businesses': business_count,
                'owners': owner_count,
                'roles': role_count
            }
    
    def _load_business_dimension(self, session) -> int:
        """Load business dimension from staging"""
        # Get unique businesses from staging
        session.execute(text("""
            INSERT INTO dim_business (account_number, legal_name, business_type, business_size_category)
            SELECT DISTINCT 
                account_number,
                legal_name,
                CASE 
                    WHEN legal_name LIKE '%LLC%' THEN 'LLC'
                    WHEN legal_name LIKE '%INC%' THEN 'Corporation'
                    WHEN legal_name LIKE '%CORP%' THEN 'Corporation'
                    WHEN legal_name LIKE '%LTD%' THEN 'Limited'
                    ELSE 'Other'
                END as business_type,
                CASE 
                    WHEN LENGTH(legal_name) < 20 THEN 'Small'
                    WHEN LENGTH(legal_name) < 50 THEN 'Medium'
                    ELSE 'Large'
                END as business_size_category
            FROM staging_business_owners 
            WHERE batch_id = :batch_id 
            AND account_number IS NOT NULL
            ON CONFLICT (account_number) DO UPDATE SET
                legal_name = EXCLUDED.legal_name,
                business_type = EXCLUDED.business_type,
                business_size_category = EXCLUDED.business_size_category,
                updated_at = CURRENT_TIMESTAMP
        """), {'batch_id': self.batch_id})
        
        return session.execute(text("SELECT COUNT(*) FROM dim_business")).scalar()
    
    def _load_owner_dimension(self, session) -> int:
        """Load owner dimension from staging"""
        # Get unique owners from staging
        session.execute(text("""
            INSERT INTO dim_owner (full_name, first_name, middle_initial, last_name, suffix,
                                 is_individual, legal_entity_name, owner_type)
            SELECT DISTINCT 
                CONCAT(COALESCE(owner_first_name, ''), ' ', COALESCE(owner_middle_initial, ''), ' ', COALESCE(owner_last_name, ''), ' ', COALESCE(suffix, '')) as full_name,
                owner_first_name,
                owner_middle_initial,
                owner_last_name,
                suffix,
                legal_entity_owner IS NULL as is_individual,
                legal_entity_owner,
                CASE 
                    WHEN legal_entity_owner IS NULL THEN 'Individual'
                    ELSE 'Corporate'
                END as owner_type
            FROM staging_business_owners 
            WHERE batch_id = :batch_id 
            AND (owner_first_name IS NOT NULL OR legal_entity_owner IS NOT NULL)
            ON CONFLICT (full_name) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                middle_initial = EXCLUDED.middle_initial,
                last_name = EXCLUDED.last_name,
                suffix = EXCLUDED.suffix,
                is_individual = EXCLUDED.is_individual,
                legal_entity_name = EXCLUDED.legal_entity_name,
                owner_type = EXCLUDED.owner_type,
                updated_at = CURRENT_TIMESTAMP
        """), {'batch_id': self.batch_id})
        
        return session.execute(text("SELECT COUNT(*) FROM dim_owner")).scalar()
    
    def load_fact_tables(self) -> Dict[str, Any]:
        """Load fact tables from staging"""
        logger.info("Loading fact tables...")
        
        with self.Session() as session:
            # Load business ownership fact table
            ownership_count = self._load_ownership_facts(session)
            
            # Load business metrics
            metrics_count = self._load_business_metrics(session)
            
            # Load owner demographics
            demo_count = self._load_owner_demographics(session)
            
            logger.info(f"Fact tables loaded: {ownership_count} ownerships, {metrics_count} metrics, {demo_count} demographics")
            
            return {
                'ownerships': ownership_count,
                'metrics': metrics_count,
                'demographics': demo_count
            }
    
    def _load_ownership_facts(self, session) -> int:
        """Load business ownership fact table"""
        session.execute(text("""
            INSERT INTO fact_business_ownership 
            (business_id, owner_id, role_id, date_id, is_primary_owner, is_current)
            SELECT 
                b.business_id,
                o.owner_id,
                r.role_id,
                CURRENT_DATE as date_id,
                ROW_NUMBER() OVER (PARTITION BY s.account_number ORDER BY s.created_at) = 1 as is_primary_owner,
                TRUE as is_current
            FROM staging_business_owners s
            JOIN dim_business b ON s.account_number = b.account_number
            JOIN dim_owner o ON (
                (s.owner_first_name = o.first_name AND s.owner_last_name = o.last_name) OR
                (s.legal_entity_owner = o.legal_entity_name)
            )
            JOIN dim_role r ON s.title = r.title
            WHERE s.batch_id = :batch_id
            ON CONFLICT (business_id, owner_id, role_id, date_id) DO UPDATE SET
                is_primary_owner = EXCLUDED.is_primary_owner,
                is_current = EXCLUDED.is_current,
                updated_at = CURRENT_TIMESTAMP
        """), {'batch_id': self.batch_id})
        
        return session.execute(text("SELECT COUNT(*) FROM fact_business_ownership")).scalar()
    
    def _load_business_metrics(self, session) -> int:
        """Load business metrics fact table"""
        # Calculate metrics for each business
        session.execute(text("""
            INSERT INTO fact_business_metrics 
            (business_id, date_id, total_owners, individual_owners, corporate_owners,
             leadership_roles, ownership_roles, diversity_score, complexity_score)
            SELECT 
                b.business_id,
                CURRENT_DATE as date_id,
                COUNT(DISTINCT fbo.owner_id) as total_owners,
                COUNT(DISTINCT CASE WHEN o.is_individual THEN fbo.owner_id END) as individual_owners,
                COUNT(DISTINCT CASE WHEN NOT o.is_individual THEN fbo.owner_id END) as corporate_owners,
                COUNT(DISTINCT CASE WHEN r.is_leadership THEN fbo.owner_id END) as leadership_roles,
                COUNT(DISTINCT CASE WHEN r.is_ownership THEN fbo.owner_id END) as ownership_roles,
                CASE 
                    WHEN COUNT(DISTINCT fbo.owner_id) > 1 
                    THEN ROUND(COUNT(DISTINCT fbo.owner_id) * 1.0 / COUNT(DISTINCT fbo.owner_id), 2)
                    ELSE 0
                END as diversity_score,
                CASE 
                    WHEN COUNT(DISTINCT fbo.owner_id) > 1 
                    THEN ROUND(LOG(COUNT(DISTINCT fbo.owner_id)), 2)
                    ELSE 0
                END as complexity_score
            FROM dim_business b
            LEFT JOIN fact_business_ownership fbo ON b.business_id = fbo.business_id
            LEFT JOIN dim_owner o ON fbo.owner_id = o.owner_id
            LEFT JOIN dim_role r ON fbo.role_id = r.role_id
            WHERE fbo.is_current = TRUE
            GROUP BY b.business_id
            ON CONFLICT (business_id, date_id) DO UPDATE SET
                total_owners = EXCLUDED.total_owners,
                individual_owners = EXCLUDED.individual_owners,
                corporate_owners = EXCLUDED.corporate_owners,
                leadership_roles = EXCLUDED.leadership_roles,
                ownership_roles = EXCLUDED.ownership_roles,
                diversity_score = EXCLUDED.diversity_score,
                complexity_score = EXCLUDED.complexity_score
        """))
        
        return session.execute(text("SELECT COUNT(*) FROM fact_business_metrics")).scalar()
    
    def _load_owner_demographics(self, session) -> int:
        """Load owner demographics fact table"""
        session.execute(text("""
            INSERT INTO fact_owner_demographics 
            (owner_id, date_id, name_length, name_complexity_score, is_unique_name, name_frequency_rank)
            SELECT 
                o.owner_id,
                CURRENT_DATE as date_id,
                LENGTH(o.full_name) as name_length,
                CASE 
                    WHEN LENGTH(o.full_name) > 20 THEN 0.8
                    WHEN LENGTH(o.full_name) > 10 THEN 0.6
                    ELSE 0.4
                END as name_complexity_score,
                COUNT(*) OVER (PARTITION BY o.full_name) = 1 as is_unique_name,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as name_frequency_rank
            FROM dim_owner o
            JOIN fact_business_ownership fbo ON o.owner_id = fbo.owner_id
            WHERE fbo.is_current = TRUE
            GROUP BY o.owner_id, o.full_name
            ON CONFLICT (owner_id, date_id) DO UPDATE SET
                name_length = EXCLUDED.name_length,
                name_complexity_score = EXCLUDED.name_complexity_score,
                is_unique_name = EXCLUDED.is_unique_name,
                name_frequency_rank = EXCLUDED.name_frequency_rank
        """))
        
        return session.execute(text("SELECT COUNT(*) FROM fact_owner_demographics")).scalar()
    
    def create_aggregations(self) -> Dict[str, Any]:
        """Create aggregated tables"""
        logger.info("Creating aggregations...")
        
        with self.Session() as session:
            # Daily business aggregations
            session.execute(text("""
                INSERT INTO agg_daily_business 
                (date_id, total_businesses, new_businesses, multi_owner_businesses, 
                 single_owner_businesses, avg_owners_per_business)
                SELECT 
                    CURRENT_DATE as date_id,
                    COUNT(DISTINCT b.business_id) as total_businesses,
                    COUNT(DISTINCT CASE WHEN b.created_at::date = CURRENT_DATE THEN b.business_id END) as new_businesses,
                    COUNT(DISTINCT CASE WHEN fbm.total_owners > 1 THEN b.business_id END) as multi_owner_businesses,
                    COUNT(DISTINCT CASE WHEN fbm.total_owners = 1 THEN b.business_id END) as single_owner_businesses,
                    ROUND(AVG(fbm.total_owners), 2) as avg_owners_per_business
                FROM dim_business b
                LEFT JOIN fact_business_metrics fbm ON b.business_id = fbm.business_id
                WHERE fbm.date_id = CURRENT_DATE
                ON CONFLICT (date_id) DO UPDATE SET
                    total_businesses = EXCLUDED.total_businesses,
                    new_businesses = EXCLUDED.new_businesses,
                    multi_owner_businesses = EXCLUDED.multi_owner_businesses,
                    single_owner_businesses = EXCLUDED.single_owner_businesses,
                    avg_owners_per_business = EXCLUDED.avg_owners_per_business
            """))
            
            # Daily owner aggregations
            session.execute(text("""
                INSERT INTO agg_daily_owners 
                (date_id, total_owners, individual_owners, corporate_owners, unique_owners, most_common_role)
                SELECT 
                    CURRENT_DATE as date_id,
                    COUNT(DISTINCT o.owner_id) as total_owners,
                    COUNT(DISTINCT CASE WHEN o.is_individual THEN o.owner_id END) as individual_owners,
                    COUNT(DISTINCT CASE WHEN NOT o.is_individual THEN o.owner_id END) as corporate_owners,
                    COUNT(DISTINCT o.full_name) as unique_owners,
                    (SELECT title FROM dim_role r 
                     JOIN fact_business_ownership fbo ON r.role_id = fbo.role_id 
                     WHERE fbo.is_current = TRUE 
                     GROUP BY r.title 
                     ORDER BY COUNT(*) DESC 
                     LIMIT 1) as most_common_role
                FROM dim_owner o
                JOIN fact_business_ownership fbo ON o.owner_id = fbo.owner_id
                WHERE fbo.is_current = TRUE
                ON CONFLICT (date_id) DO UPDATE SET
                    total_owners = EXCLUDED.total_owners,
                    individual_owners = EXCLUDED.individual_owners,
                    corporate_owners = EXCLUDED.corporate_owners,
                    unique_owners = EXCLUDED.unique_owners,
                    most_common_role = EXCLUDED.most_common_role
            """))
            
            # Role distribution aggregations
            session.execute(text("""
                INSERT INTO agg_role_distribution 
                (date_id, role_id, role_count, percentage_of_total)
                SELECT 
                    CURRENT_DATE as date_id,
                    r.role_id,
                    COUNT(*) as role_count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage_of_total
                FROM dim_role r
                JOIN fact_business_ownership fbo ON r.role_id = fbo.role_id
                WHERE fbo.is_current = TRUE
                GROUP BY r.role_id
                ON CONFLICT (date_id, role_id) DO UPDATE SET
                    role_count = EXCLUDED.role_count,
                    percentage_of_total = EXCLUDED.percentage_of_total
            """))
            
            session.commit()
            
            # Get aggregation counts
            business_agg = session.execute(text("SELECT COUNT(*) FROM agg_daily_business")).scalar()
            owner_agg = session.execute(text("SELECT COUNT(*) FROM agg_daily_owners")).scalar()
            role_agg = session.execute(text("SELECT COUNT(*) FROM agg_role_distribution")).scalar()
            
            logger.info(f"Aggregations created: {business_agg} business, {owner_agg} owner, {role_agg} role aggregations")
            
            return {
                'business_aggregations': business_agg,
                'owner_aggregations': owner_agg,
                'role_aggregations': role_agg
            }
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """Validate data quality after ETL"""
        logger.info("Validating data quality...")
        
        with self.Session() as session:
            # Check for data integrity
            integrity_checks = {
                'orphaned_ownerships': session.execute(text("""
                    SELECT COUNT(*) FROM fact_business_ownership fbo 
                    LEFT JOIN dim_business b ON fbo.business_id = b.business_id 
                    WHERE b.business_id IS NULL
                """)).scalar(),
                
                'orphaned_owners': session.execute(text("""
                    SELECT COUNT(*) FROM fact_business_ownership fbo 
                    LEFT JOIN dim_owner o ON fbo.owner_id = o.owner_id 
                    WHERE o.owner_id IS NULL
                """)).scalar(),
                
                'orphaned_roles': session.execute(text("""
                    SELECT COUNT(*) FROM fact_business_ownership fbo 
                    LEFT JOIN dim_role r ON fbo.role_id = r.role_id 
                    WHERE r.role_id IS NULL
                """)).scalar()
            }
            
            # Check data completeness
            completeness_checks = {
                'total_businesses': session.execute(text("SELECT COUNT(*) FROM dim_business")).scalar(),
                'total_owners': session.execute(text("SELECT COUNT(*) FROM dim_owner")).scalar(),
                'total_ownerships': session.execute(text("SELECT COUNT(*) FROM fact_business_ownership")).scalar(),
                'total_metrics': session.execute(text("SELECT COUNT(*) FROM fact_business_metrics")).scalar()
            }
            
            validation_results = {
                'integrity_checks': integrity_checks,
                'completeness_checks': completeness_checks,
                'validation_timestamp': datetime.now().isoformat(),
                'status': 'passed' if all(v == 0 for v in integrity_checks.values()) else 'failed'
            }
            
            logger.info(f"Data quality validation: {validation_results['status']}")
            return validation_results

if __name__ == "__main__":
    # Example usage
    pipeline = BigDataETLPipeline()
    results = pipeline.run_full_pipeline("../../Business_Owners.csv")
    print("ETL Pipeline Results:")
    for key, value in results.items():
        print(f"  {key}: {value}")
