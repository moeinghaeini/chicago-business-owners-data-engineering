"""
FastAPI Application for Chicago Business Demographics Data Lake
"""
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Any
import pandas as pd
import logging
from datetime import datetime
import json

from src.analytics.demographics_analyzer import DemographicsAnalyzer
from src.data_lake.storage_manager import DataLakeStorageManager
from config.settings import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Chicago Business Demographics Data Lake API",
    description="API for accessing and analyzing Chicago business owner demographics",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
storage_manager = DataLakeStorageManager()

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Chicago Business Demographics Data Lake API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "data_quality": "/data-quality",
            "demographics": "/demographics",
            "businesses": "/businesses",
            "owners": "/owners",
            "analytics": "/analytics"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/data-quality")
async def get_data_quality():
    """Get data quality metrics"""
    try:
        # Try to read the latest processed data
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        # Read quality report
        quality_report_path = f"processed/business_owners/date={latest_partition}/data_quality_report_{latest_partition}.json"
        # This would need to be implemented based on how quality reports are stored
        
        return {"message": "Data quality metrics would be available here"}
    except Exception as e:
        logger.error(f"Error getting data quality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/demographics/summary")
async def get_demographics_summary():
    """Get demographics summary"""
    try:
        # Read latest processed data
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        df = storage_manager.read_data(
            f"processed/business_owners/date={latest_partition}",
            "processed_data.parquet"
        )
        
        if df is None:
            raise HTTPException(status_code=404, detail="Processed data not found")
        
        # Perform demographics analysis
        analyzer = DemographicsAnalyzer(df)
        summary = analyzer.generate_comprehensive_report()
        
        return summary
    except Exception as e:
        logger.error(f"Error getting demographics summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/businesses")
async def get_businesses(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, description="Search in business names")
):
    """Get list of businesses with pagination and search"""
    try:
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        df = storage_manager.read_data(
            f"processed/business_owners/date={latest_partition}",
            "processed_data.parquet"
        )
        
        if df is None:
            raise HTTPException(status_code=404, detail="Processed data not found")
        
        # Apply search filter if provided
        if search:
            df = df[df['Legal Name'].str.contains(search, case=False, na=False)]
        
        # Get unique businesses
        businesses = df.groupby('Account Number').agg({
            'Legal Name': 'first',
            'Owner Full Name': lambda x: list(x.dropna()),
            'Title': lambda x: list(x.dropna())
        }).reset_index()
        
        # Apply pagination
        total_count = len(businesses)
        businesses_page = businesses.iloc[offset:offset + limit]
        
        return {
            "businesses": businesses_page.to_dict('records'),
            "pagination": {
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            }
        }
    except Exception as e:
        logger.error(f"Error getting businesses: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/businesses/{account_number}")
async def get_business_details(account_number: int):
    """Get detailed information about a specific business"""
    try:
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        df = storage_manager.read_data(
            f"processed/business_owners/date={latest_partition}",
            "processed_data.parquet"
        )
        
        if df is None:
            raise HTTPException(status_code=404, detail="Processed data not found")
        
        # Filter for specific business
        business_data = df[df['Account Number'] == account_number]
        
        if business_data.empty:
            raise HTTPException(status_code=404, detail="Business not found")
        
        # Format response
        business_info = {
            "account_number": account_number,
            "legal_name": business_data.iloc[0]['Legal Name'],
            "owners": []
        }
        
        for _, row in business_data.iterrows():
            owner_info = {
                "full_name": row['Owner Full Name'],
                "first_name": row['Owner First Name'],
                "last_name": row['Owner Last Name'],
                "middle_initial": row['Owner Middle Initial'],
                "suffix": row['Suffix'],
                "title": row['Title'],
                "is_individual": row['Is Individual Owner'],
                "legal_entity_owner": row['Legal Entity Owner']
            }
            business_info["owners"].append(owner_info)
        
        return business_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting business details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/owners")
async def get_owners(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, description="Search in owner names"),
    title_filter: Optional[str] = Query(None, description="Filter by owner title")
):
    """Get list of owners with pagination and filters"""
    try:
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        df = storage_manager.read_data(
            f"processed/business_owners/date={latest_partition}",
            "processed_data.parquet"
        )
        
        if df is None:
            raise HTTPException(status_code=404, detail="Processed data not found")
        
        # Apply filters
        if search:
            df = df[df['Owner Full Name'].str.contains(search, case=False, na=False)]
        
        if title_filter:
            df = df[df['Title'].str.contains(title_filter, case=False, na=False)]
        
        # Apply pagination
        total_count = len(df)
        owners_page = df.iloc[offset:offset + limit]
        
        return {
            "owners": owners_page.to_dict('records'),
            "pagination": {
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            }
        }
    except Exception as e:
        logger.error(f"Error getting owners: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/ownership-patterns")
async def get_ownership_patterns():
    """Get ownership pattern analytics"""
    try:
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        df = storage_manager.read_data(
            f"processed/business_owners/date={latest_partition}",
            "processed_data.parquet"
        )
        
        if df is None:
            raise HTTPException(status_code=404, detail="Processed data not found")
        
        analyzer = DemographicsAnalyzer(df)
        patterns = analyzer.analyze_ownership_patterns()
        
        return patterns
    except Exception as e:
        logger.error(f"Error getting ownership patterns: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/roles")
async def get_role_analytics():
    """Get business role analytics"""
    try:
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        df = storage_manager.read_data(
            f"processed/business_owners/date={latest_partition}",
            "processed_data.parquet"
        )
        
        if df is None:
            raise HTTPException(status_code=404, detail="Processed data not found")
        
        analyzer = DemographicsAnalyzer(df)
        roles = analyzer.analyze_business_roles()
        
        return roles
    except Exception as e:
        logger.error(f"Error getting role analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/names")
async def get_name_analytics():
    """Get name-based analytics"""
    try:
        latest_partition = storage_manager.get_latest_partition("processed")
        if not latest_partition:
            raise HTTPException(status_code=404, detail="No processed data found")
        
        df = storage_manager.read_data(
            f"processed/business_owners/date={latest_partition}",
            "processed_data.parquet"
        )
        
        if df is None:
            raise HTTPException(status_code=404, detail="Processed data not found")
        
        analyzer = DemographicsAnalyzer(df)
        name_analytics = analyzer.analyze_name_demographics()
        
        return name_analytics
    except Exception as e:
        logger.error(f"Error getting name analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug
    )
