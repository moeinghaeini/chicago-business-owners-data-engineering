# Chicago Business Demographics Data Lake

A data engineering solution for analyzing Chicago business owner demographics and patterns using modern data lake architecture.

## 🏗️ Architecture Overview

This project implements a modern data lake architecture with the following components:

- **Data Ingestion Pipeline**: Processes raw CSV data with validation and cleaning
- **Analytics Engine**: Performs demographics analysis and pattern recognition
- **Data Lake Storage**: Partitioned storage with local and S3 support
- **REST API**: FastAPI-based API for data access
- **Interactive Dashboard**: Streamlit dashboard for visualization
- **Automated Pipeline**: End-to-end data processing automation

## 📊 Dataset Information

- **Source**: Chicago Business Owners dataset (324,542 records)
- **Time Period**: 2002 to present
- **Update Frequency**: Daily
- **Data Size**: ~19MB CSV, ~5MB processed Parquet

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- pip or conda

### Installation

1. **Clone and setup the project:**
```bash
cd chicago-business-demographics-data-lake
pip install -r requirements.txt
```

2. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. **Run the data pipeline:**
```bash
python scripts/run_pipeline.py --source-file ../../Business_Owners.csv --mode full
```

4. **Start the API server:**
```bash
python src/api/main.py
```

5. **Launch the dashboard:**
```bash
streamlit run src/dashboard/streamlit_app.py
```

## 📁 Project Structure

```
chicago-business-demographics-data-lake/
├── config/
│   └── settings.py              # Configuration management
├── src/
│   ├── data_ingestion/
│   │   └── ingestion_pipeline.py    # Data ingestion and cleaning
│   ├── analytics/
│   │   └── demographics_analyzer.py # Demographics analysis engine
│   ├── data_lake/
│   │   └── storage_manager.py      # Data lake storage management
│   ├── api/
│   │   └── main.py                 # FastAPI REST API
│   └── dashboard/
│       └── streamlit_app.py        # Interactive dashboard
├── scripts/
│   └── run_pipeline.py            # Main pipeline script
├── data/
│   ├── raw/                       # Raw data storage
│   ├── processed/                 # Processed data storage
│   └── analytics/                 # Analytics results storage
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment configuration template
└── README.md                      # This file
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# Database Configuration
DATABASE_URL=sqlite:///./chicago_business.db

# AWS S3 Configuration (optional)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_S3_BUCKET=chicago-business-data-lake
AWS_REGION=us-east-1

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=True

# Data Processing
BATCH_SIZE=10000
MAX_WORKERS=4
```

## 📊 Data Processing Pipeline

### 1. Data Ingestion
- Loads CSV data with optimized pandas settings
- Validates data quality and completeness
- Cleans and standardizes data fields
- Creates derived fields (full names, ownership types)

### 2. Data Cleaning
- Standardizes string formatting
- Handles missing values
- Creates ownership type classifications
- Generates data quality reports

### 3. Demographics Analysis
- Ownership pattern analysis
- Name-based demographics
- Business role analysis
- Geographic pattern recognition

### 4. Data Lake Storage
- Partitioned storage by date
- Parquet format for efficiency
- Local and S3 storage support
- Automated cleanup of old partitions

## 🎯 Key Features

### Data Analytics
- **Ownership Patterns**: Multi-owner business analysis
- **Demographics**: Name-based demographic insights
- **Role Analysis**: Business role distribution and patterns
- **Geographic Patterns**: Name-based geographic analysis

### Data Quality
- **Completeness Metrics**: Field-level completeness analysis
- **Duplicate Detection**: Identifies duplicate records
- **Data Validation**: Schema and format validation
- **Quality Reports**: Automated quality assessment

### Storage Management
- **Partitioned Storage**: Date-based partitioning
- **Format Optimization**: Parquet for efficient storage
- **Cloud Integration**: S3 support for scalability
- **Lifecycle Management**: Automated cleanup

## 🔌 API Endpoints

### Core Endpoints
- `GET /` - API information
- `GET /health` - Health check
- `GET /data-quality` - Data quality metrics

### Data Access
- `GET /businesses` - List businesses with pagination
- `GET /businesses/{account_number}` - Business details
- `GET /owners` - List owners with filters

### Analytics
- `GET /demographics/summary` - Demographics overview
- `GET /analytics/ownership-patterns` - Ownership analysis
- `GET /analytics/roles` - Role distribution
- `GET /analytics/names` - Name demographics

## 📈 Dashboard Features

### Overview Page
- Key metrics and KPIs
- Ownership distribution charts
- Role distribution analysis

### Businesses Page
- Searchable business directory
- Paginated results
- Detailed business information

### Owners Page
- Owner search and filtering
- Title-based filtering
- Owner details and relationships

### Analytics Page
- Interactive charts and visualizations
- Ownership pattern analysis
- Role and demographic insights

## 🚀 Usage Examples

### Running the Pipeline
```bash
# Full pipeline
python scripts/run_pipeline.py --source-file data/Business_Owners.csv --mode full

# Ingestion only
python scripts/run_pipeline.py --source-file data/Business_Owners.csv --mode ingestion

# Analytics only
python scripts/run_pipeline.py --processed-file data/processed/business_owners_processed.parquet --mode analytics
```

### API Usage
```bash
# Get demographics summary
curl http://localhost:8000/demographics/summary

# Search businesses
curl "http://localhost:8000/businesses?search=LLC&limit=10"

# Get ownership patterns
curl http://localhost:8000/analytics/ownership-patterns
```

### Dashboard Access
- Navigate to `http://localhost:8501`
- Use the sidebar to explore different sections
- Interactive charts and filters available

## 🔍 Data Insights

### Key Findings
- **Multi-owner businesses**: Analysis of businesses with multiple owners
- **Role distribution**: Most common business roles and titles
- **Name patterns**: Demographic insights from owner names
- **Ownership types**: Individual vs corporate ownership patterns

### Analytics Capabilities
- **Pattern Recognition**: Identifies common ownership patterns
- **Demographic Analysis**: Name-based demographic insights
- **Role Classification**: Business role categorization
- **Geographic Analysis**: Name-based geographic patterns

## 🛠️ Development

### Adding New Analytics
1. Extend `DemographicsAnalyzer` class
2. Add new analysis methods
3. Update API endpoints
4. Add dashboard visualizations

### Custom Storage
1. Extend `DataLakeStorageManager` class
2. Implement custom storage backends
3. Add new data formats
4. Configure partitioning strategies

## 📋 Data Quality

### Quality Metrics
- **Completeness**: Field-level completeness percentages
- **Consistency**: Data format and value consistency
- **Accuracy**: Data validation and verification
- **Timeliness**: Data freshness and update frequency

### Quality Reports
- Automated quality assessment
- Field-level completeness analysis
- Duplicate record identification
- Data type validation

## 🔒 Security & Privacy

### Data Protection
- No sensitive personal information stored
- Business names and roles only
- Local storage by default
- Optional S3 encryption

### Access Control
- API authentication (configurable)
- Dashboard access controls
- Data access logging
- Audit trail maintenance

## 📚 Documentation

### API Documentation
- Interactive API docs at `/docs`
- OpenAPI specification
- Request/response examples
- Error handling guide

### Code Documentation
- Comprehensive docstrings
- Type hints throughout
- Configuration examples
- Usage patterns

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions and support:
- Check the documentation
- Review the API documentation
- Open an issue on GitHub
- Contact the development team

## 🔄 Updates

### Version 1.0.0
- Initial release
- Core data pipeline
- Basic analytics
- REST API
- Streamlit dashboard

### Future Enhancements
- Real-time data processing
- Advanced ML analytics
- Enhanced visualizations
- Mobile dashboard support
- Data export capabilities
