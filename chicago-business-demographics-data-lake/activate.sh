#!/bin/bash

# Chicago Business Demographics Data Lake - Virtual Environment Activation Script

echo "🏢 Chicago Business Demographics Data Lake"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Please run: python3 -m venv venv"
    exit 1
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Verify activation
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ Virtual environment activated successfully!"
    echo "📍 Virtual environment path: $VIRTUAL_ENV"
    echo "🐍 Python version: $(python --version)"
    echo ""
    echo "📦 Available commands:"
    echo "  • Run ETL pipeline: python scripts/run_pipeline.py --source-file ../../Business_Owners.csv --mode full"
    echo "  • Start API server: python src/api/main.py"
    echo "  • Launch dashboard: streamlit run src/dashboard/streamlit_app.py"
    echo "  • Run enhanced dashboard: streamlit run src/dashboard/enhanced_streamlit_app.py"
    echo ""
    echo "💡 To deactivate: deactivate"
else
    echo "❌ Failed to activate virtual environment"
    exit 1
fi
