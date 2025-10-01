#!/bin/bash

# Chicago Business Demographics Data Lake Setup Script
# This script sets up the project environment and dependencies

set -e  # Exit on any error

echo "🏢 Setting up Chicago Business Demographics Data Lake..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed. Please install Python 3.8+ and try again."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo "❌ Python $REQUIRED_VERSION+ is required, but you have $PYTHON_VERSION"
    exit 1
fi

echo "✅ Python $PYTHON_VERSION detected"

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Create data directories
echo "📁 Creating data directories..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/analytics

# Copy environment file
echo "⚙️ Setting up configuration..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "📝 Created .env file from template. Please edit it with your configuration."
else
    echo "✅ .env file already exists"
fi

# Make scripts executable
echo "🔧 Making scripts executable..."
chmod +x scripts/run_pipeline.py
chmod +x scripts/setup.sh

# Create initial data structure
echo "🗂️ Creating initial data structure..."
touch data/raw/.gitkeep
touch data/processed/.gitkeep
touch data/analytics/.gitkeep

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your configuration"
echo "2. Run the data pipeline: python scripts/run_pipeline.py --source-file ../../Business_Owners.csv --mode full"
echo "3. Start the API: python src/api/main.py"
echo "4. Launch dashboard: streamlit run src/dashboard/streamlit_app.py"
echo ""
echo "For more information, see README.md"
