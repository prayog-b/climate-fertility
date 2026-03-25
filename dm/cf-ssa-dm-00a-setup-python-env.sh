
#!/bin/bash

# Project:      Climate Change and Fertility in Sub-Saharan Africa
# Purpose:      Set up Python environment with necessary packages
# Author:       Prayog Bhattarai
# Date updated: 2025-10-29
# Notes:        Please update the PROJECT_DIR variable to your actual project path before running.



# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# CHECK PYTHON INSTALLATION
# ============================================================================

echo "🔍 Checking Python installation..."

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED} ERROR: Python 3 is not installed!${NC}"
    echo ""
    echo "Please install Python 3 first:"
    echo "  macOS:   brew install python"
    echo "  Windows: Download from https://www.python.org/downloads/"
    echo "  Linux:   sudo apt-get install python3 python3-pip"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

echo -e "${GREEN} Found Python $PYTHON_VERSION${NC}"

# Warn if Python version is too old
if [ "$PYTHON_MAJOR" -lt 3 ] || [ "$PYTHON_MINOR" -lt 8 ]; then
    echo -e "${YELLOW}  WARNING: Python $PYTHON_VERSION detected. Python 3.8+ recommended.${NC}"
    echo "Some packages may not work correctly."
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
    echo -e "${RED} ERROR: pip3 is not installed!${NC}"
    echo "Try: python3 -m ensurepip --upgrade"
    exit 1
fi

echo -e "${GREEN} pip3 is available${NC}"

# ============================================================================
# SETUP VIRTUAL ENVIRONMENT
# ============================================================================

# Accept project directory as argument, or use default
if [ -z "$1" ]; then
    PROJECT_DIR="/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/dm"
    echo -e "${YELLOW}  No project directory provided. Using default: $PROJECT_DIR${NC}"
else
    PROJECT_DIR="$1"
    echo " Using provided project directory: $PROJECT_DIR"
fi

VENV_PATH="$PROJECT_DIR/.venv"
REQUIREMENTS_FILE="$PROJECT_DIR/requirements.txt"
GITIGNORE="$PROJECT_DIR/.gitignore"

# Create project directory if it doesn't exist
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR" || exit 1

echo " Creating virtual environment in: $VENV_PATH ..."
python3 -m venv "$VENV_PATH"

if [ ! -f "$VENV_PATH/bin/activate" ]; then
    echo -e "${RED}ERROR: Virtual environment creation failed!${NC}"
    exit 1
fi

echo "Activating virtual environment ..."
source "$VENV_PATH/bin/activate"

echo "Upgrading pip and setuptools ..."
pip install --upgrade pip setuptools wheel

echo "Installing Python packages (this may take several minutes)..."
pip install \
    ipykernel \
    numpy \
    pandas \
    selenium \
    webdriver_manager \
    geopandas \
    rasterio \
    pyarrow \
    seaborn \
    typing-extensions \
    matplotlib \
    tqdm \
    glob2 \
    shapely \
    fuzzywuzzy \
    python-levenshtein \
    xarray \
    netCDF4 \
    h5netcdf \
    cartopy \
    folium \
    plotly \
    scipy \
    scikit-learn \
    cdsapi \
    libmagic

# Check if installation was successful
if [ $? -ne 0 ]; then
    echo -e "${RED} ERROR: Package installation failed!${NC}"
    echo "Check your internet connection and try again."
    exit 1
fi

echo "Registering this environment for Jupyter ..."
python3 -m ipykernel install --user --name=climate_env --display-name="Climate Fertility Env"

echo "Exporting requirements.txt ..."
pip freeze > "$REQUIREMENTS_FILE"
echo "Created: $REQUIREMENTS_FILE"

# Add common ignore patterns to .gitignore
echo "Setting up .gitignore ..."
cat > "$GITIGNORE" << 'EOF'
# Virtual environment
.venv/
venv/
env/

# Python cache files
__pycache__/
*.py[cod]
*$py.class
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Jupyter Notebook
.ipynb_checkpoints

# Environment variables
.env

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Data files
*.csv
*.xlsx
*.shp
*.nc
*.tif
EOF

echo "Created comprehensive .gitignore"

# Test the installation
echo "Testing installation ..."
python3 -c "
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
print('All packages imported successfully!')
print(f'Python version: {sys.version}')
print(f'NumPy version: {np.__version__}')
print(f'Pandas version: {pd.__version__}')
print(f'GeoPandas version: {gpd.__version__}')
"

if [ $? -ne 0 ]; then
    echo -e "${RED} ERROR: Package import test failed!${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN} Setup complete!${NC}"
echo ""
echo " Summary:"
echo "    Python version: $PYTHON_VERSION"
echo "    Environment: $VENV_PATH"
echo "    Packages: $(pip list --format=freeze | wc -l) packages"
echo "    Jupyter kernel: 'Climate Fertility Env'"
echo ""
echo "To activate the environment:"
echo "   source $VENV_PATH/bin/activate"
echo ""
echo "To deactivate:"
echo "   deactivate"