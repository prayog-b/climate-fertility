# Project:      Climate Change and Fertility in Sub-Saharan Africa
# Purpose:      Set up Python environment with necessary packages
# Author:       Prayog Bhattarai
# Date updated: 2025-10-24
# Notes:        Please update the PROJECT_DIR variable to your actual project path before running.


#!/bin/bash

# 🗂 Set to your actual project directory path
PROJECT_DIR="/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/dm"
VENV_PATH="$PROJECT_DIR/.venv"
REQUIREMENTS_FILE="$PROJECT_DIR/requirements.txt"
GITIGNORE="$PROJECT_DIR/.gitignore"


# Create project directory if it doesn't exist
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

echo "📁 Creating virtual environment in: $VENV_PATH ..."
python -m venv "$VENV_PATH"


echo "✅ Activating virtual environment ..."
source "$VENV_PATH/bin/activate"

echo "⬆️ Upgrading pip and setuptools ..."
pip install --upgrade pip setuptools wheel

echo "📦 Installing Python packages ..."
pip install \
    ipykernel \
    numpy \
    pandas \
    geopandas \
    rasterio \
    pyarrow \
    seaborn \
    typing-extensions \
    matplotlib \
    seaborn \
    tqdm \
    glob2 \
    shapely \
    fuzzywuzzy \
    python-levenshtein \
    xarray \
    netCDF4 \
    cartopy \
    folium \
    plotly \
    scipy \
    scikit-learn


echo "🧠 Registering this environment for Jupyter ..."
python -m ipykernel install --user --name=climate_env --display-name="Climate Fertility Env"


echo "📝 Exporting requirements.txt ..."
pip freeze > "$REQUIREMENTS_FILE"


echo "📄 Created: $REQUIREMENTS_FILE"

# Add common ignore patterns to .gitignore
echo "📄 Setting up .gitignore ..."
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

# PyInstaller
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
.hypothesis/
.pytest_cache/

# Jupyter Notebook
.ipynb_checkpoints

# pyenv
.python-version

# Environment variables
.env
.venv
ENV/
env/
venv/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Data files (adjust as needed)
*.csv
*.xlsx
*.xls
data/
*.h5
*.hdf5
*.nc
*.tif
*.tiff
*.shp
*.dbf
*.prj
*.shx
EOF

echo "📄 Created comprehensive .gitignore"

# Test the installation
echo "🧪 Testing installation ..."
python -c "
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
print('✅ All packages imported successfully!')
print(f'Python version: {sys.version}')
print(f'NumPy version: {np.__version__}')
print(f'Pandas version: {pd.__version__}')
print(f'GeoPandas version: {gpd.__version__}')
"

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Summary:"
echo "   🐍 Python environment: $VENV_PATH"
echo "   📦 Packages installed: $(pip list --format=freeze | wc -l) packages"
echo "   🧠 Jupyter kernel: 'Climate Fertility Env'"
echo "   📄 Requirements: $REQUIREMENTS_FILE"
echo ""
echo "👉 To activate the environment later, run:"
echo "   source $VENV_PATH/bin/activate"
echo ""
echo "👉 To deactivate when done:"
echo "   deactivate"
echo ""
echo "👉 To use in Jupyter:"
echo "   jupyter notebook"
echo "   Then select 'Climate Fertility Env' as your kernel"