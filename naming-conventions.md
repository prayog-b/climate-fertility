# Technical Documentation Supplement
This document provides additional technical guidance for maintaining and extending the Climate Change and Fertility in Sub-Saharan Africa codebase.

## File Naming Conventions
### Current Convention
```
cf-ssa-[section]-[number][letter]-[description].[extension]

Where:
- cf-ssa: Project prefix (Climate Fertility - Sub-Saharan Africa)
- section: dm (data management) or sa (statistical analysis)
- number: Major stage (01, 02, 03, ...)
- letter: Sub-step within stage (a, b, c, ...)
- description: Brief descriptive name (kebab-case)
- extension: .py, .do, .sh, .R
```

### Adding New Scripts
When adding new scripts, maintain this convention: 
1. Determine the stage
2. Assign next available letter at that stage
3. Use concise descriptive name
4. Update master script to call new script in correct order

## Script structure
### Python scripts
Here's a template preamble to draw from
```python
"""
Project:      Climate Change and Fertility in Sub-Saharan Africa
Script:       [script name]
Purpose:      [One-line description]
Author:       [Your name]
Date created: YYYY-MM-DD
Last updated: YYYY-MM-DD

Dependencies:
    - Python packages: numpy, pandas, geopandas, etc.
    - Input files: [list key inputs]
    - Prior scripts: [list dependencies]

Outputs:
    - [output file 1]: [description]
    - [output file 2]: [description]

Notes:
    - [Important considerations]
    - [Known limitations]
    - [Future improvements]
"""
```


### Stata scripts
```stata
********************************************************************************
* PROJECT:           Climate Change and Fertility in Sub-Saharan Africa
* SCRIPT:            [script name]
* PURPOSE:           [One-line description]
* AUTHOR:            [Your name]
* DATE CREATED:      YYYY-MM-DD
* LAST MODIFIED:     YYYY-MM-DD
* 
* DEPENDENCIES: 
*     - Packages: [e.g., reghdfe, estout]
*     - Input files: [list key inputs]
*     - Prior scripts: [list dependencies]
* 
* OUTPUTS:
*     - [output file 1]: [description]
*     - [output file 2]: [description]
*
* NOTES: 
*     - [Important considerations]
*     - [Known limitations]
********************************************************************************
```

## Useful Packages
### Stata packages
```stata
* Install commonly used packages
ssc install reghdfe      // High-dimensional fixed effects
ssc install estout       // Export tables to LaTeX
```

### Python packages
```python
# Geospatial
pip install geopandas rasterio shapely fiona

# Climate data
pip install xarray netCDF4 cdsapi

# Data manipulation
pip install pandas numpy dask

# Visualization
pip install matplotlib seaborn plotly folium

# Statistical
pip install scipy scikit-learn statsmodels
```
