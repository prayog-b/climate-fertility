# Shapefile processing documentation
## Script 01a: Shapefile Cleaning
**File:** `cf-ssa-dm-01a-clean-shapefile.py`
### Overview
This Python script performs comprehensive cleaning and validation of Sub-Saharan African (SSA) shapefiles from IPUMS, preparing geographic boundary data for research on climate change and fertility. The script handles geometry repair, overlap resolution, data validation, and generates detailed reporting outputs.

### Input
- **File:** `bpl_data_smallest_unit_africa.shp`
- **Path:** `Climate_Change_and_Fertility_in_SSA/data/source/shapefiles/ipums/`
- **Format:** ESRI Shapefile with SSA geographic boundaries
- **Key Fields:**
  - `COUNTRY`: Country name
  - `CNTRY_CD`: Country code
  - `SMALLEST`: Smallest administrative unit identifier
  - `BPL_NAME`: Birth place name
  - `GEOLEV1`: First-level geographic unit
  - `GEOLEV2`: Second-level geographic unit
  - `geometry`: Polygon/MultiPolygon geometries

### Outputs
1. **Cleaned Shapefile**
   - **Path:** `Climate_Change_and_Fertility_in_SSA/data/derived/shapefiles/cleaned_ssa_boundaries.shp`
   - Clean, validated geographic boundaries

2. **Cleaning Report**
   - **Path:** `Climate_Change_and_Fertility_in_SSA/data/documentation/shapefile-cleaning/cleaning-report.txt`
   - Summary of all cleaning operations and statistics

3. **LaTeX Table**
   - **Path:** `Apps/Overleaf/climate-fertility-ssa/output/table/ssa_smallest_units_table.tex`
   - Breakdown of SMALLEST units by country and geographic level

4. **Diagnostic CSVs** (optional, when verbose mode enabled):
   - `smallest-multiple-bpl-names.csv`: SMALLEST units with multiple BPL_NAME values
   - `null-geometries.csv`: Records with null geometries
   - `country-code-mismatches.csv`: Country/code inconsistencies

---

### Dependencies

#### Required Python Packages
```python
import geopandas as gpd
import pandas as pd
import numpy as np
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
```

#### Installation

```bash
pip install geopandas pandas numpy shapely --break-system-packages
```

---

### Architecture
The script is organized into three main components:

#### 1. GeometryRepairer Class
A utility class that handles geometric validation and repair operations using multiple strategies.

 **a. `validate_polygon_geometry(geom, lenient=False)`**

_**Parameters:**_
- `geom`: Shapely geometry object
- `lenient` (bool): If True, attempts to fix invalid geometries using buffer(0)

_**Returns:**_ `bool` - True if geometry is valid

_**Validation Checks:**_
- Not None or empty
- Is Polygon or MultiPolygon instance
- Is valid geometry
- Has positive area

**b. `repair_with_difference(working_gdf, idx1, idx2)`**
Removes the overlap from the second geometry using geometric difference operation.
_**Parameters:**_
- `working_gdf`: GeoDataFrame being processed
- `idx1`: Index of first overlapping geometry
- `idx2`: Index of second overlapping geometry (will be modified)

_**Returns:**_ `bool` - True if repair successful

_**Process:**_
1. Calculate intersection of two geometries
2. Remove intersection from second geometry
3. Validate result
4. Update geometry if valid

**c. `repair_with_smallest_loss(working_gdf, idx1, idx2)`**
If the first strategy does not work, removes overlap from whichever polygon loses the smallest percentage of area

_**Parameters:**_
- `working_gdf`: GeoDataFrame being processed
- `idx1`: Index of first overlapping geometry
- `idx2`: Index of second overlapping geometry

_**Returns:**_ `bool` - True if repair successful

_**Strategy:**_
- Calculates percentage loss for both geometries
- Removes overlap from geometry with smaller loss
- Preserves as much original area as possible

**d. `repair_with_buffer(working_gdf, idx)`**
If the first two strategies fail, this attempts geometry repair using buffer operations. 

_**Parameters:**_
- `working_gdf`: GeoDataFrame being processed
- `idx`: Index of geometry to repair

_**Returns:**_ `bool` - True if repair successful

**e. `repair_overlap(working_gdf, idx1, idx2)`**
Tries multiple repair strategies for overlapping geometries.

_**Parameters:**_
- `working_gdf`: GeoDataFrame being processed
- `idx1`: Index of first overlapping geometry
- `idx2`: Index of second overlapping geometry

_**Returns:**_ `bool` - True if any strategy succeeds



#### 2. SSAShapefileCleaner Class
Main cleaning workflow orchestrator with data loading, cleaning, validation, and reporting.

#### 3. Main Execution Function
Entry point that configures paths and executes the cleaning workflow.

---

### Classes and Methods






---

#### SSAShapefileCleaner Class

Main class for the complete cleaning workflow.

##### Initialization

```python
SSAShapefileCleaner(input_path, output_path, overleaf_path, verbose=True)
```

**Parameters:**
- `input_path` (str): Path to input shapefile
- `output_path` (str): Path for cleaned shapefile output
- `overleaf_path` (str): Path for LaTeX table output
- `verbose` (bool): Enable detailed logging

**Attributes:**
- `gdf`: GeoDataFrame containing shapefile data
- `cleaning_log`: Dictionary tracking cleaning operations
- `repairer`: GeometryRepairer instance

##### Data Loading Methods

##### `load_data()`

Loads the shapefile and performs initial processing.

**Returns:** `bool` - True if successful

**Operations:**
- Reads shapefile using geopandas
- Standardizes country names (e.g., "Côte d'Ivoire" → "Ivory Coast")
- Records original count
- Initializes cleaning log

##### Diagnostic Methods (Non-Modifying)

##### `run_diagnostics()`

Runs all diagnostic checks without modifying data.

**Calls:**
- `check_smallest_identifiability()`
- `check_unknown_bpl_with_valid_geometries()`

##### `check_smallest_identifiability()`

Verifies dataset is uniquely identifiable at SMALLEST level.

**Checks:**
- Unique SMALLEST values
- Duplicate SMALLEST values
- SMALLEST units with multiple BPL_NAME values

**Outputs:** CSV file if problems found (verbose mode)

##### `check_unknown_bpl_with_valid_geometries()`

Analyzes 'unknown' BPL_NAME entries.

**Reports:**
- Total 'unknown' BPL_NAME entries
- Count with valid geometries

##### Cleaning Methods (Data-Modifying)

##### `remove_null_geometries()`

**Step 1:** Removes records with null geometries.

**Process:**
1. Identifies null geometry mask
2. Saves list of null geometries (verbose mode)
3. Removes records
4. Logs operation

##### `remove_very_small_geometries(percentile_threshold=1)`

**Step 2:** Removes very small geometries (potential slivers).

**Parameters:**
- `percentile_threshold` (int): Percentile threshold for size (default: 1)

**Process:**
1. Calculates area percentile threshold
2. Identifies geometries below threshold
3. Removes small geometries
4. Logs operation

**Note:** Currently commented out in main workflow but available if needed.
--
##### `handle_missing_smallest()`

**Step 3:** Handles missing SMALLEST values.

**Process:**
1. Identifies records with missing SMALLEST
2. Removes records with no SMALLEST and no GEOLEV information
3. Retains records with GEOLEV but no SMALLEST
4. Logs operation
--
##### `fix_country_code_mismatches()`

**Step 4:** Fixes Sudan/South Sudan country code mismatches.

**Process:**
1. Filters to complete data (COUNTRY, CNTRY_CD, SMALLEST all present)
2. Identifies mismatches where SMALLEST doesn't start with CNTRY_CD
3. Saves mismatch details (verbose mode)
4. Removes mismatched records
5. Logs operation

**Purpose:** Resolves boundary issues between Sudan and South Sudan.
--
##### `resolve_sliver_overlaps(overlap_threshold=5)`

**Step 5:** Resolves small overlapping geometries (slivers).

**Parameters:**
- `overlap_threshold` (float): Percentage threshold for classifying as sliver (default: 5%)

**Process:**
1. Creates spatial index for efficient intersection queries
2. Finds overlapping geometries
3. Classifies as sliver if overlap < threshold% of either polygon
4. Repairs overlaps using GeometryRepairer strategies
5. Attempts buffer repair on problematic geometries
6. Removes geometries that couldn't be repaired
7. Logs operation with resolution statistics

**Overlap Detection:**
- Uses R-tree spatial index for performance
- Checks all geometry pairs for overlaps
- Calculates overlap percentage for both geometries

**Repair Priority:**
1. Geometric difference
2. Smallest loss strategy
3. Buffer operations
4. Removal (last resort)
--
##### `clean_invalid_geometries()`

**Step 6:** Cleans remaining invalid geometries.

**Process:**
1. Validates all geometries (non-lenient)
2. Removes invalid geometries
3. Logs operation

--
##### Validation and Output Methods

##### `final_validation()`

Performs comprehensive validation of cleaned data.

**Validates:**
- Total record count
- Valid geometries percentage
- Valid SMALLEST values percentage

**Returns:** `bool` - True if data passes validation

--
##### `save_cleaned_data()`

Saves the cleaned GeoDataFrame to shapefile.

**Returns:** `bool` - True if successful

**Output:** ESRI Shapefile with standard components (.shp, .shx, .dbf, etc.)

--
##### `generate_cleaning_report()`

Generates detailed text report of cleaning operations.

**Report Contents:**
- Header with timestamp and file paths
- Summary statistics (original, final, removed, retention rate)
- Detailed step-by-step breakdown
- Details for each operation

**Output:** `cleaning-report.txt`

--
##### `generate_smallest_units_latex_table()`

Generates LaTeX table showing SMALLEST units by country and geographic level.

**Table Columns:**
1. Country name
2. Total unique SMALLEST units
3. Count mapped to GEOLEV2
4. Count mapped to GEOLEV1 only
5. Count mapped to COUNTRY only

**Output:** LaTeX-formatted table ready for academic papers

**Format:**
```latex
\begin{tabular}{lrrrr}
\toprule
Country & # SMALLEST & GEOLEV2 & GEOLEV1 & COUNTRY \\
\midrule
...
\bottomrule
\end{tabular}
```
--
##### Main Workflow Method

##### `run_complete_cleaning(run_diagnostics=False)`

Executes the complete cleaning workflow.

**Parameters:**
- `run_diagnostics` (bool): Whether to run diagnostic checks before cleaning

**Returns:** `bool` - True if successful

**Workflow Steps:**
1. Load data
2. Optional: Run diagnostics
3. Execute cleaning steps (1-6)
4. Final validation
5. Save cleaned data
6. Generate reports
7. Generate LaTeX table

---

