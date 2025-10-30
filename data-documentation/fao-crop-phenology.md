# FAO Crop Phenology Data

## `cf-ssa-dm-03a-fao-download-crop-phenology-data.do`

This Stata script automates the download of crop phenology data from the FAO's Agricultural Stress Index System (ASIS) Google Cloud Storage repository. The script downloads geospatial data files containing information about crop growth stages across different growing seasons and land cover types.

## Prerequisites

Before running this script, ensure you have:
1. **Google Cloud SDK** installed with `gsutil` command-line tool
2. **Authentication** configured for accessing FAO's Google Cloud Storage bucket
3. **Network access** to Google Cloud Storage
4. **Sufficient disk space** for downloaded files (approximately 100+ MB)

## Script Functionality
### 1. Directory Setup

```stata
local download_path "$cf/data/source/fao/crop-phenology"
!mkdir -p "`download_path'"
```

- Defines a local macro for the download destination path
- Creates the directory structure if it doesn't exist
- Uses the `-p` flag to create parent directories as needed

### 2. Data Download

The script uses `gsutil -m cp` to download files in parallel from Google Cloud Storage:

- **`-m` flag:** Enables multi-threaded download for faster performance
- **`cp` command:** Copies files from remote storage to local directory

## Downloaded Data Files

The script downloads **26 files** organized by:

### Growing Seasons
- **GS1:** Growing Season 1 (first growing season in the year)
- **GS2:** Growing Season 2 (second growing season in the year)

### Phenological Stages
- **SOS:** Start of Season
- **MOS:** Middle/Maximum of Season
- **EOS:** End of Season

### Land Cover Types
- **LC-C:** Land Cover - Cropland
- **LC-G:** Land Cover - Grassland

### File Types
- **`.tif`:** GeoTIFF raster files containing spatial phenology data
- **`.json`:** JSON metadata files with data descriptions and parameters
- **`.html`:** HTML documentation file
- **`.json` (root):** Main JSON configuration/metadata file

## File Naming Convention

Files follow the pattern: `ASIS.PHE.[SEASON].[STAGE].[LANDCOVER].[EXTENSION]`

Example: `ASIS.PHE.GS1.EOS.LC-C.tif`
- **ASIS:** Agricultural Stress Index System
- **PHE:** Phenology dataset
- **GS1:** Growing Season 1
- **EOS:** End of Season
- **LC-C:** Land Cover Cropland
- **tif:** GeoTIFF format

## Usage

### Run from the master do file

1. Set the `$cf` global macro in the master script `cf-ssa-dm-00-master.do`
2. Execute the section below:
   ```stata
        display "Step 1: Downloading crop phenology data."
        do "$cf/dm/cf-ssa-dm-03a-fao-download-crop-phenology-data.do"
   ```

### Expected Output
After successful execution:
- All 26 files will be downloaded to `$cf/data/source/fao/crop-phenology/`

---

## `cf-ssa-dm-03b-fao-process-growing-season-data.py`
### Overview
This Python script processes FAO raster data to create indicators for whether each month is a growing season month. It extracts crop phenology information (start, middle, and end of growing seasons) from GeoTIFF files and generates summary statistics at the administrative unit level for multiple Sub-Saharan African countries.

### Prerequisites
#### Required Python packages
  ```bash
    pip install rasterio geopandas pandas numpy matplotlib seaborn shapely
  ```

#### Required Input Data
Required Input Data
**1. Raster Files:** FAO crop phenology GeoTIFF files (downloaded by companion Stata script)
  -  Location: `$CF_DIR/data/source/fao/crop-phenology/`
  -  Files include: `SOS` (Start of Season), `MOS` (Middle of Season), `EOS` (End of Season)
  -  For both growing seasons (`GS1`, `GS2`) and land cover types (Cropland, Grassland)
**2. Shapefile:** Cleaned SSA administrative boundaries
  -  Location: `$CF_DIR/data/derived/shapefiles/cleaned_ssa_boundaries.shp`
  -  Required columns: `COUNTRY`, `SMALLEST`, `geometry`

#### Environment Variables
The master do file creates system environment variables for the project directory and overleaf output paths. 
  ```python
      BASE_DIR = os.getenv("CF_DIR", "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA")
      OVERLEAF_DIR = os.getenv("CF_OVERLEAF", "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa")
  ```

Based on these environment variables, we identify the following paths:
  ```python
    shapefile_path = os.path.join(BASE_DIR, "data/derived/shapefiles/cleaned_ssa_boundaries.shp")
    phenology_dir = os.path.join(BASE_DIR, "data/source/fao/crop-phenology")
    overleaf_dir = os.path.join(OVERLEAF_DIR, "figures/gs-plots")
  ```

### Script Architecture
```
1. Load shapefile → Filter countries → Dissolve by admin units
2. For each raster file:
   a. Mask raster to country boundaries
   b. Collapse 3-year phenology data to 12-month calendar
   c. Extract pixel values and spatial join with admin units
   d. Calculate statistics (mean, median, mode, etc.)
   e. Generate plots (optional)
3. Merge SOS and EOS data
4. Create monthly growing season indicators
5. Generate LaTeX table for publication
6. Export summary statistics to CSV
```

### Core Functions
#### `load_shapefile(shapefile_path, countries)`
Loads and preprocesses administrative boundary data.

**Parameters:**
- `shapefile_path` (str): Path to shapefile
- `countries` (list): List of country names to filter
  
**Returns:**
- GeoDataFrame with filtered and dissolved administrative units

**Key Features:**
- Exact country name matching (prevents "Sudan" matching "South Sudan")
- Dissolves duplicate geometries by SMALLEST administrative unit
- Validates geometry union to prevent data loss
  
**Example:**
```python
countries = ["Kenya", "Tanzania", "Uganda"]
gdf = load_shapefile("boundaries.shp", countries)
```

#### 2. `collapse_phenology_years(data)`
Converts three-year phenology data (-36 to 72) to single-year month values (1-12).

**Parameters:**
- `data` (numpy.ndarray): Array with phenology values from -36 to 72

**Returns:**
- numpy.ndarray with values 1-12 (months) or special flags

**Special Flags:**
- `251`: No seasons / No second season
- `254`: No cropland / No grassland
  
**Algorithm:**
```
month = ((value + 36) % 36 // 3) % 12 + 1
```

**Example:**
```python
# Input: array with values -36 to 72
# Output: array with values 1-12 (representing months)
collapsed = collapse_phenology_years(phenology_data)
```

#### 3. `process_raster_file(raster_path, shapefile, ...)`
Processes a single raster file and calculates statistics.

**Parameters:**
- `raster_path` (str): Path to GeoTIFF file
- `shapefile` (GeoDataFrame): Administrative boundaries
- `output_dir` (str, optional): Directory for output files
- `plot_output_dir` (str, optional): Directory for plots
- `filename` (str, optional): Original filename for labeling
- `generate_plots` (bool): Whether to generate visualization plots

**Returns:**
- DataFrame with statistics per administrative unit

**Statistics Calculated:**
- Mean, median, mode, standard deviation
- Min, max values
- Valid pixel count, flagged pixel count
- Valid share, flagged share

**Processing Steps:**
1. Mask raster to shapefile boundaries
    ```python
      #Key Operations:
       # Union all geometries: Creates a single combined boundary from all administrative units
       # Mask operation: Clips the raster to this boundary
       # Crop: Reduces the raster extent to only the area of interest (saves memory)
       # NoData handling: Sets pixels outside boundaries to NaN

    geometry = shapefile.geometry.union_all()
    out_image, out_transform = mask(src, [geometry], crop=True, nodata=np.nan)
    band_data = out_image[0]
    ```
    
2. Extract valid pixel values
    ```python
      rows, cols = np.where(~np.isnan(band_data))
      pixel_values = band_data[rows, cols]
      pixel_values = collapse_phenology_years(pixel_values)
    ```

3. Collapse phenology years to months
    **Example Transformation:**
    ```
    Input:  [0, 3, 6, 9, 12, 251, 254]
    Output: [1, 2, 3, 4, 5, 251, 254]
              ↑              ↑   ↑
            months 1-5     special flags preserved
    ```
4. Spatial join with administrative units
  ```python
    all_joined = gpd.sjoin(
        points_gdf, 
        shapefile[['SMALLEST', 'COUNTRY', 'geometry']], 
        how="inner", 
        predicate="within"
    )
    valid_joined = gpd.sjoin(valid_points, ..., predicate="within")
    flagged_joined = gpd.sjoin(flagged_points, ..., predicate="within")
  ```

5. Calculate statistics separately for valid and flagged pixels
    ```python
    if not all_joined.empty:
        base_stats = all_joined.groupby(["SMALLEST", "COUNTRY"]).size().reset_index(name="total_points_temp")
        base_stats = base_stats[["SMALLEST", "COUNTRY"]]
    else:
        return pd.DataFrame(columns=[...])  # Empty structure
    ```
---

### 4. `create_growing_season_indicators(sos_df, eos_df, statistic='mode')`
Creates binary indicators for whether each month is part of the growing season.

**Parameters:**
- `sos_df` (DataFrame): Start of Season statistics
- `eos_df` (DataFrame): End of Season statistics
- `statistic` (str): Which statistic to use ('median', 'mode', or 'mean')

**Returns:**
- DataFrame with columns `growing_month_1` through `growing_month_12`

**Logic:**
- For each growing season (GS1, GS2):
  - If SOS ≤ EOS: months = [SOS, SOS+1, ..., EOS]
  - If SOS > EOS (season wraps around year): months = [SOS, ..., 12, 1, ..., EOS]
- Any month in either GS1 or GS2 is marked as growing month

**Data Quality Flags:**
- `valid_gs1`: 1 if GS1 has valid SOS and EOS data
- `valid_gs2`: 1 if GS2 has valid SOS and EOS data

**Example:**
```python
# If GS1: SOS=April(4), EOS=September(9)
# Then growing_month_4 through growing_month_9 = 1
indicators = create_growing_season_indicators(sos_df, eos_df, statistic='mode')
```

### 5. `process_all_phenology_files(shapefile_path, phenology_dir, countries, overleaf_dir, generate_plots=True)`
Master function that orchestrates the entire processing pipeline.
**Parameters:**
- `shapefile_path` (str): Path to administrative boundaries shapefile
- `phenology_dir` (str): Directory containing FAO raster files
- `countries` (list): List of countries to process
- `overleaf_dir` (str): Directory for Overleaf plots
- `generate_plots` (bool): Whether to generate visualizations

**Returns:**
DataFrame with merged statistics and growing season indicators

**Processing Steps:**
1. Load and filter shapefile
2. Process all SOS raster files (cropland only)
3. Process all EOS raster files (cropland only)
4. Create growing season indicators
5. Merge all results
6. Return final dataset

--- 

## Data Source
**Source:** FAO Agricultural Stress Index System (ASIS)  
**Repository:** `gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/`  
**Organization:** Food and Agriculture Organization of the United Nations

## Notes
- The script uses shell commands (prefixed with `!`) to execute system-level operations
- The download is performed as a single batch operation for efficiency

## Troubleshooting
If the download fails:

1. **Test gsutil access:**
   ```bash
   gsutil ls gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/
   ```

2. **Verify authentication:**
   ```bash
   gcloud auth list
   ```

3. **Check Stata's shell access:**
   ```stata
   !echo "Shell access working"
   ```

## Related Documentation
For more information about the ASIS phenology data, refer to:
- The downloaded `ASIS.PHE.html` documentation file
- The `ASIS.PHE.json` metadata file
- [FAO ASIS Website](https://www.fao.org/giews/earthobservation/asis/)
