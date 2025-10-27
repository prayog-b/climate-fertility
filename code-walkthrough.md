# Code Walkthrough: Implementation Details
This document provides a detailed walkthrough of the code implementation in the Climate Change and Fertility in Sub-Saharan Africa project. It complements the main README by explaining how the code works internally.

## Script 01a: Shapefile Cleaning
**File:** `cf-ssa-dm-01a-clean-shapefile.py`
### Overview
This script uses an object-oriented approach with a `SSAShapefileCleaner` class that systematically cleans the raw administrative boundary shapefile while maintaining detailed logs of changes made in the cleaned dataset.

### Class Structure
```python
class SSAShapefileCleaner:
  def __init__(self, input_path, output_path, overleaf_path):
    self.input_path = input_path
    self.output_path = output_path
    self.overleaf_path = overleaf_path
    self.gdf = None # Will store GeoDataFrame
    self.cleaning.log = {
      'original_count': 0,
      'steps': [],
      'final_count': 0 
    }
```

### Key Methods
#### `load_data()`
Loads the raw shapefile and performs initial standardization
```python
def load_data(self):
    self.gdf = gpd.read_file(self.input_path)
    # Standardize country name
    self.gdf.loc[self.gdf['COUNTRY'] == "Côte d'Ivoire", 'COUNTRY'] = 'Ivory Coast'
```

#### `check_smallest_identifiability()`
Checks whether the dataset is uniquely identifiable at the SMALLEST geographic level. The raw data is actually only identifiable at the `BPL_NAME` (birthplace name) level, so we check for multiple BPL_NAME values for a single SMALLEST.
```python
# Check if SMALLEST values are unique
smallest_counts = valid_data['SMALLEST'].value_counts()
duplicates = smallest_counts[smallest_counts > 1]

# Check for multiple BPL_NAME per SMALLEST
smallest_bpl_groups = valid_data.groupby('SMALLEST')['BPL_NAME'].apply(lambda x: x.nunique())
multiple_bpl = smallest_bpl_groups[smallest_bpl_groups > 1]
```
The census microdata uses the variable `SMALLEST` to identify birthplace locations. This step helps us understand the extent to which the raw shapefile can be used to link weather and census data. 

#### `remove_null_geometries()`
Removes `BPL_NAME` units with missing geometries / geographic boundaries:
```python
null_geoms_mask = self.gdf.geometry.isnull()
null_geoms = self.gdf.geometry.isnull().sum()

# Save list of removed units for documentation
null_records = self.gdf.loc[null_geoms_mask, ["COUNTRY", "SMALLEST", "BPL_NAME"]]
null_records.to_csv(csv_path, index=False)

# Remove null geometries
self.gdf = self.gdf[~null_geoms_mask].copy()
```
We create a `.csv` file to create an audit trail showing which `BPL_NAME` units were dropped due to there being null geometries.


#### `fix_country_code_mismatches()`
In the raw data, there are several entries where the SMALLEST values for Sudan and South Sudan do not match the right country code; i.e. entries in South Sudan have birthplace names with SMALLEST values that are supposed to be for Sudan, and vice versa. To resolve this, we remove cases where the SMALLEST value and the country code do not match. 


#### `resolve_sliver_overlaps()`
The raw data may have overlapping polygons between adjacent BPL_NAME units. We try to identify such polygons, and assign those slivers to the unit with larger original area. Assigning the sliver to the polygon was a more consistent and reliable method than relying on the code to randomly assign based on the order of the dataset or any other feature.
```python
# Identify overlapping polygons
overlaps = gpd.overlay(self.gdf, self.gdf, how='intersection')

# Filter to only slivers (very small overlaps)
overlaps['overlap_area'] = overlaps.geometry.area
sliver_threshold = 0.0001  # Area threshold
slivers = overlaps[overlaps['overlap_area'] < sliver_threshold]

# Assign slivers to the unit with larger original area
for idx, row in slivers.iterrows():
    if row['original_area_1'] > row['original_area_2']:
        # Remove sliver from unit 2
        unit2_geom = unit2_geom.difference(row['geometry'])
```
This process avoids double-counting issues when aggregating weather data to administrative units. 

#### `generate_smallest_units_latex_table()`
Creates a LaTeX table summarizing the number of SMALLEST units by country. 
```python
latex_lines.append("\\begin{tabular}{lrrrr}")
latex_lines.append("\\toprule")
latex_lines.append("Country & \# SMALLEST & GEOLEV2 & GEOLEV1 & COUNTRY \\\\")
# ... data rows ...
latex_lines.append("\\bottomrule")
latex_lines.append("\\end{tabular}")

# Write directly to Overleaf output folder
with open(self.overleaf_path, 'w') as f:
    f.write("\n".join(latex_lines))
```
This table is written directly to the synced Overleaf folder, automatically updating tables in the data documentation `.tex` file there.

### Workflow
```python
def run_complete_cleaning(self):
    self.load_data()
    self.check_smallest_identifiability()
    self.remove_null_geometries()
    self.handle_missing_smallest()
    self.fix_country_code_mismatches()
    self.resolve_sliver_overlaps()
    self.clean_invalid_geometries()
    self.final_validation()
    self.save_cleaned_data()
    self.generate_cleaning_report()
    self.generate_smallest_units_latex_table()
```


## Script 02a: ERA5 Data Download
**File:** `cf-ssa-dm-02a-era-cdsapi-climate-data-download.py`
### Overview
Downloads ERA5 climate reanalysis data using the Climate Data Store (CDS) API with multi-threaded parallel downloads to speed up the process.

### Key components
#### Coordinates Dictionary
Defines the spatial extent for downloads: 
```python
coordinates = {
    "Africa": {
        "north": 37.8,   # Northern-most latitude
        "south": -39.3,  # Southern-most latitude  
        "east": 55.2,    # Eastern-most longitude
        "west": -25.5    # Western-most longitude
    }
}
```
**Coverage:** Encompasses entire African continent and the outlying islands of Seychelles, Mauritius, Madagascar, Sao Tome and Principe, and Cape Verde to ensure no country is clipped.

#### Worker function (threading)
Each API key gets its own thread to download years in parallel: 
```python
def worker(api_key, country, queue):
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=api_key)
    
    while True:
        try:
            year = queue.get_nowait()  # Get next year to download
        except Empty:
            break  # No more years, exit thread
        
        try:
            request = {
                "product_type": "reanalysis",
                "variable": ["2m_temperature", "total_precipitation"],
                "year": [str(year)],
                "month": ["01", "02", ..., "12"],  # All months
                "day": ["01", "02", ..., "31"],    # All days
                "time": ["00:00", "01:00", ..., "23:00"],  # All hours
                "format": "netcdf",
                "area": [north, west, south, east]
            }
            c.retrieve("reanalysis-era5-single-levels", request, f"{folder_path}/Africa_{year}.nc")
        except Exception as e:
            print(f"Failed to download {year}: {e}")
            queue.put(year)  # Re-add to queue for retry
        finally:
            queue.task_done()
```
- Years are added to a queue, threads pull from it
- Failed downloads are re-queued
- Each year is one file, minimizing partial downloads

#### Main workflow
Sets up the download process
```python
def main():
    years = list(range(1940, 2020))  # 80 years of data
    queue = Queue()
    
    for year in years:
        queue.put(year)
    
    api_keys = [
        "enterAPIkey1",
        "enterAPIkey2"
    ]
    
    threads = []
    for key in api_keys:
        thread = threading.Thread(target=worker, args=(key, "Africa", queue))
        thread.start()
        threads.append(thread)
    
    queue.join()  # Wait until all downloads complete
    
    for thread in threads:
        thread.join()
```

### Configuration for replication/extension
To use this script:
**1. Get CDS API keys:**
  - Create account at https://cds.climate.copernicus.eu/
  - Get API key from profile
  - You can create multiple accounts for multiple keys

**2. Replace placeholders**
```python
folder_path = "/your/path/to/data/source/era"
   
   api_keys = [
       "12345:abcd-efgh-1234-5678",
       "67890:ijkl-mnop-9012-3456"
   ]
```

 **3. Adjust years if needed**
 ```python
years = list(range(1940, 2020))  # Modify range as needed
```

## Script 02b: Country-Year Processing
**File:** `cf-ssa-dm-02b-era-create-country-year-parquet-data.py`

### Overview
Processes continent-wide ERA5 NetCDF files into country-specific daily data, converting units, and aggregating from hourly to daily resolution. 

### Key Components
#### Country bounding boxes
Dictionary defining spatial extent for each country
```python
country_coordinates = {
    "Kenya": {
        "north": 5.0,
        "south": -4.5,
        "east": 41.0,
        "west": 34.5
    },
    "Tanzania": {
        "north": 1.5,
        "south": -11.5,
        "east": 40.5,
        "west": 29.3
    },
    # ... 23 more countries
}
```

#### Extract Year from Filename
Parse the year being processed from NetCDF filename:
```python
def extract_year_from_filename(file_name):
    # Expected format: "Africa_YYYY.nc"
    base_name = os.path.basename(file_name)
    parts = base_name.split('_')
    
    if len(parts) > 1 and parts[1].isdigit():
        return int(parts[1].split('.')[0])
    else:
        raise ValueError(f"Invalid file name format: {file_name}")
```

#### Process temperature data
Converts hourly temperature in Kelvin to daily level statistics:
def process_temperature_data(df, country_name, year):
```python
    # Convert Kelvin to Celsius
    df['temp'] = df['t2m'] - 273.15
    
    # Daily aggregation: mean and max temperature
    daily_temp = df.groupby([
        pd.Grouper(freq='D'),      # Group by day
        'latitude', 
        'longitude'
    ])['temp'].agg(
        temp_mean=np.mean,         # Average temperature
        temp_max=np.max            # Maximum temperature
    ).reset_index()
    
    # Format date
    daily_temp['date'] = daily_temp['datetime'].dt.strftime('%Y-%m-%d')
    
    # Save as Parquet 
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_temp.parquet")
    daily_temp.to_parquet(output_path, index=False)
```


#### Process precipitation data
Converts hourly precipitation (in meters) to daily totals in milimeters:
```python
def process_precipitation_data(df, country_name, year):
    # Daily precipitation sum (convert meters to millimeters)
    daily_precip = df.groupby([
        pd.Grouper(freq='D'),
        'latitude',
        'longitude'
    ])['tp'].sum() * 1000  # m → mm
    
    daily_precip = daily_precip.reset_index()
    daily_precip.rename(columns={'tp': 'precip'}, inplace=True)
    
    # Format date
    daily_precip['date'] = daily_precip['datetime'].dt.strftime('%Y-%m-%d')
    
    # Save as Parquet
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_precip.parquet")
    daily_precip.to_parquet(output_path, index=False)
```
ERA5 reports precipitation in meters, so we convert it to milimeters as a more conventional measure for daily rainfall.

#### Main workflow
Process all NetCDF files for all countries: 
```python
def main():
    # Get all NetCDF files
    nc_files = glob(os.path.join(INPUT_DIR, "*.nc"))
    print(f"Found {len(nc_files)} NetCDF files in {INPUT_DIR}.")
    
    # Process each file
    for nc_file in nc_files:
        process_netcdf_file(nc_file)  # Opens and processes by country
    
    print("All files processed successfully!")

def process_netcdf_file(nc_file):
    ds = xr.open_dataset(nc_file)  # Open NetCDF
    year = extract_year_from_netcdf(ds)
    
    # Process each country
    for country_name, bbox in country_coordinates.items():
        process_country_data(ds, country_name, bbox, year)
    
    ds.close()
```
This opens each NetCDF file once, extracts all countries, then closes and moves on. 

### Output Structure
data/derived/era/
├── Kenya/
│   ├── Kenya_1940_temp.parquet
│   ├── Kenya_1940_precip.parquet
│   ├── Kenya_1941_temp.parquet
│   ├── Kenya_1941_precip.parquet
│   └── ...
├── Tanzania/
│   ├── Tanzania_1940_temp.parquet
│   └── ...
└── [23 more countries]/


## Script 02c: SMALLEST-Day Processing
**File:** `cf-ssa-dm-02c-era-create-subunit-day.py`
### Overview
This script aggregates gridded climate data to the SMALLEsT unit level using spatial joins and weighting based on the share of each grid cell that falls within a SMALLEST unit. 

### Class Structure: 
#### DualLogger class:
This implements Stata-style logging to provide greater monitoring and detailed report of processing steps taken. 
```python
class DualLogger:
    def __init__(self, log_file: Path, level: str = "INFO"):
        # Create logger with both file and console handlers
        self.logger = logging.getLogger('ClimateProcessor')
        
        # File handler: detailed logs with timestamps
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        
        # Console handler: simpler output for monitoring
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(message)s'))
    
    def section(self, title: str, level: int = 1):
        """Log a section header with separators."""
        if level == 1:
            separator = "=" * 60
        elif level == 2:
            separator = "-" * 50
        self.info(separator)
        self.info(f" {title}")
        self.info(separator)
```

### ClimateDataProcessor class
```python
class ClimateDataProcessor:
    def __init__(
        self,
        project_dir: Path,
        country_name: str,
        exact_match: bool = False,
        chunk_size: int = 100000,
        interpolate_missing: bool = True,
        interpolation_method: str = "nearest_neighbor",
        buffer_radius_km: float = 50.0,
        max_neighbors: int = 5,
        idw_power: float = 2.0
    ):
        self.country_name = country_name
        self.shapefile_path = project_dir / "data/derived/shapefiles/cleaned_ssa_boundaries.shp"
        self.output_dir = project_dir / "data/derived/era/00-subunit-day-data"
        self.chunk_size = chunk_size
        
        # Interpolation settings (for handling missing data)
        self.interpolate_missing = interpolate_missing
        self.interpolation_method = interpolation_method
        self.buffer_radius_km = buffer_radius_km

   # ...
```

### Core Processing Methods
#### Load and filter shapefile
This function loads the shapefile, filters to the country being processed, and dissolves the shapefiles boundaries such that the boundaries between BPL_NAME units dissolve and the SMALLEST variable becomes the identifying variable in the shapefile data.
```python
def _load_and_prepare_shapefile(self) -> None:
    self.logger.section("LOADING SHAPEFILE")
    # ...
    gdf = gpd.read_file(self.shapefile_path)
    # ...
    gdf["SMALLEST_STR"] = gdf[self.smallest_unit_col].astype(str).str.strip()
    filtered_gdf = gdf[gdf["SMALLEST_STR"].str[:country_code_length] == country_code]
    # ...
    dissolved_gdf = filtered_gdf.dissolve(by=self.smallest_unit_col, as_index=False)
    dissolved_gdf = dissolved_gdf[[self.smallest_unit_col, "geometry"]].copy()
    dissolved_gdf = dissolved_gdf.to_crs("EPSG:4326")  # Ensure WGS84
    self.admin_gdf = dissolved_gdf
```


#### Load Climate Data in Chunks
Efficiently handles large climate datasets:
```python
    def _read_parquet_chunked(self, file_path: Path, columns: list = None) -> Iterator[pd.DataFrame]:
        """Read parquet file in chunks using pyarrow."""
        parquet_file = pq.ParquetFile(file_path)
        
        for batch in parquet_file.iter_batches(batch_size=self.chunk_size, columns=columns):
            yield batch.to_pandas()
```
This ensures that we process the data in more manageable chunks. 


#### Spatial join (Grid Cells to SMALLEST units)
This is the core function that maps climate grid cells to administrative units: 
```python
def _calculate_intersections(self) -> None:
    # ...

    
    predicates_to_try = ['intersects', 'overlaps', 'within']
        
    for predicate in predicates_to_try:
        try:
            self.logger.info(f"Attempting spatial join with '{predicate}' predicate...")
            joined = gpd.sjoin(
                self.grid_gdf,
                self.admin_gdf,
                how="inner",
                predicate=predicate
            )
            
            result_count = len(joined)
            self.logger.info(f"✅ '{predicate}' succeeded: {result_count} intersections")
            
            if result_count > 0:
                break
                
        except Exception as e:
            self.logger.warning(f"'{predicate}' failed: {str(e)}")
            continue
    
    if joined is None or len(joined) == 0:
        error_msg = "All spatial join attempts failed!"
        self.logger.error(error_msg)
        raise ValueError(error_msg)
    # ... 
    return joined
```
- Each climate grid cell (0.25 x 0.25 degrees) is a point
- Check which SMALLEST unit(s) contains each point
- Assign that polygon's SMALLEST code to the grid cell.

#### Handling admin units that are missing weather data
We use several interpolation strategies to assign weather data to SMALLEST units that are missing weather data. 
```python
    def _handle_missing_units(self, final_df: pd.DataFrame) -> pd.DataFrame:
        if not self.interpolate_missing or self.interpolation_method == "none":
            return final_df 
        
        # Track missing units
        self._track_missing_units()

        if not hasattr(self, 'units_without_data') or not self.units_without_data:
            self.logger.info("No admin units missing weather data - skipping interpolation.")
            return final_df

        self.logger.section(f"INTERPOLATING MISSING ADMIN UNITS ({self.interpolation_method.upper()})")
        self.logger.info(f"Units needing interpolation: {len(self.units_without_data)}")

        # Choose interpolation method
        if self.interpolation_method == "nearest_neighbor":
            return self._assign_nearest_neighbor_weather(final_df)
        elif self.interpolation_method == "buffer":
            return self._assign_buffered_weather(final_df)
        elif self.interpolation_method == "idw":
            return self._assign_idw_weather(final_df)
        else:
            self.logger.warning(f"Unknown interpolation method: {self.interpolation_method}")
            return final_df 
```

#### Interpolation for Missing Values:
Handles grid cells that don't fall within any administrative unit:
```python
def interpolate_missing_values(self, joined_df: pd.DataFrame, unmatched: pd.DataFrame, 
                                shapefile_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Interpolate climate values for grid cells without spatial match."""
    
    if self.interpolation_method == "nearest_neighbor":
        # Find nearest administrative unit for each unmatched grid cell
        for idx, row in unmatched.iterrows():
            point = row.geometry
            
            # Calculate distance to all subunits
            distances = shapefile_gdf.geometry.distance(point)
            nearest_idx = distances.idxmin()
            nearest_unit = shapefile_gdf.loc[nearest_idx, self.smallest_unit_col]
            
            # Assign to nearest unit
            joined_df.loc[idx, self.smallest_unit_col] = nearest_unit
    
    elif self.interpolation_method == "buffer":
        # Create buffer around subunits
        buffer_degrees = self.buffer_radius_km / 111  # Approximate km to degrees
        buffered_gdf = shapefile_gdf.copy()
        buffered_gdf['geometry'] = buffered_gdf.geometry.buffer(buffer_degrees)
        
        # Re-join with buffered geometries
        # ... (similar logic)
    
    return joined_df
```
Grids near borders or over water may not fall within any SMALLEST unit, but we still want to attribute their weather to nearby units. 


#### Process temperature
We aggregate to the SMALLEST-day level by weighing grid cells based on the share of a grid cell that falls within a SMALLEST unit. 
```python
def _process_temp_file(self, file_path: Path) -> pd.DataFrame:
        
        # Process in chunks
        chunks = []
        total_merged_rows = 0
        chunk_count = 0
        
        for chunk in self._read_parquet_chunked(file_path):
            chunk_count += 1
            self.logger.debug(f"Processing chunk {chunk_count}: {len(chunk)} rows")
            
            # Round coordinates
            chunk['latitude'] = chunk['latitude'].round(3)
            chunk['longitude'] = chunk['longitude'].round(3)
            
            # Merge with intersection data
            merged = pd.merge(
                chunk,
                self.intersection_gdf,
                on=["latitude", "longitude"],
                how="inner"
            )
            
            merge_rate = len(merged) / len(chunk) if len(chunk) > 0 else 0
            self.logger.debug(f"Chunk {chunk_count} merge rate: {merge_rate:.1%} ({len(merged)}/{len(chunk)})")
            total_merged_rows += len(merged)

            if len(merged) == 0:
                self.logger.warning(f"Chunk {chunk_count}: No merge results")
                continue
            
            # Convert time
            merged["valid_time"] = pd.to_datetime(merged["valid_time"])
            merged["year"] = merged["valid_time"].dt.year
            merged["month"] = merged["valid_time"].dt.month
            merged["day"] = merged["valid_time"].dt.day
            
            # Aggregate
            grouped = merged.groupby([self.smallest_unit_col, "year", "month", "day"])
            result = grouped.apply(
                lambda x: pd.Series({
                    "temp_mean": self._safe_weighted_average(x["temp_mean"], x["shr_of_subunit"]),
                    "temp_max": x["temp_max"].max()
                }),
                include_groups=False
            ).reset_index()
            
            chunks.append(result)
        
        processing_summary = {
            "Chunks Processed": chunk_count,
            "Total Merged Rows": f"{total_merged_rows:,}",
            "Result Chunks": len(chunks)
        }
        self.logger.summary_table(f"Temperature Processing - {file_path.name}", processing_summary)
        
        if chunks:
            final_result = pd.concat(chunks, ignore_index=True)
            nan_summary = {
                "Final Rows": len(final_result),
                "temp_mean NaN": final_result['temp_mean'].isna().sum(),
                "temp_max NaN": final_result['temp_max'].isna().sum()
            }
            self.logger.summary_table("Temperature Result Quality", nan_summary)
            return final_result
        else:
            self.logger.warning("No chunks to concatenate!")
            return pd.DataFrame()
```

#### Process precipitation data
We follow a similar process to derive our precipitation variable.
```python
def _process_precip_file(self, file_path: Path) -> pd.DataFrame:
        # ...
        chunks = []
        chunk_count = 0
        
        for chunk in self._read_parquet_chunked(file_path):
            chunk_count += 1
            chunk['latitude'] = chunk['latitude'].round(3)
            chunk['longitude'] = chunk['longitude'].round(3)
            
            merged = pd.merge(
                chunk,
                self.intersection_gdf,
                on=["latitude", "longitude"],
                how="inner"
            )
            
            if len(merged) == 0:
                continue
            
            merged["valid_time"] = pd.to_datetime(merged["valid_time"])
            merged["year"] = merged["valid_time"].dt.year
            merged["month"] = merged["valid_time"].dt.month
            merged["day"] = merged["valid_time"].dt.day
            
            grouped = merged.groupby([self.smallest_unit_col, "year", "month", "day"])
            result = grouped.apply(
                lambda x: pd.Series({
                    "precip": self._safe_weighted_average(x["precip"], x["shr_of_subunit"])
                }),
                include_groups=False
            ).reset_index()
            
            chunks.append(result)
        
        self.logger.info(f"Processed {chunk_count} chunks for {file_path.name}")
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
```

#### Merge temperature and precipitation data 
This section merges the SMALLEST-day level data into a larger dataset. 
```python
def _merge_temp_precip_data(
        self, 
        temp_dfs: List[pd.DataFrame], 
        precip_dfs: List[pd.DataFrame]
    ) -> pd.DataFrame:
        """Merge temperature and precipitation data with logging."""
        self.logger.section("DATA MERGING AND FINALIZATION")
        
        # Combine temperature data
        if temp_dfs:
            temp_combined = pd.concat(temp_dfs, ignore_index=True)
            temp_combined = temp_combined.groupby([self.smallest_unit_col, "year", "month", "day"]).mean().reset_index()
            self.logger.info(f"Combined temperature data: {len(temp_combined)} records")
        else:
            temp_combined = pd.DataFrame(columns=[self.smallest_unit_col, "year", "month", "day", "temp_mean", "temp_max"])
            self.logger.warning("No temperature data to combine")
        
        # Combine precipitation data
        if precip_dfs:
            precip_combined = pd.concat(precip_dfs, ignore_index=True)
            precip_combined = precip_combined.groupby([self.smallest_unit_col, "year", "month", "day"]).mean().reset_index()
            self.logger.info(f"Combined precipitation data: {len(precip_combined)} records")
        else:
            precip_combined = pd.DataFrame(columns=[self.smallest_unit_col, "year", "month", "day", "precip"])
            self.logger.warning("No precipitation data to combine")
        
        # Merge datasets
        if not temp_combined.empty and not precip_combined.empty:
            merged = pd.merge(
                temp_combined,
                precip_combined,
                on=[self.smallest_unit_col, "year", "month", "day"],
                how="outer"
            )
            self.logger.info("✅ Successfully merged temperature and precipitation data")
        elif not temp_combined.empty:
            merged = temp_combined
            merged["precip"] = np.nan
            self.logger.info("Using temperature data only (no precipitation)")
        elif not precip_combined.empty:
            merged = precip_combined
            merged["temp_mean"] = np.nan
            merged["temp_max"] = np.nan
            self.logger.info("Using precipitation data only (no temperature)")
        else:
            error_msg = "No temperature or precipitation data to merge"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Add country column
        merged["COUNTRY"] = self.country_name
```

#### Main workflow
```python
def process_country(self) -> None:
            # ...
            # Step 1-3: Initial setup
            available_countries = [d.name for d in self.parquet_dir.iterdir() if d.is_dir()]
            self.country_name = self._match_country_name(available_countries)
            
            temp_files, precip_files = self._get_country_files()
            self._detect_year_range(temp_files, precip_files)
            
            # Step 4-5: Spatial setup
            self._load_and_prepare_shapefile()
            
            # more code ... 
            
            # Step 6-7: Data processing
            self.logger.section("CLIMATE DATA FILE PROCESSING")
            
            temp_dfs = []
            for i, file in enumerate(temp_files, 1):
                self.logger.info(f"Processing temperature file {i}/{len(temp_files)}: {file.name}")
                temp_df = self._process_temp_file(file)
                if not temp_df.empty:
                    temp_dfs.append(temp_df)
                
            precip_dfs = []
            for i, file in enumerate(precip_files, 1):
                self.logger.info(f"Processing precipitation file {i}/{len(precip_files)}: {file.name}")
                precip_df = self._process_precip_file(file)
                if not precip_df.empty:
                    precip_dfs.append(precip_df)
                
            # Step 8: Final merge and save
            final_df = self._merge_temp_precip_data(temp_dfs, precip_dfs)
                
            output_file = self.output_dir / f"{self.country_name}-era-subunit-day-{self.start_year}-{self.end_year}.dta"
            print(f"DEBUG: Final output file will be: {output_file}")
            
            self.logger.info("Optimizing data types for Stata export...")
            final_df[self.smallest_unit_col] = final_df[self.smallest_unit_col].astype("category")
            final_df["COUNTRY"] = final_df["COUNTRY"].astype("category")
            for col in ["year", "month", "day"]:
                final_df[col] = final_df[col].astype("int16")
            for col in ["temp_mean", "temp_max", "precip"]:
                final_df[col] = final_df[col].astype("float32")
            # ... 
```

### Output Structure
data/derived/era/00-subunit-day-data/
├── Kenya_subunit_day_1940_2019.dta
├── Tanzania_subunit_day_1940_2019.dta
└── [23 more countries]
**Each file contains:**
- Variables: `SMALLEST`, `date`, `temp_mean`, `temp_max`, `precip`, `n_grid_cells`


## Extending the code
### Adding a New Country
1. **Add to country coordinates (dm-02b)**
```python
country_coordinates = {
    # ... existing countries ...
    "NewCountry": {
        "north": 10.0,
        "south": 5.0,
        "east": 45.0,
        "west": 40.0
    }
}
```
2. **Ensure shapefile includes the country**
3. **Process the new country**

### Adding a new climate variable
To add a nwe variable (e.g. relative humidity):
1. **Update download request (02a):**
```python
request = {
    "variable": [
        "2m_temperature",
        "total_precipitation",
        "2m_relative_humidity"  # Add new variable
    ],
    # ... other parameters ...
}
```
2. **Update processing functions (02b):**
```python
def process_humidity_data(df, country_name, year):
    # Daily mean humidity
    daily_humidity = df.groupby([
        pd.Grouper(freq='D'),
        'latitude',
        'longitude'
    ])['r2'].mean()  # r2 is ERA5 variable name
    
    # ... save to parquet ...
```
3. 
