# --------------------------------------------------------
# PROJECT:            Climate Change and Fertility in Sub-Saharan Africa
# PURPOSE:            Create country-year level climate data from ERA5 data
# AUTHOR:             Prayog Bhattarai
# DATE MODIFIED:      [Current Date]
# DESCRIPTION:        This script processes ERA5 climate data (temperature and precipitation)
#                     for specified Sub-Saharan African countries, resampling the data to daily
#                     levels and saving it in Parquet format.
# --------------------------------------------------------

import os
import pandas as pd
import xarray as xr
import numpy as np
import geopandas as gpd
import zipfile
import tempfile
from glob import glob
from datetime import datetime

# Find the Climate_Change_and_Fertility_in_SSA directory by searching upward
current_dir = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = None

while current_dir != os.path.dirname(current_dir):  # Stop at filesystem root
    if os.path.basename(current_dir) == 'Climate_Change_and_Fertility_in_SSA':
        BASE_DIR = current_dir
        break
    current_dir = os.path.dirname(current_dir)

if not BASE_DIR:
    raise FileNotFoundError(
        "Could not find 'Climate_Change_and_Fertility_in_SSA' directory. "
        "This script must be run from within the project directory structure."
    )

# Set up base directories
DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_DIR = os.path.join(DATA_DIR, "source", "era", "continent")
OUTPUT_DIR = os.path.join(DATA_DIR, "derived", "dhs", "01-dhs-country-hourly-data")
# Updated shapefile path to use the cleaned DHS regions shapefile
SHP_PATH = os.path.join(DATA_DIR, "derived", "shapefiles", "dhs", "cleaned-ssa-dhs-regions.shp")


def standardize_country_name(country_name):
    """Standardize country names from the shapefile."""
    name = str(country_name).strip().lower()
    
    special_cases = {
        "côte d'ivoire": "Ivory Coast",
        "cote d'ivoire": "Ivory Coast",
        "ivory coast": "Ivory Coast",
        "democratic republic of congo": "Democratic Republic of Congo",
        "congo, democratic republic": "Democratic Republic of Congo",
        "congo, rep.": "Congo",
        "eswatini": "Swaziland"
    }
    
    if name in special_cases:
        return special_cases[name]
    
    return name.title()


def load_country_coordinates_from_shapefile(shapefile_path):
    """Load country coordinates from a shapefile using CNTRYNAMEE field."""
    print(f"Reading shapefile from: {shapefile_path}")
    
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"Shapefile not found at: {shapefile_path}")
    
    gdf = gpd.read_file(shapefile_path)
    
    # Check for required column - using CNTRYNAMEE instead of COUNTRY
    if 'CNTRYNAMEE' not in gdf.columns:
        available_cols = gdf.columns.tolist()
        raise ValueError(f"Shapefile must contain a 'CNTRYNAMEE' field. Found columns: {available_cols}")
    
    country_coordinates = {}
    
    # Dissolve by CNTRYNAMEE to get country boundaries
    dissolved_gdf = gdf.dissolve(by='CNTRYNAMEE')
    print(f"Loaded {len(dissolved_gdf)} countries")
    
    for country_name_raw, row in dissolved_gdf.iterrows():
        country_name = standardize_country_name(country_name_raw)
        geometry = row.geometry
        
        # Get the bounding box for the country
        minx, miny, maxx, maxy = geometry.bounds
        
        country_coordinates[country_name] = {
            "north": maxy,
            "south": miny,
            "east": maxx,
            "west": minx
        }
        
        print(f"  {country_name}: bbox [{minx:.2f}, {miny:.2f}, {maxx:.2f}, {maxy:.2f}]")
    
    return country_coordinates


def is_zipfile(filepath):
    """Check if a file is a valid ZIP file."""
    try:
        with open(filepath, 'rb') as f:
            return f.read(2) == b'PK'
    except Exception:
        return False


def extract_year_from_netcdf(ds):
    """Extract the year from a netCDF dataset."""
    time_var_names = ['valid_time', 'time', 'Time', 'DATE']
    
    for time_var in time_var_names:
        if time_var in ds.coords or time_var in ds.data_vars:
            time_values = ds[time_var].values
            first_time = pd.to_datetime(time_values[0])
            return first_time.year
    
    raise ValueError(f"Could not find time variable. Available coords: {list(ds.coords.keys())}")


def process_precipitation_data(df, country_name, year):
    """Process precipitation data and save as Parquet with standardized columns."""
    # Calculate daily precipitation sum (convert from meters to millimeters)
    daily_precip = df.groupby([pd.Grouper(freq='D'), 'latitude', 'longitude'])['tp'].sum() * 1000
    daily_precip = daily_precip.reset_index()
    
    # Create standardized output
    output_df = pd.DataFrame({
        'valid_time': daily_precip['datetime'].dt.strftime('%Y-%m-%d'),
        'latitude': daily_precip['latitude'],
        'longitude': daily_precip['longitude'],
        'precip': daily_precip['tp']  # Already converted to mm
    })
    
    # Save to parquet
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_precip.parquet")
    output_df.to_parquet(output_path, index=False)
    print(f"      ✓ Saved precipitation: {len(output_df)} records")


def process_temperature_data(df, country_name, year):
    """Process temperature data and save as Parquet with standardized columns."""
    # Convert temperature data from Kelvin to Celsius
    df['temp'] = df['t2m'] - 273.15
    
    # Calculate daily mean and max temperatures
    daily_temp = df.groupby([pd.Grouper(freq='D'), 'latitude', 'longitude'])['temp'].agg(
        temp_mean='mean',
        temp_max='max'
    ).reset_index()
    
    # Create standardized output
    output_df = pd.DataFrame({
        'valid_time': daily_temp['datetime'].dt.strftime('%Y-%m-%d'),
        'latitude': daily_temp['latitude'],
        'longitude': daily_temp['longitude'],
        'temp_mean': daily_temp['temp_mean'],
        'temp_max': daily_temp['temp_max']
    })
    
    # Save to parquet
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_temp.parquet")
    output_df.to_parquet(output_path, index=False)
    print(f"      ✓ Saved temperature: {len(output_df)} records")


def process_country_data(ds, country_name, bbox, year, data_type):
    """Process climate data for a specific country and save as parquet files."""
    print(f"    Processing {country_name}...")
    
    north = bbox['north']
    south = bbox['south']
    east = bbox['east']
    west = bbox['west']
    
    try:
        # Identify latitude and longitude variable names
        lat_var = None
        lon_var = None
        
        for coord in ds.coords:
            if 'lat' in coord.lower() and lat_var is None:
                lat_var = coord
            if 'lon' in coord.lower() and lon_var is None:
                lon_var = coord
        
        if lat_var is None or lon_var is None:
            print(f"      ✗ Could not find lat/lon coordinates. Available: {list(ds.coords.keys())}")
            return
        
        # Get coordinate values
        lats = ds[lat_var].values
        lons = ds[lon_var].values
        
        # Check if country bbox overlaps with data
        if north < lats.min() or south > lats.max() or east < lons.min() or west > lons.max():
            print(f"      ⚠ Country outside data range")
            return
        
        # Determine latitude slice direction
        lat_ascending = lats[0] < lats[-1]
        lat_slice = slice(south, north) if lat_ascending else slice(north, south)
        
        # Select the subset
        subset = ds.sel({lat_var: lat_slice, lon_var: slice(west, east)})
        
        # Check if we got any data
        if subset[lat_var].size == 0 or subset[lon_var].size == 0:
            print(f"      ⚠ No data points selected")
            return
        
        # Convert to dataframe
        df = subset.to_dataframe().reset_index()
        
        if df.empty:
            print(f"      ⚠ DataFrame is empty after conversion")
            return
        
        # Find time variable
        time_var = None
        for possible_time_var in ['valid_time', 'time', 'Time', 'DATE']:
            if possible_time_var in df.columns:
                time_var = possible_time_var
                break
        
        if time_var is None:
            print(f"      ✗ No time variable found. Available columns: {df.columns.tolist()}")
            return
        
        # Set datetime as index for resampling
        df['datetime'] = pd.to_datetime(df[time_var])
        df.set_index('datetime', inplace=True)
        
        # Process based on data type
        if data_type == 'precipitation' and 'tp' in df.columns:
            process_precipitation_data(df.copy(), country_name, year)
        elif data_type == 'temperature' and 't2m' in df.columns:
            process_temperature_data(df.copy(), country_name, year)
        else:
            print(f"      ✗ Expected variable not found")
        
    except Exception as e:
        print(f"      ✗ Error: {str(e)}")


def process_netcdf_by_type(nc_path, country_coordinates, data_type):
    """Process a single netCDF file of a specific type."""
    filename = os.path.basename(nc_path)
    print(f"\n  Processing {data_type.upper()}: {filename}")
    
    try:
        ds = xr.open_dataset(nc_path)
        year = extract_year_from_netcdf(ds)
        print(f"  Year: {year}")
        
        successful = 0
        for country_name, bbox in country_coordinates.items():
            try:
                process_country_data(ds, country_name, bbox, year, data_type)
                successful += 1
            except Exception as e:
                print(f"    ✗ Failed {country_name}: {str(e)}")
                continue
        
        print(f"  Summary: {successful}/{len(country_coordinates)} countries processed")
        ds.close()
        
    except Exception as e:
        print(f"  ✗ Error processing file: {str(e)}")


def process_zip_archive(zip_path, country_coordinates):
    """Process a ZIP archive containing netCDF files."""
    archive_name = os.path.basename(zip_path)
    print(f"\nProcessing archive: {archive_name}")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Extract the ZIP file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Find precipitation and temperature files
            precip_files = []
            temp_files = []
            
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    if file.endswith('.nc') or file.endswith('.nc4'):
                        if 'accum' in file:
                            precip_files.append(full_path)
                        elif 'instant' in file:
                            temp_files.append(full_path)
            
            # Process precipitation files
            for precip_file in precip_files:
                process_netcdf_by_type(precip_file, country_coordinates, 'precipitation')
            
            # Process temperature files
            for temp_file in temp_files:
                process_netcdf_by_type(temp_file, country_coordinates, 'temperature')
                
        except Exception as e:
            print(f"  ✗ Error processing archive: {str(e)}")


def process_nc_file(nc_file, country_coordinates):
    """Process a .nc file (which is actually a ZIP archive)."""
    if is_zipfile(nc_file):
        process_zip_archive(nc_file, country_coordinates)
    else:
        print(f"⚠ Warning: {os.path.basename(nc_file)} is not a ZIP file")


def main():
    """Main workflow function to process all .nc files."""
    print("\n" + "="*60)
    print("ERA5 CLIMATE DATA PROCESSING")
    print("="*60)
    
    # Load country coordinates from shapefile
    print("\n[STEP 1] Loading country boundaries...")
    try:
        country_coordinates = load_country_coordinates_from_shapefile(SHP_PATH)
    except Exception as e:
        print(f"✗ Error loading shapefile: {str(e)}")
        return
    
    # Create output directories
    print("\n[STEP 2] Creating output directories...")
    for country in country_coordinates.keys():
        country_dir = os.path.join(OUTPUT_DIR, country)
        os.makedirs(country_dir, exist_ok=True)
    print(f"✓ Created {len(country_coordinates)} country directories")
    
    # Get all .nc files
    print("\n[STEP 3] Finding input files...")
    nc_files = glob(os.path.join(INPUT_DIR, "*.nc"))
    
    if not nc_files:
        print(f"✗ No .nc files found in {INPUT_DIR}")
        return
    
    print(f"✓ Found {len(nc_files)} .nc files to process")
    
    # Process each file
    print("\n[STEP 4] Processing files...")
    for i, nc_file in enumerate(nc_files, 1):
        print(f"\n[File {i}/{len(nc_files)}]")
        try:
            process_nc_file(nc_file, country_coordinates)
        except Exception as e:
            print(f"✗ Failed file {i}/{len(nc_files)}: {str(e)}")
            continue
    
    print("\n" + "="*60)
    print("PROCESSING COMPLETE!")
    print("="*60)


if __name__ == "__main__":
    main()