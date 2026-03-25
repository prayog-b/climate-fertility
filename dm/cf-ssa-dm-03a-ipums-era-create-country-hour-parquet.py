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
from concurrent.futures import ThreadPoolExecutor

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
OUTPUT_DIR = os.path.join(DATA_DIR, "derived", "era")
SHP_PATH = os.path.join(DATA_DIR, "derived", "shapefiles", "cleaned_ssa_boundaries.shp")

# Set to True to skip country-year files that already exist (useful for resuming
# interrupted runs). Set to False to overwrite all existing output files.
SKIP_EXISTING = True


def standardize_country_name(country_name):
    """Standardize country names from the shapefile."""
    name = str(country_name).strip().lower()
    
    special_cases = {
        "côte d'ivoire": "Ivory Coast",
        "cote d'ivoire": "Ivory Coast",
        "ivory coast": "Ivory Coast",
    }
    
    if name in special_cases:
        return special_cases[name]
    
    return name.title()


def load_country_coordinates_from_shapefile(shapefile_path):
    """Load country coordinates from a shapefile."""
    print(f"Reading shapefile from: {shapefile_path}")
    
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"Shapefile not found at: {shapefile_path}")
    
    gdf = gpd.read_file(shapefile_path)
    
    if 'COUNTRY' not in gdf.columns:
        raise ValueError(f"Shapefile must contain a 'COUNTRY' field. Found columns: {gdf.columns.tolist()}")
    
    # Compute per-row bounds and aggregate to country level with groupby.
    # This avoids gdf.dissolve() which runs an expensive geometric union_all
    # on every country's polygons — unnecessary since we only need the bbox.
    bounds = gdf.geometry.bounds  # minx, miny, maxx, maxy per row
    gdf2 = gdf[['COUNTRY']].copy()
    gdf2[['minx', 'miny', 'maxx', 'maxy']] = bounds

    country_bounds = gdf2.groupby('COUNTRY').agg(
        minx=('minx', 'min'),
        miny=('miny', 'min'),
        maxx=('maxx', 'max'),
        maxy=('maxy', 'max'),
    )
    print(f"Loaded {len(country_bounds)} countries")

    country_coordinates = {}
    for country_name_raw, row in country_bounds.iterrows():
        country_name = standardize_country_name(country_name_raw)
        country_coordinates[country_name] = {
            "north": row['maxy'],
            "south": row['miny'],
            "east":  row['maxx'],
            "west":  row['minx'],
        }

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


def process_precipitation_data(subset, lat_var, lon_var, time_var, country_name, year):
    """Process precipitation data and save as Parquet with standardized columns.

    Uses xarray's native resample instead of converting to a full pandas DataFrame first.
    This avoids materialising ~8760 × N_lat × N_lon rows; only the 365 × N_lat × N_lon
    daily-aggregated array is ever converted to a DataFrame.
    """
    # Resample directly in xarray (metres → millimetres)
    daily = subset['tp'].resample({time_var: '1D'}).sum() * 1000
    df = daily.to_dataframe().reset_index()

    output_df = pd.DataFrame({
        'valid_time': pd.to_datetime(df[time_var]).dt.strftime('%Y-%m-%d'),
        'latitude':   df[lat_var],
        'longitude':  df[lon_var],
        'precip':     df['tp'],
    })

    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_precip.parquet")
    output_df.to_parquet(output_path, index=False, compression='lz4')
    print(f"      ✓ Saved precipitation: {len(output_df)} records")


def process_temperature_data(subset, lat_var, lon_var, time_var, country_name, year):
    """Process temperature data and save as Parquet with standardized columns.

    The parent dataset is already fully loaded into memory before this function
    is called, so no explicit .load() is needed here.
    """
    temp_c = subset['t2m'] - 273.15

    daily_ds = xr.Dataset({
        'temp_mean': temp_c.resample({time_var: '1D'}).mean(),
        'temp_max':  temp_c.resample({time_var: '1D'}).max(),
    })
    df = daily_ds.to_dataframe().reset_index()

    output_df = pd.DataFrame({
        'valid_time': pd.to_datetime(df[time_var]).dt.strftime('%Y-%m-%d'),
        'latitude':   df[lat_var],
        'longitude':  df[lon_var],
        'temp_mean':  df['temp_mean'],
        'temp_max':   df['temp_max'],
    })

    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_temp.parquet")
    output_df.to_parquet(output_path, index=False, compression='lz4')
    print(f"      ✓ Saved temperature: {len(output_df)} records")


def process_country_data(ds, country_name, bbox, year, data_type, lat_var, lon_var, time_var):
    """Process climate data for a specific country and save as parquet files.

    lat_var, lon_var, and time_var are detected once per file in process_netcdf_by_type
    and passed down here to avoid repeating the coordinate scan for every country.
    """
    # Check for existing output before doing any heavy work
    suffix = 'precip' if data_type == 'precipitation' else 'temp'
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_{suffix}.parquet")
    if SKIP_EXISTING and os.path.exists(output_path):
        print(f"    ↷ Skipping {country_name} ({year} {data_type}) — file exists")
        return

    print(f"    Processing {country_name}...")

    north = bbox['north']
    south = bbox['south']
    east  = bbox['east']
    west  = bbox['west']

    try:
        lats = ds[lat_var].values
        lons = ds[lon_var].values

        # Check if country bbox overlaps with data
        if north < lats.min() or south > lats.max() or east < lons.min() or west > lons.max():
            print(f"      ⚠ Country outside data range")
            return

        # Determine latitude slice direction
        lat_ascending = lats[0] < lats[-1]
        lat_slice = slice(south, north) if lat_ascending else slice(north, south)

        # Select the spatial subset
        subset = ds.sel({lat_var: lat_slice, lon_var: slice(west, east)})

        if subset[lat_var].size == 0 or subset[lon_var].size == 0:
            print(f"      ⚠ No data points selected")
            return

        # Dispatch to the appropriate processor
        if data_type == 'precipitation' and 'tp' in ds.data_vars:
            process_precipitation_data(subset, lat_var, lon_var, time_var, country_name, year)
        elif data_type == 'temperature' and 't2m' in ds.data_vars:
            process_temperature_data(subset, lat_var, lon_var, time_var, country_name, year)
        else:
            print(f"      ✗ Expected variable not found")

    except Exception as e:
        print(f"      ✗ Error: {str(e)}")


def process_netcdf_by_type(nc_path, country_coordinates, data_type):
    """Process a single netCDF file of a specific type.

    Coordinate variable names are detected once here rather than inside the
    per-country loop. Countries are then processed in parallel: each worker
    opens its own file handle to avoid netCDF4 thread-safety issues, while
    the OS disk cache ensures repeated opens are cheap.
    """
    filename = os.path.basename(nc_path)
    print(f"\n  Processing {data_type.upper()}: {filename}")

    try:
        # Load only the variable needed for this data type. Loading the full file
        # wastes memory and time on auxiliary variables; coordinates (lat, lon,
        # time) are always included automatically by xarray.
        var_name = 'tp' if data_type == 'precipitation' else 't2m'
        ds = xr.open_dataset(nc_path)[[var_name]].load()
        year = extract_year_from_netcdf(ds)
        print(f"  Year: {year}")

        # Detect coordinate variable names once (not per-country)
        lat_var = lon_var = time_var = None
        for coord in ds.coords:
            cl = coord.lower()
            if 'lat' in cl and lat_var is None:
                lat_var = coord
            if 'lon' in cl and lon_var is None:
                lon_var = coord
        for t in ['valid_time', 'time', 'Time', 'DATE']:
            if t in ds.coords or t in ds.data_vars:
                time_var = t
                break

        if lat_var is None or lon_var is None:
            print(f"  ✗ Could not find lat/lon coordinates")
            ds.close()
            return
        if time_var is None:
            print(f"  ✗ Could not find time coordinate")
            ds.close()
            return

        def _process_one(item):
            country_name, bbox = item
            try:
                # ds is fully in-memory — no file I/O occurs inside this thread
                process_country_data(
                    ds, country_name, bbox, year,
                    data_type, lat_var, lon_var, time_var
                )
                return True
            except Exception as e:
                print(f"    ✗ Failed {country_name}: {str(e)}")
                return False

        n_workers = min(8, os.cpu_count() or 4, len(country_coordinates))
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            results = list(executor.map(_process_one, country_coordinates.items()))

        successful = sum(results)
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
            
            # Precipitation and temperature files are fully independent — run in parallel
            tasks = (
                [(f, 'precipitation') for f in precip_files] +
                [(f, 'temperature')   for f in temp_files]
            )

            def _run_nc(args):
                nc_path, dtype = args
                process_netcdf_by_type(nc_path, country_coordinates, dtype)

            with ThreadPoolExecutor(max_workers=min(2, len(tasks))) as executor:
                list(executor.map(_run_nc, tasks))
                
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
    nc_files = sorted(glob(os.path.join(INPUT_DIR, "*.nc")))
    
    if not nc_files:
        print(f"✗ No .nc files found in {INPUT_DIR}")
        return
    
    print(f"✓ Found {len(nc_files)} .nc files to process")
    
    # Process archive files in parallel. With selective variable loading (~536 MB
    # per file) and 16 GB RAM, 4 workers peak at ~4.3 GB — well within budget.
    print("\n[STEP 4] Processing files...")
    n_file_workers = min(4, len(nc_files))

    def _process_file(args):
        i, nc_file = args
        print(f"\n[File {i}/{len(nc_files)}]")
        try:
            process_nc_file(nc_file, country_coordinates)
        except Exception as e:
            print(f"✗ Failed file {i}/{len(nc_files)}: {str(e)}")

    with ThreadPoolExecutor(max_workers=n_file_workers) as executor:
        list(executor.map(_process_file, enumerate(nc_files, 1)))
    
    print("\n" + "="*60)
    print("PROCESSING COMPLETE!")
    print("="*60)


if __name__ == "__main__":
    main()