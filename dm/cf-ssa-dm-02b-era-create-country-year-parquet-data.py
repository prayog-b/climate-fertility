# --------------------------------------------------------
# PROJECT :           Climate Change and Fertility in Sub-Saharan Africa
# PURPOSE:            Create country-year level climate data from ERA5 data
# AUTHOR:             Prayog Bhattarai
# DATE MODIFIED:      09 October 2025
# DESCRIPTION:        This script processes ERA5 climate data (temperature and precipitation)
#                     for specified Sub-Saharan African countries, resampling the data to daily
#                     levels and saving it in Parquet format for efficient storage and analysis.
#                     The script handles both temperature and precipitation data, converting
#                     temperature from Kelvin to Celsius and precipitation from meters to millimeters.
#                     It organizes the output files by country and year.

# Notes:              Adjust the BASE_DIR variable to point to your local data directory.
#                     Ensure that the required libraries (pandas, xarray, numpy, etc.) are installed.
#                     Make sure that the input ERA5 data files are in netCDF format and located in the specified INPUT_DIR.
# --------------------------------------------------------          


# Import necessary libraries
import os
import pandas as pd
import xarray as xr
import numpy as np
from glob import glob
from datetime import datetime

# Set up base directories
BASE_DIR = "D:/prayog/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_DIR = os.path.join(DATA_DIR, "source", "era", "continent")
TEMP_ZIP_DIR = os.path.join(DATA_DIR, "temp_zip")
OUTPUT_DIR = os.path.join(DATA_DIR, "derived", "era")

# Dictionary of countries that we need to process and their bounding boxes
country_coordinates = {
    "Benin": {"north": 13, "south": 6.32, "east": 3.2, "west": 0.73},
    "Botswana": {"north": -17.5, "south": -27.0, "east": 29.4, "west": 19.8},
    "Burkina Faso": {"north": 15.3, "south": 9.35, "east": 2.43, "west": -5.54},
    "Cameroon": {"north": 13.3, "south": 1.63, "east": 16.23, "west": 8.45},
    "Ivory Coast": {"north": 11.5, "south": 4.3, "east": -2.4, "west": -8.7},
    "Ethiopia": {"north": 15.0, "south": 3.0, "east": 48.0, "west": 33.0},
    "Ghana": {"north": 11.5, "south": 4.5, "east": 1.3, "west": -3.25},
    "Guinea": {"north": 13.5, "south": 7.0, "east": -7.6, "west": -15},
    "Kenya": {"north": 5.0, "south": -4.5, "east": 41.0, "west": 34.5},
    "Lesotho": {"north": -28.5, "south": -30.7, "east": 29.5, "west": 27},
    "Liberia": {"north": 9.5, "south": 4.3, "east": -7.0, "west": -11.5},
    "Malawi": {"north": -9.3, "south": -17.2, "east": 35.93, "west": 32.6},
    "Mali": {"north": 25.0, "south": 10.1, "east": 4.3, "west": -12.3},
    "Mozambique": {"north": -10.4, "south": -26.9, "east": 41.0, "west": 30.1},
    "Zimbabwe": {"north": -15.0, "south": -22.48, "east": 33.0, "west": 25.2},
    "Zambia": {"north": -8.0, "south": -18.1, "east": 33.7, "west": 21.96},
    "Uganda": {"north": 4.5, "south": -1.5, "east": 35.05, "west": 29.54},
    "Tanzania": {"north": 1.5, "south": -11.5, "east": 40.5, "west": 29.3},
    "Rwanda": {"north": -1.04, "south": -2.9, "east": 30.9, "west": 28.8},
    "South Africa": {"north": -22.0, "south": -35, "east": 33, "west": 16},
    "Sudan": {"north": 22.1, "south": 9.32, "east": 38.6, "west": 21.75},
    "South Sudan": {"north": 12.5, "south": 3.4, "east": 35.96, "west": 23.4},
    "Sierra Leone": {"north": 10, "south": 6.89, "east": -10.2, "west": -13.4},
    "Senegal": {"north": 16.74, "south": 12.32, "east": -11.3, "west": -17.5},
    "Togo": {"north": 11.3, "south": 6.0, "east": 1.64, "west": -0.15},
}

# Create the temporary directory if it doesn't exist
os.makedirs(TEMP_ZIP_DIR, exist_ok = True)

# Create the output directory structure if it doesn't exist:
for country in country_coordinates.keys():
    country_dir = os.path.join(OUTPUT_DIR, country)
    os.makedirs(country_dir, exist_ok=True)

def extract_year_from_filename(file_name):
    """
    Extract the year from a file name.
    
    The file names are expected to be in the format "Africa_YYYY.nc", where YYYY is the year.
    
    Parameters:
    - file_name: Name of the file (string)
    
    Returns:
    - year: Extracted year as an integer
    """
    # Extract the base name without the directory path
    base_name = os.path.basename(file_name)
    
    # Split the base name to isolate the year
    parts = base_name.split('_')
    if len(parts) > 1 and parts[1].isdigit():
        return int(parts[1].split('.')[0])  # Extract the year and convert to integer
    else:
        raise ValueError(f"Invalid file name format: {file_name}")


def convert_and_process_nc_files(nc_file):
    """
    Convert .nc files to zip, extract and process based on file type.
    
    Parameters:
    - nc_file: Path to the .nc file
    """
    # Create a base name for the zip file (without the path and extension)
    base_name = os.path.basename(nc_file).replace('.nc', '')
    zip_path = os.path.join(TEMP_ZIP_DIR, f"{base_name}.zip")
    
    # Create a zip file
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        zipf.write(nc_file, os.path.basename(nc_file))
    
    # Process the zip file
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        # Extract all files
        zipf.extractall(TEMP_ZIP_DIR)
        
        # Process each .nc file
        for file in zipf.namelist():
            if file.endswith('.nc'):
                file_path = os.path.join(TEMP_ZIP_DIR, file)
                year = extract_year_from_filename(file)
                
                # Process based on file suffix
                if '_accum' in file:
                    # Process precipitation data
                    process_precipitation_data(file_path, country, year)
                elif '_instant' in file:
                    # Process temperature data
                    process_temperature_data(file_path, country, year)
                else:
                    print(f"Unknown file type: {file}")
                
                # Clean up extracted file
                os.remove(file_path)
    
    # Clean up zip file
    os.remove(zip_path)

def process_zips_to_data(zip_dir, output_base, countries):
    """
    Process all zip files containing netCDF data in a directory
    and save processed parquet files to appropriate directories.
    
    Args:
        INPUT_DIR (str): Path to directory containing zip files
        OUTPUT_DIR (str): Base path for output parquet files
        countries (dict): Country bounding boxes 
    """
    # Create output directory if needed
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process each zip file in zipfile directory directory
    for zip_file in os.listdir(INPUT_DIR):
        if not zip_file.endswith('.zip'):
            continue
            
        zip_path = os.path.join(INPUT_DIR, zip_file)
        print(f"Processing zip file: {zip_path}")
        
        # Use temporary directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Extract zip contents
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.extractall(temp_dir)
                
                # Process extracted files
                has_nc_files = False
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith('.grib') or file.endswith('.grb') or file.endswith('.grib2'):
                            print("This zipfile has GRIB format data. Moving on...")
                            continue
                        elif file.endswith('.nc'):
                            has_nc_files = True
                            process_nc_file(root, file, output_base, countries)
                            
                if not has_nc_files:
                    print(f"No netCDF files found in {zip_file}. Moving on...")
            except Exception as e:
                print(f"Failed to process {zip_file}: {str(e)}")
                continue



def process_precipitation_file(nc_file, country_name, bbox, year):
    """
    Process precipitation data, resample to daily level, and save as parquet
    
    Parameters:
    - nc_file: Path to netCDF file with precipitation data
    - country_name: Name of the country
    - bbox: Bounding box of the country (north, south, east, west)
    - year: Year of the data
    """
    print(f"Processing precipitation data for {country_name} for year {year}")
    
    # Extract coordinates
    north, south, east, west = bbox
    
    # Open the dataset
    ds = xr.open_dataset(nc_file)
    
    # Subset to the country's bounding box
    subset = ds.sel(latitude=slice(south, north), longitude=slice(west, east))
    
    # Convert to dataframe
    df = subset.to_dataframe().reset_index()
    
    # Set datetime as index for resampling
    df['datetime'] = pd.to_datetime(df['valid_time'])
    df.set_index('datetime', inplace=True)
    
    # Calculate daily precipitation sum (convert from meters to millimeters for easier interpretation)
    # Group by latitude, longitude, and date
    daily_precip = df.groupby([pd.Grouper(freq='D'), 'latitude', 'longitude'])['tp'].sum() * 1000  # Convert to mm
    daily_precip = daily_precip.reset_index()
    
    # Rename column for clarity
    daily_precip.rename(columns={'tp': 'precipitation_mm'}, inplace=True)
    
    # Format date as YYYY-MM-DD
    daily_precip['date'] = daily_precip['datetime'].dt.strftime('%Y-%m-%d')
    
    # Save to parquet
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_precip.parquet")
    daily_precip.to_parquet(output_path, index=False)
    print(f"Saved precipitation data to {output_path}")
    
    # Close the dataset
    ds.close()
    
    return output_path


def process_temperature_file(nc_file, country_name, bbox, year):
    """
    Process temperature data, resample to daily level, and save as parquet
    
    Parameters:
    - nc_file: Path to netCDF file with temperature data
    - country_name: Name of the country
    - bbox: Bounding box of the country (north, south, east, west)
    - year: Year of the data
    """
    print(f"Processing temperature data for {country_name} for year {year}")
    
    # Extract coordinates
    north, south, east, west = bbox
    
    # Open the dataset
    ds = xr.open_dataset(nc_file)
    
    # Subset to the country's bounding box
    subset = ds.sel(latitude=slice(south, north), longitude=slice(west, east))
    
    # Convert to dataframe
    df = subset.to_dataframe().reset_index()
    
    # Set datetime as index for resampling
    df['datetime'] = pd.to_datetime(df['valid_time'])
    df.set_index('datetime', inplace=True)
    
    # Convert temperature from Kelvin to Celsius
    df['t2m_celsius'] = df['t2m'] - 273.15
    
    # Calculate daily mean and max temperature
    # Group by latitude, longitude, and date
    daily_temp = df.groupby([pd.Grouper(freq='D'), 'latitude', 'longitude'])['t2m_celsius'].agg(
        temp_mean=np.mean,
        temp_max=np.max
    ).reset_index()
    
    # Format date as YYYY-MM-DD
    daily_temp['date'] = daily_temp['datetime'].dt.strftime('%Y-%m-%d')
    
    # Save to parquet
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_temp.parquet")
    daily_temp.to_parquet(output_path, index=False)
    print(f"Saved temperature data to {output_path}")
    
    # Close the dataset
    ds.close()
    
    return output_path



def process_netcdf_file(nc_file):
    """Process a single netCDF file containing both temperature and precipitation data."""
    print(f"Processing netCDF file: {nc_file}")

    # Open the dataset
    ds = xr.open_dataset(nc_file)

    # Determine the year from the file
    year = extract_year_from_netcdf(ds)

    # Process each country:
    for country_name, bbox in country_coordinates.items():
        process_country_data(ds, country_name, bbox, year)

    ds.close()

# Function to process data for a specific country
def process_country_data(ds, country_name, bbox, year):
    """
    Process climate data for a specific country and save as parquet files
    
    Parameters:
    - ds: xarray Dataset containing both temperature and precipitation data
    - country_name: Name of the country
    - bbox: Bounding box of the country (north, south, east, west)
    - year: Year of the data
    """
    print(f"Processing data for {country_name} for year {year}")

    # Extract coordinates
    north, south, east, west = bbox

    # Assertions to validate bounding box
    assert north > south, "North coordinate must be greater than South coordinate"
    assert east > west, "East coordinate must be greater than West coordinate"

    # Subset to the country's bounding box
    subset = ds.sel(latitude = slice(south, north), longitude=slice(west, east))

    # Convert to dataframe
    df = subset.to_dataframe().reset_index()

    # Set datetime as index for resampling
    df['datetime'] = pd.to_datetime(df['valid_time'])
    df.set_index('datetime', inplace=True)

    # Process precipitation data
    if 'tp' in df.columns:
        process_precipitation_data(df, country_name, year)
    else:
        print(f"Warning: No precipitation data (tp) found for {country_name}_{year}")

    # Process temperature data
    if 't2m' in df.columns:
        process_temperature_data(df, country_name, year)
    else:
        print(f"Warning: No temperature data (t2m) found for {country_name}")

# Function to process precipitation data
def process_precipitation_data(df, country_name, year):
    """Process precipitation data and save as Parquet."""

    # calculate daily precipitation sum (convert from meters to milimeters for easier interpretation)
    # group by latitude, longitude and date
    # Important: in this stage, the conversion to millimeters is done by multiplying by 1000
    daily_precip = df.groupby([pd.Grouper(freq='D'), 'latitude', 'longitude'])['tp'].sum() * 1000
    daily_precip = daily_precip.reset_index()

    # rename column for clarity
    daily_precip.rename(columns={'tp': 'precip'}, inplace = True)

    # format date as YYYY-MM-DD
    daily_precip['date'] = daily_precip['datetime'].dt.strftime('%Y-%m-%d')

    # save to parquet
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_precip.parquet")
    daily_precip.to_parquet(output_path, index = False)
    print(f"Saved precipitation data to {output_path}")

# Function to process temperature data
def process_temperature_data(df, country_name, year):
    """Process temperature data and save as Parquet."""
    
    # convert temperature data from Kelvin to Celsius
    df['temp']= df['t2m'] - 273.15

    # calculate daily mean and max temperatures
    daily_temp = df.groupby([pd.Grouper(freq='D'), 'latitude', 'longitude'])['temp'].agg(
        temp_mean = np.mean,
        temp_max = np.max
    ).reset_index()

    # Format date as YYYY-MM-DD
    daily_temp['date'] = daily_temp['datetime'].dt.strftime('%Y-%m-%d')

    # Save to parquet
    output_path = os.path.join(OUTPUT_DIR, country_name, f"{country_name}_{year}_temp.parquet")
    daily_temp.to_parquet(output_path, index = False)
    print(f"Saved temperature data to {output_path}")

# Main workflow function to process all netCDF files
def main():
    """Main workflow function to process all netCDF files."""
    # Get all netCDF files in the input directory
    nc_files = glob(os.path.join(INPUT_DIR, "*.nc"))
    
    if not nc_files:
        print(f"No netCDF files found in {INPUT_DIR}.")
        return
    else:
        print(f"Found {len(nc_files)} netCDF files in {INPUT_DIR}.")

    # Process each netCDF file
    for nc_file in nc_files:
        process_netcdf_file(nc_file)
    
    print("All files processed successfully!")

# Running the main function
if __name__ == "__main__":
    main()