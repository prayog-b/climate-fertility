# --------------------------------------------------------
# PROJECT :          Climate Change and Fertility in SSA
# PURPOSE:           Create subunit-day level climate data
# AUTHOR:            Prayog Bhattarai
# DATE MODIFIED:     09 October 2025
# DESCRIPTION:       Process gridded climate data using cleaned shapefile. 
# Notes: 
# --------------------------------------------------------



import os
import re
import warnings
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Iterator

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fuzzywuzzy import process
from shapely.geometry import Polygon, box
from tqdm import tqdm


class DualLogger:
    """Custom logger that outputs to both console and file, similar to Stata's log system."""
    
    def __init__(self, log_file: Path, level: str = "INFO"):
        """
        Initialize dual logger.
        
        Args:
            log_file: Path to log file
            level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR')
        """
        self.log_file = log_file
        
        # Create logger
        self.logger = logging.getLogger('ClimateProcessor')
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Create formatters
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_formatter = logging.Formatter('%(message)s')
        
        # File handler
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Log session start
        self.info("=" * 80)
        self.info(f"CLIMATE DATA PROCESSING LOG SESSION STARTED")
        self.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.info(f"Log file: {log_file}")
        self.info("=" * 80)
    
    def info(self, message: str):
        """Log info message."""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning message."""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Log error message."""
        self.logger.error(message)
    
    def debug(self, message: str):
        """Log debug message."""
        self.logger.debug(message)
    
    def section(self, title: str, level: int = 1):
        """Log a section header."""
        if level == 1:
            separator = "=" * 60
        elif level == 2:
            separator = "-" * 50
        else:
            separator = "." * 40
            
        self.info("")
        self.info(separator)
        self.info(f" {title}")
        self.info(separator)
    
    def subsection(self, title: str):
        """Log a subsection header."""
        self.section(title, level=2)
    
    def step(self, step_num: int, description: str):
        """Log a processing step."""
        self.info(f"\nSTEP {step_num}: {description}")
        self.info("-" * 30)
    
    def summary_table(self, title: str, data: dict):
        """Log a summary table."""
        self.info(f"\n{title}:")
        max_key_len = max(len(str(k)) for k in data.keys())
        for key, value in data.items():
            self.info(f"  {str(key):<{max_key_len}} : {value}")
    
    def close(self):
        """Close the logging session."""
        self.info("")
        self.info("=" * 80)
        self.info(f"LOG SESSION ENDED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.info("=" * 80)
        
        # Close all handlers
        for handler in self.logger.handlers:
            handler.close()
        self.logger.handlers.clear()


class ClimateDataProcessor:
    """Process gridded climate data with comprehensive logging."""
    
    def __init__(
        self,
        project_dir: Union[str, Path] = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA",
        country_name: str = "Kenya",
        exact_match: bool = False,
        smallest_unit_col: str = "SMALLEST",
        bpl_name_col: str = "BPL_NAME",
        country_col: str = "COUNTRY",
        chunk_size: int = 100000,
        log_level: str = "INFO",
        # Interpolation parameters
        interpolate_missing: bool = True,
        interpolation_method: str = "nearest_neighbor",
        # options are "nearest_neighbor", "buffer", "inverse_distance_weight", "none"
        buffer_radius_km: float = 50.0,   # For buffer method
        max_neighbors: int = 5, # For IDW
        idw_power: float = 2.0, # For IDW (power parameter)
    ):
        """Initialize the ClimateDataProcessor with logging."""
        self.project_dir = Path(project_dir)
        self.parquet_dir = self.project_dir / "data" / "derived" / "era"
        self.shapefile_path = (
            self.project_dir / "data" / "derived" / "shapefiles" / "cleaned_ssa_boundaries.shp"
        )
        self.output_dir = self.project_dir / "data" / "derived" / "era" / "00-subunit-day-data"
        self.country_name = country_name
        self.exact_match = exact_match
        self.smallest_unit_col = smallest_unit_col
        self.bpl_name_col = bpl_name_col
        self.country_col = country_col
        self.chunk_size = chunk_size
        self.interpolate_missing = interpolate_missing
        self.interpolation_method = interpolation_method
        self.buffer_radius_km = buffer_radius_km
        self.max_neighbors = max_neighbors
        self.idw_power = idw_power 
        
        # Create output and log directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        log_dir = self.project_dir / "logs" / "climate_processing"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize logger
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{country_name}_climate_processing_{timestamp}.log"
        self.logger = DualLogger(log_file, level=log_level)
        
        # Log initialization
        self.logger.section("INITIALIZATION")
        init_params = {
            "Country": country_name,
            "Project Directory": str(self.project_dir),
            "Shapefile Path": str(self.shapefile_path),
            "Output Directory": str(self.output_dir),
            "Chunk Size": f"{chunk_size:,}",
            "Log Level": log_level
        }
        self.logger.summary_table("Processing Parameters", init_params)

        interpolation_params = {
            "Interpolate Missing": interpolate_missing,
            "Method": interpolation_method,
            "Buffer radius (km)": buffer_radius_km if interpolation_method == "buffer" else "N/A",
            "Max Neighbors": max_neighbors if interpolation_method == "idw" else "N/A",
            "IDW Power": idw_power if interpolation_method == "idw" else "N/A"
        }
        self.logger.summary_table("Interpolation Parameters", interpolation_params)
        
        # Initialize attributes
        self.country_dir = None
        self.admin_gdf = None
        self.grid_gdf = None
        self.intersection_gdf = None
        self.start_year = None
        self.end_year = None

    def _match_country_name(self, available_countries: List[str]) -> str:
        """Fuzzy match country name against available folders."""
        self.logger.step(1, "Country Name Matching")

        if self.exact_match:
            self.logger.info(f"Using exact match for: {self.country_name}")
            if self.country_name not in available_countries:
                error_msg = (f"Exact country name '{self.country_name}' not found. "
                           f"Available countries: {', '.join(available_countries)}")
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            return self.country_name
        
        # Fuzzy matching
        self.logger.info(f"Performing fuzzy match for: {self.country_name}")
        self.logger.info(f"Available countries: {', '.join(available_countries)}")
        
        matches = process.extractBests(
            self.country_name, 
            available_countries, 
            score_cutoff=80, 
            limit=2
        )
        
        if not matches:
            error_msg = (f"No close match found for '{self.country_name}'. "
                        f"Available countries: {', '.join(available_countries)}")
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        if len(matches) > 1 and matches[0][1] - matches[1][1] < 5:
            error_msg = (f"Ambiguous match for '{self.country_name}'. "
                        f"Top matches: {', '.join(m[0] for m in matches)}")
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        matched_name = matches[0][0]
        confidence = matches[0][1]
        
        self.logger.info(f"✅ Match found: '{matched_name}' (confidence: {confidence:.1f}%)")
        return matched_name
    
    def _get_country_files(self) -> Tuple[List[Path], List[Path]]:
        """Get temperature and precipitation files for the country."""
        self.logger.step(2, "File Discovery")
        
        country_dir = self.parquet_dir / self.country_name
        self.logger.info(f"Searching directory: {country_dir}")
        
        if not country_dir.exists():
            error_msg = f"Country directory not found: {country_dir}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        all_files = list(country_dir.glob("*.parquet"))
        self.logger.info(f"Found {len(all_files)} parquet files")
        
        if not all_files:
            error_msg = f"No parquet files found in {country_dir}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        temp_files = [f for f in all_files if f.name.endswith("_temp.parquet")]
        precip_files = [f for f in all_files if f.name.endswith("_precip.parquet")]
        
        file_summary = {
            "Total Files": len(all_files),
            "Temperature Files": len(temp_files),
            "Precipitation Files": len(precip_files)
        }
        self.logger.summary_table("File Summary", file_summary)
        
        if temp_files:
            self.logger.info("Temperature files:")
            for f in temp_files:
                self.logger.info(f"  - {f.name}")
                
        if precip_files:
            self.logger.info("Precipitation files:")
            for f in precip_files:
                self.logger.info(f"  - {f.name}")
        
        if not temp_files and not precip_files:
            error_msg = (f"No temperature or precipitation files found in {country_dir}. "
                        "Files must end with '_temp.parquet' or '_precip.parquet'")
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        return temp_files, precip_files
    
    def _detect_year_range(self, temp_files: List[Path], precip_files: List[Path]) -> None:
        """Detect start and end year from file names."""
        self.logger.step(3, "Year Range Detection")
        
        all_files = temp_files + precip_files
        years = []
        
        for file in all_files:
            match = re.search(r"_(\d{4})_", file.name)
            if match:
                year = int(match.group(1))
                years.append(year)
                self.logger.debug(f"Extracted year {year} from {file.name}")
                
        if not years:
            error_msg = "Could not detect years from filenames"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.start_year = min(years)
        self.end_year = max(years)
        
        year_summary = {
            "Start Year": self.start_year,
            "End Year": self.end_year,
            "Total Years": self.end_year - self.start_year + 1,
            "Files with Years": len(set(years))
        }
        self.logger.summary_table("Year Range Summary", year_summary)

    def _load_and_prepare_shapefile(self) -> None:
        """Load and prepare administrative boundaries shapefile."""
        self.logger.step(4, "Shapefile Loading and Preparation")
        
        self.logger.info(f"Loading shapefile: {self.shapefile_path}")
        
        try:
            gdf = gpd.read_file(self.shapefile_path)
            self.logger.info(f"✅ Shapefile loaded successfully: {len(gdf)} records")
        except Exception as e:
            error_msg = f"Failed to load shapefile: {e}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Log shapefile info
        shapefile_info = {
            "Total Records": len(gdf),
            "Columns": list(gdf.columns),
            "CRS": str(gdf.crs),
            "Geometry Types": list(gdf.geometry.type.value_counts().to_dict().keys())
        }
        self.logger.summary_table("Shapefile Information", shapefile_info)
        
        # Verify required columns
        required_cols = ["CNTRY_CD", self.smallest_unit_col, self.country_col, "geometry"]
        missing_cols = [col for col in required_cols if col not in gdf.columns]
        if missing_cols:
            error_msg = f"Shapefile missing required columns: {missing_cols}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Country matching
        self.logger.info(f"Matching country name: {self.country_name}")
        available_countries = gdf[self.country_col].unique()
        print("Available countries: ")
        print(available_countries)
        self.logger.info(f"Available countries in shapefile: {len(available_countries)}")
        
        country_matches = process.extractBests(
            self.country_name,
            available_countries,
            score_cutoff=90,
            limit=1
        )
        
        if not country_matches:
            error_msg = f"No matching country found in shapefile for: {self.country_name}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        matched_country = country_matches[0][0]
        confidence = country_matches[0][1]
        self.logger.info(f"✅ Country matched: '{matched_country}' (confidence: {confidence:.1f}%)")
        
        # Get country code and filter
        country_rows = gdf[gdf[self.country_col] == matched_country]
        country_codes = country_rows["CNTRY_CD"].unique()
        
        if len(country_codes) == 0:
            error_msg = f"No CNTRY_CD found for country: {matched_country}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        country_code = str(country_codes[0])
        country_code_length = len(country_code)
        
        self.logger.info(f"Country code: {country_code} (length: {country_code_length})")
        
        # Filter administrative units
        gdf = gdf.copy()
        gdf["SMALLEST_STR"] = gdf[self.smallest_unit_col].astype(str).str.strip()
        filtered_gdf = gdf[gdf["SMALLEST_STR"].str[:country_code_length] == country_code]
        
        if filtered_gdf.empty:
            error_msg = (f"No administrative units found with SMALLEST starting with {country_code} "
                        f"for country: {self.country_name}")
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.info(f"Administrative units found: {len(filtered_gdf)}")
        
        # Dissolve to smallest units
        self.logger.info("Dissolving to smallest administrative units...")
        dissolved_gdf = filtered_gdf.dissolve(by=self.smallest_unit_col, as_index=False)
        dissolved_gdf = dissolved_gdf[[self.smallest_unit_col, "geometry"]].copy()
        dissolved_gdf = dissolved_gdf.to_crs("EPSG:4326")  # Ensure WGS84
        
        self.admin_gdf = dissolved_gdf

        # Check how many SMALLEST units there are in the shapefile before and after dissolve
        pre_subunits = sorted(filtered_gdf[self.smallest_unit_col].unique())
        self.logger.info(f"Administrative units found before dissolve: {len(filtered_gdf)}")
        self.logger.info(f"Available SMALLEST units before dissolve operation: {pre_subunits}")
        post_subunits = sorted(dissolved_gdf[self.smallest_unit_col].unique())
        self.logger.info(f"Administrative units found after dissolve: {len(dissolved_gdf)}")
        self.logger.info(f"Available SMALLEST units after dissolve operation: {post_subunits}")
        
        # Log final results
        admin_summary = {
            "Administrative Units": len(self.admin_gdf),
            "Country Code": country_code,
            "CRS": str(self.admin_gdf.crs),
            "Bounds": f"[{', '.join(f'{x:.3f}' for x in self.admin_gdf.total_bounds)}]"
        }
        self.logger.summary_table("Administrative Boundaries Summary", admin_summary)

    def _create_grid_from_first_file(self, first_file: Path) -> None:
        """Create grid geometry from the first file with detailed logging."""
        self.logger.step(5, "Grid Creation")
        
        self.logger.info(f"Creating grid from: {first_file.name}")
        
        try:
            # Read coordinates
            df = pd.read_parquet(
                first_file, 
                columns=["latitude", "longitude"],
                engine="pyarrow"
            )
            self.logger.info(f"✅ File read successfully: {len(df)} coordinate records")
        except Exception as e:
            error_msg = f"Failed to read first file: {e}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        if df.empty:
            error_msg = "First file is empty"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Process coordinates
        self.logger.info("Processing coordinates...")
        df["latitude"] = df["latitude"].round(3)
        df["longitude"] = df["longitude"].round(3)

        unique_coords = df.drop_duplicates()
        unique_coords = unique_coords.sort_values(["latitude", "longitude"])
        
        coord_summary = {
            "Total Records": len(df),
            "Unique Coordinates": len(unique_coords),
            "Latitude Range": f"[{df['latitude'].min():.3f}, {df['latitude'].max():.3f}]",
            "Longitude Range": f"[{df['longitude'].min():.3f}, {df['longitude'].max():.3f}]"
        }
        self.logger.summary_table("Coordinate Summary", coord_summary)
        
        # Calculate grid resolution
        self.logger.info("Calculating grid resolution...")
        unique_lats = sorted(unique_coords["latitude"].unique())
        unique_lons = sorted(unique_coords["longitude"].unique())
        
        resolution_info = {
            "Unique Latitudes": len(unique_lats),
            "Unique Longitudes": len(unique_lons),
            "Latitude Range": f"[{min(unique_lats):.3f}, {max(unique_lats):.3f}]",
            "Longitude Range": f"[{min(unique_lons):.3f}, {max(unique_lons):.3f}]"
        }
        self.logger.summary_table("Grid Resolution Analysis", resolution_info)
        
        # Calculate grid steps
        if len(unique_lats) > 1:
            lat_diffs = np.diff(unique_lats)
            lat_step = np.median(lat_diffs[lat_diffs > 0])
        else:
            lat_step = 0.25  # Default fallback
            self.logger.warning("All latitudes identical, using default lat_step of 0.25°")
            
        if len(unique_lons) > 1:
            lon_diffs = np.diff(unique_lons)
            lon_step = np.median(lon_diffs[lon_diffs > 0])
        else:
            lon_step = 0.25  # Default fallback
            self.logger.warning("All longitudes identical, using default lon_step of 0.25°")
        
        if lat_step == 0:
            self.logger.warning("Calculated lat_step is 0, using default of 0.25°")
            lat_step = 0.25
        if lon_step == 0:
            self.logger.warning("Calculated lon_step is 0, using default of 0.25°")
            lon_step = 0.25
            
        grid_resolution = {
            "Latitude Step": f"{lat_step:.4f}°",
            "Longitude Step": f"{lon_step:.4f}°",
            "Cell Area (approx)": f"{lat_step * lon_step:.8f} sq degrees"
        }
        self.logger.summary_table("Grid Resolution", grid_resolution)
        
        # Create grid cells
        self.logger.info("Creating grid cell geometries...")
        geometries = []
        invalid_count = 0
        
        for i, (_, row) in enumerate(unique_coords.iterrows()):
            lat, lon = row["latitude"], row["longitude"]
            
            min_lon = lon - lon_step/2
            max_lon = lon + lon_step/2
            min_lat = lat - lat_step/2
            max_lat = lat + lat_step/2
            
            # Ensure valid coordinates
            if min_lon >= max_lon:
                max_lon = min_lon + 0.001
                invalid_count += 1
            if min_lat >= max_lat:
                max_lat = min_lat + 0.001
                invalid_count += 1
                
            cell = box(min_lon, min_lat, max_lon, max_lat)
            geometries.append(cell)
        
        if invalid_count > 0:
            self.logger.warning(f"Fixed {invalid_count} cells with invalid bounds")
            
        self.logger.info(f"Created {len(geometries)} grid cell geometries")
        
        # Create GeoDataFrame
        grid_gdf = gpd.GeoDataFrame(
            unique_coords,
            geometry=geometries,
            crs="EPSG:4326"
        )
        
        # Validate geometries
        valid_geometries = grid_gdf.geometry.is_valid
        valid_count = valid_geometries.sum()
        
        geometry_validation = {
            "Total Geometries": len(grid_gdf),
            "Valid Geometries": valid_count,
            "Invalid Geometries": len(grid_gdf) - valid_count,
            "Validation Rate": f"{100 * valid_count / len(grid_gdf):.1f}%"
        }
        self.logger.summary_table("Geometry Validation", geometry_validation)
        
        if valid_count == 0:
            self.logger.error("No valid geometries created!")
            # Log sample geometry details for debugging
            for i, geom in enumerate(geometries[:3]):
                bounds = geom.bounds
                self.logger.error(f"Sample geometry {i}: bounds = {bounds}, is_valid = {geom.is_valid}")
            raise ValueError("Failed to create any valid grid geometries")
        
        # Keep only valid geometries
        if valid_count < len(grid_gdf):
            self.logger.warning(f"Removing {len(grid_gdf) - valid_count} invalid geometries")
            grid_gdf = grid_gdf[valid_geometries].copy()
        
        self.grid_gdf = grid_gdf
        
        final_grid_summary = {
            "Valid Grid Cells": len(self.grid_gdf),
            "CRS": str(self.grid_gdf.crs),
            "Bounds": f"[{', '.join(f'{x:.3f}' for x in self.grid_gdf.total_bounds)}]"
        }
        self.logger.summary_table("Final Grid Summary", final_grid_summary)

    def _handle_missing_units(self, final_df: pd.DataFrame) -> pd.DataFrame:
        """ Handle admin units that are missing weather data using specified interpolation method."""
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
        
    def _track_missing_units(self):
        """Track which admin units have no weather data."""
        if hasattr(self, 'intersection_gdf') and self.intersection_gdf is not None:
            units_with_data = set(self.intersection_gdf[self.smallest_unit_col].unique())
            print("Units with data:")
            print(units_with_data)
            all_units = set(self.admin_gdf[self.smallest_unit_col].unique())
            print("All unique units")
            print(all_units)
            self.units_without_data = list(all_units - units_with_data)
            

            if self.units_without_data:
                missing_info = {
                    "Missing units": len(self.units_without_data),
                    "Total units": len(all_units),
                    "Coverage rate": f"{(len(units_with_data) / len(all_units)) * 100:.1f}"
                }
                self.logger.summary_table("Missing units summary", missing_info)
    
    def _assign_nearest_neighbor_weather(self, final_df: pd.DataFrame) -> pd.DataFrame:
        """Assign weather data from nearest admin unit for missing values."""
        self.logger.info("Performing nearest neighbor assignment")

        # Get centroids of all admin units
        admin_centroids = self.admin_gdf.copy()
        admin_centroids['centroid'] = admin_centroids.geometry.centroid
        admin_centroids['longitude'] = admin_centroids.centroid.x 
        admin_centroids['latitude'] = admin_centroids.centroid.y 

        # Create spatial index for fast nearest neighbor search
        from scipy.spatial import ckdtree

        valid_units = admin_centroids[~admin_centroids[self.smallest_unit_col].isin(self.units_without_data)]
        dropped_units = admin_centroids[admin_centroids[self.smallest_unit_col].isin(self.units_without_data)]

        if valid_units.empty or dropped_units.empty:
            return final_df 
        
        tree = ckdtree(valid_units[['longitude', 'latitude']].values)

        interpolation_results = []
        success_count = 0 

        for _, dropped_unit in dropped_units.iterrows():
            try:
                dist, idx = tree.query([dropped_unit.longitude, dropped_unit.latitude], k = 1)
                nearest_valid_unit = valid_units.iloc[idx][self.smallest_unit_col]

                # Get all the data for nearest valid unit 
                valid_data = final_df[final_df[self.smallest_unit_col] == nearest_valid_unit].copy()

                if not valid_data.empty:
                    # Replace the admin unit ID but keep all weather data 
                    valid_data[self.smallest_unit_col] = dropped_unit[self.smallest_unit_col]
                    interpolation_results.append(valid_data)
                    success_count += 1
                
            except Exception as e:
                self.logger.warning(f"Failed to interpolate for {dropped_unit[self.smallest_unit_col]}: {e}")

            if interpolation_results:
                interpolated_data = pd.concat(interpolation_results, ignore_index = True)
                final_df  = pd.concat([final_df, interpolated_data], ignore_index = True)

                success_rate = (success_count / len(self.units_without_data)) * 100
                self.logger.info(f"Nearest neighbor completed: {success_count}/{len(self.units_without_data)} units ({success_rate:.1f}%)")

                return final_df 
            
    def _assign_buffered_weather(self, final_df: pd.DataFrame) -> pd.DataFrame:
        """ Assign weather data using buffered search for nearby grid cells"""
        self.logger.info(f"Performing buffered assignment (radius: {self.buffer_radius_km} km ...)")

        # Convert buffer from km to degrees (approx)
        buffer_deg = self.buffer_radius_km / 111.32 

        # Prepare admin units with centroids
        admin_centroids = self.admin_gdf.copy()
        admin_centroids['centroid'] = admin_centroids.geometry.centroid
        admin_centroids['longitude'] = admin_centroids.centroid.x 
        admin_centroids['latitude'] = admin_centroids.centroid.y 

        dropped_units = admin_centroids[admin_centroids[self.smallest_unit_col].isin(self.units_without_data)]
    
        interpolation_results = []
        success_count = 0

        for _, dropped_unit in dropped_units.iterrows():
            try:
                # Create buffer around centroid
                buffer_geom = dropped_unit.centroid(buffer(buffer_deg))

                # Find intersecting grid cells
                intersecting_grid = self.grid_gdf[self.grid_gdf.intersects(buffer_geom)]

                if not intersecting_grid.empty:
                    # Get weather data for these grid cells
                    grid_coords = list(zip(intersecting_grid.latitude, intersecting_grid.longitude))
                    relevant_intersections = self.intersection_gdf[
                        self.intersection_gdf[['latitude', 'longitude']].apply(tuple, axis = 1).isin(grid_coords)
                    ]
                    
                    if not relevant_intersections.empty:
                        # We use the average of nearby grid cells
                        # It is possible to change this averaging mechanism here. 
                        avg_weather = self._calculate_buffer_average(relevant_intersections, final_df)

                        if avg_weather is not None: 
                            # Create records for the dates
                            date_range = final_df[['year', 'month', 'day']].drop_duplicates
            except Exception as e:
                self.logger.warning(f"Failed buffer interpolation for {dropped_unit[self.smallest_unit_col]}: {e}")

        if interpolation_results:
            interpolated_data = pd.concat(interpolation_results, ignore_index=True)
            final_df = pd.concat([final_df, interpolated_data], ignore_index = True)

            success_rate = (success_count / len(self.units_without_data)) * 100
            self.logger.info(f"Buffer assignment completed: {success_count} / {len(self.units_without_data)} units ({success_rate:.1f}%)")

        return final_df 
    
    def _assign_idw_weather(self, final_df: pd.DataFrame) -> pd.DataFrame:
        """Assign weather data using Inverse Distance Weighting."""
        self.logger.info("Performing IDW interpolation...")
        self.logger.warning("IDW interpolation not yet implemented - using nearest neighbor instead")
        
        # Placeholder: I haven't yet figured out how to set up IDW. 
        # For now, fall back to nearest neighbor
        return self._assign_nearest_neighbor_weather(final_df)
    
    def _calculate_buffer_average(self, intersections: pd.DataFrame, final_df: pd.DataFrame) -> Optional[Dict]:
        """Calculate average weather values from buffer intersections."""
        try:
            # Get unique grid points in buffer
            grid_points = intersections[['latitude', 'longitude']].drop_duplicates()

            # For each grid point, get its average contribution
            weather_values = {'temp_mean': [], 'temp_max': [], 'precip': []}
            weights = []

            for _, grid_point in grid_points.iterrows():
                lat, lon = grid_point['latitude'], grid_point['longitude']
                # Find intersection info for this grid points
                grid_intersection = intersections[
                    (intersections['latitude'] == lat) &
                    (intersections['longitude'] == lon)
                ]

                if not grid_intersection.empty:
                    weight = grid_intersection['shr_of_subunit'].mean()
                    weights.append(weight)

                    # Get weather data for this grid point (simplified)
                    # In a real implementation, we need to handle temporal aspects
                    for col in weather_values.keys():
                        weather_values[col].append(weight) # Placeholder

            if weights:
                # calculate weighted averages
                result = {}
                for col in weather_values.keys():
                    if weather_values[col]:
                        result[col] = np.average(weather_values[col], weights = weights)
                    else:
                        result[col] = np.man 

                return result 
        except Exception as e:
            self.logger.warning(f"Error calculating buffer average: {e}")

        return None

    def _calculate_intersections(self) -> None:
        """Calculate intersections with comprehensive logging."""
        self.logger.section("GRID-ADMIN INTERSECTION CALCULATION")
        
        # Initial diagnostics
        self.logger.subsection("Initial Diagnostics")
        diagnostic_info = {
            "Grid CRS": str(self.grid_gdf.crs),
            "Admin CRS": str(self.admin_gdf.crs),
            "Grid Cells": len(self.grid_gdf),
            "Admin Units": len(self.admin_gdf)
        }
        self.logger.summary_table("Initial State", diagnostic_info)
        
        # Ensure both in WGS84
        if self.grid_gdf.crs != "EPSG:4326":
            self.logger.info("Converting grid to WGS84...")
            self.grid_gdf = self.grid_gdf.to_crs("EPSG:4326")
        if self.admin_gdf.crs != "EPSG:4326":
            self.logger.info("Converting admin boundaries to WGS84...")
            self.admin_gdf = self.admin_gdf.to_crs("EPSG:4326")
        
        # Spatial overlap check
        self.logger.subsection("Spatial Overlap Analysis")
        grid_bounds = self.grid_gdf.total_bounds
        admin_bounds = self.admin_gdf.total_bounds
        
        bounds_info = {
            "Grid Bounds": f"[{', '.join(f'{x:.6f}' for x in grid_bounds)}]",
            "Admin Bounds": f"[{', '.join(f'{x:.6f}' for x in admin_bounds)}]"
        }
        self.logger.summary_table("Spatial Bounds", bounds_info)
        


        # Check overlap
        grid_minx, grid_miny, grid_maxx, grid_maxy = grid_bounds
        admin_minx, admin_miny, admin_maxx, admin_maxy = admin_bounds
        
        overlap_x = not (grid_maxx < admin_minx or admin_maxx < grid_minx)
        overlap_y = not (grid_maxy < admin_miny or admin_maxy < grid_miny)
        
        overlap_info = {
            "X-axis Overlap": "✅ Yes" if overlap_x else "❌ No",
            "Y-axis Overlap": "✅ Yes" if overlap_y else "❌ No",
            "Overall Overlap": "✅ Yes" if (overlap_x and overlap_y) else "❌ No"
        }
        self.logger.summary_table("Overlap Analysis", overlap_info)
        
        if not (overlap_x and overlap_y):
            error_msg = ("No spatial overlap between grid and administrative boundaries! "
                        "Check coordinate systems and data sources.")
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Sample intersection test
        self.logger.subsection("Sample Intersection Test")
        from shapely.geometry import Point
        
        sample_size = min(10, len(self.grid_gdf))
        sample_grid = self.grid_gdf.sample(sample_size)
        manual_intersections = 0
        
        for idx, row in sample_grid.iterrows():
            point = Point(row['longitude'], row['latitude'])
            for admin_idx, admin_row in self.admin_gdf.iterrows():
                if admin_row.geometry.contains(point) or admin_row.geometry.intersects(point):
                    manual_intersections += 1
                    break
        
        sample_test_info = {
            "Sample Size": sample_size,
            "Successful Intersections": manual_intersections,
            "Success Rate": f"{100 * manual_intersections / sample_size:.1f}%"
        }
        self.logger.summary_table("Manual Intersection Test", sample_test_info)
        
        if manual_intersections == 0:
            self.logger.warning("Manual test found no intersections - investigating...")
            # Additional diagnostic would go here
        
        # Geometry preparation
        self.logger.subsection("Geometry Preparation")
        
        invalid_grid = (~self.grid_gdf.geometry.is_valid).sum()
        invalid_admin = (~self.admin_gdf.geometry.is_valid).sum()
        
        geometry_status = {
            "Invalid Grid Geometries": invalid_grid,
            "Invalid Admin Geometries": invalid_admin
        }
        self.logger.summary_table("Geometry Validation Status", geometry_status)
        
        if invalid_grid > 0:
            self.logger.warning(f"Attempting to fix {invalid_grid} invalid grid geometries...")
            self.grid_gdf = self.grid_gdf.copy()
            invalid_geoms = ~self.grid_gdf.geometry.is_valid
            self.grid_gdf.loc[invalid_geoms, 'geometry'] = self.grid_gdf.loc[invalid_geoms, 'geometry'].buffer(0.0001)
            
            still_invalid = (~self.grid_gdf.geometry.is_valid).sum()
            if still_invalid > 0:
                self.logger.warning(f"Removing {still_invalid} geometries that couldn't be fixed")
                self.grid_gdf = self.grid_gdf[self.grid_gdf.geometry.is_valid].copy()
        
        if invalid_admin > 0:
            self.logger.warning(f"Removing {invalid_admin} invalid admin geometries...")
            self.admin_gdf = self.admin_gdf[self.admin_gdf.geometry.is_valid].copy()
        
        if len(self.grid_gdf) == 0:
            error_msg = "No valid grid geometries remaining after cleanup!"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Spatial join
        self.logger.subsection("Spatial Join Operation")
        
        joined = None
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
        
        # After spatial join, we need to track which admin units were dropped because there were no intersections.
        included_units = set(joined[self.smallest_unit_col].unique())
        all_units = set(self.admin_gdf[self.smallest_unit_col].unique())
        self.dropped_units = list(all_units - included_units)

        if self.dropped_units:
                self.logger.warning(f"{len(self.dropped_units)} admin units had no weather data intersections.")
                for unit in sorted(self.dropped_units)[:10]:
                    self.logger.warning(f"   -  {unit}")
                if len(self.dropped_units) > 10:
                    self.logger.warning(f"  ... and {len(self.dropped_units) - 10} more")

        # Intersection geometry calculation
        self.logger.subsection("Intersection Geometry Calculation")
        
        self.logger.info("Computing detailed intersection geometries...")
        joined["intersection_geom"] = joined.apply(
            lambda row: row["geometry"].intersection(
                self.admin_gdf.loc[row["index_right"], "geometry"]
            ),
            axis=1
        )
        
        # Area calculations
        self.logger.info("Computing intersection areas...")
        joined["intersection_area"] = joined["intersection_geom"].area
        joined["cell_area"] = joined["geometry"].area
        
        # Validation
        valid_mask = (
            (joined["intersection_area"] > 0) & 
            (joined["cell_area"] > 0) & 
            (~joined["intersection_area"].isna()) & 
            (~joined["cell_area"].isna())
        )
        
        validation_summary = {
            "Before Validation": len(joined),
            "After Validation": valid_mask.sum(),
            "Removed": len(joined) - valid_mask.sum()
        }
        self.logger.summary_table("Area Validation", validation_summary)
        
        joined = joined[valid_mask].copy()
        
        if len(joined) == 0:
            error_msg = "No valid intersections after geometry calculations!"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Weight calculation
        self.logger.subsection("Weight Calculation")
        
        joined["shr_of_subunit"] = joined["intersection_area"] / joined["cell_area"]
        joined["shr_of_subunit"] = np.clip(joined["shr_of_subunit"], 0, 1)
        
        area_stats = {
            "Intersection Area - Min": f"{joined['intersection_area'].min():.10f}",
            "Intersection Area - Max": f"{joined['intersection_area'].max():.10f}",
            "Cell Area - Min": f"{joined['cell_area'].min():.10f}",
            "Cell Area - Max": f"{joined['cell_area'].max():.10f}",
            "Weight - Min": f"{joined['shr_of_subunit'].min():.6f}",
            "Weight - Max": f"{joined['shr_of_subunit'].max():.6f}",
            "Weight - Mean": f"{joined['shr_of_subunit'].mean():.6f}",
            "Zero Weights": (joined['shr_of_subunit'] == 0).sum(),
            "NaN Weights": joined['shr_of_subunit'].isna().sum()
        }
        self.logger.summary_table("Area and Weight Statistics", area_stats)
        
        if joined["shr_of_subunit"].max() == 0:
            error_msg = "All intersection weights are zero!"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Final result preparation
        self.logger.subsection("Final Result Preparation")
        
        result = joined[[
            "latitude", "longitude", self.smallest_unit_col,
            "intersection_area", "cell_area", "shr_of_subunit"
        ]].copy()
        
        # Round coordinates
        result["latitude"] = result["latitude"].round(3)
        result["longitude"] = result["longitude"].round(3)
        
        # Group by coordinates and admin unit
        result = result.groupby(['latitude', 'longitude', self.smallest_unit_col]).agg({
            'intersection_area': 'sum',
            'cell_area': 'first',
            'shr_of_subunit': 'sum'
        }).reset_index()
        
        result["shr_of_subunit"] = np.clip(result["shr_of_subunit"], 0, 1)
        
        self.intersection_gdf = result
        
        final_summary = {
            "Total Intersections": len(self.intersection_gdf),
            "Unique Grid Points": len(result.groupby(['latitude', 'longitude'])),
            "Admin Units Covered": len(result[self.smallest_unit_col].unique()),
            "Average Weight": f"{result['shr_of_subunit'].mean():.4f}"
        }
        self.logger.summary_table("Final Intersection Summary", final_summary)
        
        # Sample results
        self.logger.info("Sample intersection results:")
        sample_result = result[["latitude", "longitude", self.smallest_unit_col, "shr_of_subunit"]].head()
        for _, row in sample_result.iterrows():
            self.logger.info(f"  ({row['latitude']:.3f}, {row['longitude']:.3f}) -> {row[self.smallest_unit_col]} (weight: {row['shr_of_subunit']:.4f})")

    def _read_parquet_chunked(self, file_path: Path, columns: list = None) -> Iterator[pd.DataFrame]:
        """Read parquet file in chunks using pyarrow."""
        parquet_file = pq.ParquetFile(file_path)
        
        for batch in parquet_file.iter_batches(batch_size=self.chunk_size, columns=columns):
            yield batch.to_pandas()

    def _process_temp_file(self, file_path: Path) -> pd.DataFrame:
        """Process a temperature file with detailed logging."""
        self.logger.info(f"Processing temperature file: {file_path.name}")
        
        # Check schema
        schema = pq.read_schema(file_path)
        required_cols = {"valid_time", "latitude", "longitude", "temp_mean", "temp_max"}
        missing_cols = required_cols - set(schema.names)
        if missing_cols:
            error_msg = f"Temperature file missing required columns: {missing_cols}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
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

    def _safe_weighted_average(self, values, weights):
        """Calculate weighted average with robust handling."""
        values = np.array(values)
        weights = np.array(weights)
        
        mask = ~(pd.isna(weights) | pd.isna(values) | (weights <= 0))
        
        if not mask.any():
            return np.nan
                
        valid_values = values[mask]
        valid_weights = weights[mask]
        
        if len(valid_values) == 0:
            return np.nan
        
        return np.average(valid_values, weights=valid_weights)

    def _process_precip_file(self, file_path: Path) -> pd.DataFrame:
        """Process precipitation file with logging."""
        self.logger.info(f"Processing precipitation file: {file_path.name}")
        
        # Similar implementation to temp file but for precipitation
        schema = pq.read_schema(file_path)
        required_cols = {"valid_time", "latitude", "longitude", "precip"}
        missing_cols = required_cols - set(schema.names)
        if missing_cols:
            error_msg = f"Precipitation file missing required columns: {missing_cols}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
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
        
        # Create complete date index
        self.logger.info("Creating complete date index...")
        all_dates = pd.date_range(
            start=f"{self.start_year}-01-01",
            end=f"{self.end_year}-12-31",
            freq="D"
        )
        
        all_admin_units = self.admin_gdf[self.smallest_unit_col].unique()
        
        date_index = pd.MultiIndex.from_product(
            [all_admin_units, all_dates],
            names=[self.smallest_unit_col, "valid_time"]
        )
        
        complete_df = pd.DataFrame(index=date_index).reset_index()
        complete_df["year"] = complete_df["valid_time"].dt.year
        complete_df["month"] = complete_df["valid_time"].dt.month
        complete_df["day"] = complete_df["valid_time"].dt.day
        
        # Final merge
        final_df = pd.merge(
            complete_df,
            merged,
            on=[self.smallest_unit_col, "year", "month", "day"],
            how="left"
        )
        
        final_df["COUNTRY"] = final_df["COUNTRY"].fillna(self.country_name)
        final_df = final_df.drop(columns=["valid_time"])
        
        # Reorder columns
        final_df = final_df[[
            self.smallest_unit_col,
            "COUNTRY",
            "year",
            "month",
            "day",
            "temp_mean",
            "temp_max",
            "precip"
        ]]
        
        # Final summary
        final_summary = {
            "Total Records": f"{len(final_df):,}",
            "Date Range": f"{self.start_year}-{self.end_year}",
            "Admin Units": len(all_admin_units),
            "Years": self.end_year - self.start_year + 1,
            "Missing temp_mean": f"{final_df['temp_mean'].isna().sum():,}",
            "Missing temp_max": f"{final_df['temp_max'].isna().sum():,}",
            "Missing precip": f"{final_df['precip'].isna().sum():,}"
        }
        self.logger.summary_table("Final Dataset Summary", final_summary)

        # Handle missing admin units with interpolation
        final_df = self._handle_missing_units(final_df)
        
        return final_df

    def process_country(self) -> None:
        """Main processing method with comprehensive logging."""
        try:
            self.logger.section("CLIMATE DATA PROCESSING WORKFLOW", level=1)
            self.logger.info(f"Starting processing for: {self.country_name}")
            
            # Step 1-3: Initial setup
            available_countries = [d.name for d in self.parquet_dir.iterdir() if d.is_dir()]
            self.country_name = self._match_country_name(available_countries)
            
            temp_files, precip_files = self._get_country_files()
            self._detect_year_range(temp_files, precip_files)
            
            # Step 4-5: Spatial setup
            self._load_and_prepare_shapefile()
            
            if temp_files:
                self._create_grid_from_first_file(temp_files[0])
            elif precip_files:
                self._create_grid_from_first_file(precip_files[0])
            else:
                raise ValueError("No files available to create grid")
                    
            self._calculate_intersections()
            
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
            
            self.logger.info(f"Saving output to: {output_file}")
            try:
                # Try with encoding parameter (newer pandas)
                final_df.to_stata(output_file, write_index=False, version=118, encoding="utf-8")
            except TypeError as e:
                if "encoding" in str(e):
                    # Fallback for older pandas versions without encoding parameter
                    self.logger.warning("Older pandas version detected, using Stata export without encoding parameter")
                    # Normalize text columns to handle Unicode issues
                    final_df = self._normalize_text_columns(final_df, [self.smallest_unit_col, "COUNTRY"], form="NFC")
                    final_df.to_stata(output_file, write_index=False, version=118)
                else:
                    raise
            
            # Success summary - USE THE RENAMED COUNTRY NAME
            success_summary = {
                "Country": self.country_name,
                "Output File": str(output_file),
                "File Size": f"{output_file.stat().st_size / (1024*1024):.1f} MB",
                "Records": f"{len(final_df):,}",
                "Processing Time": "Complete"
            }
            self.logger.summary_table("PROCESSING COMPLETED SUCCESSFULLY", success_summary)
            
        except Exception as e:
            error_msg = f"Error processing country {self.country_name}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error("Processing failed - see log for details")
            raise
        finally:
            # Always close the logger
            self.logger.close()


    def _normalize_text_columns(self, df: pd.DataFrame, columns: List[str], form: str = "NFC") -> pd.DataFrame:
        """Normalize Unicode text columns to handle encoding issues."""
        df = df.copy()
        for col in columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.normalize(form)
        return df

# Example usage with logging
if __name__ == "__main__":
    # countries = [
    #     "Benin", "Botswana", "Burkina Faso", "Cameroon", "Ethiopia", 
    #     "Ghana", "Guinea", "Ivory Coast", "Kenya", "Lesotho", "Liberia", "Mali"
    #     "Malawi", "Mozambique", "Rwanda", "Senegal", "Sierra Leone", 
    #     "South Africa", "South Sudan", "Sudan", "Tanzania", 
    #     "Togo", "Uganda", "Zambia", "Zimbabwe"
    # ]
    countries = ["Rwanda"]
    
    for country in countries:
        print(f"\n{'='*60}")
        print(f"PROCESSING {country.upper()}")
        print(f"{'='*60}")
        
        try:
            processor = ClimateDataProcessor(
                country_name=country, 
                exact_match=False,
                log_level="INFO"  # Can be DEBUG, INFO, WARNING, ERROR
            )
            processor.process_country()
            print(f"✅ {country} processing completed successfully")
            
        except Exception as e:
            print(f"❌ {country} processing failed: {e}")