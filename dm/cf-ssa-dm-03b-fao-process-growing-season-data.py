# --------------------------------------------------------
# PROJECT :         Climate Fertility
# PURPOSE:          Process growing season data
# AUTHOR:           Prayog Bhattarai
# DATE MODIFIED:    September 4, 2025
# DESCRIPTION:      I use raster data from FAO to create indicators for whether a month is a growing season month.
# Notes:            Ensure that required packages are installed: rasterio, geopandas, pandas, numpy, matplotlib, seaborn
# Output:           Climate_Change_and_Fertility_in_SSA/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv" 
# --------------------------------------------------------


import os
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point
import traceback
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as colors
import seaborn as sns

def load_shapefile(shapefile_path, countries):
    """Load and filter shapefile for specified countries with exact matching, then dissolve by SMALLEST."""
    gdf = gpd.read_file(shapefile_path)
    
    # Check required columns
    required_columns = {"COUNTRY", "SMALLEST", "geometry"}
    if missing := required_columns - set(gdf.columns):
        raise ValueError(f"Shapefile missing required columns: {missing}")
    
    # Normalize country names (strip + lowercase)
    gdf["COUNTRY_NORM"] = gdf["COUNTRY"].str.strip().str.lower()
    countries_norm = [c.strip().lower() for c in countries]
    
    # Debug: Print unique country names in shapefile
    print("Countries in shapefile:", gdf["COUNTRY"].unique())
    
    # Exact match (avoids "Sudan" matching "South Sudan")
    gdf = gdf[gdf["COUNTRY_NORM"].isin(countries_norm)]
    
    # Ensure SMALLEST is not NA
    gdf = gdf[gdf["SMALLEST"].notna()]
    
    if gdf.empty:
        raise ValueError(
            f"No features found after filtering. "
            f"Requested: {countries} | "
            f"Available: {gdf['COUNTRY'].unique()}"
        )
    
    # Check for duplicate SMALLEST values before dissolving
    duplicate_check = gdf.groupby(['SMALLEST']).size()
    duplicates = duplicate_check[duplicate_check > 1]
    if len(duplicates) > 0:
        print(f"Found {len(duplicates)} SMALLEST values with multiple geometries:")
        print(f"Total duplicate geometries: {duplicates.sum() - len(duplicates)}")
        print("Sample duplicates:")
        for smallest_val in list(duplicates.index)[:5]:  # Show first 5
            count = duplicates[smallest_val]
            country = gdf[gdf['SMALLEST'] == smallest_val]['COUNTRY'].iloc[0]
            print(f"  SMALLEST {smallest_val} ({country}): {count} geometries")
        
        print("Dissolving geometries by SMALLEST...")
        
        # Dissolve by SMALLEST - this will union all geometries for each SMALLEST
        # aggfunc='first' only applies to non-geometry columns (like COUNTRY)
        # The geometry column is automatically unioned/merged
        gdf_dissolved = gdf.dissolve(by='SMALLEST', aggfunc='first').reset_index()
        
        # Verify that geometries were actually unioned
        print("Verifying geometry union...")
        for smallest_val in list(duplicates.index)[:3]:  # Check first 3 examples
            original_geoms = gdf[gdf['SMALLEST'] == smallest_val]['geometry']
            dissolved_geom = gdf_dissolved[gdf_dissolved['SMALLEST'] == smallest_val]['geometry'].iloc[0]
            
            original_area = original_geoms.area.sum()
            dissolved_area = dissolved_geom.area
            
            print(f"  SMALLEST {smallest_val}: {len(original_geoms)} parts → 1 part")
            print(f"    Original total area: {original_area:.6f}")
            print(f"    Dissolved area: {dissolved_area:.6f}")
            print(f"    Area preserved: {abs(original_area - dissolved_area) < 1e-10}")  # Account for floating point precision
        
        print(f"Before dissolve: {len(gdf)} features")
        print(f"After dissolve: {len(gdf_dissolved)} features")
        print(f"Reduction: {len(gdf) - len(gdf_dissolved)} features")
        
        # Verify no duplicates remain
        final_check = gdf_dissolved.groupby(['SMALLEST']).size()
        remaining_duplicates = final_check[final_check > 1]
        if len(remaining_duplicates) > 0:
            raise ValueError(f"Dissolve failed: {len(remaining_duplicates)} duplicates remain")
        else:
            print("✓ Dissolve successful: No duplicate SMALLEST values remain")
        
        gdf = gdf_dissolved
    else:
        print("✓ No duplicate SMALLEST values found")
    
    # Clean up temporary column
    gdf = gdf.drop(columns=['COUNTRY_NORM'], errors='ignore')
    
    return gdf

def collapse_phenology_years(data):
    """
    Convert three-year phenology data (-36 to 72) to single-year month values (1-12).
    
    Parameters:
    data : numpy.ndarray
        Input array containing phenology values ranging from -36 to 72,
        with special flags (251, 254)
        
    Returns:
    numpy.ndarray
        Array with values converted to month numbers (1-12) or original flags
    """
    # Create output array with same shape as input
    result = np.full_like(data, -9999)  # Initialize with -9999 for unhandled cases
    
    # Handle special flags first (keep them unchanged)
    result[data == 251] = 251  # no seasons/no season 2
    result[data == 254] = 254  # no cropland/no grassland
    
    # Handle valid phenology values (-36 to 72)
    mask = (data >= -36) & (data <= 72)
    
    # Convert to month number (1-12)
    # Since every 3 units = 1 month, we can:
    # 1. Add 36 to shift range to 0-108
    # 2. Mod by 36 to get position in 3-year cycle (0-35)
    # 3. Divide by 3 to get month number (0-11)
    # 4. Mod by 12 to wrap around to single year (0-11)
    # 5. Add 1 to get 1-12
    result[mask] = ((data[mask] + 36) % 36 // 3) % 12 + 1
    
    return result

def plot_value_distribution(joined_data, smallest, plot_output_dir, filename):
    """
    Plot the distribution of 'value' for a given 'SMALLEST' with data quality notes.
    
    Args:
        joined_data: DataFrame containing the data to plot
        smallest: Name of the administrative boundary to plot
        plot_output_dir: Directory to save the plot
        filename: Either a string filename or a set of filenames used to extract info
    """
    # Handle the filename input (could be string or set)
    if isinstance(filename, set):
        if len(filename) == 1:
            # If set contains exactly one filename, use it
            filename_str = next(iter(filename))
        else:
            # If multiple filenames, create a combined description
            filename_str = "_".join(sorted(filename))
            print(f"Warning: Multiple filenames provided, using combined description: {filename_str}")
    else:
        filename_str = str(filename)  # Convert to string if not already

    # Get descriptive suffix from filename
    descriptive_suffix = "data"  # default value
    try:
        descriptive_suffix = get_descriptive_suffix(filename_str)
    except (ValueError, AttributeError) as e:
        print(f"Warning: Could not parse filename '{filename_str}'. Using default naming. Error: {e}")

    # Rest of the function remains the same...
    if smallest.endswith('_ALL'):
        country = smallest.replace('_ALL', '')
        bpl_data = joined_data[joined_data['COUNTRY'] == country]
    else:
        bpl_data = joined_data[joined_data['SMALLEST'] == smallest]
    
    if len(bpl_data) == 0:
        print(f"No data found for SMALLEST: {smallest}")
        return
    
    country_name = bpl_data['COUNTRY'].iloc[0] if 'COUNTRY' in bpl_data.columns else "Unknown Country"
    
    valid_pixels = bpl_data[~bpl_data['value'].isin([251, 254])]
    flagged_pixels = bpl_data[bpl_data['value'].isin([251, 254])]
    flag_counts = flagged_pixels['value'].value_counts()
    
    plt.figure(figsize=(10, 6))
    
    if len(valid_pixels) == 0:
        plt.text(0.5, 0.5, 
                "No valid pixels found for this administrative subunit.\n"
                f"Flag counts:\n"
                f"251 (no seasons): {flag_counts.get(251, 0)}\n"
                f"254 (no cropland): {flag_counts.get(254, 0)}",
                ha='center', va='center', fontsize=12)
        plt.axis('off')
        title_suffix = " - NO VALID DATA"
    else:
        value_counts = valid_pixels['value'].value_counts(normalize=True).sort_index()
        ax = sns.barplot(x=value_counts.index, y=value_counts.values, color='skyblue')
        plt.xticks(range(12), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        plt.ylim(0, 1)
        plt.xlabel('Month')
        plt.ylabel('Share of pixels')
        plt.annotate(
            f"Country: {country_name}\n"
            f"Valid pixels: {len(valid_pixels)}\n"
            f"Flagged pixels: {len(flagged_pixels)}\n"
            f"(251: {flag_counts.get(251, 0)}, 254: {flag_counts.get(254, 0)})",
            xy=(0.98, 0.98), xycoords='axes fraction',
            ha='right', va='top', bbox=dict(boxstyle='round', alpha=0.2))
        title_suffix = ""
    
    plt.title(f'Distribution of Growing Season Months for {smallest}\n({descriptive_suffix}){title_suffix}')
    plot_filename = os.path.join(plot_output_dir, f'gs_months_{country_name}_{smallest}_{descriptive_suffix}.png')
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved plot to {plot_filename}.")
        
def get_descriptive_suffix(filename):
    """Convert filename components to descriptive suffixes"""
    parts = filename.split('.')
    if len(parts) < 6:
        raise ValueError(f"Invalid filename format: {filename}")
    
    # Mapping dictionary for component replacements
    component_map = {
        'MOS': 'max_of_season',
        'SOS': 'start_of_season',
        'EOS': 'end_of_season',
        'LC-C': 'cropland',
        'LC-G': 'grassland',
        'GS1': 'growing_season_1',
        'GS2': 'growing_season_2'
    }
    
    # Get components
    metric = component_map.get(parts[3], parts[3])
    land_cover = component_map.get(parts[4], parts[4])
    season = component_map.get(parts[2], parts[2])
    
    return f"{season}_{metric}_{land_cover}"


def process_raster_file(raster_path, shapefile, output_dir=None, plot_output_dir=None, filename=None, generate_plots=True):
    """Process a single raster file, plot value distributions, and return summary stats."""
    with rasterio.open(raster_path) as src:
        shapefile = shapefile.to_crs(src.crs)
        print("Countries in filtered shapefile:", shapefile["COUNTRY"].unique())

        print("Calculating union of geometries for masking...")
        geometry = shapefile.geometry.union_all()
        out_image, out_transform = mask(src, [geometry], crop=True, nodata=np.nan)
        band_data = out_image[0]
        
        print("Extract valid pixels")
        rows, cols = np.where(~np.isnan(band_data))
        pixel_values = band_data[rows, cols]
        
        print("Applying phenology year collapse...")
        pixel_values = collapse_phenology_years(pixel_values)
        
        print("Creating points GeoDataFrame")
        xs, ys = rasterio.transform.xy(out_transform, rows, cols)
        geometry = [Point(x, y) for x, y in zip(xs, ys)]
        points_gdf = gpd.GeoDataFrame({"value": pixel_values, "geometry": geometry}, crs=shapefile.crs)
        
        print("Classifying points into valid and flagged")
        flagged_points = points_gdf[points_gdf["value"].isin([251, 254])]
        valid_points = points_gdf[~points_gdf["value"].isin([251, 254])]
        
        print("Spatial joins - KEEP COUNTRY COLUMN")
        all_joined = gpd.sjoin(points_gdf, shapefile[['SMALLEST', 'COUNTRY', 'geometry']], how="inner", predicate="within")
        valid_joined = gpd.sjoin(valid_points, shapefile[['SMALLEST', 'COUNTRY', 'geometry']], how="inner", predicate="within")
        flagged_joined = gpd.sjoin(flagged_points, shapefile[['SMALLEST', 'COUNTRY', 'geometry']], how="inner", predicate="within")

        if not all_joined.empty:
            unexpected_countries = set(all_joined["COUNTRY"]) - set(shapefile["COUNTRY"])
            if unexpected_countries:
                raise ValueError(f"Unexpected countries in results: {unexpected_countries}")
        
        # Plot distribution of 'value' for each SMALLEST
        if generate_plots and plot_output_dir:
            os.makedirs(plot_output_dir, exist_ok=True)
            for country_name in all_joined['COUNTRY'].dropna().unique():
                plot_value_distribution(all_joined, f"{country_name}_ALL", plot_output_dir, filename)
            for smallest in all_joined['SMALLEST'].unique():
                plot_value_distribution(all_joined, smallest, output_dir, filename)
        
        # ========================================================================
        # FIXED SECTION: Start with all SMALLEST units that have ANY points
        # ========================================================================
        
        print("Creating base dataframe with all SMALLEST units that have any points (valid OR flagged)")
        if not all_joined.empty:
            base_stats = all_joined.groupby(["SMALLEST", "COUNTRY"]).size().reset_index(name="total_points_temp")
            base_stats = base_stats[["SMALLEST", "COUNTRY"]]  # Keep only ID columns
        else:
            # If no points at all, return empty results but preserve structure
            return pd.DataFrame(columns=["SMALLEST", "COUNTRY", "mean", "median", "std", "min", "max", 
                                       "mode", "valid_count", "flag_count", "total_pixels", "valid_share", "flag_share"])
        
        print("Calculating statistics for valid points only")
        if len(valid_joined) > 0:
            valid_stats = valid_joined.groupby(["SMALLEST", "COUNTRY"])["value"].agg([
                ("mean", "mean"),
                ("median", "median"),
                ("std", "std"),
                ("min", "min"),
                ("max", "max"),
                ("mode", lambda x: x.mode()[0] if len(x.mode()) > 0 else np.nan),
                ("valid_count", "count")
            ]).reset_index()
        else:
            # No valid points - create empty stats with proper structure
            valid_stats = pd.DataFrame(columns=["SMALLEST", "COUNTRY", "mean", "median", "std", 
                                               "min", "max", "mode", "valid_count"])
        
        print("Calculating flag counts")
        if len(flagged_joined) > 0:
            flagged_counts = flagged_joined.groupby(["SMALLEST", "COUNTRY"]).size().reset_index(name="flag_count")
        else:
            # No flagged points - create empty flag counts
            flagged_counts = pd.DataFrame(columns=["SMALLEST", "COUNTRY", "flag_count"])
        
        # ========================================================================
        # MERGE EVERYTHING BACK TO BASE - PRESERVING ALL SMALLEST UNITS
        # ========================================================================
        
        # Start with base (all units with any points)
        stats = base_stats.copy()
        
        # Left join with valid stats (preserves units with no valid points)
        if not valid_stats.empty:
            stats = pd.merge(stats, valid_stats, on=["SMALLEST", "COUNTRY"], how="left")
        else:
            # Add empty columns if no valid stats
            for col in ["mean", "median", "std", "min", "max", "mode", "valid_count"]:
                stats[col] = np.nan
        
        # Left join with flag counts (preserves units with no flagged points)
        if not flagged_counts.empty:
            stats = pd.merge(stats, flagged_counts, on=["SMALLEST", "COUNTRY"], how="left")
        else:
            stats["flag_count"] = 0
        
        # Fill missing values
        stats["valid_count"] = stats["valid_count"].fillna(0).astype(int)
        stats["flag_count"] = stats["flag_count"].fillna(0).astype(int)
        
        # Calculate derived metrics
        stats["total_pixels"] = stats["valid_count"] + stats["flag_count"]
        stats["valid_share"] = np.where(stats["total_pixels"] > 0, 
                                       stats["valid_count"] / stats["total_pixels"], 0)
        stats["flag_share"] = np.where(stats["total_pixels"] > 0,
                                      stats["flag_count"] / stats["total_pixels"], 0)
        
        # ========================================================================
        # VERIFICATION: Check that we preserved units correctly
        # ========================================================================
        
        # Units with only valid points should have flag_count = 0
        only_valid = set(valid_joined['SMALLEST'].unique()) - set(flagged_joined['SMALLEST'].unique()) if len(flagged_joined) > 0 else set(valid_joined['SMALLEST'].unique())
        
        # Units with only flagged points should have valid_count = 0
        only_flagged = set(flagged_joined['SMALLEST'].unique()) - set(valid_joined['SMALLEST'].unique()) if len(valid_joined) > 0 else set(flagged_joined['SMALLEST'].unique())
        
        # Units with both types
        both_types = set(valid_joined['SMALLEST'].unique()) & set(flagged_joined['SMALLEST'].unique())
        
        expected_total = len(only_valid) + len(only_flagged) + len(both_types)
        print(f"Units with only valid points: {len(only_valid)}")
        print(f"Units with only flagged points: {len(only_flagged)}")
        print(f"Units with both valid and flagged points: {len(both_types)}")
        print(f"Sum of above three categories: {expected_total}")
        print(f"Actual units in final results: {len(stats)}")
        if len(stats) == expected_total:
            print("✓ Verification passed: All units accounted for")
        else:
            print(f"⚠ Warning: Mismatch detected! Lost {expected_total - len(stats)} units")
        
        return stats
        
    
def create_growing_season_indicators(sos_df, eos_df, statistic='mode'):
    """
    Create monthly growing season indicators based on SOS and EOS for GS1 and GS2.

    Parameters:
    sos_df : DataFrame
        SOS statistics for multiple seasons
    eos_df : DataFrame
        EOS statistics for multiple seasons
    statistic : str
        Which statistic to use ('median', 'mode', or 'mean')

    Returns:
    DataFrame
        With columns 'growing_month_1' through 'growing_month_12' (1 if growing in that month)
    """
    valid_statistics = ['median', 'mode', 'mean']
    if statistic not in valid_statistics:
        raise ValueError(f"statistic must be one of {valid_statistics}")

    # Identify relevant SOS/EOS columns
    def find_column(df, key):
        for col in df.columns:
            if statistic in col and key in col:
                return col
        return None

    # Find GS1 and GS2 SOS/EOS columns
    gs1_sos_col = find_column(sos_df, 'growing_season_1_start_of_season')
    gs1_eos_col = find_column(eos_df, 'growing_season_1_end_of_season')
    gs2_sos_col = find_column(sos_df, 'growing_season_2_start_of_season')
    gs2_eos_col = find_column(eos_df, 'growing_season_2_end_of_season')

    # Validate presence
    if not gs1_sos_col or not gs1_eos_col:
        raise ValueError(f"Missing GS1 SOS or EOS {statistic} columns.")
    if not gs2_sos_col or not gs2_eos_col:
        raise ValueError(f"Missing GS2 SOS or EOS {statistic} columns.")

    print(f"Using columns:\n  GS1: {gs1_sos_col}, {gs1_eos_col}\n  GS2: {gs2_sos_col}, {gs2_eos_col}")

    # Merge both growing seasons
    merged = sos_df[['SMALLEST', 'COUNTRY', gs1_sos_col, gs2_sos_col]].rename(
        columns={gs1_sos_col: 'gs1_sos', gs2_sos_col: 'gs2_sos'})
    merged = pd.merge(
        merged,
        eos_df[['SMALLEST', 'COUNTRY', gs1_eos_col, gs2_eos_col]].rename(
            columns={gs1_eos_col: 'gs1_eos', gs2_eos_col: 'gs2_eos'}),
        on=['SMALLEST', 'COUNTRY'],
        how='outer'
    )

    # Add data quality flags
    merged['has_gs1'] = merged['gs1_sos'].between(1, 12) & merged['gs1_eos'].between(1, 12)
    merged['has_gs2'] = merged['gs2_sos'].between(1, 12) & merged['gs2_eos'].between(1, 12)
    
    # Initialize all month columns
    for month in range(1, 13):
        merged[f'growing_month_{month}'] = 0
    
    # Calculate growing months only where valid data exists
    for idx, row in merged.iterrows():
        if row['has_gs1']:
            gs1_sos = int(row['gs1_sos'])
            gs1_eos = int(row['gs1_eos'])
            
            if gs1_sos <= gs1_eos:
                months = range(gs1_sos, gs1_eos + 1)
            else:
                months = list(range(gs1_sos, 13)) + list(range(1, gs1_eos + 1))
            
            for month in months:
                merged.at[idx, f'growing_month_{month}'] = 1
        
        if row['has_gs2']:
            gs2_sos = int(row['gs2_sos'])
            gs2_eos = int(row['gs2_eos'])
            
            if gs2_sos <= gs2_eos:
                months = range(gs2_sos, gs2_eos + 1)
            else:
                months = list(range(gs2_sos, 13)) + list(range(1, gs2_eos + 1))
            
            for month in months:
                merged.at[idx, f'growing_month_{month}'] = 1

    # Add data quality columns to final output
    merged['valid_gs1'] = merged['has_gs1'].astype(int)
    merged['valid_gs2'] = merged['has_gs2'].astype(int)
    
    print("Value ranges before assertion:")
    print(f"GS1 SOS: {merged['gs1_sos'].min()} - {merged['gs1_sos'].max()}")
    print(f"GS1 EOS: {merged['gs1_eos'].min()} - {merged['gs1_eos'].max()}")
    print(f"GS2 SOS: {merged['gs2_sos'].min()} - {merged['gs2_sos'].max()}")
    print(f"GS2 EOS: {merged['gs2_eos'].min()} - {merged['gs2_eos'].max()}")

    # Show rows with invalid values
    invalid_sos = merged[~merged['gs2_sos'].between(1,12)]
    print("\nRows with invalid GS2 SOS values:")
    print(invalid_sos[['SMALLEST','COUNTRY','gs2_sos']])

    def clean_month_values(df):
        """Ensure all month values are between 1-12 or NaN"""
        month_cols = ['gs1_sos', 'gs1_eos', 'gs2_sos', 'gs2_eos']
        
        for col in month_cols:
            # Convert values outside 1-12 to NaN
            df[col] = df[col].where(df[col].between(1, 12), np.nan)
            
            # Add flag column indicating validity
            df[f'{col}_valid'] = df[col].notna().astype(int)
    
        return df

    # Apply cleaning before creating indicators
    merged = clean_month_values(merged)

    # Optional: check lengths of each season
    gs1_len = np.where(merged['gs1_sos'] <= merged['gs1_eos'],
                       merged['gs1_eos'] - merged['gs1_sos'] + 1,
                       12 - merged['gs1_sos'] + merged['gs1_eos'] + 1)
    gs2_len = np.where(merged['gs2_sos'] <= merged['gs2_eos'],
                       merged['gs2_eos'] - merged['gs2_sos'] + 1,
                       12 - merged['gs2_sos'] + merged['gs2_eos'] + 1)

    print(f"GS1 season length range: {gs1_len.min()}–{gs1_len.max()} months")
    print(f"GS2 season length range: {gs2_len.min()}–{gs2_len.max()} months")

    return merged

def process_all_phenology_files(shapefile_path, phenology_dir, countries, overleaf_dir, generate_plots = True):
    """Master function to process all files with descriptive column names"""
    print(f"\n{'='*50}\nStarting batch processing")
    print(f"Countries: {countries}")
    print(f"Phenology directory: {phenology_dir}")

    try:
        # Load shapefile
        shapefile = load_shapefile(shapefile_path, countries)
        if shapefile is None or len(shapefile) == 0:
            raise ValueError("No features found in shapefile after filtering")

        # Initialize separate dataframes for SOS and EOS cropland data
        sos_df = None
        eos_df = None
        processed_files = 0
        failed_files = 0

        # Get list of files, processing only cropland (LC-C) files
        file_list = sorted([f for f in os.listdir(phenology_dir) 
                            if f.startswith("ASIS.PHE") and f.endswith(".tif") and "LC-C" in f])

        print(f"\nFound {len(file_list)} raster files to process")

        for filename in file_list:
            raster_path = os.path.join(phenology_dir, filename)

            try:
                file_suffix = get_descriptive_suffix(filename)
            
            except ValueError as ve:
                print(f"Skipping invalid filename: {filename} | Reason: {ve}")
                continue

            # Use the suffix to determine if it's SOS or EOS
            if "start_of_season" in file_suffix:
                target_df = "sos"
            elif "end_of_season" in file_suffix:
                target_df = "eos"
            else:
                continue  # Only process SOS/EOS for now

            # Define plot directory for current file
            plot_dir = os.path.join(overleaf_dir, file_suffix) if (generate_plots and overleaf_dir) else None
                
            if generate_plots and plot_dir:
                os.makedirs(plot_dir, exist_ok=True)

            # Process file with output_dir passed in
            print(f"\nProcessing {filename} as {file_suffix}...")
            file_stats = process_raster_file(raster_path, shapefile, output_dir=plot_dir, plot_output_dir=plot_dir, filename={filename}, generate_plots = generate_plots)

            if file_stats is None:
                failed_files += 1
                continue

            # Add to appropriate dataframe with descriptive columns
            file_stats = file_stats.set_index(["SMALLEST", "COUNTRY"])
            file_stats.columns = [f"{col}_{file_suffix}" for col in file_stats.columns]

            if target_df == "sos":
                if sos_df is None:
                    sos_df = file_stats
                else:
                    sos_df = sos_df.join(file_stats, how='outer')
            else:
                if eos_df is None:
                    eos_df = file_stats
                else:
                    eos_df = eos_df.join(file_stats, how='outer')

            processed_files += 1

            # except Exception as e:
            #     failed_files += 1
            #     print(f"\nERROR in file processing pipeline for {filename}: {str(e)}")
            #     print(traceback.format_exc())
            #     continue

        # Check we have both SOS and EOS data
        if sos_df is None or eos_df is None:
            raise ValueError("Missing either SOS or EOS data - cannot create growing season indicators")

        # Create growing season indicators
        print("\nCreating growing season indicators...")
        growing_season_df = create_growing_season_indicators(
            sos_df.reset_index(),
            eos_df.reset_index(),
            statistic='mode'
        )

        # Merge with original statistics
        final_df = pd.merge(
            sos_df.reset_index(),
            eos_df.reset_index(),
            on=['SMALLEST', 'COUNTRY'],
            how='inner'
        )
        final_df = pd.merge(
            final_df,
            growing_season_df,
            on=['SMALLEST', 'COUNTRY'],
            how='left'
        )

        print(f"\n{'='*50}\nProcessing complete!")
        print(f"Successfully processed {processed_files} files")
        print(f"Failed to process {failed_files} files")
        print(f"Final output shape: {final_df.shape}")

        return final_df

    except Exception as e:
        print(f"\nFATAL ERROR in batch processing: {str(e)}")
        print(traceback.format_exc())
        raise

def generate_latex_growing_season_table(df, output_path):
    """
    Generate a LaTeX table showing the share of administrative units 
    with growing season in each month for all countries.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing COUNTRY, SMALLEST, and growing_month_1 through growing_month_12 columns
    output_path : str
        Path where the LaTeX table file should be saved
    
    Returns:
    --------
    str : Path to the saved LaTeX file
    """
    print("\n" + "="*60)
    print("Generating LaTeX table for growing season data...")
    
    # Get the list of growing_month columns
    growing_month_cols = [f'growing_month_{i}' for i in range(1, 13)]
    
    # Verify all columns exist
    missing_cols = [col for col in growing_month_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing columns: {missing_cols}")
        return None
    
    # Get unique countries (sorted)
    countries = sorted(df['COUNTRY'].unique())
    print(f"Found {len(countries)} countries to include in table")
    
    # Month names for header
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    # Build LaTeX table
    latex = []
    # latex.append("\\begin{table}[htbp]")
    # latex.append("\\centering")
    # latex.append("\\caption{Share of Administrative Units with Growing Season by Month}")
    # latex.append("\\label{tab:growing_season_all_countries}")
    
    # Table structure: 13 columns (Country + 12 months)
    latex.append("\\begin{tabular}{l" + "c" * 12 + "}")
    latex.append("\\hline")
    
    # Header row
    header = "Country & " + " & ".join(month_names) + " \\\\"
    latex.append(header)
    latex.append(" & (1) & (2) & (3) & (4) & (5) & (6) & (7) & (8) & (9) & (10) & (11) & (12) \\\\")
    latex.append("\\hline")
    
    # Data rows for each country
    for country in countries:
        country_data = df[df['COUNTRY'] == country]
        total_units = len(country_data)
        
        # Calculate share for each month
        shares = []
        for month in range(1, 13):
            col_name = f'growing_month_{month}'
            if col_name in country_data.columns:
                count_with_season = (country_data[col_name] == 1).sum()
                share = count_with_season / total_units if total_units > 0 else 0
                shares.append(share)
            else:
                shares.append(0)
        
        # Format data row
        shares_str = " & ".join([f"{share:.3f}" for share in shares])
        data_row = f"{country} & {shares_str} \\\\"
        latex.append(data_row)
        print(f"  Added row for {country} ({total_units} units)")
    
    latex.append("\\hline")
    latex.append("\\end{tabular}")
    # latex.append("\\end{table}")
    
    # Write to file
    latex_content = "\n".join(latex)
    with open(output_path, 'w') as f:
        f.write(latex_content)
    
    print(f"\nLaTeX table saved to: {output_path}")
    print(f"Table contains {len(countries)} countries")
    print("="*60 + "\n")
    
    return output_path

if __name__ == "__main__":
    # Configuration
    shapefile_path = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data/derived/shapefiles/cleaned_ssa_boundaries.shp"
    phenology_dir = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data/source/fao/crop-phenology"
    overleaf_dir = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/figures/gs-plots"
    countries = ["Kenya", "Benin", "Botswana", "Burkina Faso", 
         "Cameroon", "Ivory Coast", "Ethiopia", "Ghana", "Guinea", "Liberia", 
         "Lesotho", "Malawi", "Mali", "Mozambique", "Rwanda", 
         "Senegal", "Sierra Leone", "South Africa", "South Sudan", "Sudan", "Togo", "Tanzania", "Uganda", "Zambia", "Zimbabwe"]  # Can add multiple countries
    
    # Process files
    final_results = process_all_phenology_files(shapefile_path, phenology_dir, countries, overleaf_dir=overleaf_dir, generate_plots = False)
    
    # Save and display results
    output_path = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv"
    final_results.to_csv(output_path, index=False)
    
    print("\nProcessing complete!")
    print(f"Results saved to: {output_path}")
    print("\nSample output:")
    print(final_results.head())

    # Generate LaTeX table
    latex_output_path = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output/table/growing-season-table.tex"
    generate_latex_growing_season_table(final_results, latex_output_path)