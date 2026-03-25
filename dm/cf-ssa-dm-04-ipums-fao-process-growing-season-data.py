# --------------------------------------------------------
# PROJECT :         Climate Fertility
# PURPOSE:          Process growing season data (NEW STRATEGY)
# AUTHOR:           Prayog Bhattarai
# DATE MODIFIED:    September 2024
# DESCRIPTION:      Uses FAO SOS/EOS data to create indicators for growing season months
#                   Implements new strategy that preserves cross-year growing seasons
# Notes:            Ensure required packages: rasterio, geopandas, pandas, numpy, matplotlib, seaborn
# Output:           Climate_Change_and_Fertility_in_SSA/data/derived/fao/crop-phenology/crop-phenology-new-strategy.csv
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
        for smallest_val in list(duplicates.index)[:5]:
            count = duplicates[smallest_val]
            country = gdf[gdf['SMALLEST'] == smallest_val]['COUNTRY'].iloc[0]
            print(f"  SMALLEST {smallest_val} ({country}): {count} geometries")
        
        print("Dissolving geometries by SMALLEST...")
        gdf_dissolved = gdf.dissolve(by='SMALLEST', aggfunc='first').reset_index()
        
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

def convert_phenology_to_calendar_months(sos_raw, eos_raw):
    """
    Convert raw SOS/EOS values to calendar months (1-12) for the complete growing season.
    Preserves cross-year seasons - e.g., Nov Y-1 to May Y gives [11, 12, 1, 2, 3, 4, 5]
    
    Parameters:
    -----------
    sos_raw : float
        Raw Start of Season value from FAO dataset (-36 to 72)
    eos_raw : float
        Raw End of Season value from FAO dataset (-36 to 72)
    
    Returns:
    --------
    list : Calendar months (1-12) that fall within this growing season, regardless of year
    """
    # Handle missing/invalid data
    if pd.isna(sos_raw) or pd.isna(eos_raw) or sos_raw in [251, 254] or eos_raw in [251, 254]:
        return []
    
    def get_month_from_phenology_value(value):
        """
        Convert a phenology value to a calendar month (1-12).
        The year information is discarded - we only care about the month.
        
        FAO scale: -36 to 72, where 3 units = 1 month
        -36 = October, -33 = November, -30 = December, -27 = January, etc.
        """
        # Calculate months from reference point (-36 = October)
        months_from_ref = (value + 36) / 3
        
        # Convert to absolute month number (0 = October)
        absolute_month = int(months_from_ref)
        
        # Map to calendar month numbers (1-12)
        # 0=Oct, 1=Nov, 2=Dec, 3=Jan, 4=Feb, 5=Mar, 6=Apr, 7=May, 8=Jun, 9=Jul, 10=Aug, 11=Sep
        month_mapping = {0: 10, 1: 11, 2: 12, 3: 1, 4: 2, 5: 3, 6: 4, 7: 5, 8: 6, 9: 7, 10: 8, 11: 9}
        
        calendar_month = month_mapping[absolute_month % 12]
        return calendar_month
    
    def get_all_months_between(start_value, end_value):
        """Get all months between start and end values (inclusive)."""
        start_month = get_month_from_phenology_value(start_value)
        end_month = get_month_from_phenology_value(end_value)
        
        months = []
        
        if start_month <= end_month:
            # Simple case: season within same "phenological year"
            months = list(range(start_month, end_month + 1))
        else:
            # Cross-year season: e.g., Nov to May
            months = list(range(start_month, 13)) + list(range(1, end_month + 1))
        
        return months
    
    # Get all months between SOS and EOS
    return get_all_months_between(sos_raw, eos_raw)

def process_raster_file_new_strategy(raster_path, shapefile, output_dir=None, plot_output_dir=None, filename=None, generate_plots=True):
    """Process raster file using the new SOS/EOS strategy."""
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
        
        print("Creating points GeoDataFrame")
        xs, ys = rasterio.transform.xy(out_transform, rows, cols)
        geometry = [Point(x, y) for x, y in zip(xs, ys)]
        points_gdf = gpd.GeoDataFrame({"value": pixel_values, "geometry": geometry}, crs=shapefile.crs)
        
        print("Classifying points into valid and flagged")
        flagged_points = points_gdf[points_gdf["value"].isin([251, 254])]
        valid_points = points_gdf[~points_gdf["value"].isin([251, 254])]
        
        print("Spatial joins")
        all_joined = gpd.sjoin(points_gdf, shapefile[['SMALLEST', 'COUNTRY', 'geometry']], how="inner", predicate="within")
        valid_joined = gpd.sjoin(valid_points, shapefile[['SMALLEST', 'COUNTRY', 'geometry']], how="inner", predicate="within")
        flagged_joined = gpd.sjoin(flagged_points, shapefile[['SMALLEST', 'COUNTRY', 'geometry']], how="inner", predicate="within")

        # Calculate modal values for each SMALLEST unit
        print("Calculating modal values for each administrative unit...")
        if not valid_joined.empty:
            # Calculate mode for valid pixels
            modal_values = valid_joined.groupby(['SMALLEST', 'COUNTRY'])['value'].agg(
                lambda x: x.mode()[0] if len(x.mode()) > 0 else np.nan
            ).reset_index()
            modal_values = modal_values.rename(columns={'value': 'modal_value'})
            
            # Calculate valid share for data quality
            total_counts = all_joined.groupby(['SMALLEST', 'COUNTRY']).size().reset_index(name='total_pixels')
            valid_counts = valid_joined.groupby(['SMALLEST', 'COUNTRY']).size().reset_index(name='valid_pixels')
            counts_merged = pd.merge(total_counts, valid_counts, on=['SMALLEST', 'COUNTRY'], how='left')
            counts_merged['valid_share'] = counts_merged['valid_pixels'] / counts_merged['total_pixels']
            
            modal_values = pd.merge(modal_values, counts_merged[['SMALLEST', 'COUNTRY', 'valid_share']], 
                                   on=['SMALLEST', 'COUNTRY'], how='left')
        else:
            modal_values = pd.DataFrame(columns=['SMALLEST', 'COUNTRY', 'modal_value', 'valid_share'])
        
        return modal_values

def create_growing_season_indicators_new(sos_modes, eos_modes, min_valid_share=0.05, require_both_sos_eos=True):
    """
    Create monthly growing season indicators using the new SOS/EOS strategy.
    Preserves cross-year growing seasons.
    
    Parameters:
    -----------
    sos_modes : DataFrame
        Modal SOS values for GS1 and GS2 with valid_share
    eos_modes : DataFrame
        Modal EOS values for GS1 and GS2 with valid_share
    min_valid_share : float
        Minimum valid share threshold (0.0 to 1.0)
    require_both_sos_eos : bool
        If True, requires both SOS and EOS to meet threshold for a season to be included
        If False, uses available data even if only one meets threshold
    
    Returns:
    --------
    DataFrame with growing season indicators
    """
    print(f"\nCreating growing season indicators with:")
    print(f"  - Minimum valid share: {min_valid_share:.1%}")
    print(f"  - Require both SOS and EOS: {require_both_sos_eos}")
    
    # Merge SOS and EOS data
    merged = pd.merge(
        sos_modes[['SMALLEST', 'COUNTRY', 'modal_value_growing_season_1_start_of_season_cropland', 
                  'modal_value_growing_season_2_start_of_season_cropland',
                  'valid_share_growing_season_1_start_of_season_cropland',
                  'valid_share_growing_season_2_start_of_season_cropland']].rename(
            columns={'modal_value_growing_season_1_start_of_season_cropland': 'gs1_sos_raw',
                    'modal_value_growing_season_2_start_of_season_cropland': 'gs2_sos_raw',
                    'valid_share_growing_season_1_start_of_season_cropland': 'gs1_sos_valid_share',
                    'valid_share_growing_season_2_start_of_season_cropland': 'gs2_sos_valid_share'}),
        eos_modes[['SMALLEST', 'COUNTRY', 'modal_value_growing_season_1_end_of_season_cropland',
                  'modal_value_growing_season_2_end_of_season_cropland',
                  'valid_share_growing_season_1_end_of_season_cropland',
                  'valid_share_growing_season_2_end_of_season_cropland']].rename(
            columns={'modal_value_growing_season_1_end_of_season_cropland': 'gs1_eos_raw',
                    'modal_value_growing_season_2_end_of_season_cropland': 'gs2_eos_raw',
                    'valid_share_growing_season_1_end_of_season_cropland': 'gs1_eos_valid_share',
                    'valid_share_growing_season_2_end_of_season_cropland': 'gs2_eos_valid_share'}),
        on=['SMALLEST', 'COUNTRY'],
        how='inner'
    )
    
    # Convert raw values to calendar months for each growing season
    growing_seasons = []
    
    # Track data quality statistics
    quality_stats = {
        'total_units': len(merged),
        'gs1_passed': 0,
        'gs2_passed': 0,
        'gs1_failed_share': 0,
        'gs2_failed_share': 0,
        'gs1_failed_missing': 0,
        'gs2_failed_missing': 0
    }
    
    for idx, row in merged.iterrows():
        # Check data quality based on configuration
        if require_both_sos_eos:
            # Require both SOS and EOS to meet threshold
            gs1_has_data = (row['gs1_sos_valid_share'] >= min_valid_share and 
                            row['gs1_eos_valid_share'] >= min_valid_share and
                            not pd.isna(row['gs1_sos_raw']) and not pd.isna(row['gs1_eos_raw']))
            
            gs2_has_data = (row['gs2_sos_valid_share'] >= min_valid_share and 
                            row['gs2_eos_valid_share'] >= min_valid_share and
                            not pd.isna(row['gs2_sos_raw']) and not pd.isna(row['gs2_eos_raw']))
        else:
            # Use available data even if only one meets threshold
            gs1_has_data = ((row['gs1_sos_valid_share'] >= min_valid_share or 
                             row['gs1_eos_valid_share'] >= min_valid_share) and
                            (not pd.isna(row['gs1_sos_raw']) or not pd.isna(row['gs1_eos_raw'])))
            
            gs2_has_data = ((row['gs2_sos_valid_share'] >= min_valid_share or 
                             row['gs2_eos_valid_share'] >= min_valid_share) and
                            (not pd.isna(row['gs2_sos_raw']) or not pd.isna(row['gs2_eos_raw'])))
        
        # Update quality statistics
        if gs1_has_data:
            quality_stats['gs1_passed'] += 1
        else:
            if pd.isna(row['gs1_sos_raw']) or pd.isna(row['gs1_eos_raw']):
                quality_stats['gs1_failed_missing'] += 1
            else:
                quality_stats['gs1_failed_share'] += 1
                
        if gs2_has_data:
            quality_stats['gs2_passed'] += 1
        else:
            if pd.isna(row['gs2_sos_raw']) or pd.isna(row['gs2_eos_raw']):
                quality_stats['gs2_failed_missing'] += 1
            else:
                quality_stats['gs2_failed_share'] += 1
        
        # Process GS1 if data quality is sufficient
        gs1_months = []
        if gs1_has_data:
            gs1_months = convert_phenology_to_calendar_months(row['gs1_sos_raw'], row['gs1_eos_raw'])
        
        # Process GS2 if data quality is sufficient
        gs2_months = []
        if gs2_has_data:
            gs2_months = convert_phenology_to_calendar_months(row['gs2_sos_raw'], row['gs2_eos_raw'])
        
        # Combine months from both seasons
        all_growing_months = sorted(set(gs1_months + gs2_months))
        
        # Create binary indicators for each month
        month_indicators = {}
        for month in range(1, 13):
            month_indicators[f'growing_month_{month}'] = 1 if month in all_growing_months else 0
        
        growing_seasons.append({
            'SMALLEST': row['SMALLEST'],
            'COUNTRY': row['COUNTRY'],
            'gs1_sos_raw': row['gs1_sos_raw'],
            'gs1_eos_raw': row['gs1_eos_raw'],
            'gs2_sos_raw': row['gs2_sos_raw'],
            'gs2_eos_raw': row['gs2_eos_raw'],
            'gs1_sos_valid_share': row['gs1_sos_valid_share'],
            'gs1_eos_valid_share': row['gs1_eos_valid_share'],
            'gs2_sos_valid_share': row['gs2_sos_valid_share'],
            'gs2_eos_valid_share': row['gs2_eos_valid_share'],
            'gs1_months': gs1_months,
            'gs2_months': gs2_months, 
            'all_growing_months': all_growing_months,
            'gs1_has_data': int(gs1_has_data),
            'gs2_has_data': int(gs2_has_data),
            **month_indicators
        })
    
    # Print quality report
    print(f"\nData Quality Report:")
    print(f"  Total administrative units: {quality_stats['total_units']}")
    print(f"  GS1 passed threshold: {quality_stats['gs1_passed']} ({quality_stats['gs1_passed']/quality_stats['total_units']*100:.1f}%)")
    print(f"  GS2 passed threshold: {quality_stats['gs2_passed']} ({quality_stats['gs2_passed']/quality_stats['total_units']*100:.1f}%)")
    print(f"  GS1 failed - insufficient valid share: {quality_stats['gs1_failed_share']}")
    print(f"  GS2 failed - insufficient valid share: {quality_stats['gs2_failed_share']}")
    print(f"  GS1 failed - missing data: {quality_stats['gs1_failed_missing']}")
    print(f"  GS2 failed - missing data: {quality_stats['gs2_failed_missing']}")
    
    return pd.DataFrame(growing_seasons)

def process_all_phenology_files_new_strategy(shapefile_path, phenology_dir, countries, overleaf_dir, 
                                           generate_plots=True, min_valid_share=0.10, require_both_sos_eos=True):
    """Master function using the new SOS/EOS strategy."""
    print(f"\n{'='*50}\nStarting batch processing with NEW STRATEGY")
    print(f"Countries: {countries}")
    print(f"Configuration:")
    print(f"  - Minimum valid share: {min_valid_share:.1%}")
    print(f"  - Require both SOS and EOS: {require_both_sos_eos}")

    try:
        # Load shapefile
        shapefile = load_shapefile(shapefile_path, countries)
        if shapefile is None or len(shapefile) == 0:
            raise ValueError("No features found in shapefile after filtering")

        # Initialize data collectors
        sos_modes = None
        eos_modes = None
        processed_files = 0

        # Process files - only cropland (LC-C) files
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

            # Determine if SOS or EOS
            if "start_of_season" in file_suffix:
                target_df = "sos"
            elif "end_of_season" in file_suffix:
                target_df = "eos" 
            else:
                continue

            print(f"\nProcessing {filename} as {file_suffix}...")
            modal_values = process_raster_file_new_strategy(
                raster_path, shapefile, filename=filename, generate_plots=generate_plots
            )

            if modal_values is not None and not modal_values.empty:
                # Rename columns with descriptive suffix
                modal_values = modal_values.rename(
                    columns={'modal_value': f'modal_value_{file_suffix}',
                            'valid_share': f'valid_share_{file_suffix}'}
                )
                
                if target_df == "sos":
                    if sos_modes is None:
                        sos_modes = modal_values
                    else:
                        sos_modes = pd.merge(sos_modes, modal_values, on=['SMALLEST', 'COUNTRY'], how='outer')
                else:
                    if eos_modes is None:
                        eos_modes = modal_values
                    else:
                        eos_modes = pd.merge(eos_modes, modal_values, on=['SMALLEST', 'COUNTRY'], how='outer')
                
                processed_files += 1
                print(f"✓ Successfully processed {filename}")

        print(f"\nProcessed {processed_files} files successfully")
        
        # Check if we have both SOS and EOS data
        if sos_modes is None or eos_modes is None:
            raise ValueError("Missing either SOS or EOS data - cannot create growing season indicators")
        
        print("SOS columns:", sos_modes.columns.tolist())
        print("EOS columns:", eos_modes.columns.tolist())

        # Create final growing season indicators
        print("\nCreating growing season indicators...")
        growing_season_df = create_growing_season_indicators_new(
            sos_modes, eos_modes, 
            min_valid_share=min_valid_share,
            require_both_sos_eos=require_both_sos_eos
        )
        
        # Merge with original modal values for complete dataset
        final_df = pd.merge(sos_modes, eos_modes, on=['SMALLEST', 'COUNTRY'], how='inner')
        final_df = pd.merge(final_df, growing_season_df, on=['SMALLEST', 'COUNTRY'], how='left')

        print(f"\n{'='*50}\nProcessing complete!")
        print(f"Final output shape: {final_df.shape}")
        print(f"Sample of growing season data:")
        sample_cols = ['SMALLEST', 'COUNTRY', 'all_growing_months', 'gs1_has_data', 'gs2_has_data']
        if all(col in final_df.columns for col in sample_cols):
            print(final_df[sample_cols].head())
        
        return final_df

    except Exception as e:
        print(f"\nFATAL ERROR in batch processing: {str(e)}")
        print(traceback.format_exc())
        raise

def generate_latex_growing_season_table(df, output_path):
    """
    Generate a LaTeX table showing the share of administrative units 
    with growing season in each month for all countries.
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
    latex.append("\\begin{tabular}{l" + "c" * 12 + "}")
    latex.append("\\hline")
    
    # Header row
    header = "Country & " + " & ".join(month_names) + " \\\\"
    latex.append(header)
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
    
    # Write to file
    latex_content = "\n".join(latex)
    with open(output_path, 'w') as f:
        f.write(latex_content)
    
    print(f"\nLaTeX table saved to: {output_path}")
    print(f"Table contains {len(countries)} countries")
    print("="*60 + "\n")
    
    return output_path

if __name__ == "__main__":
    # Configuration - using the global BASE_DIR found by upward search
    parent_dir = os.path.dirname(BASE_DIR)
    OVERLEAF_DIR = os.path.join(parent_dir, "Apps/Overleaf/climate-fertility-ssa")
    shapefile_path = os.path.join(BASE_DIR, "data/derived/shapefiles/cleaned_ssa_boundaries.shp")
    phenology_dir = os.path.join(BASE_DIR, "data/source/fao/crop-phenology")
    overleaf_dir = os.path.join(OVERLEAF_DIR, "figures/gs-plots")
    
    countries = ["Kenya", "Benin", "Botswana", "Burkina Faso", 
                 "Cameroon", "Ivory Coast", "Ethiopia", "Ghana", "Guinea", "Liberia", 
                 "Lesotho", "Malawi", "Mali", "Mozambique", "Rwanda", 
                 "Senegal", "Sierra Leone", "South Africa", "South Sudan", "Sudan", 
                 "Togo", "Tanzania", "Uganda", "Zambia", "Zimbabwe"]
    
    # CONFIGURABLE PARAMETERS - Adjust these as needed
    MIN_VALID_SHARE = 0.05      # 5% threshold - adjust between 0.05 and 0.50
    REQUIRE_BOTH_SOS_EOS = True  # Set to False if you want to use partial data
    
    # Process files using new strategy with configurable parameters
    final_results = process_all_phenology_files_new_strategy(
        shapefile_path, phenology_dir, countries, overleaf_dir=overleaf_dir, 
        generate_plots=False, 
        min_valid_share=MIN_VALID_SHARE,
        require_both_sos_eos=REQUIRE_BOTH_SOS_EOS
    )
    
    # Save results with threshold in filename
    output_path = os.path.join(BASE_DIR, f"data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv")
    final_results.to_csv(output_path, index=False)
    
    print(f"\nNew strategy results saved to: {output_path}")
    print(f"Used threshold: {MIN_VALID_SHARE:.1%}")
    print("\nSample output:")
    sample_cols = ['SMALLEST', 'COUNTRY', 'all_growing_months', 'growing_month_1', 'growing_month_6', 'growing_month_12']
    available_cols = [col for col in sample_cols if col in final_results.columns]
    if available_cols:
        print(final_results[available_cols].head(10))

    # Generate LaTeX table
    latex_output_path = os.path.join(OVERLEAF_DIR, "output/table/growing-season-table.tex")
    generate_latex_growing_season_table(final_results, latex_output_path)