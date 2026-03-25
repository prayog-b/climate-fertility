# --------------------------------------------------------
# PROJECT :         Climate Fertility
# PURPOSE:          Process growing season data for DHS regions (NEW STRATEGY)
# AUTHOR:           Prayog Bhattarai
# DATE MODIFIED:    [Current Date]
# DESCRIPTION:      Uses FAO SOS/EOS data to create indicators for growing season months
#                   Adapted for DHS regions instead of SMALLEST units
# Notes:            Ensure required packages: rasterio, geopandas, pandas, numpy, matplotlib, seaborn
# Output:           Climate_Change_and_Fertility_in_SSA/data/derived/dhs/03-dhs-fao-growing-season/dhs-crop-phenology-summary-stats.csv
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
    """Load and filter shapefile for specified countries with exact matching, using DHSREGEN and CNTRYNAMEE."""
    gdf = gpd.read_file(shapefile_path)
    
    # Track initial count
    initial_count = len(gdf)
    print(f"Initial shapefile contains {initial_count} administrative units")
    print(f"Available columns: {list(gdf.columns)}")  # Debug: show available columns
    
    # Check required columns - updated for DHS regions
    required_columns = {"CNTRYNAMEE", "DHSREGEN", "id_id", "geometry"}
    if missing := required_columns - set(gdf.columns):
        raise ValueError(f"Shapefile missing required columns: {missing}")
    
    # Normalize country names (strip + lowercase)
    gdf["COUNTRY_NORM"] = gdf["CNTRYNAMEE"].str.strip().str.lower()
    countries_norm = [c.strip().lower() for c in countries]
    
    # Debug: Print unique country names in shapefile
    print(f"Countries in shapefile: {sorted(gdf['CNTRYNAMEE'].unique())}")
    
    # Track dropped by country filter
    before_country_filter = len(gdf)
    gdf = gdf[gdf["COUNTRY_NORM"].isin(countries_norm)]
    after_country_filter = len(gdf)
    dropped_by_country = before_country_filter - after_country_filter
    
    if dropped_by_country > 0:
        print(f"⚠️ Dropped {dropped_by_country} administrative units due to country filtering")
        # Identify which countries were dropped
        original_countries = set([c.strip().lower() for c in gdf['CNTRYNAMEE'].str.strip().str.lower().unique()])
        requested_countries = set(countries_norm)
        dropped_countries = requested_countries - original_countries
        if dropped_countries:
            print(f"  Countries not found: {sorted(dropped_countries)}")
    
    # Ensure DHSREGEN is not NA
    before_dhsreg_filter = len(gdf)
    gdf = gdf[gdf["DHSREGEN"].notna()]
    after_dhsreg_filter = len(gdf)
    dropped_by_dhsreg = before_dhsreg_filter - after_dhsreg_filter
    
    if dropped_by_dhsreg > 0:
        print(f"⚠️ Dropped {dropped_by_dhsreg} administrative units due to missing DHSREGEN")
    
    if gdf.empty:
        raise ValueError(
            f"No features found after filtering. "
            f"Requested: {countries} | "
            f"Available: {gdf['CNTRYNAMEE'].unique()}"
        )
    
    # Calculate total dropped in load_shapefile
    total_dropped_in_load = initial_count - len(gdf)
    print(f"✓ Loaded {len(gdf)} administrative units after filtering")
    if total_dropped_in_load > 0:
        print(f"  Total dropped in load_shapefile: {total_dropped_in_load} units ({total_dropped_in_load/initial_count*100:.1f}%)")
    
    # Exact match (avoids "Sudan" matching "South Sudan")
    gdf = gdf[gdf["COUNTRY_NORM"].isin(countries_norm)]
    
    # Ensure DHSREGEN is not NA
    gdf = gdf[gdf["DHSREGEN"].notna()]
    
    if gdf.empty:
        raise ValueError(
            f"No features found after filtering. "
            f"Requested: {countries} | "
            f"Available: {gdf['CNTRYNAMEE'].unique()}"
        )
    
    # Check for duplicate DHSREGEN values within countries
    print("Checking for duplicate DHSREGEN values within countries...")
    duplicate_check = gdf.groupby(['CNTRYNAMEE', 'DHSREGEN']).size()
    duplicates = duplicate_check[duplicate_check > 1]
    
    if len(duplicates) > 0:
        print(f"⚠️ Found {len(duplicates)} DHSREGEN values with multiple geometries within countries:")
        print(f"Total duplicate geometries: {duplicates.sum() - len(duplicates)}")
        print("Sample duplicates:")
        for (country, dhsregion), count in list(duplicates.items())[:5]:
            print(f"  {country} - {dhsregion}: {count} geometries")
        
        print("Dissolving geometries by CNTRYNAMEE and DHSREGEN...")
        gdf_dissolved = gdf.dissolve(by=['CNTRYNAMEE', 'DHSREGEN'], aggfunc='first').reset_index()
        
        # Verify no duplicates remain
        final_check = gdf_dissolved.groupby(['CNTRYNAMEE', 'DHSREGEN']).size()
        remaining_duplicates = final_check[final_check > 1]
        if len(remaining_duplicates) > 0:
            raise ValueError(f"Dissolve failed: {len(remaining_duplicates)} duplicates remain")
        else:
            print("✓ Dissolve successful: No duplicate DHSREGEN values remain")
        
        gdf = gdf_dissolved
    else:
        print("✓ No duplicate DHSREGEN values found within countries")
    
    # Additional validation: Check if DHSREGEN uniquely identifies regions across all countries
    print("Checking if DHSREGEN uniquely identifies regions across all countries...")
    cross_country_duplicates = gdf.groupby('DHSREGEN')['CNTRYNAMEE'].nunique()
    cross_duplicates = cross_country_duplicates[cross_country_duplicates > 1]
    
    if len(cross_duplicates) > 0:
        print(f"⚠️ Found {len(cross_duplicates)} DHSREGEN values used in multiple countries:")
        for dhsregion, country_count in list(cross_duplicates.items())[:5]:
            countries_using = gdf[gdf['DHSREGEN'] == dhsregion]['CNTRYNAMEE'].unique()
            print(f"  {dhsregion}: used in {country_count} countries - {list(countries_using)}")
        print("This is acceptable as long as DHSREGEN is unique within each country")
    else:
        print("✓ All DHSREGEN values are unique across countries")
    
    # Clean up temporary column
    gdf = gdf.drop(columns=['COUNTRY_NORM'], errors='ignore')
    
    # Final count summary
    country_counts = gdf.groupby('CNTRYNAMEE').size()
    print(f"\nFinal DHS region counts by country:")
    for country, count in country_counts.items():
        print(f"  {country}: {count} regions")
    
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

def process_raster_file(raster_path, shapefile, output_dir=None, plot_output_dir=None, filename=None, generate_plots=True):
    """Process raster file using the new SOS/EOS strategy for DHS regions."""
    with rasterio.open(raster_path) as src:
        shapefile = shapefile.to_crs(src.crs)
        print(f"Processing raster for {len(shapefile)} DHS regions")
        
        # Get list of all DHS regions before spatial join - FIXED to use id_id
        all_regions_before = set(zip(shapefile['DHSREGEN'], shapefile['CNTRYNAMEE'], shapefile['id_id']))
        print(f"Total DHS regions before spatial join: {len(all_regions_before)}")
        print(f"Sample regions: {list(all_regions_before)[:3] if len(all_regions_before) > 3 else list(all_regions_before)}")

        print("Calculating union of geometries for masking...")
        geometry = shapefile.geometry.union_all()
        out_image, out_transform = mask(src, [geometry], crop=True, nodata=np.nan)
        band_data = out_image[0]
        
        print("Extract valid pixels")
        rows, cols = np.where(~np.isnan(band_data))
        pixel_values = band_data[rows, cols]
        
        print(f"Found {len(pixel_values)} non-NaN pixels")
        
        print("Creating points GeoDataFrame")
        xs, ys = rasterio.transform.xy(out_transform, rows, cols)
        geometry = [Point(x, y) for x, y in zip(xs, ys)]
        points_gdf = gpd.GeoDataFrame({"value": pixel_values, "geometry": geometry}, crs=shapefile.crs)
        
        print("Classifying points into valid and flagged")
        flagged_points = points_gdf[points_gdf["value"].isin([251, 254])]
        valid_points = points_gdf[~points_gdf["value"].isin([251, 254])]
        
        print(f"Valid pixels: {len(valid_points)}, Flagged pixels: {len(flagged_points)}")
        
        print("Performing spatial joins with DHS regions...")
        # Track spatial join drops
        all_joined = gpd.sjoin(points_gdf, shapefile[['DHSREGEN', 'CNTRYNAMEE', 'id_id', 'geometry']], 
                              how="inner", predicate="within")
        all_joined = all_joined.rename(columns={'id_id': 'DHS_code'})  # Rename here
        
        # Calculate pixels that didn't join
        pixels_outside = len(points_gdf) - len(all_joined)
        if pixels_outside > 0:
            print(f"⚠️ {pixels_outside} pixels ({pixels_outside/len(points_gdf)*100:.1f}%) fell outside DHS regions")
        
        # Track regions with no pixels - FIXED to use tuple comparison
        joined_regions = set(zip(all_joined['DHSREGEN'], all_joined['CNTRYNAMEE'], all_joined['DHS_code']))
        regions_without_pixels = all_regions_before - joined_regions
        
        if len(regions_without_pixels) > 0:
            print(f"⚠️ {len(regions_without_pixels)} DHS regions have no raster pixels:")
            for i, (region, country, code) in enumerate(sorted(list(regions_without_pixels))[:5]):
                print(f"  - {country}: {region} (code: {code})")
            if len(regions_without_pixels) > 5:
                print(f"  ... and {len(regions_without_pixels) - 5} more")
        
        valid_joined = gpd.sjoin(valid_points, shapefile[['DHSREGEN', 'CNTRYNAMEE', 'id_id', 'geometry']], 
                                how="inner", predicate="within")
        valid_joined = valid_joined.rename(columns={'id_id': 'DHS_code'})
        
        flagged_joined = gpd.sjoin(flagged_points, shapefile[['DHSREGEN', 'CNTRYNAMEE', 'id_id', 'geometry']], 
                                   how="inner", predicate="within")
        flagged_joined = flagged_joined.rename(columns={'id_id': 'DHS_code'})

        # Calculate modal values for each DHSREGEN unit
        print("Calculating modal values for each DHS region...")
        if not valid_joined.empty:
            # Calculate mode for valid pixels
            modal_values = valid_joined.groupby(['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'])['value'].agg(
                lambda x: x.mode()[0] if len(x.mode()) > 0 else np.nan
            ).reset_index()
            modal_values = modal_values.rename(columns={'value': 'modal_value'})
            
            # Calculate valid share for data quality
            total_counts = all_joined.groupby(['DHSREGEN', 'CNTRYNAMEE', 'DHS_code']).size().reset_index(name='total_pixels')
            valid_counts = valid_joined.groupby(['DHSREGEN', 'CNTRYNAMEE', 'DHS_code']).size().reset_index(name='valid_pixels')
            counts_merged = pd.merge(total_counts, valid_counts, on=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'], how='left')
            counts_merged['valid_share'] = counts_merged['valid_pixels'] / counts_merged['total_pixels']
            
            modal_values = pd.merge(modal_values, counts_merged[['DHSREGEN', 'CNTRYNAMEE', 'DHS_code', 'valid_share']], 
                                   on=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'], how='left')
            
            # Track regions with modal values - FIXED
            regions_with_modes = set(zip(modal_values['DHSREGEN'], modal_values['CNTRYNAMEE'], modal_values['DHS_code']))
            regions_without_modes = joined_regions - regions_with_modes
            
            if len(regions_without_modes) > 0:
                print(f"⚠️ {len(regions_without_modes)} DHS regions have no valid modal values:")
                for i, (region, country, code) in enumerate(sorted(list(regions_without_modes))[:5]):
                    print(f"  - {country}: {region} (code: {code})")
                if len(regions_without_modes) > 5:
                    print(f"  ... and {len(regions_without_modes) - 5} more")
        else:
            modal_values = pd.DataFrame(columns=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code', 'modal_value', 'valid_share'])
            print("⚠️ No valid pixels found for any region")
        
        # Summary of spatial join results
        regions_with_data = len(modal_values) if not modal_values.empty else 0
        print(f"✓ Spatial join complete: {regions_with_data} regions have data, {len(regions_without_pixels)} regions have no pixels")
        
        return modal_values
            
def create_growing_season_indicators_new(sos_modes, eos_modes, min_valid_share=0.05, require_both_sos_eos=True):
    """
    Create monthly growing season indicators using the new SOS/EOS strategy.
    Preserves cross-year growing seasons.
    """
    print(f"\nCreating growing season indicators with:")
    print(f"  - Minimum valid share: {min_valid_share:.1%}")
    print(f"  - Require both SOS and EOS: {require_both_sos_eos}")
    
    # Track drops due to data quality
    drop_tracker = {
        'total_regions_in_input': len(sos_modes),  # Assuming SOS and EOS have same regions
        'gs1_passed': 0,
        'gs2_passed': 0,
        'gs1_failed_share': 0,
        'gs2_failed_share': 0,
        'gs1_failed_missing': 0,
        'gs2_failed_missing': 0,
        'regions_dropped_completely': 0  # Regions with neither GS1 nor GS2
    }
    
    # Track regions with low coverage
    low_coverage_regions = {
        'gs1_sos_low': [],
        'gs1_eos_low': [],
        'gs2_sos_low': [],
        'gs2_eos_low': []
    }
    
    # Merge SOS and EOS data
    merged = pd.merge(
        sos_modes[['DHSREGEN', 'CNTRYNAMEE', 'DHS_code', 
                  'modal_value_growing_season_1_start_of_season_cropland', 
                  'modal_value_growing_season_2_start_of_season_cropland',
                  'valid_share_growing_season_1_start_of_season_cropland',
                  'valid_share_growing_season_2_start_of_season_cropland']].rename(
            columns={'modal_value_growing_season_1_start_of_season_cropland': 'gs1_sos_raw',
                    'modal_value_growing_season_2_start_of_season_cropland': 'gs2_sos_raw',
                    'valid_share_growing_season_1_start_of_season_cropland': 'gs1_sos_valid_share',
                    'valid_share_growing_season_2_start_of_season_cropland': 'gs2_sos_valid_share'}),
        eos_modes[['DHSREGEN', 'CNTRYNAMEE', 'DHS_code',
                  'modal_value_growing_season_1_end_of_season_cropland',
                  'modal_value_growing_season_2_end_of_season_cropland',
                  'valid_share_growing_season_1_end_of_season_cropland',
                  'valid_share_growing_season_2_end_of_season_cropland']].rename(
            columns={'modal_value_growing_season_1_end_of_season_cropland': 'gs1_eos_raw',
                    'modal_value_growing_season_2_end_of_season_cropland': 'gs2_eos_raw',
                    'valid_share_growing_season_1_end_of_season_cropland': 'gs1_eos_valid_share',
                    'valid_share_growing_season_2_end_of_season_cropland': 'gs2_eos_valid_share'}),
        on=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'],
        how='inner'
    )
    
    print(f"Total regions after SOS-EOS merge: {len(merged)}")
    
    # Convert raw values to calendar months for each growing season
    growing_seasons = []
    
    for idx, row in merged.iterrows():
        region_id = f"{row['CNTRYNAMEE']} - {row['DHSREGEN']}"
        
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
        
        # Track low coverage
        if row['gs1_sos_valid_share'] < min_valid_share:
            low_coverage_regions['gs1_sos_low'].append(region_id)
        if row['gs1_eos_valid_share'] < min_valid_share:
            low_coverage_regions['gs1_eos_low'].append(region_id)
        if row['gs2_sos_valid_share'] < min_valid_share:
            low_coverage_regions['gs2_sos_low'].append(region_id)
        if row['gs2_eos_valid_share'] < min_valid_share:
            low_coverage_regions['gs2_eos_low'].append(region_id)
        
        # Update quality statistics
        if gs1_has_data:
            drop_tracker['gs1_passed'] += 1
        else:
            if pd.isna(row['gs1_sos_raw']) or pd.isna(row['gs1_eos_raw']):
                drop_tracker['gs1_failed_missing'] += 1
            else:
                drop_tracker['gs1_failed_share'] += 1
                
        if gs2_has_data:
            drop_tracker['gs2_passed'] += 1
        else:
            if pd.isna(row['gs2_sos_raw']) or pd.isna(row['gs2_eos_raw']):
                drop_tracker['gs2_failed_missing'] += 1
            else:
                drop_tracker['gs2_failed_share'] += 1
        
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
        
        # Check if region has any growing season data
        if not (gs1_has_data or gs2_has_data):
            drop_tracker['regions_dropped_completely'] += 1
        
        # Create binary indicators for each month
        month_indicators = {}
        for month in range(1, 13):
            month_indicators[f'growing_month_{month}'] = 1 if month in all_growing_months else 0
        
        growing_seasons.append({
            'DHSREGEN': row['DHSREGEN'],
            'CNTRYNAMEE': row['CNTRYNAMEE'],
            'DHS_code': row['DHS_code'],
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
    print(f"\n{'='*60}")
    print("DATA QUALITY REPORT - Growing Season Indicators")
    print('='*60)
    print(f"Total DHS regions in input: {drop_tracker['total_regions_in_input']}")
    print(f"Regions after SOS-EOS merge: {len(merged)}")
    print(f"\nGS1: {drop_tracker['gs1_passed']} passed ({drop_tracker['gs1_passed']/len(merged)*100:.1f}%)")
    print(f"     {drop_tracker['gs1_failed_share']} failed - insufficient valid share")
    print(f"     {drop_tracker['gs1_failed_missing']} failed - missing data")
    print(f"\nGS2: {drop_tracker['gs2_passed']} passed ({drop_tracker['gs2_passed']/len(merged)*100:.1f}%)")
    print(f"     {drop_tracker['gs2_failed_share']} failed - insufficient valid share")
    print(f"     {drop_tracker['gs2_failed_missing']} failed - missing data")
    print(f"\n⚠️ {drop_tracker['regions_dropped_completely']} regions have NO growing season data")
    print(f"   ({drop_tracker['regions_dropped_completely']/len(merged)*100:.1f}% of merged regions)")
    
    # Report low coverage by component
    print(f"\nLow Coverage Summary (valid share < {min_valid_share:.1%}):")
    for key, regions in low_coverage_regions.items():
        if regions:
            print(f"  {key}: {len(regions)} regions")
            if len(regions) <= 5:  # Show all if few
                for region in regions:
                    print(f"    - {region}")
    
    return pd.DataFrame(growing_seasons)

def process_all_phenology_files(shapefile_path, phenology_dir, countries, overleaf_dir, 
                                           generate_plots=True, min_valid_share=0.10, require_both_sos_eos=True):
    """Master function using the new SOS/EOS strategy for DHS regions."""
    print(f"\n{'='*50}\nStarting batch processing with NEW STRATEGY (DHS REGIONS)")
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
            modal_values = process_raster_file(
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
                        # Merge and handle duplicate columns
                        sos_modes = pd.merge(sos_modes, modal_values, 
                                           on=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'], 
                                           how='outer', suffixes=('', '_dup'))
                        # Remove duplicate columns (those ending with '_dup')
                        dup_cols = [col for col in sos_modes.columns if col.endswith('_dup')]
                        if dup_cols:
                            sos_modes = sos_modes.drop(columns=dup_cols)
                else:
                    if eos_modes is None:
                        eos_modes = modal_values
                    else:
                        # Merge and handle duplicate columns
                        eos_modes = pd.merge(eos_modes, modal_values, 
                                           on=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'], 
                                           how='outer', suffixes=('', '_dup'))
                        # Remove duplicate columns (those ending with '_dup')
                        dup_cols = [col for col in eos_modes.columns if col.endswith('_dup')]
                        if dup_cols:
                            eos_modes = eos_modes.drop(columns=dup_cols)
                
                processed_files += 1
                print(f"✓ Successfully processed {filename}")

        print(f"\nProcessed {processed_files} files successfully")
        
        # Check if we have both SOS and EOS data
        if sos_modes is None or eos_modes is None:
            raise ValueError("Missing either SOS or EOS data - cannot create growing season indicators")
        
        # Clean up any duplicate columns that might have been created
        print("\nCleaning up duplicate columns...")
        
        # Function to clean duplicate columns in a DataFrame
        def clean_duplicate_columns(df):
            # Find columns that end with _x or _y (created by merges)
            suffix_cols = [col for col in df.columns if col.endswith(('_x', '_y'))]
            for col in suffix_cols:
                base_col = col[:-2]  # Remove '_x' or '_y'
                if base_col in df.columns:
                    # This is a duplicate, keep the original and drop the suffixed one
                    df = df.drop(columns=[col])
            return df
        
        sos_modes = clean_duplicate_columns(sos_modes)
        eos_modes = clean_duplicate_columns(eos_modes)
        
        print("Cleaned SOS columns:", sos_modes.columns.tolist())
        print("Cleaned EOS columns:", eos_modes.columns.tolist())
        
        # Ensure DHS_code column exists and is properly named
        for df, name in [(sos_modes, 'SOS'), (eos_modes, 'EOS')]:
            # Check for any DHS_code columns (including suffixed versions)
            dhs_code_cols = [col for col in df.columns if 'DHS_code' in col]
            print(f"\n{name} DHS_code columns found: {dhs_code_cols}")
            
            if len(dhs_code_cols) == 0:
                raise ValueError(f"No DHS_code column found in {name} data")
            elif len(dhs_code_cols) > 1:
                # Keep the first one (without suffix) and drop others
                print(f"Multiple DHS_code columns found in {name}, cleaning up...")
                # Find the clean 'DHS_code' column (without suffix)
                if 'DHS_code' in dhs_code_cols:
                    primary_col = 'DHS_code'
                else:
                    # Take the first one and rename it
                    primary_col = dhs_code_cols[0]
                    df = df.rename(columns={primary_col: 'DHS_code'})
                    primary_col = 'DHS_code'
                
                # Drop all other DHS_code columns
                cols_to_drop = [col for col in dhs_code_cols if col != primary_col]
                df = df.drop(columns=cols_to_drop)
                print(f"Kept column '{primary_col}', dropped: {cols_to_drop}")
        
        print("\nFinal column check:")
        print("SOS columns:", sos_modes.columns.tolist())
        print("EOS columns:", eos_modes.columns.tolist())

        # Create final growing season indicators
        print("\nCreating growing season indicators...")
        growing_season_df = create_growing_season_indicators_new(
            sos_modes, eos_modes, 
            min_valid_share=min_valid_share,
            require_both_sos_eos=require_both_sos_eos
        )
        
        # Before final merge, track what will be dropped
        print(f"\n{'='*60}")
        print("FINAL MERGE TRACKING")
        print('='*60)
        
        # Get unique regions in each dataset
        sos_regions = set(zip(sos_modes['CNTRYNAMEE'], sos_modes['DHSREGEN'], sos_modes['DHS_code']))
        eos_regions = set(zip(eos_modes['CNTRYNAMEE'], eos_modes['DHSREGEN'], eos_modes['DHS_code']))
        
        print(f"SOS regions: {len(sos_regions)}")
        print(f"EOS regions: {len(eos_regions)}")
        
        # Find regions that will be dropped in inner merge
        regions_only_in_sos = sos_regions - eos_regions
        regions_only_in_eos = eos_regions - sos_regions
        
        if regions_only_in_sos:
            print(f"\n⚠️ {len(regions_only_in_sos)} regions will be dropped (in SOS but not EOS):")
            for i, (country, region, code) in enumerate(sorted(list(regions_only_in_sos))[:5]):
                print(f"  - {country}: {region} (code: {code})")
            if len(regions_only_in_sos) > 5:
                print(f"  ... and {len(regions_only_in_sos) - 5} more")
        
        if regions_only_in_eos:
            print(f"\n⚠️ {len(regions_only_in_eos)} regions will be dropped (in EOS but not SOS):")
            for i, (country, region, code) in enumerate(sorted(list(regions_only_in_eos))[:5]):
                print(f"  - {country}: {region} (code: {code})")
            if len(regions_only_in_eos) > 5:
                print(f"  ... and {len(regions_only_in_eos) - 5} more")
        

        # Merge with original modal values for complete dataset - include DHS_code
        print(f"\nPerforming inner merge...")
        final_df = pd.merge(sos_modes, eos_modes, on=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'], how='inner')
        
        print(f"Regions after inner merge: {len(final_df)}")
        print(f"Regions dropped in inner merge: {len(sos_regions) + len(eos_regions) - 2*len(final_df)}")
        
        # Continue with growing season indicators
        print("\nCreating growing season indicators...")
        growing_season_df = create_growing_season_indicators_new(
            sos_modes, eos_modes, 
            min_valid_share=min_valid_share,
            require_both_sos_eos=require_both_sos_eos
        )
        
        # Merge with growing season data
        final_df = pd.merge(final_df, growing_season_df, on=['DHSREGEN', 'CNTRYNAMEE', 'DHS_code'], how='left')
        print(f"\n{'='*60}")
        print("FINAL SUMMARY")
        print('='*60)
        print(f"Total regions in final output: {len(final_df)}")
        print(f"Regions with any growing season data: {final_df['gs1_has_data'].sum() + final_df['gs2_has_data'].sum()}")
        print(f"\n{'='*50}\nProcessing complete!")
        print(f"Final output shape: {final_df.shape}")
        print(f"Sample of growing season data:")
        sample_cols = ['DHSREGEN', 'CNTRYNAMEE', 'DHS_code', 'all_growing_months', 'gs1_has_data', 'gs2_has_data']
        if all(col in final_df.columns for col in sample_cols):
            print(final_df[sample_cols].head())
        
        return final_df

    except Exception as e:
        print(f"\nFATAL ERROR in batch processing: {str(e)}")
        print(traceback.format_exc())
        raise

def generate_country_level_summary(df, output_path=None):
    """
    Generate a country-level summary of regions with valid growing season data.
    
    Parameters:
    -----------
    df : DataFrame
        The final results dataframe with growing season indicators
    output_path : str, optional
        Path to save the summary CSV file
        
    Returns:
    --------
    DataFrame with country-level summary
    """
    print("\n" + "="*60)
    print("GENERATING COUNTRY-LEVEL SUMMARY")
    print("="*60)
    
    # Ensure required columns exist
    required_cols = ['CNTRYNAMEE', 'DHSREGEN', 'DHS_code', 'gs1_has_data', 'gs2_has_data']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"Warning: Missing columns for summary: {missing_cols}")
        return None
    
    # Create summary statistics for each country
    country_summary = []
    
    for country in sorted(df['CNTRYNAMEE'].unique()):
        country_data = df[df['CNTRYNAMEE'] == country]
        total_regions = len(country_data)
        
        # Count regions with valid data
        regions_with_gs1 = country_data['gs1_has_data'].sum()
        regions_with_gs2 = country_data['gs2_has_data'].sum()
        
        # Regions with any growing season data (GS1 OR GS2)
        regions_with_any = (country_data['gs1_has_data'] | country_data['gs2_has_data']).sum()
        
        # Regions with NO growing season data
        regions_with_none = total_regions - regions_with_any
        
        # Calculate percentages
        pct_gs1 = (regions_with_gs1 / total_regions * 100) if total_regions > 0 else 0
        pct_gs2 = (regions_with_gs2 / total_regions * 100) if total_regions > 0 else 0
        pct_any = (regions_with_any / total_regions * 100) if total_regions > 0 else 0
        pct_none = (regions_with_none / total_regions * 100) if total_regions > 0 else 0
        
        # Get list of regions with no data
        regions_no_data = country_data[
            (country_data['gs1_has_data'] == 0) & 
            (country_data['gs2_has_data'] == 0)
        ]['DHSREGEN'].tolist()
        
        country_summary.append({
            'Country': country,
            'Total_Regions': total_regions,
            'Regions_with_GS1': regions_with_gs1,
            'Regions_with_GS2': regions_with_gs2,
            'Regions_with_Any_GS': regions_with_any,
            'Regions_with_No_GS': regions_with_none,
            'Pct_with_GS1': round(pct_gs1, 1),
            'Pct_with_GS2': round(pct_gs2, 1),
            'Pct_with_Any_GS': round(pct_any, 1),
            'Pct_with_No_GS': round(pct_none, 1),
            'Regions_No_Data_List': ', '.join(regions_no_data) if regions_no_data else 'None'
        })
    
    # Convert to DataFrame
    summary_df = pd.DataFrame(country_summary)
    
    # Calculate totals
    totals = {
        'Country': 'TOTAL',
        'Total_Regions': summary_df['Total_Regions'].sum(),
        'Regions_with_GS1': summary_df['Regions_with_GS1'].sum(),
        'Regions_with_GS2': summary_df['Regions_with_GS2'].sum(),
        'Regions_with_Any_GS': summary_df['Regions_with_Any_GS'].sum(),
        'Regions_with_No_GS': summary_df['Regions_with_No_GS'].sum(),
        'Pct_with_GS1': round(summary_df['Regions_with_GS1'].sum() / summary_df['Total_Regions'].sum() * 100, 1),
        'Pct_with_GS2': round(summary_df['Regions_with_GS2'].sum() / summary_df['Total_Regions'].sum() * 100, 1),
        'Pct_with_Any_GS': round(summary_df['Regions_with_Any_GS'].sum() / summary_df['Total_Regions'].sum() * 100, 1),
        'Pct_with_No_GS': round(summary_df['Regions_with_No_GS'].sum() / summary_df['Total_Regions'].sum() * 100, 1),
        'Regions_No_Data_List': 'N/A'
    }
    
    summary_df = pd.concat([summary_df, pd.DataFrame([totals])], ignore_index=True)
    
    # Display summary
    print("\nCOUNTRY-LEVEL GROWING SEASON DATA COVERAGE:")
    print("-" * 80)
    print(f"{'Country':<25} {'Total':>6} {'Any GS':>8} {'GS1':>6} {'GS2':>6} {'No GS':>6}")
    print("-" * 80)
    
    for _, row in summary_df.iterrows():
        if row['Country'] == 'TOTAL':
            print("-" * 80)
        print(f"{row['Country']:<25} {row['Total_Regions']:>6} {row['Pct_with_Any_GS']:>7.1f}% "
              f"{row['Pct_with_GS1']:>6.1f}% {row['Pct_with_GS2']:>6.1f}% {row['Pct_with_No_GS']:>6.1f}%")
    
    print("-" * 80)
    
    # Identify countries with regions that have no data
    countries_with_no_data = summary_df[
        (summary_df['Country'] != 'TOTAL') & 
        (summary_df['Regions_with_No_GS'] > 0)
    ].copy()
    
    if len(countries_with_no_data) > 0:
        print(f"\nCOUNTRIES WITH REGIONS HAVING NO GROWING SEASON DATA:")
        print("-" * 60)
        for _, row in countries_with_no_data.iterrows():
            print(f"{row['Country']}: {row['Regions_with_No_GS']} of {row['Total_Regions']} regions "
                  f"({row['Pct_with_No_GS']}%)")
            if row['Regions_No_Data_List'] != 'None':
                print(f"  Regions: {row['Regions_No_Data_List']}")
    
    # Save to file if output path provided
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        summary_df.to_csv(output_path, index=False)
        print(f"\nCountry-level summary saved to: {output_path}")
    
    print("=" * 60)
    
    return summary_df

def generate_latex_growing_season_table(df, output_path):
    """
    Generate a LaTeX table showing the share of DHS regions 
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
    countries = sorted(df['CNTRYNAMEE'].unique())
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
        country_data = df[df['CNTRYNAMEE'] == country]
        total_regions = len(country_data)
        
        # Calculate share for each month
        shares = []
        for month in range(1, 13):
            col_name = f'growing_month_{month}'
            if col_name in country_data.columns:
                count_with_season = (country_data[col_name] == 1).sum()
                share = count_with_season / total_regions if total_regions > 0 else 0
                shares.append(share)
            else:
                shares.append(0)
        
        # Format data row
        shares_str = " & ".join([f"{share:.3f}" for share in shares])
        data_row = f"{country} & {shares_str} \\\\"
        latex.append(data_row)
        print(f"  Added row for {country} ({total_regions} regions)")
    
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
    # Configuration
    BASE_DIR = os.getenv("CF_DIR", "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA")
    OVERLEAF_DIR = os.getenv("CF_OVERLEAF", "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa")
    
    # Updated paths for DHS regions
    shapefile_path = os.path.join(BASE_DIR, "data/derived/shapefiles/dhs/cleaned-ssa-dhs-regions.shp")
    phenology_dir = os.path.join(BASE_DIR, "data/source/fao/crop-phenology")
    overleaf_dir = os.path.join(OVERLEAF_DIR, "figures/gs-plots")
    
    # All 35 SSA countries
    countries = [
        "Benin", "Burkina Faso", "Burundi", "Cameroon", "Central African Republic",
        "Chad", "Comoros", "Congo", "Ivory Coast", "Congo Democratic Republic",
        "Ethiopia", "Gabon", "The Gambia", "Ghana", "Guinea", "Kenya", "Lesotho",
        "Liberia", "Madagascar", "Malawi", "Mali", "Mozambique", "Namibia",
        "Niger", "Nigeria", "Rwanda", "Senegal", "Sierra Leone", "South Africa",
        "Swaziland", "Tanzania", "Togo", "Uganda", "Zambia", "Zimbabwe"
    ]
    
    # CONFIGURABLE PARAMETERS - Adjust these as needed
    MIN_VALID_SHARE = 0.05      # 5% threshold - adjust between 0.05 and 0.50
    REQUIRE_BOTH_SOS_EOS = True  # Set to False if you want to use partial data
    
    # Process files using new strategy with configurable parameters
    final_results = process_all_phenology_files(
        shapefile_path, phenology_dir, countries, overleaf_dir=overleaf_dir, 
        generate_plots=False, 
        min_valid_share=MIN_VALID_SHARE,
        require_both_sos_eos=REQUIRE_BOTH_SOS_EOS
    )
    
    # Save results with DHS-specific output path
    output_dir = os.path.join(BASE_DIR, "data/derived/dhs/03-dhs-fao-growing-season")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "dhs-crop-phenology-summary-stats.csv")
    final_results.to_csv(output_path, index=False)
    
    print(f"\nDHS region growing season results saved to: {output_path}")
    print(f"Used threshold: {MIN_VALID_SHARE:.1%}")
    print(f"Total DHS regions processed: {len(final_results)}")
    print("\nSample output:")
    sample_cols = ['DHSREGEN', 'CNTRYNAMEE', 'all_growing_months', 'growing_month_1', 'growing_month_6', 'growing_month_12']
    available_cols = [col for col in sample_cols if col in final_results.columns]
    if available_cols:
        print(final_results[available_cols].head(10))

    # Generate country-level summary
    country_summary_path = os.path.join(output_dir, "country-growing-season-summary.csv")
    country_summary = generate_country_level_summary(final_results, country_summary_path)
    
    # Generate LaTeX table
    latex_output_path = os.path.join(OVERLEAF_DIR, "output/table/dhs-growing-season-table.tex")
    generate_latex_growing_season_table(final_results, latex_output_path)