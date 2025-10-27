# Project:          Climate Fertility
# Author:           Prayog Bhattarai
# Date modified:    09 October 2025
# Description:      Cleaning raw shapefile 
# Input:            Climate_Change_and_Fertility_in_SSA/data/source/shapefiles/ipums/bpl_data_smallest_unit_africa.shp
# Output:           Climate_Change_and_Fertility_in_SSA/data/derived/shapefiles/cleaned_ssa_boundaries.shp


# Import packages
import geopandas as gpd
import pandas as pd
import numpy as np
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
import warnings
warnings.filterwarnings('ignore')


# Class:    ShapefileCleaner
# Purpose:  Class that contains functions for cleaning tasks and recording stats on number of observations affected
class SSAShapefileCleaner:
    def __init__(self, input_path, output_path, overleaf_path):
        self.input_path = input_path
        self.output_path = output_path
        self.overleaf_path = overleaf_path
        self.gdf = None
        self.cleaning_log = {
            'original_count': 0,
            'steps': [],
            'final_count': 0
        }

    # Log a cleaning step 
    def log_step(self, step_name, removed_count, remaining_count, details=""):
        """Log a cleaning step"""
        step_info = {
            'step': step_name,
            'removed': removed_count,
            'remaining': remaining_count,
            'details': details
        }
        self.cleaning_log['steps'].append(step_info)
        print(f"✓ {step_name}: removed {removed_count:,}, remaining {remaining_count:,}")
        if details:
            print(f"  Details: {details}")
    
    # Load raw shapefile 
    def load_data(self):
        """Load the shapefile"""
        print("LOADING SHAPEFILE")
        print("=" * 20)
        
        try:
            self.gdf = gpd.read_file(self.input_path)
            self.gdf.loc[self.gdf['COUNTRY'] == "Côte d'Ivoire", 'COUNTRY'] = 'Ivory Coast'
            self.cleaning_log['original_count'] = len(self.gdf)
            print(f"✓ Loaded {len(self.gdf):,} records from {self.input_path}")
            return True
        except Exception as e:
            print(f"❌ Error loading shapefile: {e}")
            return False
        
    # Check whether dataset is uniquely identifiable at SMALLEST level
    def check_smallest_identifiability(self):
        """Check if dataset is uniquely identifiable at SMALLEST level and detect duplicate BPL_NAME mappings"""
        print("\nCHECKING SMALLEST LEVEL IDENTIFIABILITY")
        print("-" * 45)
        
        # Filter for valid SMALLEST values
        valid_data = self.gdf[self.gdf['SMALLEST'].notna()].copy()
        
        print(f"Total records with valid SMALLEST: {len(valid_data):,}")
        
        # Check 1: Are SMALLEST values unique (one row per SMALLEST)?
        smallest_counts = valid_data['SMALLEST'].value_counts()
        duplicates = smallest_counts[smallest_counts > 1]
        
        print(f"\nUnique SMALLEST values: {len(smallest_counts):,}")
        print(f"Duplicate SMALLEST values: {len(duplicates):,}")
        
        if len(duplicates) > 0:
            print(f"⚠️  WARNING: Dataset is NOT uniquely identifiable at SMALLEST level")
            print(f"   {len(duplicates):,} SMALLEST values have multiple records")
            
            # Show examples
            print("\nExamples of duplicate SMALLEST values:")
            for smallest_val, count in duplicates.head(5).items():
                print(f"  SMALLEST {smallest_val}: {count} records")
                dup_rows = valid_data[valid_data['SMALLEST'] == smallest_val][['COUNTRY', 'BPL_NAME', 'SMALLEST']]
                print(dup_rows.to_string(index=False))
                print()
        else:
            print(f"✓ Dataset IS uniquely identifiable at SMALLEST level (no duplicates)")
        
        # Check 2: Multiple BPL_NAME values for same SMALLEST?
        smallest_bpl_groups = valid_data.groupby('SMALLEST')['BPL_NAME'].apply(lambda x: x.nunique())
        multiple_bpl = smallest_bpl_groups[smallest_bpl_groups > 1]
        
        print(f"\nSMALLEST units with multiple BPL_NAME values: {len(multiple_bpl):,}")
        
        if len(multiple_bpl) > 0:
            print(f"⚠️  WARNING: {len(multiple_bpl):,} SMALLEST units have multiple BPL_NAME values")
            
            # Show examples
            print("\nExamples of SMALLEST units with multiple BPL_NAME values:")
            for smallest_val, bpl_count in multiple_bpl.head(5).items():
                bpl_names = valid_data[valid_data['SMALLEST'] == smallest_val]['BPL_NAME'].unique()
                country = valid_data[valid_data['SMALLEST'] == smallest_val]['COUNTRY'].iloc[0]
                print(f"  SMALLEST {smallest_val} ({country}): {bpl_count} different BPL_NAMEs")
                for bpl in bpl_names:
                    print(f"    - '{bpl}'")
                print()
            
            # Export detailed list
            problematic_records = valid_data[valid_data['SMALLEST'].isin(multiple_bpl.index)][
                ['COUNTRY', 'SMALLEST', 'BPL_NAME', 'GEOLEV1', 'GEOLEV2']
            ].sort_values(['SMALLEST', 'BPL_NAME'])
            
            csv_path = self.output_path.replace(
                "data/derived/shapefiles/cleaned_ssa_boundaries.shp", 
                "data/documentation/shapefile-cleaning/smallest-multiple-bpl-names.csv"
            )
            problematic_records.to_csv(csv_path, index=False)
            print(f"✓ Full list saved to: {csv_path}")
        else:
            print(f"✓ Each SMALLEST unit has a unique BPL_NAME")
        
        # Summary
        print("\n" + "=" * 45)
        print("IDENTIFIABILITY SUMMARY:")
        print(f"  Unique identifier at SMALLEST level: {'NO' if len(duplicates) > 0 else 'YES'}")
        print(f"  One-to-one SMALLEST-BPL_NAME mapping: {'NO' if len(multiple_bpl) > 0 else 'YES'}")
        
        return {
            'unique_smallest': len(duplicates) == 0,
            'one_to_one_mapping': len(multiple_bpl) == 0,
            'duplicate_smallest_count': len(duplicates),
            'multiple_bpl_count': len(multiple_bpl)
        }

    # Check how many BPL_NAME entries with `unknown` values have valid geometries 
    def check_unknown_bpl_with_valid_geometries(self):
        """Check how many observations with 'unknown' in BPL_NAME have valid geometries"""
        print("\nCHECKING UNKNOWN BPL_NAME WITH VALID GEOMETRIES")
        print("-" * 55)
        
        # Case-insensitive search for 'unknown' in BPL_NAME
        unknown_bpl_mask = self.gdf['BPL_NAME'].astype(str).str.lower().str.contains('unknown', na=False)
        unknown_count = unknown_bpl_mask.sum()
        
        # Check which of these have valid geometries
        valid_geom_mask = self.gdf.geometry.notna()
        unknown_with_valid_geom = unknown_bpl_mask & valid_geom_mask
        unknown_with_valid_geom_count = unknown_with_valid_geom.sum()
        
        print(f"Total 'unknown' BPL_NAME entries: {unknown_count:,}")
        print(f"'Unknown' entries with valid geometries: {unknown_with_valid_geom_count:,}")
        print(f"'Unknown' entries with null geometries: {unknown_count - unknown_with_valid_geom_count:,}")
        
        if unknown_with_valid_geom_count > 0:
            # Show examples of unknown entries with valid geometries
            unknown_valid_examples = self.gdf[unknown_with_valid_geom][['COUNTRY', 'BPL_NAME', 'SMALLEST']].head(5)
            print("Examples of 'unknown' BPL_NAME entries with valid geometries:")
            for idx, row in unknown_valid_examples.iterrows():
                area = self.gdf.loc[idx, 'geometry'].area if hasattr(self.gdf.loc[idx, 'geometry'], 'area') else 'N/A'
                print(f"  {row['COUNTRY']} - '{row['BPL_NAME']}' (SMALLEST: {row['SMALLEST']}, Area: {area:.8f})")
        
        return unknown_with_valid_geom_count

    # Remove entries if they have null geometries
    def remove_null_geometries(self):
        """Remove null geometries (63 records)"""
        print("\nSTEP 1: REMOVING NULL GEOMETRIES")
        print("-" * 35)
        
        null_geoms_mask = self.gdf.geometry.isnull()
        null_geoms = self.gdf.geometry.isnull().sum()
        print(f"Found {null_geoms:,} null geometries")
        
        if null_geoms > 0:
            print("\nListing COUNTRY and SMALLEST for null geometries")
            print(self.gdf.loc[null_geoms_mask, ["COUNTRY", "SMALLEST"]])
            null_records = self.gdf.loc[null_geoms_mask, ["COUNTRY", "SMALLEST", "BPL_NAME"]]
            print(null_records)
            csv_path = self.output_path.replace("data/derived/shapefiles/cleaned_ssa_boundaries.shp", "data/documentation/shapefile-cleaning/ssa-shapefile-null-geometries.csv")
            null_records.to_csv(csv_path, index = False)

            # Show distribution by country
            null_by_country = self.gdf[self.gdf.geometry.isnull()]['COUNTRY'].value_counts()
            print("Null geometries by country:")
            for country, count in null_by_country.head(10).items():
                print(f"  {country}: {count}")
            
            # Check how many of these null geometries have 'unknown' in BPL_NAME
            null_geoms_mask = self.gdf.geometry.isnull()
            unknown_in_null = self.gdf[null_geoms_mask]['BPL_NAME'].astype(str).str.lower().str.contains('unknown', na=False).sum()
            print(f"Null geometries with 'unknown' in BPL_NAME: {unknown_in_null}")
            
            # Remove null geometries
            self.gdf = self.gdf[self.gdf.geometry.notna()].copy()
            self.log_step("Remove null geometries", null_geoms, len(self.gdf))
        else:
            print("No null geometries found")
    
    # Check whether there is an overlap between `unknown` BPL_NAME and null geometries
    def check_unknown_bpl_geometry_overlap(self):
        """Check overlap between unknown BPL_NAME and null geometries"""
        print("\nCHECKING UNKNOWN BPL_NAME vs NULL GEOMETRY OVERLAP")
        print("-" * 55)
        
        # Since we already removed null geometries, check in remaining data
        unknown_bpl = self.gdf['BPL_NAME'].astype(str).str.lower().str.contains('unknown', na=False)
        unknown_count = unknown_bpl.sum()
        
        print(f"Unknown BPL_NAME entries in remaining data: {unknown_count}")
        
        if unknown_count > 0:
            unknown_examples = self.gdf[unknown_bpl][['COUNTRY', 'BPL_NAME', 'SMALLEST']].head(5)
            print("Examples of remaining unknown BPL_NAME entries:")
            for idx, row in unknown_examples.iterrows():
                area = self.gdf.loc[idx, 'geometry'].area if hasattr(self.gdf.loc[idx, 'geometry'], 'area') else 'N/A'
                print(f"  {row['COUNTRY']} - '{row['BPL_NAME']}' (SMALLEST: {row['SMALLEST']}, Area: {area:.8f})")

    # Remove very small geometries (potential slivers) 
        # If a geometry falls below the 1st percentile of area sizes, consider it a sliver and remove it
    def remove_very_small_geometries(self, percentile_threshold=1):
        """Remove very small geometries (potential slivers)"""
        print(f"\nSTEP 2: REMOVING VERY SMALL GEOMETRIES (< {percentile_threshold}st PERCENTILE)")
        print("-" * 65)
        
        # Calculate areas
        areas = self.gdf.geometry.area
        threshold = areas.quantile(percentile_threshold / 100)
        very_small_mask = areas < threshold
        very_small_count = very_small_mask.sum()
        
        print(f"Area threshold (1st percentile): {threshold:.8f}")
        print(f"Very small geometries found: {very_small_count}")
        
        if very_small_count > 0:
            # Show examples
            small_examples = self.gdf[very_small_mask][['COUNTRY', 'BPL_NAME', 'SMALLEST']].head(5)
            print("Examples of very small geometries:")
            for idx, row in small_examples.iterrows():
                area_val = areas.loc[idx]
                print(f"  {row['COUNTRY']} - {row['BPL_NAME']} (Area: {area_val:.8f})")
            
            # Remove very small geometries
            self.gdf = self.gdf[~very_small_mask].copy()
            self.log_step("Remove very small geometries", very_small_count, len(self.gdf), 
                         f"Threshold: {threshold:.8f}")
        else:
            print("No very small geometries found")
    
    # Identify missing SMALLEST values and handle them
    def handle_missing_smallest(self):
        """Handle missing SMALLEST values"""
        print("\nSTEP 3: HANDLING MISSING SMALLEST VALUES")
        print("-" * 42)
        
        missing_smallest = self.gdf['SMALLEST'].isnull()
        missing_count = missing_smallest.sum()
        
        print(f"Records with missing SMALLEST: {missing_count}")
        
        if missing_count > 0:
            # Check if these records have GEOLEV1 or GEOLEV2 information
            missing_data = self.gdf[missing_smallest]
            has_geolev1 = missing_data['GEOLEV1'].notna().sum()
            has_geolev2 = missing_data['GEOLEV2'].notna().sum()
            has_either_geolev = missing_data[(missing_data['GEOLEV1'].notna()) | 
                                           (missing_data['GEOLEV2'].notna())].shape[0]
            
            print(f"Missing SMALLEST records analysis:")
            print(f"  Have GEOLEV1: {has_geolev1}")
            print(f"  Have GEOLEV2: {has_geolev2}")
            print(f"  Have either GEOLEV1 or GEOLEV2: {has_either_geolev}")
            
            # Show examples
            print("Examples of records with missing SMALLEST:")
            for idx, row in missing_data[['COUNTRY', 'BPL_NAME', 'GEOLEV1', 'GEOLEV2']].head().iterrows():
                print(f"  {row['COUNTRY']} - {row['BPL_NAME']} (GEOLEV1: {row['GEOLEV1']}, GEOLEV2: {row['GEOLEV2']})")
            
            # Remove records with no SMALLEST and no GEOLEV information
            no_admin_info = missing_smallest & self.gdf['GEOLEV1'].isnull() & self.gdf['GEOLEV2'].isnull()
            no_admin_count = no_admin_info.sum()
            
            if no_admin_count > 0:
                print(f"Removing {no_admin_count} records with no SMALLEST and no GEOLEV info")
                self.gdf = self.gdf[~no_admin_info].copy()
                self.log_step("Remove records with no admin info", no_admin_count, len(self.gdf))
            
            # For remaining records with missing SMALLEST but have GEOLEV, keep them for now
            remaining_missing = self.gdf['SMALLEST'].isnull().sum()
            if remaining_missing > 0:
                print(f"Keeping {remaining_missing} records with missing SMALLEST but have GEOLEV info")
        else:
            print("No missing SMALLEST values found")
    
    # Fix country code mismatches (Sudan/South Sudan issue)
    def fix_country_code_mismatches(self):
        """Fix Sudan/South Sudan country code mismatches"""
        print("\nSTEP 4: FIXING COUNTRY CODE MISMATCHES")
        print("-" * 42)
        
        # Work with records that have complete country/code data
        complete_data = self.gdf[(self.gdf['COUNTRY'].notna()) & 
                               (self.gdf['CNTRY_CD'].notna()) & 
                               (self.gdf['SMALLEST'].notna())].copy()
        
        if len(complete_data) == 0:
            print("No records with complete country/code data")
            return
        
        complete_data['SMALLEST_str'] = complete_data['SMALLEST'].astype(str)
        complete_data['CNTRY_CD_str'] = complete_data['CNTRY_CD'].astype(str)
        
        # Find mismatches
        def check_mismatch(row):
            return not str(row['SMALLEST_str']).startswith(str(row['CNTRY_CD_str']))
        
        mismatch_mask = complete_data.apply(check_mismatch, axis=1)
        mismatched_indices = complete_data[mismatch_mask].index
        mismatch_count = len(mismatched_indices)
        
        print(f"Country code mismatches found: {mismatch_count}")
        
        if mismatch_count > 0:
            # Show by country
            mismatched_data = complete_data[mismatch_mask]
            mismatch_by_country = mismatched_data['COUNTRY'].value_counts()
            print("Mismatches by country:")
            for country, count in mismatch_by_country.items():
                print(f"  {country}: {count}")

            print("\n" + "="*80)
            print("ALL RECORDS BEING DELETED.")
            print("="*80)

            # Display all mismatched records with more detail
            for idx, row in mismatched_data.iterrows():
                print(f"\nIndex: {idx}")
                print(f"  COUNTRY: {row['COUNTRY']}")
                print(f"  CNTRY_CD: {row['CNTRY_CD']}")
                print(f"  SMALLEST: {row['SMALLEST']}")
                print(f"  BPL_NAME: {row.get('BPL_NAME', 'N/A')}")

                # Show all available columns for this record
                print("All columns: ")
                for col in mismatched_data.columns:
                    if col not in ['COUNTRY', 'CNTRY_CD', 'SMALLEST', 'BPL_NAME', 'geometry']:
                        value = row[col]
                        if pd.notna(value) and str(value).strip() != '':
                            print(f".    {col}: {value}")

            print("\n" + "="*80)
        
            # Create a summary DataFrame of deleted records
            deleted_summary = mismatched_data[['COUNTRY', 'CNTRY_CD', 'SMALLEST', 'BPL_NAME']].copy()
            deleted_summary['mismatch_reason'] = deleted_summary.apply(
                lambda x: f"SMALLEST ({x['SMALLEST']}) doesn't start with CNTRY_CD ({x['CNTRY_CD']})", axis=1
            )

            print("SUMMARY OF DELETED RECORDS:")
            print(deleted_summary.to_string(index=True))    
            
            # Show examples
            # print("Examples of mismatched records:")
            # for idx, row in mismatched_data[['COUNTRY', 'CNTRY_CD', 'SMALLEST', 'BPL_NAME']].head().iterrows():
            #     print(f"  {row['COUNTRY']} (code: {row['CNTRY_CD']}) has SMALLEST: {row['SMALLEST']} - {row['BPL_NAME']}")
            
            # Remove mismatched records
            self.gdf = self.gdf.drop(mismatched_indices)
            self.log_step("Remove country code mismatches", mismatch_count, len(self.gdf), 
                         "Sudan/South Sudan boundary issues")
        else:
            print("No country code mismatches found")
    
    # Resolve sliver overlaps with multiple strategies
    def resolve_sliver_overlaps(self):
        """Resolve sliver overlaps with multiple repair strategies"""
        print("\nSTEP 5: RESOLVING SLIVER OVERLAPS")
        print("-" * 35)
        
        print("Identifying overlapping geometries...")
        
        # Create a copy for processing
        working_gdf = self.gdf.copy()
        spatial_index = working_gdf.sindex
        overlaps_to_resolve = []

        total_geometries = len(working_gdf)
        processed = 0
        
        print(f"Processing {total_geometries} geometries with spatial index...")
        
        # Get the actual index values as a list for lookup
        df_index_values = working_gdf.index.tolist()
        
        for idx, row in working_gdf.iterrows():
            processed += 1
            if processed % 100 == 0:
                print(f"  Processed {processed}/{total_geometries} geometries...")
            
            geom = row['geometry']
            
            # Get potential candidates using spatial index
            possible_matches_positions = list(spatial_index.intersection(geom.bounds))
            
            for candidate_pos in possible_matches_positions:
                # Convert positional index to DataFrame index
                candidate_idx = df_index_values[candidate_pos]
                
                # Avoid self-comparison and duplicates
                if idx >= candidate_idx:
                    continue
                    
                candidate_geom = working_gdf.loc[candidate_idx, 'geometry']
                
                try: 
                    # Check for actual overlap
                    if geom.overlaps(candidate_geom):
                        intersection = geom.intersection(candidate_geom)
                        overlap_area = intersection.area 

                        # Calculate overlap percentages
                        area1 = geom.area
                        area2 = candidate_geom.area
                        overlap_pct1 = (overlap_area / area1 * 100) if area1 > 0 else 0
                        overlap_pct2 = (overlap_area / area2 * 100) if area2 > 0 else 0
                        
                        # Consider as sliver if overlap is < 5% of either polygon
                        if overlap_pct1 < 5 and overlap_pct2 < 5:
                            overlaps_to_resolve.append({
                                'idx1': idx,
                                'idx2': candidate_idx,
                                'overlap_area': overlap_area,
                                'overlap_pct1': overlap_pct1,
                                'overlap_pct2': overlap_pct2
                            })
                            
                except Exception as e:
                    continue
        
        print(f"Found {len(overlaps_to_resolve)} sliver overlaps to resolve")
        
        if len(overlaps_to_resolve) > 0:
            # Resolve overlaps by modifying geometries
            resolved_count = 0
            problematic_geometries = []
            
            print("Resolving overlaps with multiple repair strategies...")
            
            for i, overlap in enumerate(overlaps_to_resolve):
                if i % 50 == 0:
                    print(f"  Processed {i}/{len(overlaps_to_resolve)} overlaps...")
                    
                idx1, idx2 = overlap['idx1'], overlap['idx2']
                
                # Skip if either geometry was already removed
                if idx2 not in working_gdf.index or idx1 not in working_gdf.index:
                    continue
                    
                success = self._repair_overlap(working_gdf, idx1, idx2, overlap)
                
                if success:
                    resolved_count += 1
                else:
                    problematic_geometries.append(idx2)
            
            # Only remove geometries if absolutely necessary
            if problematic_geometries:
                # Remove duplicates
                problematic_geometries = list(set(problematic_geometries))
                print(f"⚠️  Could not clean {len(problematic_geometries)} geometries")
                print("Attempting buffer-based repair before removal...")
                
                # Try buffer repair on problematic geometries
                final_problematic = []
                for idx in problematic_geometries:
                    if self._try_buffer_repair(working_gdf, idx):
                        print(f"  ✓ Repaired geometry {idx} with buffer method")
                        resolved_count += 1
                    else:
                        final_problematic.append(idx)
                        print(f"  ✗ Could not repair geometry {idx}")
                
                # Only remove if all repair attempts failed
                if final_problematic:
                    print(f"Removing {len(final_problematic)} geometries that couldn't be cleaned")
                    working_gdf = working_gdf.drop(final_problematic)
                    removed_count = len(final_problematic)
                else:
                    removed_count = 0
            else:
                removed_count = 0
            
            self.gdf = working_gdf
            self.log_step("Resolve sliver overlaps", removed_count, len(self.gdf), 
                        f"Modified {resolved_count} overlapping geometries, removed {removed_count} problematic ones")
        else:
            print("No sliver overlaps found to resolve")

    def _repair_overlap(self, working_gdf, idx1, idx2, overlap):
        """Try multiple strategies to repair overlapping geometries"""
        strategies = [
            self._repair_difference,
            self._repair_symmetric_difference, 
            self._repair_smallest_overlap,
            self._repair_boundary_union
        ]
        
        for strategy in strategies:
            try:
                if strategy(working_gdf, idx1, idx2, overlap):
                    return True
            except Exception as e:
                continue
        
        return False

    # Difference-based repair
    def _repair_difference(self, working_gdf, idx1, idx2, overlap):
        """Original difference-based repair"""
        geom1 = working_gdf.loc[idx1, 'geometry']
        geom2 = working_gdf.loc[idx2, 'geometry']
        intersection = geom1.intersection(geom2)
        
        geom2_cleaned = geom2.difference(intersection)
        
        if self.validate_polygon_geometry(geom2_cleaned):
            working_gdf.loc[idx2, 'geometry'] = geom2_cleaned
            return True
        return False

    # Symmetric difference-based repair
    def _repair_symmetric_difference(self, working_gdf, idx1, idx2, overlap):
        """Use symmetric difference and take larger part"""
        geom1 = working_gdf.loc[idx1, 'geometry']
        geom2 = working_gdf.loc[idx2, 'geometry']
        
        # Get the union of boundaries to create a clean split line
        boundary = geom1.boundary.union(geom2.boundary)
        geom2_cleaned = geom2.intersection(boundary.buffer(0.0001))  # Small buffer to ensure polygon
        
        if self.validate_polygon_geometry(geom2_cleaned) and geom2_cleaned.area > geom2.area * 0.8:
            working_gdf.loc[idx2, 'geometry'] = geom2_cleaned
            return True
        return False

    # Remove overlap from polygon that loses least area
    def _repair_smallest_overlap(self, working_gdf, idx1, idx2, overlap):
        """Remove overlap from the polygon that loses least area"""
        geom1 = working_gdf.loc[idx1, 'geometry']
        geom2 = working_gdf.loc[idx2, 'geometry']
        intersection = geom1.intersection(geom2)
        
        # Remove from the geometry that will lose smaller percentage of area
        pct_loss1 = (intersection.area / geom1.area * 100) if geom1.area > 0 else 0
        pct_loss2 = (intersection.area / geom2.area * 100) if geom2.area > 0 else 0
        
        if pct_loss1 <= pct_loss2:
            # Remove from geom1
            geom1_cleaned = geom1.difference(intersection)
            if self.validate_polygon_geometry(geom1_cleaned):
                working_gdf.loc[idx1, 'geometry'] = geom1_cleaned
                return True
        else:
            # Remove from geom2 (original approach)
            geom2_cleaned = geom2.difference(intersection)
            if self.validate_polygon_geometry(geom2_cleaned):
                working_gdf.loc[idx2, 'geometry'] = geom2_cleaned
                return True
        return False

    # Boundary union-based repair
    def _repair_boundary_union(self, working_gdf, idx1, idx2, overlap):
        """Use boundary union for clean separation"""
        geom1 = working_gdf.loc[idx1, 'geometry']
        geom2 = working_gdf.loc[idx2, 'geometry']
        
        # Create a clean boundary between the two
        union_boundary = geom1.boundary.union(geom2.boundary)
        
        # Use the boundary to split the second geometry
        geom2_cleaned = geom2.intersection(union_boundary.convex_hull)
        
        if self.validate_polygon_geometry(geom2_cleaned) and geom2_cleaned.area > 0:
            working_gdf.loc[idx2, 'geometry'] = geom2_cleaned
            return True
        return False

    # Buffer-based repair attempts
    def _try_buffer_repair(self, working_gdf, idx):
        """Try to repair geometry using buffer tricks"""
        try:
            geom = working_gdf.loc[idx, 'geometry']
            
            # Strategy 1: Simple buffer(0)
            repaired = geom.buffer(0)
            if self.validate_polygon_geometry(repaired):
                working_gdf.loc[idx, 'geometry'] = repaired
                return True
            
            # Strategy 2: Small positive then negative buffer
            repaired = geom.buffer(0.0001).buffer(-0.0001)
            if self.validate_polygon_geometry(repaired):
                working_gdf.loc[idx, 'geometry'] = repaired
                return True
                
            # Strategy 3: Convex hull as last resort
            if geom.geom_type == 'Polygon':
                repaired = geom.convex_hull
                if self.validate_polygon_geometry(repaired) and repaired.area > geom.area * 0.5:
                    working_gdf.loc[idx, 'geometry'] = repaired
                    return True
                    
        except Exception as e:
            pass
        
        return False

    # Validate polygon geometry
    def validate_polygon_geometry(self, geom):
        """More lenient validation for sliver overlap repair"""
        if geom is None or geom.is_empty:
            return False
        
        # Check if it's a polygon or multipolygon
        if not isinstance(geom, (Polygon, MultiPolygon)):
            return False
        
        # Check if it's valid (be more tolerant)
        if not geom.is_valid:
            # Try to auto-repair with buffer(0)
            try:
                geom_repaired = geom.buffer(0)
                if geom_repaired.is_valid and isinstance(geom_repaired, (Polygon, MultiPolygon)):
                    return True
            except:
                pass
            return False
        
        # More lenient area check for sliver repair
        if geom.area <= 0:
            return False
        
        return True     

    # Validate polygon geometry     
    def validate_polygon_geometry(self, geom):
        """Validate that geometry is a valid polygon or multipolygon"""
        if geom is None or geom.is_empty:
            return False
        
        # Check if it's a polygon or multipolygon
        if not isinstance(geom, (Polygon, MultiPolygon)):
            return False
        
        # Check if it's valid
        if not geom.is_valid:
            return False
        
        # Check if it has reasonable area
        if geom.area <= 0:
            return False
        
        return True
    
    # Clean any remaining invalid geometries
    def clean_invalid_geometries(self):
        """Clean any remaining invalid geometries"""
        print("\nSTEP 5.5: CLEANING INVALID GEOMETRIES")
        print("-" * 38)
        
        # Check for invalid geometries
        invalid_mask = ~self.gdf.geometry.apply(self.validate_polygon_geometry)
        invalid_count = invalid_mask.sum()
        
        print(f"Invalid geometries found: {invalid_count}")
        
        if invalid_count > 0:
            # Show examples of invalid geometries
            invalid_examples = self.gdf[invalid_mask][['COUNTRY', 'BPL_NAME', 'SMALLEST']].head(5)
            print("Examples of invalid geometries:")
            for idx, row in invalid_examples.iterrows():
                geom = self.gdf.loc[idx, 'geometry']
                geom_type = type(geom).__name__ if geom is not None else "None"
                print(f"  {row['COUNTRY']} - {row['BPL_NAME']} (Type: {geom_type})")
            
            # Remove invalid geometries
            self.gdf = self.gdf[~invalid_mask].copy()
            self.log_step("Remove invalid geometries", invalid_count, len(self.gdf), 
                         "Non-polygon or invalid geometries")
        else:
            print("No invalid geometries found")
    
    # Final validation of cleaned data
    def final_validation(self):
        """Perform final validation of cleaned data"""
        print("\nFINAL VALIDATION")
        print("-" * 20)
        
        # Basic counts
        total = len(self.gdf)
        valid_geoms = self.gdf.geometry.notna().sum()
        valid_smallest = self.gdf['SMALLEST'].notna().sum()

        print(f"Final dataset summary:")
        print(f"  Total records: {total:,}")
        print(f"  Valid geometries: {valid_geoms:,} ({valid_geoms/total*100:.1f}%)")
        print(f"  Valid SMALLEST: {valid_smallest:,} ({valid_smallest/total*100:.1f}%)")
        
        # Check for remaining issues
        null_geoms = self.gdf.geometry.isnull().sum()
        if null_geoms > 0:
            print(f"  ⚠️  WARNING: {null_geoms} null geometries remain")
        else:
            print(f"  ✓ No null geometries")
        
        # Check country code consistency
        complete_data = self.gdf[(self.gdf['COUNTRY'].notna()) & 
                               (self.gdf['CNTRY_CD'].notna()) & 
                               (self.gdf['SMALLEST'].notna())].copy()
        
        if len(complete_data) > 0:
            complete_data['SMALLEST_str'] = complete_data['SMALLEST'].astype(str)
            complete_data['CNTRY_CD_str'] = complete_data['CNTRY_CD'].astype(str)
            
            consistent = complete_data.apply(
                lambda row: str(row['SMALLEST_str']).startswith(str(row['CNTRY_CD_str'])), axis=1
            ).sum()
            
            print(f"  Country code consistency: {consistent}/{len(complete_data)} ({consistent/len(complete_data)*100:.1f}%)")
        
        # Calculate readiness for climate analysis
        usable = self.gdf[(self.gdf.geometry.notna()) & (self.gdf['SMALLEST'].notna())].shape[0]
        print(f"  Climate analysis ready: {usable:,} ({usable/total*100:.1f}%)")
        
        self.cleaning_log['final_count'] = total
        
        return total > 0
    
    # Save cleaned shapefile
    def save_cleaned_data(self):
        """Save cleaned shapefile"""
        print(f"\nSAVING CLEANED SHAPEFILE")
        print("-" * 30)
        
        try:
            self.gdf.to_file(self.output_path)  
            print(f"✓ Cleaned shapefile saved to: {self.output_path}")  
            print(f"✓ Final record count: {len(self.gdf):,}")
            return True
        except Exception as e:
            print(f"❌ Error saving shapefile: {e}")
            return False
    
    def generate_cleaning_report(self):
        """Generate cleaning summary report"""
        print(f"\nCLEANING SUMMARY REPORT")
        print("=" * 30)
        
        original = self.cleaning_log['original_count']
        final = self.cleaning_log['final_count']
        total_removed = original - final
        
        print(f"Original records: {original:,}")
        print(f"Final records: {final:,}")
        print(f"Total removed: {total_removed:,} ({total_removed/original*100:.1f}%)")
        print(f"Retention rate: {final/original*100:.1f}%")
        
        print(f"\nDetailed cleaning steps:")
        for step in self.cleaning_log['steps']:
            print(f"  • {step['step']}: removed {step['removed']:,}")
            if step['details']:
                print(f"    ({step['details']})")
        
        # Save report to text file
        report_path = self.output_path.replace('data/derived/shapefiles/cleaned_ssa_boundaries.shp', 'data/documentation/shapefile-cleaning/ssa-shapefile-cleaning-report.txt')
        try:
            with open(report_path, 'w') as f:
                f.write("SSA SHAPEFILE CLEANING REPORT\n")
                f.write("=" * 35 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Input: {self.input_path}\n")
                f.write(f"Output: {self.output_path}\n\n")
                
                f.write(f"SUMMARY\n")
                f.write("-" * 10 + "\n")
                f.write(f"Original records: {original:,}\n")
                f.write(f"Final records: {final:,}\n")
                f.write(f"Total removed: {total_removed:,} ({total_removed/original*100:.1f}%)\n")
                f.write(f"Retention rate: {final/original*100:.1f}%\n\n")
                
                f.write(f"CLEANING STEPS\n")
                f.write("-" * 15 + "\n")
                for step in self.cleaning_log['steps']:
                    f.write(f"• {step['step']}: removed {step['removed']:,}\n")
                    if step['details']:
                        f.write(f"  Details: {step['details']}\n")
                
            print(f"✓ Cleaning report saved to: {report_path}")
        except Exception as e:
            print(f"⚠️  Could not save cleaning report: {e}")
    

    def generate_smallest_units_latex_table(self):
        """
        Generate a LaTeX table showing the breakdown of UNIQUE SMALLEST units by country.
        
        The table includes:
        - Country name
        - Total number of UNIQUE SMALLEST units
        - Number of unique SMALLEST units corresponding to GEOLEV2
        - Number of unique SMALLEST units corresponding to GEOLEV1 (but not GEOLEV2)
        - Number of unique SMALLEST units corresponding to neither
        
        Columns 3-5 sum to column 2 for each country.
        """
        print(f"\nGENERATING UNIQUE SMALLEST UNITS LATEX TABLE")
        print("=" * 45)
        
        # Filter for valid SMALLEST values
        valid_data = self.gdf[self.gdf['SMALLEST'].notna()].copy()
        
        # Group by country and calculate statistics
        country_stats = []
        
        for country in sorted(valid_data['COUNTRY'].unique()):
            country_data = valid_data[valid_data['COUNTRY'] == country]
            
            # Get unique SMALLEST values for this country
            unique_smallest = country_data['SMALLEST'].unique()
            total_unique = len(unique_smallest)
            
            # For each unique SMALLEST, determine its classification
            # We'll take the first occurrence of each SMALLEST to check GEOLEV values
            geolev2_count = 0
            geolev1_only_count = 0
            neither_count = 0
            
            for smallest_val in unique_smallest:
                # Get first row for this SMALLEST value
                smallest_row = country_data[country_data['SMALLEST'] == smallest_val].iloc[0]
                
                has_geolev2 = pd.notna(smallest_row['GEOLEV2'])
                has_geolev1 = pd.notna(smallest_row['GEOLEV1'])
                
                if has_geolev2:
                    geolev2_count += 1
                elif has_geolev1:  # has GEOLEV1 but not GEOLEV2
                    geolev1_only_count += 1
                else:  # has neither
                    neither_count += 1
            
            # Verify columns sum correctly
            calculated_total = geolev2_count + geolev1_only_count + neither_count
            if calculated_total != total_unique:
                print(f"⚠️  WARNING: Mismatch for {country}: {calculated_total} != {total_unique}")
            
            country_stats.append({
                'country': country,
                'total': total_unique,
                'geolev2': geolev2_count,
                'geolev1': geolev1_only_count,
                'neither': neither_count
            })
        
        # Create LaTeX table
        latex_lines = []
        latex_lines.append("\\begin{tabular}{lrrrr}")
        latex_lines.append("\\toprule")
        latex_lines.append("Country & \# SMALLEST & GEOLEV2 & GEOLEV1 & COUNTRY \\\\")
        latex_lines.append("    & (1) & (2) & (3) & (4) \\\\")
        latex_lines.append("\\midrule")
        
        # Add data rows
        for stats in country_stats:
            latex_lines.append(
                f"{stats['country']} & "
                f"{stats['total']:,} & "
                f"{stats['geolev2']:,} & "
                f"{stats['geolev1']:,} & "
                f"{stats['neither']:,} \\\\"
            )
        
        # Add total row
        total_all = sum(s['total'] for s in country_stats)
        total_geolev2 = sum(s['geolev2'] for s in country_stats)
        total_geolev1 = sum(s['geolev1'] for s in country_stats)
        total_neither = sum(s['neither'] for s in country_stats)
        
        latex_lines.append("\\midrule")
        latex_lines.append(
            f"\\textbf{{Total}} & "
            f"\\textbf{{{total_all:,}}} & "
            f"\\textbf{{{total_geolev2:,}}} & "
            f"\\textbf{{{total_geolev1:,}}} & "
            f"\\textbf{{{total_neither:,}}} \\\\"
        )
        
        latex_lines.append("\\bottomrule")
        latex_lines.append("\\end{tabular}")
        
        latex_table = "\n".join(latex_lines)
        
        # Save to file
        table_path = self.overleaf_path
        
        try:
            with open(table_path, 'w') as f:
                f.write(latex_table)
            print(f"✓ LaTeX table saved to: {table_path}")
            print(f"✓ Total countries: {len(country_stats)}")
            print(f"✓ Total UNIQUE SMALLEST units: {total_all:,}")
            print(f"  - With GEOLEV2: {total_geolev2:,} ({total_geolev2/total_all*100:.1f}%)")
            print(f"  - With GEOLEV1 only: {total_geolev1:,} ({total_geolev1/total_all*100:.1f}%)")
            print(f"  - With neither: {total_neither:,} ({total_neither/total_all*100:.1f}%)")
            
            # Print country-by-country breakdown
            print("\nCountry-by-country breakdown:")
            print("-" * 70)
            print(f"{'Country':<20} {'Total':<10} {'GEOLEV2':<10} {'GEOLEV1':<10} {'Neither':<10}")
            print("-" * 70)
            for stats in country_stats:
                print(f"{stats['country']:<20} {stats['total']:<10,} {stats['geolev2']:<10,} "
                    f"{stats['geolev1']:<10,} {stats['neither']:<10,}")
            print("-" * 70)
            print(f"{'TOTAL':<20} {total_all:<10,} {total_geolev2:<10,} "
                f"{total_geolev1:<10,} {total_neither:<10,}")
            
            # Also print LaTeX table preview
            print("\n" + "=" * 70)
            print("LaTeX table preview:")
            print("=" * 70)
            print(latex_table)
            print("=" * 70)
            
            return table_path
        except Exception as e:
            print(f"❌ Error saving LaTeX table: {e}")
            return None



    # Run the complete cleaning workflow
    def run_complete_cleaning(self):
        """Run complete cleaning workflow"""
        print("SSA SHAPEFILE COMPREHENSIVE CLEANING WORKFLOW")
        print("=" * 55)
        
        # Step 0: Load data
        if not self.load_data():
            return False
        
        # NEW: Check SMALLEST identifiability
        self.check_smallest_identifiability()
        
        # NEW: Check unknown BPL_NAME entries with valid geometries
        self.check_unknown_bpl_with_valid_geometries()
        
        # Step 1: Remove null geometries
        self.remove_null_geometries()
        
        # Check overlap between unknown BPL_NAME and null geometries
        self.check_unknown_bpl_geometry_overlap()
        
        # Step 2: Remove very small geometries
        # self.remove_very_small_geometries()
        
        # Step 3: Handle missing SMALLEST values
        self.handle_missing_smallest()
        
        # Step 4: Fix country code mismatches
        self.fix_country_code_mismatches()
        
        # Step 5: Resolve sliver overlaps
        self.resolve_sliver_overlaps()
        
        # Step 5.5: Clean any invalid geometries created
        self.clean_invalid_geometries()
        
        # Final validation
        if not self.final_validation():
            return False
        
        # Save cleaned data
        if not self.save_cleaned_data():
            return False
        
        # Generate report
        self.generate_cleaning_report()

        # Generate LaTeX table for Overleaf
        self.generate_smallest_units_latex_table()
        
        print(f"\n" + "=" * 55)
        print("CLEANING WORKFLOW COMPLETED SUCCESSFULLY!")
        print("=" * 55)
        
        return True

# Main execution function
def run_ssa_cleaning_workflow():
    """Execute the complete SSA cleaning workflow"""
    
    # File paths
    input_path = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data/source/shapefiles/ipums/bpl_data_smallest_unit_africa.shp"
    output_path = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data/derived/shapefiles/cleaned_ssa_boundaries.shp"
    overleaf_path = "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output/table/ssa_smallest_units_table.tex"
    print("Starting SSA shapefile cleaning workflow...")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"LaTeX Table Output: {overleaf_path}")
    print()
    
    # Create cleaner instance
    cleaner = SSAShapefileCleaner(input_path, output_path, overleaf_path)
    
    # Run complete cleaning
    success = cleaner.run_complete_cleaning()
    
    if success:
        print(f"\n✅ Cleaning completed successfully!")
        print(f"✅ Cleaned shapefile: {output_path}")
        print(f"✅ Final record count: {len(cleaner.gdf):,}")
    else:
        print(f"\n❌ Cleaning failed!")
    
    return cleaner

# Execute the cleaning workflow
if __name__ == "__main__":
    cleaning_result = run_ssa_cleaning_workflow()