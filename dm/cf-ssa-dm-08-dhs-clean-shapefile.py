# --------------------------------------------------------
# PROJECT :               Climate Change and Fertility in SSA
# PURPOSE:                Clean DHS Shapefile
# AUTHOR:                 Prayog Bhattarai
# DATE CREATED:           January 14, 2026
# DATE MODIFIED:          January 14, 2026
# DESCRIPTION:            Clean DHS Shapefile


# Notes:  
# --------------------------------------------------------

# --------------------------------------------------------
# Section 1:    Setup
# Description: Import packages and set up configuration
# --------------------------------------------------------

# Import packages
import geopandas as gpd
import pandas as pd
import numpy as np
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
import warnings
import os
warnings.filterwarnings('ignore')

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------

# List of target countries in sub-Saharan Africa
SSA_COUNTRIES = [
    'Benin', 'Burkina Faso', 'Burundi', 'Cameroon', 'Central African Republic',
    'Chad', 'Comoros', 'Congo', 'Ivory Coast', 'Congo Democratic Republic',
    'Ethiopia', 'Gabon', 'The Gambia', 'Ghana', 'Guinea', 'Kenya', 'Lesotho',
    'Liberia', 'Madagascar', 'Malawi', 'Mali', 'Mozambique', 'Namibia',
    'Niger', 'Nigeria', 'Rwanda', 'Senegal', 'Sierra Leone', 'South Africa',
    'Swaziland', 'Tanzania', 'Togo', 'Uganda', 'Zambia', 'Zimbabwe'
]

# Country name mappings for consistency
COUNTRY_NAME_MAPPINGS = {
    "Côte d'Ivoire": "Ivory Coast",
    "Cote d'Ivoire": "Ivory Coast",  # Alternative spelling
    "Côte d'ivoire": "Ivory Coast",  # Case variation
    "Congo, Democratic Republic of the": "Democratic Republic of Congo",
    "Congo, Republic of the": "Congo",
    "Eswatini": "Swaziland"
}

# ------------------------------------------------------------
# Section 2: Geometry Repairs
# Description: Handle repairs of overlapping geometries 
# ------------------------------------------------------------

class GeometryRepairer:
    """Handles strategies for repairing overlapping geometries."""
    @staticmethod
    def validate_polygon_geometry(geom, lenient=False):
        """Validate that geometry is a valid polygon or multipolygon."""
        if geom is None or geom.is_empty:
            return False
        
        if not isinstance(geom, (Polygon, MultiPolygon)):
            return False
        
        if not geom.is_valid:
            if lenient:
                try:
                    geom_repaired = geom.buffer(0)
                    if geom_repaired.is_valid and isinstance(geom_repaired, (Polygon, MultiPolygon)):
                        return True
                except:
                    pass
            return False
        
        if geom.area <= 0:
            return False
        
        return True 
    
    @staticmethod
    def repair_with_difference(working_gdf, idx1, idx2):
        """Remove overlap using difference operation"""
        geom1 = working_gdf.loc[idx1, 'geometry']
        geom2 = working_gdf.loc[idx2, 'geometry']
        intersection = geom1.intersection(geom2)
        
        geom2_cleaned = geom2.difference(intersection)
        
        if GeometryRepairer.validate_polygon_geometry(geom2_cleaned, lenient=True):
            working_gdf.loc[idx2, 'geometry'] = geom2_cleaned
            return True
        return False 
    
    @staticmethod
    def repair_with_smallest_loss(working_gdf, idx1, idx2):
        """Remove overlap from the polygon that loses least area percentage"""
        geom1 = working_gdf.loc[idx1, 'geometry']
        geom2 = working_gdf.loc[idx2, 'geometry']
        intersection = geom1.intersection(geom2)
        
        pct_loss1 = (intersection.area / geom1.area * 100) if geom1.area > 0 else 0
        pct_loss2 = (intersection.area / geom2.area * 100) if geom2.area > 0 else 0
        
        if pct_loss1 <= pct_loss2:
            geom_cleaned = geom1.difference(intersection)
            target_idx = idx1
        else:
            geom_cleaned = geom2.difference(intersection)
            target_idx = idx2
        
        if GeometryRepairer.validate_polygon_geometry(geom_cleaned, lenient=True):
            working_gdf.loc[target_idx, 'geometry'] = geom_cleaned
            return True
        return False
    
    @staticmethod
    def repair_with_buffer(working_gdf, idx):
        """Try to repair geometry using buffer operations"""
        try:
            geom = working_gdf.loc[idx, 'geometry']
            
            # Strategy 1: Simple buffer(0)
            repaired = geom.buffer(0)
            if GeometryRepairer.validate_polygon_geometry(repaired, lenient=True):
                working_gdf.loc[idx, 'geometry'] = repaired
                return True
            
            # Strategy 2: Small positive then negative buffer
            repaired = geom.buffer(0.0001).buffer(-0.0001)
            if GeometryRepairer.validate_polygon_geometry(repaired, lenient=True):
                working_gdf.loc[idx, 'geometry'] = repaired
                return True
            
            # Strategy 3: Convex hull as last resort
            if geom.geom_type == 'Polygon':
                repaired = geom.convex_hull
                if GeometryRepairer.validate_polygon_geometry(repaired, lenient=True) and repaired.area > geom.area * 0.5:
                    working_gdf.loc[idx, 'geometry'] = repaired
                    return True
        except Exception:
            pass
        
        return False
    
    @classmethod
    def repair_overlap(cls, working_gdf, idx1, idx2):
        """Try multiple strategies to repair overlapping geometries"""
        strategies = [
            lambda: cls.repair_with_difference(working_gdf, idx1, idx2),
            lambda: cls.repair_with_smallest_loss(working_gdf, idx1, idx2),
        ]
        
        for strategy in strategies:
            try:
                if strategy():
                    return True
            except Exception:
                continue
        
        return False

# -------------------------------------------------------------------------------
# Section 3: DHS shapefile cleaner class
# -------------------------------------------------------------------------------

class DHSShapefileCleaner:
    """Class for cleaning DHS shapefiles with quality checks"""
    
    def __init__(self, input_path, output_path, verbose=True):
        self.input_path = input_path
        self.output_path = output_path
        self.verbose = verbose
        self.gdf = None
        self.cleaning_log = {
            'original_count': 0,
            'steps': [],
            'final_count': 0
        }
        self.repairer = GeometryRepairer()

    def _print(self, message, force=False):
        """Print message if verbose mode is on or force is True"""
        if self.verbose or force:
            print(message)
    
    def log_step(self, step_name, removed_count, remaining_count, details=""):
        """Log a cleaning step"""
        step_info = {
            'step': step_name,
            'removed': removed_count,
            'remaining': remaining_count,
            'details': details
        }
        self.cleaning_log['steps'].append(step_info)
        self._print(f"✓ {step_name}: removed {removed_count:,}, remaining {remaining_count:,}")
        if details and self.verbose:
            self._print(f"  {details}")

    # ========================================================================
    # DATA LOADING AND FILTERING
    # ========================================================================   
    def load_and_filter_data(self):
        """Load the shapefile and filter to SSA countries"""
        self._print("LOADING AND FILTERING SHAPEFILE", force=True)
        self._print("=" * 40)
        
        try:
            # Load the shapefile
            self.gdf = gpd.read_file(self.input_path)
            self.cleaning_log['original_count'] = len(self.gdf)
            self._print(f"✓ Loaded {len(self.gdf):,} records", force=True)
            
            # Check required columns
            required_columns = ['CNTRYNAMEE', 'DHSREGEN', 'ISO', 'REG_ID', 'LEVELCO', 'LEVELNA']
            missing_columns = [col for col in required_columns if col not in self.gdf.columns]
            if missing_columns:
                self._print(f"⚠️ Missing columns: {missing_columns}", force=True)
            
            # Standardize country names
            self._standardize_country_names()
            
            # Filter to SSA countries
            return self._filter_to_ssa_countries()
            
        except Exception as e:
            self._print(f"❌ Error loading shapefile: {e}", force=True)
            return False
        
    def _standardize_country_names(self):
        """Standardize country names using mappings"""
        self._print("Standardizing country names...")
        
        # Apply country name mappings
        changes_made = 0
        for old_name, new_name in COUNTRY_NAME_MAPPINGS.items():
            mask = self.gdf['CNTRYNAMEE'] == old_name
            if mask.any():
                change_count = mask.sum()
                self.gdf.loc[mask, 'CNTRYNAMEE'] = new_name
                self._print(f"  Renamed {old_name} -> {new_name}: {change_count} regions")
                changes_made += change_count
        
        # Also check for case-insensitive matches and partial matches
        ivory_coast_variations = ["Côte d'Ivoire", "Cote d'Ivoire", "Ivory Coast", "Côte d’Ivoire"]
        current_countries = self.gdf['CNTRYNAMEE'].unique()
        
        for variation in ivory_coast_variations:
            if variation in current_countries and variation != "Ivory Coast":
                mask = self.gdf['CNTRYNAMEE'] == variation
                change_count = mask.sum()
                self.gdf.loc[mask, 'CNTRYNAMEE'] = "Ivory Coast"
                self._print(f"  Renamed {variation} -> Ivory Coast: {change_count} regions")
                changes_made += change_count
        
        if changes_made == 0:
            self._print("  No country name standardizations needed")
        else:
            self._print(f"  Total country name changes: {changes_made}")
    
    def _filter_to_ssa_countries(self):
        """Filter the dataframe to only include SSA countries"""
        self._print("Filtering to SSA countries...")
        
        original_count = len(self.gdf)
        
        # Filter to our target countries
        ssa_mask = self.gdf['CNTRYNAMEE'].isin(SSA_COUNTRIES)
        self.gdf = self.gdf[ssa_mask].copy()
        
        filtered_count = len(self.gdf)
        removed_count = original_count - filtered_count
        
        self.log_step("Filter to SSA countries", removed_count, filtered_count)
        
        # Print country summary
        country_counts = self.gdf['CNTRYNAMEE'].value_counts()
        self._print(f"\nCountries included ({len(country_counts)}):")
        for country, count in country_counts.items():
            self._print(f"  {country}: {count:,} regions")
        
        return filtered_count > 0

    # ========================================================================
    # DATA QUALITY CHECKS
    # ========================================================================
    def check_region_identifiability(self):
        """Check if REG_ID values are unique and properly identify regions"""
        self._print("\nChecking REG_ID identifiability...")
        
        # Check for missing REG_ID values
        missing_reg_id = self.gdf['REG_ID'].isnull().sum()
        self._print(f"  Missing REG_ID values: {missing_reg_id:,}")
        
        # Check for duplicate REG_ID values
        reg_id_counts = self.gdf['REG_ID'].value_counts()
        duplicate_reg_ids = reg_id_counts[reg_id_counts > 1]
        self._print(f"  Duplicate REG_ID values: {len(duplicate_reg_ids):,}")
        
        # Check if duplicate REG_IDs have different DHSREGEN names (problematic)
        if len(duplicate_reg_ids) > 0:
            problematic_duplicates = []
            for reg_id in duplicate_reg_ids.index:
                unique_names = self.gdf[self.gdf['REG_ID'] == reg_id]['DHSREGEN'].nunique()
                if unique_names > 1:
                    problematic_duplicates.append(reg_id)
            
            self._print(f"  REG_ID with multiple DHSREGEN names: {len(problematic_duplicates):,}")
            
            if self.verbose and problematic_duplicates:
                problematic_data = self.gdf[self.gdf['REG_ID'].isin(problematic_duplicates)][
                    ['CNTRYNAMEE', 'REG_ID', 'DHSREGEN', 'LEVELCO', 'LEVELNA']
                ].sort_values(['REG_ID', 'DHSREGEN'])
                
                csv_path = self.output_path.replace('.shp', '_problematic_reg_ids.csv')
                problematic_data.to_csv(csv_path, index=False)
                self._print(f"  Saved details to: {csv_path}")
        
        return len(duplicate_reg_ids) == 0
    
    def check_admin_level_consistency(self):
        """Check consistency of administrative level information"""
        self._print("\nChecking administrative level consistency...")
        
        # Check LEVELCO and LEVELNA values
        levelco_counts = self.gdf['LEVELCO'].value_counts()
        levelna_counts = self.gdf['LEVELNA'].value_counts()
        
        self._print("  LEVELCO distribution:")
        for level, count in levelco_counts.items():
            self._print(f"    {level}: {count:,}")
        
        self._print("  LEVELNA distribution:")
        for level, count in levelna_counts.items():
            self._print(f"    {level}: {count:,}")
        
        # Check for regions with inconsistent admin levels
        country_level_summary = self.gdf.groupby('CNTRYNAMEE').agg({
            'LEVELCO': 'nunique',
            'LEVELNA': 'nunique',
            'REG_ID': 'count'
        }).rename(columns={'REG_ID': 'region_count'})
        
        multiple_levels = country_level_summary[
            (country_level_summary['LEVELCO'] > 1) | 
            (country_level_summary['LEVELNA'] > 1)
        ]
        
        if len(multiple_levels) > 0:
            self._print(f"  Countries with multiple admin levels: {len(multiple_levels):,}")
            if self.verbose:
                csv_path = self.output_path.replace('.shp', '_multiple_admin_levels.csv')
                multiple_levels.to_csv(csv_path)
                self._print(f"  Saved details to: {csv_path}")
    
    def check_geometry_validity(self):
        """Check for invalid geometries"""
        self._print("\nChecking geometry validity...")
        
        # Check for null geometries
        null_geometries = self.gdf.geometry.isnull().sum()
        self._print(f"  Null geometries: {null_geometries:,}")
        
        # Check for empty geometries
        empty_geometries = self.gdf.geometry.is_empty.sum()
        self._print(f"  Empty geometries: {empty_geometries:,}")
        
        # Check for invalid geometries - FIXED LOGIC
        invalid_count = 0
        for geom in self.gdf.geometry:
            if not GeometryRepairer.validate_polygon_geometry(geom, lenient=False):
                invalid_count += 1
        
        self._print(f"  Invalid geometries: {invalid_count:,}")
        
        # Check geometry types
        geom_types = self.gdf.geometry.geom_type.value_counts()
        self._print("  Geometry types:")
        for geom_type, count in geom_types.items():
            self._print(f"    {geom_type}: {count:,}")
        
        return null_geometries + empty_geometries + invalid_count == 0
    
    def check_area_statistics(self):
        """Calculate and report area statistics"""
        self._print("\nChecking area statistics...")
        
        # Calculate areas in square kilometers
        self.gdf['area_sqkm'] = self.gdf.geometry.area * (111.32 ** 2)  # Approximate conversion
        
        area_stats = self.gdf['area_sqkm'].describe()
        self._print(f"  Min area: {area_stats['min']:.2f} sq km")
        self._print(f"  Mean area: {area_stats['mean']:.2f} sq km")
        self._print(f"  Max area: {area_stats['max']:.2f} sq km")
        
        # Identify very small regions (potential slivers)
        small_threshold = self.gdf['area_sqkm'].quantile(0.01)  # Bottom 1%
        very_small = (self.gdf['area_sqkm'] < small_threshold).sum()
        self._print(f"  Very small regions (< {small_threshold:.2f} sq km): {very_small:,}")
        
        # Identify very large regions
        large_threshold = self.gdf['area_sqkm'].quantile(0.99)  # Top 1%
        very_large = (self.gdf['area_sqkm'] > large_threshold).sum()
        self._print(f"  Very large regions (> {large_threshold:.2f} sq km): {very_large:,}")
    
    def check_boundary_overlaps(self, overlap_threshold=0.01):
        """Check for boundary overlaps between regions"""
        self._print(f"\nChecking boundary overlaps (threshold: {overlap_threshold}%)...")
        
        working_gdf = self.gdf.copy()
        spatial_index = working_gdf.sindex
        df_index_values = working_gdf.index.tolist()
        overlaps_found = []
        
        total_overlap_area = 0
        overlap_pairs = 0
        
        for idx, row in working_gdf.iterrows():
            geom = row['geometry']
            possible_matches = list(spatial_index.intersection(geom.bounds))
            
            for candidate_pos in possible_matches:
                candidate_idx = df_index_values[candidate_pos]
                
                if idx >= candidate_idx:
                    continue
                
                candidate_geom = working_gdf.loc[candidate_idx, 'geometry']
                
                try:
                    if geom.overlaps(candidate_geom):
                        intersection = geom.intersection(candidate_geom)
                        overlap_area = intersection.area
                        total_overlap_area += overlap_area
                        
                        area1 = geom.area
                        area2 = candidate_geom.area
                        overlap_pct1 = (overlap_area / area1 * 100) if area1 > 0 else 0
                        overlap_pct2 = (overlap_area / area2 * 100) if area2 > 0 else 0
                        
                        # Only count significant overlaps
                        if overlap_pct1 > overlap_threshold or overlap_pct2 > overlap_threshold:
                            overlap_pairs += 1
                            overlaps_found.append({
                                'index1': idx,
                                'index2': candidate_idx,
                                'country1': row['CNTRYNAMEE'],
                                'country2': working_gdf.loc[candidate_idx, 'CNTRYNAMEE'],
                                'overlap_area_sqkm': overlap_area * (111.32 ** 2),
                                'overlap_pct1': overlap_pct1,
                                'overlap_pct2': overlap_pct2
                            })
                except Exception as e:
                    if self.verbose:
                        self._print(f"  Error checking overlap between {idx} and {candidate_idx}: {e}")
                    continue
        
        self._print(f"  Significant overlap pairs: {overlap_pairs:,}")
        self._print(f"  Total overlap area: {total_overlap_area * (111.32 ** 2):.2f} sq km")
        
        if overlaps_found and self.verbose:
            overlaps_df = pd.DataFrame(overlaps_found)
            csv_path = self.output_path.replace('.shp', '_overlaps.csv')
            overlaps_df.to_csv(csv_path, index=False)
            self._print(f"  Saved overlap details to: {csv_path}")
        
        return overlap_pairs

    # ========================================================================
    # DATA CLEANING METHODS
    # ========================================================================
    def remove_null_geometries(self):
        """Remove null geometries"""
        self._print("\nSTEP 1: Removing null geometries...")
        
        null_mask = self.gdf.geometry.isnull()
        null_count = null_mask.sum()
        
        if null_count > 0:
            if self.verbose:
                null_records = self.gdf.loc[null_mask, ["CNTRYNAMEE", "REG_ID", "DHSREGEN"]]
                csv_path = self.output_path.replace('.shp', '_null_geometries.csv')
                null_records.to_csv(csv_path, index=False)
                self._print(f"  Saved null geometry list to: {csv_path}")
            
            self.gdf = self.gdf[~null_mask].copy()
            self.log_step("Remove null geometries", null_count, len(self.gdf))
        else:
            self._print("  No null geometries found")

    def remove_very_small_geometries(self, percentile_threshold=1):
        """Remove very small geometries (potential slivers)"""
        self._print(f"\nSTEP 2: Removing very small geometries (< {percentile_threshold}th percentile)...")
        
        areas = self.gdf.geometry.area
        threshold = areas.quantile(percentile_threshold / 100)
        small_mask = areas < threshold
        small_count = small_mask.sum()
        
        if small_count > 0:
            if self.verbose:
                small_records = self.gdf.loc[small_mask, ["CNTRYNAMEE", "REG_ID", "DHSREGEN", "area_sqkm"]]
                csv_path = self.output_path.replace('.shp', '_small_geometries.csv')
                small_records.to_csv(csv_path, index=False)
                self._print(f"  Saved small geometry list to: {csv_path}")
            
            self.gdf = self.gdf[~small_mask].copy()
            self.log_step("Remove very small geometries", small_count, len(self.gdf), 
                         f"Threshold: {threshold:.8f}")
        else:
            self._print("  No very small geometries found")

    def clean_invalid_geometries(self):
        """Clean any remaining invalid geometries"""
        self._print("\nSTEP 3: Cleaning invalid geometries...")
        
        # FIXED: Use the same counting logic as check_geometry_validity
        invalid_indices = []
        for idx, geom in self.gdf.geometry.items():
            if not GeometryRepairer.validate_polygon_geometry(geom, lenient=False):
                invalid_indices.append(idx)
        
        invalid_count = len(invalid_indices)
        
        if invalid_count > 0:
            # Try to repair invalid geometries first
            repairable_count = 0
            remaining_invalid = []
            
            for idx in invalid_indices:
                if GeometryRepairer.repair_with_buffer(self.gdf, idx):
                    repairable_count += 1
                else:
                    remaining_invalid.append(idx)
            
            # Remove geometries that couldn't be repaired
            if remaining_invalid:
                if self.verbose:
                    invalid_records = self.gdf.loc[remaining_invalid, ["CNTRYNAMEE", "REG_ID", "DHSREGEN"]]
                    csv_path = self.output_path.replace('.shp', '_invalid_geometries.csv')
                    invalid_records.to_csv(csv_path, index=False)
                    self._print(f"  Saved invalid geometry list to: {csv_path}")
                
                self.gdf = self.gdf.drop(remaining_invalid)
                self.log_step("Remove invalid geometries", len(remaining_invalid), len(self.gdf),
                            f"Repaired {repairable_count} geometries")
            else:
                self._print(f"  Successfully repaired all {repairable_count} invalid geometries")
        else:
            self._print("  No invalid geometries found")

    def resolve_boundary_overlaps(self, overlap_threshold=5):
        """Resolve boundary overlaps between regions"""
        self._print(f"\nSTEP 4: Resolving boundary overlaps (> {overlap_threshold}% area)...")
        
        working_gdf = self.gdf.copy()
        spatial_index = working_gdf.sindex
        df_index_values = working_gdf.index.tolist()
        overlaps_to_resolve = []
        
        # Find significant overlaps
        for idx, row in working_gdf.iterrows():
            geom = row['geometry']
            possible_matches = list(spatial_index.intersection(geom.bounds))
            
            for candidate_pos in possible_matches:
                candidate_idx = df_index_values[candidate_pos]
                
                if idx >= candidate_idx:
                    continue
                
                candidate_geom = working_gdf.loc[candidate_idx, 'geometry']
                
                try:
                    if geom.overlaps(candidate_geom):
                        intersection = geom.intersection(candidate_geom)
                        overlap_area = intersection.area
                        
                        area1 = geom.area
                        area2 = candidate_geom.area
                        overlap_pct1 = (overlap_area / area1 * 100) if area1 > 0 else 0
                        overlap_pct2 = (overlap_area / area2 * 100) if area2 > 0 else 0
                        
                        # Only resolve significant overlaps
                        if overlap_pct1 > overlap_threshold or overlap_pct2 > overlap_threshold:
                            overlaps_to_resolve.append((idx, candidate_idx))
                except Exception:
                    continue
        
        self._print(f"  Found {len(overlaps_to_resolve)} significant overlaps to resolve")
        
        if len(overlaps_to_resolve) > 0:
            resolved_count = 0
            problematic_geometries = []
            
            # Repair overlaps
            for idx1, idx2 in overlaps_to_resolve:
                if idx1 not in working_gdf.index or idx2 not in working_gdf.index:
                    continue
                
                if GeometryRepairer.repair_overlap(working_gdf, idx1, idx2):
                    resolved_count += 1
                else:
                    problematic_geometries.append(idx2)
            
            # Try buffer repair on problematic geometries
            if problematic_geometries:
                problematic_geometries = list(set(problematic_geometries))
                final_problematic = []
                
                for idx in problematic_geometries:
                    if not GeometryRepairer.repair_with_buffer(working_gdf, idx):
                        final_problematic.append(idx)
                    else:
                        resolved_count += 1
                
                # Remove geometries that couldn't be repaired
                if final_problematic:
                    working_gdf = working_gdf.drop(final_problematic)
                    removed_count = len(final_problematic)
                else:
                    removed_count = 0
            else:
                removed_count = 0
            
            self.gdf = working_gdf
            self.log_step("Resolve boundary overlaps", removed_count, len(self.gdf), 
                        f"Modified {resolved_count} overlaps, removed {removed_count}")
        else:
            self._print("  No significant overlaps found")

    # ========================================================================
    # VALIDATION AND REPORTING
    # ========================================================================
    def final_validation(self):
        """Perform final validation of cleaned data"""
        self._print("\nFINAL VALIDATION", force=True)
        self._print("-" * 20)
        
        total = len(self.gdf)
        valid_geoms = self.gdf.geometry.notna().sum()
        valid_reg_id = self.gdf['REG_ID'].notna().sum()
        
        self._print(f"  Total records: {total:,}", force=True)
        self._print(f"  Valid geometries: {valid_geoms:,} ({valid_geoms/total*100:.1f}%)")
        self._print(f"  Valid REG_ID: {valid_reg_id:,} ({valid_reg_id/total*100:.1f}%)")
        
        # Check final geometry validity
        final_invalid = ~self.gdf.geometry.apply(
            lambda g: GeometryRepairer.validate_polygon_geometry(g, lenient=False)
        ).sum()
        self._print(f"  Final invalid geometries: {final_invalid:,}")
        
        self.cleaning_log['final_count'] = total
        return total > 0

    def generate_cleaning_report(self):
        """Generate cleaning summary report"""
        self._print(f"\nGenerating cleaning report...", force=True)
        
        original = self.cleaning_log['original_count']
        final = self.cleaning_log['final_count']
        total_removed = original - final
        
        report_path = self.output_path.replace('.shp', '_cleaning_report.txt')
        
        try:
            with open(report_path, 'w') as f:
                f.write("DHS SHAPEFILE CLEANING REPORT\n")
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
                
                f.write(f"COUNTRIES INCLUDED\n")
                f.write("-" * 20 + "\n")
                country_counts = self.gdf['CNTRYNAMEE'].value_counts()
                for country, count in country_counts.items():
                    f.write(f"{country}: {count:,} regions\n")
                f.write(f"\nTotal countries: {len(country_counts)}\n\n")
                
                f.write(f"CLEANING STEPS\n")
                f.write("-" * 15 + "\n")
                for step in self.cleaning_log['steps']:
                    f.write(f"• {step['step']}: removed {step['removed']:,}\n")
                    if step['details']:
                        f.write(f"  Details: {step['details']}\n")
            
            self._print(f"✓ Report saved to: {report_path}", force=True)
        except Exception as e:
            self._print(f"⚠️ Could not save report: {e}")

    def save_cleaned_data(self):
        """Save cleaned shapefile"""
        self._print(f"\nSaving cleaned shapefile...", force=True)
        
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            self.gdf.to_file(self.output_path)
            self._print(f"✓ Saved to: {self.output_path}", force=True)
            self._print(f"✓ Final count: {len(self.gdf):,} regions", force=True)
            return True
        except Exception as e:
            self._print(f"❌ Error saving: {e}", force=True)
            return False

    # ========================================================================
    # MAIN WORKFLOW
    # ========================================================================
    def run_complete_cleaning(self, run_checks=True, run_cleaning=True):
        """Run complete cleaning workflow
        
        Args:
            run_checks: If True, run data quality checks
            run_cleaning: If True, run cleaning procedures
        """
        self._print("=" * 60, force=True)
        self._print("DHS SHAPEFILE CLEANING WORKFLOW", force=True)
        self._print("=" * 60, force=True)
        
        # Load and filter data
        if not self.load_and_filter_data():
            return False
        
        # Run data quality checks
        if run_checks:
            self._print("\n" + "=" * 50)
            self._print("RUNNING DATA QUALITY CHECKS")
            self._print("=" * 50)
            
            self.check_region_identifiability()
            self.check_admin_level_consistency()
            self.check_geometry_validity()
            self.check_area_statistics()
            self.check_boundary_overlaps()
        
        # Run cleaning procedures
        if run_cleaning:
            self._print("\n" + "=" * 50)
            self._print("RUNNING CLEANING PROCEDURES")
            self._print("=" * 50)
            
            self.remove_null_geometries()
            self.clean_invalid_geometries()
            self.resolve_boundary_overlaps()
            # Optional: uncomment if you want to remove small geometries
            # self.remove_very_small_geometries()
        
        # Final validation and saving
        if not self.final_validation():
            return False
        
        if not self.save_cleaned_data():
            return False
        
        # Generate report
        self.generate_cleaning_report()
        
        self._print("\n" + "=" * 60, force=True)
        self._print("✅ CLEANING COMPLETED SUCCESSFULLY!", force=True)
        self._print("=" * 60, force=True)
        
        return True

# -------------------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------------------

def run_dhs_cleaning_workflow(input_shapefile, output_shapefile, verbose=True, run_checks=True, run_cleaning=True):
    """Execute the complete DHS shapefile cleaning workflow
    
    Args:
        input_shapefile: Path to input shapefile
        output_shapefile: Path to output cleaned shapefile
        verbose: If True, print detailed progress messages
        run_checks: If True, run data quality checks
        run_cleaning: If True, run cleaning procedures
    """
    
    # Create cleaner instance
    cleaner = DHSShapefileCleaner(input_shapefile, output_shapefile, verbose=verbose)
    
    # Run cleaning
    success = cleaner.run_complete_cleaning(run_checks=run_checks, run_cleaning=run_cleaning)
    
    if success:
        print(f"\n✅ SUCCESS: {len(cleaner.gdf):,} regions in cleaned shapefile")
        print(f"✅ Countries: {cleaner.gdf['CNTRYNAMEE'].nunique()}")
    else:
        print(f"\n❌ CLEANING FAILED")
    
    return cleaner

# Example usage
if __name__ == "__main__":
    # Find the Climate_Change_and_Fertility_in_SSA directory by searching upward
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = None

    # Search upward for the project root directory
    while current_dir != os.path.dirname(current_dir):  # Stop at filesystem root
        if os.path.basename(current_dir) == 'Climate_Change_and_Fertility_in_SSA':
            base_dir = current_dir
            break
        current_dir = os.path.dirname(current_dir)

    if not base_dir:
        print("❌ ERROR: Could not find 'Climate_Change_and_Fertility_in_SSA' directory!")
        print("This script must be run from within the project directory structure.")
        exit(1)

    print(f"✓ Found project directory: {base_dir}")

    # Construct file paths using discovered base directory
    input_shapefile = os.path.join(base_dir, "data/source/shapefiles/dhs/all_dhs_merged_id.shp")
    output_shapefile = os.path.join(base_dir, "data/derived/shapefiles/dhs/cleaned-ssa-dhs-regions.shp")

    # Run the cleaning workflow
    cleaning_result = run_dhs_cleaning_workflow(
        input_shapefile=input_shapefile,
        output_shapefile=output_shapefile,
        verbose=True,
        run_checks=True,
        run_cleaning=True
    )