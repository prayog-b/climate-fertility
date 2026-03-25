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
import os
warnings.filterwarnings('ignore')

# Find the Climate_Change_and_Fertility_in_SSA directory by searching upward
current_dir = os.path.dirname(os.path.abspath(__file__))
CF_DIR = None

while current_dir != os.path.dirname(current_dir):  # Stop at filesystem root
    if os.path.basename(current_dir) == 'Climate_Change_and_Fertility_in_SSA':
        CF_DIR = current_dir
        break
    current_dir = os.path.dirname(current_dir)

if not CF_DIR:
    raise FileNotFoundError(
        "Could not find 'Climate_Change_and_Fertility_in_SSA' directory. "
        "This script must be run from within the project directory structure."
    )


# ============================================================================
# GEOMETRY REPAIR CLASS
# ============================================================================
class GeometryRepairer:
    """Handles strategies for repairing overlapping geometries."""
    @staticmethod
    def validate_polygon_geometry(geom, lenient=False):
        """Validate that geometry is a valid polygon or multipolygon.
        
        Args:
            geom: Shapely geometry object
            lenient: If True, attempt to fix invalid geometries with buffer(0)
        """

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
    

# ============================================================================
# SHAPEFILE CLEANER CLASS
# ============================================================================
class SSAShapefileCleaner:
    """Class for cleaning SSA shapefiles with optional diagnostics"""
    
    def __init__(self, input_path, output_path, overleaf_path, verbose=True):
        self.input_path = input_path
        self.output_path = output_path
        self.overleaf_path = overleaf_path
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
    # DATA LOADING
    # ========================================================================   
    def load_data(self):
        """Load the shapefile"""
        self._print("LOADING SHAPEFILE", force=True)
        self._print("=" * 20)
        
        try:
            self.gdf = gpd.read_file(self.input_path)
            self.gdf.loc[self.gdf['COUNTRY'] == "Côte d'Ivoire", 'COUNTRY'] = 'Ivory Coast'
            self.cleaning_log['original_count'] = len(self.gdf)
            self._print(f"✓ Loaded {len(self.gdf):,} records", force=True)
            return True
        except Exception as e:
            self._print(f"❌ Error loading shapefile: {e}", force=True)
            return False
        
    # ========================================================================
    # OPTIONAL DIAGNOSTIC METHODS THAT DO NOT MODIFY DATA
    # ========================================================================
    def run_diagnostics(self):
        """Run all diagnostic checks (does not modify data)"""
        self._print("\n" + "=" * 60, force=True)
        self._print("RUNNING DIAGNOSTICS", force=True)
        self._print("=" * 60, force=True)
        
        self.check_smallest_identifiability()
        self.check_unknown_bpl_with_valid_geometries()
        
        self._print("\n" + "=" * 60, force=True)
        self._print("DIAGNOSTICS COMPLETE", force=True)
        self._print("=" * 60 + "\n", force=True)

    def check_smallest_identifiability(self):
        """Check if dataset is uniquely identifiable at SMALLEST level"""
        self._print("\nChecking SMALLEST level identifiability...")
        
        valid_data = self.gdf[self.gdf['SMALLEST'].notna()].copy()
        smallest_counts = valid_data['SMALLEST'].value_counts()
        duplicates = smallest_counts[smallest_counts > 1]
        
        self._print(f"  Unique SMALLEST values: {len(smallest_counts):,}")
        self._print(f"  Duplicate SMALLEST values: {len(duplicates):,}")
        
        # Check for multiple BPL_NAME per SMALLEST
        smallest_bpl_groups = valid_data.groupby('SMALLEST')['BPL_NAME'].apply(lambda x: x.nunique())
        multiple_bpl = smallest_bpl_groups[smallest_bpl_groups > 1]
        
        self._print(f"  SMALLEST units with multiple BPL_NAMEs: {len(multiple_bpl):,}")
        
        if self.verbose and len(multiple_bpl) > 0:
            # Save detailed list
            problematic_records = valid_data[valid_data['SMALLEST'].isin(multiple_bpl.index)][
                ['COUNTRY', 'SMALLEST', 'BPL_NAME', 'GEOLEV1', 'GEOLEV2']
            ].sort_values(['SMALLEST', 'BPL_NAME'])
            
            csv_path = self.output_path.replace(
                "data/derived/shapefiles/cleaned_ssa_boundaries.shp", 
                "data/documentation/shapefile-cleaning/smallest-multiple-bpl-names.csv"
            )
            problematic_records.to_csv(csv_path, index=False)
            self._print(f"  Saved details to: {csv_path}")
        
        return len(duplicates) == 0 and len(multiple_bpl) == 0
    
    def check_unknown_bpl_with_valid_geometries(self):
        """Check how many 'unknown' BPL_NAME entries have valid geometries"""
        self._print("\nChecking 'unknown' BPL_NAME entries...")
        
        unknown_bpl_mask = self.gdf['BPL_NAME'].astype(str).str.lower().str.contains('unknown', na=False)
        unknown_count = unknown_bpl_mask.sum()
        
        valid_geom_mask = self.gdf.geometry.notna()
        unknown_with_valid_geom = (unknown_bpl_mask & valid_geom_mask).sum()
        
        self._print(f"  Total 'unknown' BPL_NAME: {unknown_count:,}")
        self._print(f"  With valid geometries: {unknown_with_valid_geom:,}")
    
    # ========================================================================
    # CLEANING METHODS (MODIFY DATA)
    # ========================================================================
    def remove_null_geometries(self):
        """Remove null geometries"""
        self._print("\nSTEP 1: Removing null geometries...")
        
        null_mask = self.gdf.geometry.isnull()
        null_count = null_mask.sum()
        
        if null_count > 0:
            # Save list of null geometries
            if self.verbose:
                null_records = self.gdf.loc[null_mask, ["COUNTRY", "SMALLEST", "BPL_NAME"]]
                csv_path = self.output_path.replace(
                    "data/derived/shapefiles/cleaned_ssa_boundaries.shp", 
                    "data/documentation/shapefile-cleaning/null-geometries.csv"
                )
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
            self.gdf = self.gdf[~small_mask].copy()
            self.log_step("Remove very small geometries", small_count, len(self.gdf), 
                         f"Threshold: {threshold:.8f}")
        else:
            self._print("  No very small geometries found")
    
    def handle_missing_smallest(self):
        """Handle missing SMALLEST values"""
        self._print("\nSTEP 3: Handling missing SMALLEST values...")
        
        missing_smallest = self.gdf['SMALLEST'].isnull()
        missing_count = missing_smallest.sum()
        
        if missing_count > 0:
            # Remove records with no SMALLEST and no GEOLEV information
            no_admin_info = missing_smallest & self.gdf['GEOLEV1'].isnull() & self.gdf['GEOLEV2'].isnull()
            no_admin_count = no_admin_info.sum()
            
            if no_admin_count > 0:
                self.gdf = self.gdf[~no_admin_info].copy()
                self.log_step("Remove records with no admin info", no_admin_count, len(self.gdf))
            
            remaining_missing = self.gdf['SMALLEST'].isnull().sum()
            if remaining_missing > 0:
                self._print(f"  Keeping {remaining_missing} records with GEOLEV but no SMALLEST")
        else:
            self._print("  No missing SMALLEST values found")
    

    def fix_country_code_mismatches(self):
        """Fix Sudan/South Sudan country code mismatches"""
        self._print("\nSTEP 4: Fixing country code mismatches...")
        
        complete_data = self.gdf[
            (self.gdf['COUNTRY'].notna()) & 
            (self.gdf['CNTRY_CD'].notna()) & 
            (self.gdf['SMALLEST'].notna())
        ].copy()
        
        if len(complete_data) == 0:
            self._print("  No complete country/code data found")
            return
        
        complete_data['SMALLEST_str'] = complete_data['SMALLEST'].astype(str)
        complete_data['CNTRY_CD_str'] = complete_data['CNTRY_CD'].astype(str)
        
        # Find mismatches
        mismatch_mask = ~complete_data.apply(
            lambda row: row['SMALLEST_str'].startswith(row['CNTRY_CD_str']), 
            axis=1
        )
        
        mismatched_indices = complete_data[mismatch_mask].index
        mismatch_count = len(mismatched_indices)
        
        if mismatch_count > 0:
            # Save summary if verbose
            if self.verbose:
                mismatched_data = complete_data[mismatch_mask]
                summary = mismatched_data[['COUNTRY', 'CNTRY_CD', 'SMALLEST', 'BPL_NAME']].copy()
                csv_path = self.output_path.replace(
                    "data/derived/shapefiles/cleaned_ssa_boundaries.shp",
                    "data/documentation/shapefile-cleaning/country-code-mismatches.csv"
                )
                summary.to_csv(csv_path, index=False)
                self._print(f"  Saved mismatch details to: {csv_path}")
            
            self.gdf = self.gdf.drop(mismatched_indices)
            self.log_step("Remove country code mismatches", mismatch_count, len(self.gdf), 
                         "Sudan/South Sudan boundary issues")
        else:
            self._print("  No country code mismatches found")

    def resolve_sliver_overlaps(self, overlap_threshold=5):
        """Resolve sliver overlaps with multiple repair strategies"""
        self._print("\nSTEP 5: Resolving sliver overlaps...")
        
        working_gdf = self.gdf.copy()
        spatial_index = working_gdf.sindex
        df_index_values = working_gdf.index.tolist()
        overlaps_to_resolve = []
        
        total_geometries = len(working_gdf)
        
        # Find overlaps
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
                        
                        # Consider as sliver if overlap is < threshold% of either polygon
                        if overlap_pct1 < overlap_threshold and overlap_pct2 < overlap_threshold:
                            overlaps_to_resolve.append((idx, candidate_idx))
                except Exception:
                    continue
        
        self._print(f"  Found {len(overlaps_to_resolve)} sliver overlaps")
        
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
            self.log_step("Resolve sliver overlaps", removed_count, len(self.gdf), 
                        f"Modified {resolved_count} overlaps, removed {removed_count}")
        else:
            self._print("  No sliver overlaps found")
    
    def clean_invalid_geometries(self):
        """Clean any remaining invalid geometries"""
        self._print("\nSTEP 6: Cleaning invalid geometries...")
        
        invalid_mask = ~self.gdf.geometry.apply(
            lambda g: GeometryRepairer.validate_polygon_geometry(g, lenient=False)
        )
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            self.gdf = self.gdf[~invalid_mask].copy()
            self.log_step("Remove invalid geometries", invalid_count, len(self.gdf))
        else:
            self._print("  No invalid geometries found")

        
    # ========================================================================
    # VALIDATION AND REPORTING
    # ========================================================================
    
    def final_validation(self):
        """Perform final validation of cleaned data"""
        self._print("\nFINAL VALIDATION", force=True)
        self._print("-" * 20)
        
        total = len(self.gdf)
        valid_geoms = self.gdf.geometry.notna().sum()
        valid_smallest = self.gdf['SMALLEST'].notna().sum()
        
        self._print(f"  Total records: {total:,}", force=True)
        self._print(f"  Valid geometries: {valid_geoms:,} ({valid_geoms/total*100:.1f}%)")
        self._print(f"  Valid SMALLEST: {valid_smallest:,} ({valid_smallest/total*100:.1f}%)")
        
        self.cleaning_log['final_count'] = total
        return total > 0
    
    def save_cleaned_data(self):
        """Save cleaned shapefile"""
        self._print(f"\nSaving cleaned shapefile...", force=True)
        
        try:
            self.gdf.to_file(self.output_path)
            self._print(f"✓ Saved to: {self.output_path}", force=True)
            self._print(f"✓ Final count: {len(self.gdf):,}", force=True)
            return True
        except Exception as e:
            self._print(f"❌ Error saving: {e}", force=True)
            return False
        
    def generate_cleaning_report(self):
        """Generate cleaning summary report"""
        self._print(f"\nGenerating cleaning report...", force=True)
        
        original = self.cleaning_log['original_count']
        final = self.cleaning_log['final_count']
        total_removed = original - final
        
        report_path = self.output_path.replace(
            'data/derived/shapefiles/cleaned_ssa_boundaries.shp', 
            'data/documentation/shapefile-cleaning/cleaning-report.txt'
        )
        
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
            
            self._print(f"✓ Report saved to: {report_path}", force=True)
        except Exception as e:
            self._print(f"⚠️ Could not save report: {e}")

    def generate_smallest_units_latex_table(self):
        """Generate LaTeX table showing SMALLEST units by country"""
        self._print("\nGenerating LaTeX table...")
        
        valid_data = self.gdf[self.gdf['SMALLEST'].notna()].copy()
        country_stats = []
        
        for country in sorted(valid_data['COUNTRY'].unique()):
            country_data = valid_data[valid_data['COUNTRY'] == country]
            unique_smallest = country_data['SMALLEST'].unique()
            total_unique = len(unique_smallest)
            
            geolev2_count = 0
            geolev1_only_count = 0
            neither_count = 0
            
            for smallest_val in unique_smallest:
                smallest_row = country_data[country_data['SMALLEST'] == smallest_val].iloc[0]
                
                has_geolev2 = pd.notna(smallest_row['GEOLEV2'])
                has_geolev1 = pd.notna(smallest_row['GEOLEV1'])
                
                if has_geolev2:
                    geolev2_count += 1
                elif has_geolev1:
                    geolev1_only_count += 1
                else:
                    neither_count += 1
            
            country_stats.append({
                'country': country,
                'total': total_unique,
                'geolev2': geolev2_count,
                'geolev1': geolev1_only_count,
                'neither': neither_count
            })
        
        # Create LaTeX table
        latex_lines = [
            "\\begin{tabular}{lrrrr}",
            "\\toprule",
            "Country & \\# SMALLEST & GEOLEV2 & GEOLEV1 & COUNTRY \\\\",
            "    & (1) & (2) & (3) & (4) \\\\",
            "\\midrule"
        ]
        
        for stats in country_stats:
            latex_lines.append(
                f"{stats['country']} & "
                f"{stats['total']:,} & "
                f"{stats['geolev2']:,} & "
                f"{stats['geolev1']:,} & "
                f"{stats['neither']:,} \\\\"
            )
        
        total_all = sum(s['total'] for s in country_stats)
        total_geolev2 = sum(s['geolev2'] for s in country_stats)
        total_geolev1 = sum(s['geolev1'] for s in country_stats)
        total_neither = sum(s['neither'] for s in country_stats)
        
        latex_lines.extend([
            "\\midrule",
            f"\\textbf{{Total}} & "
            f"\\textbf{{{total_all:,}}} & "
            f"\\textbf{{{total_geolev2:,}}} & "
            f"\\textbf{{{total_geolev1:,}}} & "
            f"\\textbf{{{total_neither:,}}} \\\\",
            "\\bottomrule",
            "\\end{tabular}"
        ])
        
        latex_table = "\n".join(latex_lines)
        
        try:
            with open(self.overleaf_path, 'w') as f:
                f.write(latex_table)
            self._print(f"✓ LaTeX table saved to: {self.overleaf_path}")
            self._print(f"  Total SMALLEST units: {total_all:,}")
        except Exception as e:
            self._print(f"⚠️ Could not save LaTeX table: {e}")

    
    # ========================================================================
    # MAIN WORKFLOW
    # ========================================================================
    
    def run_complete_cleaning(self, run_diagnostics=False):
        """Run complete cleaning workflow
        
        Args:
            run_diagnostics: If True, run diagnostic checks before cleaning
        """
        self._print("=" * 60, force=True)
        self._print("SSA SHAPEFILE CLEANING WORKFLOW", force=True)
        self._print("=" * 60, force=True)
        
        # Load data
        if not self.load_data():
            return False
        
        # Optional diagnostics
        if run_diagnostics:
            self.run_diagnostics()
        
        # Cleaning steps
        self.remove_null_geometries()
        # self.remove_very_small_geometries()  # Optional - uncomment if needed
        self.handle_missing_smallest()
        self.fix_country_code_mismatches()
        self.resolve_sliver_overlaps()
        self.clean_invalid_geometries()
        
        # Validation and saving
        if not self.final_validation():
            return False
        
        if not self.save_cleaned_data():
            return False
        
        # Generate outputs
        self.generate_cleaning_report()
        self.generate_smallest_units_latex_table()
        
        self._print("\n" + "=" * 60, force=True)
        self._print("✅ CLEANING COMPLETED SUCCESSFULLY!", force=True)
        self._print("=" * 60, force=True)
        
        return True
    
# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_ssa_cleaning_workflow(verbose=True, run_diagnostics=False):
    """Execute the complete SSA cleaning workflow

    Args:
        verbose: If True, print detailed progress messages
        run_diagnostics: If True, run diagnostic checks before cleaning
    """

    # File paths - using the global CF_DIR found by upward search
    print(f"CF_DIR: {CF_DIR}")

    input_path = os.path.join(CF_DIR, "data/source/shapefiles/ipums/bpl_data_smallest_unit_africa.shp")
    output_path = os.path.join(CF_DIR, "data/derived/shapefiles/cleaned_ssa_boundaries.shp")

    # For overleaf path, try to construct it relative to CF_DIR
    # Assuming overleaf is a sibling of Climate_Change_and_Fertility_in_SSA or in a known location
    parent_dir = os.path.dirname(CF_DIR)
    overleaf_path = os.path.join(parent_dir, "Apps/Overleaf/climate-fertility-ssa/output/table/ssa_smallest_units_table.tex")
    
    # Create cleaner instance
    cleaner = SSAShapefileCleaner(input_path, output_path, overleaf_path, verbose=verbose)
    
    # Run cleaning
    success = cleaner.run_complete_cleaning(run_diagnostics=run_diagnostics)
    
    if success:
        print(f"\n✅ SUCCESS: {len(cleaner.gdf):,} records in cleaned shapefile")
    else:
        print(f"\n❌ FAILED")
    
    return cleaner

# Execute the cleaning workflow
if __name__ == "__main__":
    # Run with verbose=True and diagnostics if you want detailed output
    # Run with verbose=False for cleaner console output
    cleaning_result = run_ssa_cleaning_workflow(verbose=True, run_diagnostics=False)