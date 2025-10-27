********************************************************************************
* PROJECT:              Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:               Prayog Bhattarai
* DATE MODIFIED:        2025-10-14
* DESCRIPTION:          Generate temperature variables using country code mapping
* DEPENDENCIES:         
* 
* NOTES: We use the subunit-day data created in previous steps to generate 
*        subunit-cohort-census level temperature data
*        Input data is in data/derived/era/00-subunit-day-data
*        Output data is in data/derived/era/`country_name'/`country_name'-allcensus-cohort-temperature.dta
********************************************************************************

********************************************************************************
* Setup and Configuration
********************************************************************************

capture program drop ClimateAnalysisConfig
program define ClimateAnalysisConfig
    syntax, method(string)
    
    if "`method'" == "init" {
        di "Initializing configuration for user: `c(username)'"
        
        if "`c(username)'" == "prayogbhattarai" {
            global dir "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
        }
        else if inlist("`c(username)'", "yogita") {
            global dir "/Users/`c(username)'/NUS Dropbox/`c(username)'/Climate_Change_and_Fertility_in_SSA"
        }
        else if inlist("`c(username)'", "celin", "CE.4875") {
            global dir "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
        }
        else if inlist("`c(username)'", "yun") {
            global dir "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
        }
        else {
            di as error "Unknown user: `c(username)'. Set global dir manually."
            exit 198
        }
        
        global cf "$dir"
        global era_derived "$dir/data/derived/era"
        global era_raw "$dir/data/raw/era"
        
        di "Configuration successful:"
        di "  Base directory: $cf"
        di "  ERA derived: $era_derived"
    }
end

********************************************************************************
* Country Code to Name Mapping
********************************************************************************

capture program drop GetCountryName
program define GetCountryName, rclass
    syntax, code(string)
    
    * Define the mapping
    if "`code'" == "BEN" {
        local country_name "Benin"
    }
    else if "`code'" == "BWA" {
        local country_name "Botswana"
    }
    else if "`code'" == "BFA" {
        local country_name "Burkina Faso"
    }
    else if "`code'" == "CMR" {
        local country_name "Cameroon"
    }
    else if "`code'" == "ETH" {
        local country_name "Ethiopia"
    }
    else if "`code'" == "GHA" {
        local country_name "Ghana"
    }
    else if "`code'" == "GIN" {
        local country_name "Guinea"
    }
    else if "`code'" == "IVC" {
        local country_name "Ivory Coast"
    }
    else if "`code'" == "KEN" {
        local country_name "Kenya"
    }
    else if "`code'" == "LSO" {
        local country_name "Lesotho"
    }
    else if "`code'" == "LBR" {
        local country_name "Liberia"
    }
	else if "`code'" == "MLI" {
		local country_name "Mali"
	}
    else if "`code'" == "MWI" {
        local country_name "Malawi"
    }
    else if "`code'" == "MOZ" {
        local country_name "Mozambique"
    }
    else if "`code'" == "RWA" {
        local country_name "Rwanda"
    }
    else if "`code'" == "SLE" {
        local country_name "Sierra Leone"
    }
    else if "`code'" == "ZAF" {
        local country_name "South Africa"
    }
    else if "`code'" == "SSD" {
        local country_name "South Sudan"
    }
    else if "`code'" == "SEN" {
        local country_name "Senegal"
    }
    else if "`code'" == "SDN" {
        local country_name "Sudan"
    }
    else if "`code'" == "TZA" {
        local country_name "Tanzania"
    }
    else if "`code'" == "TGO" {
        local country_name "Togo"
    }
    else if "`code'" == "UGA" {
        local country_name "Uganda"
    }
    else if "`code'" == "ZMB" {
        local country_name "Zambia"
    }
    else if "`code'" == "ZWE" {
        local country_name "Zimbabwe"
    }
    else {
        local country_name ""
    }
    
    return local name "`country_name'"
end

********************************************************************************
* Degree Days Calculator
********************************************************************************

capture program drop calculate_degree_days
program define calculate_degree_days
    syntax, lowerbound(real) upperbound(real) intervals(real)
    
    di "  Calculating degree days for thresholds `lowerbound'(`intervals')`upperbound'"
    
    * Validate required temperature variables exist
    foreach var in temp_max temp_mean {
        capture confirm numeric variable `var'
        if _rc {
            di as error "    ERROR: Required variable `var' not found or not numeric"
            exit 111
        }
    }
    
    * Calculate degree days for each threshold
    forvalues threshold = `lowerbound'(`intervals')`upperbound' {
        foreach stat in max mean {
            qui gen temp_`stat'_dd_`threshold' = max(temp_`stat' - `threshold', 0)
            label variable temp_`stat'_dd_`threshold' "Degree days above `threshold'C (`stat' temp)"
        }
    }
    
    * Clean up original temperature variables
    drop temp_mean temp_max
    
    di "  Degree days calculation completed"
end

********************************************************************************
* Age Range Exposure Generator
********************************************************************************

capture program drop generate_age_range_exposure
program define generate_age_range_exposure
    
    di "  Generating age-specific temperature exposure variables"
    
    * Validate required variables
    foreach var in cohort year {
        capture confirm numeric variable `var'
        if _rc {
            di as error "    ERROR: Required variable `var' not found or not numeric"
            exit 111
        }
    }
    
    * Calculate age and create age group indicators
    cap drop age
    qui gen age = year - cohort
    qui gen age_15_29 = inrange(age, 15, 29)
    qui gen age_30_44 = inrange(age, 30, 44)
    
    label variable age "Age in year"
    label variable age_15_29 "Ages 15-29 indicator"
    label variable age_30_44 "Ages 30-44 indicator"
    
    * Create age-specific variants for all degree day variables
    qui ds temp_*_dd_*
    local dd_vars "`r(varlist)'"
    
    foreach var of local dd_vars {
        qui gen `var'_age_15_29 = `var' * age_15_29
        qui gen `var'_age_30_44 = `var' * age_30_44
        
        label variable `var'_age_15_29 "`var' exposure ages 15-29"
        label variable `var'_age_30_44 "`var' exposure ages 30-44"
    }
    
    di "  Age-specific exposure variables created"
end

********************************************************************************
* Temperature Data Processing
********************************************************************************

capture program drop TemperatureDataProcessor
program define TemperatureDataProcessor
    syntax, method(string) country_code(string) [census_years(string) max_cohort(string)]
    
    * Get country name from code
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"
    
    if "`country_name'" == "" {
        di as error "ERROR: Unknown country code: `country_code'"
        exit 198
    }
    
    if "`method'" == "load_and_aggregate" {
        di "  Loading and aggregating temperature data for `country_name' (`country_code')"
        
        * Check if ERA data file exists
        local data_file "$era_derived/00-subunit-day-data/`country_name'-era-subunit-day-1940-2019.dta"
        di "    Looking for: `data_file'"
        
        capture confirm file "`data_file'"
        if _rc {
            di as error "    ERROR: ERA data file not found: `data_file'"
            exit 601
        }
        
        use "`data_file'", clear
        
        qui count
        di "    Loaded `r(N)' observations"
        
        if r(N) == 0 {
            di as error "    ERROR: ERA data file is empty"
            exit 459
        }
        
        * Check required variables
        local required_vars "SMALLEST temp_max temp_mean day month year"
        foreach var of local required_vars {
            capture confirm variable `var'
            if _rc {
                di as error "    ERROR: Required variable `var' not found in ERA data"
                exit 111
            }
        }
        
        * Special handling for overlapping country codes
        if "`country_code'" == "BWA" | "`country_code'" == "SDN" {
            di "    Special handling for `country_code' - removing invalid regions"
            drop if inlist(floor(SMALLEST/10000), 728, 729)
        }
        
        di "    Keeping only the variables we need"
        keep SMALLEST temp_max temp_mean day month year
        drop if missing(temp_max) & missing(temp_mean)
        
        di "    Preparing to merge with crop phenology data..."
        decode SMALLEST, gen(smallest_decoded)
        if "`country_code'" == "BWA" {
            drop if inlist(substr(smallest_decoded, 1, 3), "728", "729")
        }
        if "`country_code'" == "SDN" {
            drop if inlist(substr(smallest_decoded, 1, 3), "728")
        }
        destring smallest_decoded, replace  
        drop SMALLEST 
        rename smallest_decoded SMALLEST            
        
        preserve 
            local phenology_file "$cf/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv"
            di "    Looking for phenology file: `phenology_file'"
            
            capture confirm file "`phenology_file'"
            if _rc {
                di as error "    ERROR: Phenology file not found: `phenology_file'"
                exit 601
            }
            
            import delimited using "`phenology_file'", clear
            
            qui count
            di "    Phenology data has `r(N)' observations"
            
            ren smallest SMALLEST
            replace country = proper(country)
            
            di "    Looking for phenology data for: `country_name'"
            keep if country == "`country_name'"
            
            qui count
            if r(N) == 0 {
                di "    WARNING: No phenology data found for `country_name'"
                di "    Creating empty phenology dataset"
                clear
                gen SMALLEST = .
                forvalues m = 1/12 {
                    gen growing_month_`m' = 0
                }
            }
            else {
                di "    Found `r(N)' phenology records for `country_name'"
                keep SMALLEST growing_month_*
            }
            
            if "`country_code'" == "SDN" {
                tostring SMALLEST, replace
                drop if inlist(substr(SMALLEST, 1, 3), "728")
                destring SMALLEST, replace
            }
            
            tempfile crop_phenology
            save `crop_phenology'
        restore

        di "    Merging ERA and phenology data..."
        merge m:1 SMALLEST using `crop_phenology'
        di "    Examining merge success ..."
        tab _merge
        drop _merge

        di "    Creating growing and non-growing season indicators ..."
        gen byte gs = 0
        gen byte ngs = 0
        forvalues m = 1/12 {
            capture confirm variable growing_month_`m'
            if !_rc {
                replace gs = 1 if growing_month_`m' == 1 & month == `m'
                replace ngs = 1 if growing_month_`m' == 0 & month == `m'
            }
        }
        
        * Ensure SMALLEST is numeric
        capture confirm string variable SMALLEST
        if !_rc {
            destring SMALLEST, replace force
        }
        
        di "    Calculating degree days..."
        calculate_degree_days, lowerbound(22) upperbound(28) intervals(1)
        
        di "    Creating growing/non-growing season variants..."
        qui ds temp_*_dd_*
        local dd_vars "`r(varlist)'"
        
        foreach var of local dd_vars {
            qui gen `var'_gs = `var' * gs
            qui gen `var'_ngs = `var' * ngs
            
            label variable `var'_gs "`var' growing season"
            label variable `var'_ngs "`var' non-growing season"
        }
        
        di "    Collapsing to annual level..."
        qui ds temp_*_dd_*
        local all_temp_vars "`r(varlist)'"
        
        collapse (sum) `all_temp_vars', by(SMALLEST year)
        recast float year
        
        qui count
        di "    Annual data has `r(N)' observations"

        * Create directory if it doesn't exist
        capture mkdir "$era_derived/`country_name'"
        
        local save_file "$era_derived/`country_name'/`country_name'_temperature_annual.dta"
        di "    Saving to: `save_file'"
        save "`save_file'", replace
        di "  Annual temperature data saved successfully"
    }
end

********************************************************************************
* Cohort Metrics Calculator (Temperature)
********************************************************************************

capture program drop CohortMetricsCalculator
program define CohortMetricsCalculator
    syntax, country_code(string) [reproductive_span(real 30) census_year(real 2019) save_file(string)]
    
    di "    Calculating cohort metrics for census `census_year'"
    
    * Get country name from code
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"
    
    if "`country_name'" == "" {
        di as error "ERROR: Unknown country code: `country_code'"
        exit 198
    }
    
    * Check current dataset
    qui count
    di "      Starting with `r(N)' observations"
    di "      Generating age variables ..."
    
    generate_age_range_exposure
    
    di "      Collapsing to cohort level..."
    
    * Get all temperature variables
    qui ds temp_*_dd_*
    local temp_vars "`r(varlist)'"
    
    if "`temp_vars'" == "" {
        di as error "      ERROR: No temperature variables to collapse"
        exit 459
    }
    
    collapse (sum) `temp_vars', by(SMALLEST cohort)
    
    gen census = `census_year'
    
    local save_file "$era_derived/`country_name'/`country_code'_metrics_`census_year'.dta"
    
    local output_path "`save_file'"
    di "      Saving cohort metrics to: `output_path'"
    save "`output_path'", replace
    
    qui count
    di "    Cohort metrics saved (`r(N)' observations)"
end

********************************************************************************
* Census Selection
********************************************************************************

capture program drop CensusSelector
program define CensusSelector
    syntax, country_code(string) [log_file(string)]
    
    gen age_in_census = census - cohort
    
    bys SMALLEST cohort: egen max_age_observed = max(age_in_census)
    gen cohort_reaches_reproductive = (max_age_observed >= 15)
    gen exposure_years = max(0, min(age_in_census, 44) - 15 + 1)
    replace exposure_years = 0 if age_in_census < 15
    gen full_exposure = (exposure_years == 30)
    
    drop max_age_observed
end

********************************************************************************
* Main Temperature Workflow
********************************************************************************

capture program drop TemperatureAnalysisWorkflow
program define TemperatureAnalysisWorkflow
    syntax, method(string) country_code(string) [census_years(string) reproductive_span(real 30) cleanup(string) temp_files(string) target_age(real 45)]
    
    * Get country name from code
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"
    
    if "`country_name'" == "" {
        di as error "ERROR: Unknown country code: `country_code'"
        exit 198
    }
    
    if "`method'" == "run_full_analysis" {
        di ""
        di "=== Starting temperature analysis for `country_name' (`country_code') ==="
        
        ClimateAnalysisConfig, method("init")
        
        * Check IPUMS mapping file
        preserve 
            local ipums_file "$cf/data/derived/ipums/country-census-years-mapping.dta"
            di "  Checking IPUMS mapping file: `ipums_file'"
            
            capture confirm file "`ipums_file'"
            if _rc {
                di as error "  ERROR: IPUMS mapping file not found"
                restore
                exit 601
            }
            
            use "`ipums_file'", clear
            
            qui count
            di "  IPUMS file has `r(N)' total records"
            di "  Keeping only the records of `country_name'"
            keep if COUNTRY == "`country_name'"
            
            qui count
            if r(N) == 0 {
                di as error "  ERROR: No census years found for `country_name' in IPUMS mapping"
                restore
                exit 459
            }
            
            levelsof census, local(census_years)
            local n_census = wordcount("`census_years'")
            di "  Found `n_census' census years: `census_years'"
        restore
        
        * Step 1: Load and aggregate
        capture noisily TemperatureDataProcessor, method("load_and_aggregate") country_code("`country_code'") 
        if _rc {
            di as error "  Failed at load_and_aggregate step"
            exit _rc
        }
        
        * Step 2: Process cohort metrics by census year
        di "  Processing cohort metrics by census year..."
        local temp_files ""
        local census_num = 0
        foreach census_year in `census_years' {
            local ++census_num
            di "    Census `census_num'/`n_census': `census_year'"
            
            * Check cohort file
            local cohort_file "$cf/data/derived/cohort_year.dta"
            capture confirm file "`cohort_file'"
            if _rc {
                di as error "    ERROR: Cohort year file not found: `cohort_file'"
                continue
            }
            
            use "`cohort_file'", clear
            
            keep if year <= `census_year'
            keep if cohort <= `census_year' - 15
            
            qui count
            di "      Cohort-year combinations: `r(N)'"
            
            if r(N) == 0 {
                di "      WARNING: No valid cohort-year combinations for census `census_year'"
                continue
            }
            
            local temp_file "$era_derived/`country_name'/`country_name'_temperature_annual.dta"
            
            capture confirm file "`temp_file'"
            if _rc {
                di as error "      ERROR: Temperature file not found: `temp_file'"
                continue
            }
            
            di "      Joining with temperature data"
            capture noisily joinby year using "`temp_file'"
            if _rc {
                di as error "      ERROR: Failed to join temperature data"
                continue
            }
            
            local temp_file_name "`country_code'_metrics_`census_year'"
            
            capture noisily CohortMetricsCalculator, ///
                country_code("`country_code'") ///
                reproductive_span(`reproductive_span') ///
                census_year(`census_year') ///
                save_file("`temp_file_name'")
            
            if _rc == 0 {
                local temp_files "`temp_files' `temp_file_name'"
            }
            else {
                di "      WARNING: Failed to calculate metrics for census `census_year'"
            }
        }

        if "`temp_files'" == "" {
            di as error "  ERROR: No census years successfully processed"
            exit 459
        }
        
        di "  Combining census years..."
        TemperatureAnalysisWorkflow, method("combine_and_select_census") ///
            country_code("`country_code'") ///
            temp_files("`temp_files'") ///
            target_age(`target_age')
        
        if "`cleanup'" == "true" {
            di "  Cleaning up temporary files..."
            TemperatureAnalysisWorkflow, method("cleanup_temp_files") ///
                country_code("`country_code'") ///
                temp_files("`temp_files'")
        }
        
        di "=== Analysis complete for `country_name' (`country_code') ==="
    }
    
    else if "`method'" == "combine_and_select_census" {
        di "    Combining census datasets..."
        
        local first_file: word 1 of `temp_files'
        
        capture confirm file "$era_derived/`country_name'/`first_file'.dta"
        if _rc {
            di as error "    ERROR: First temp file not found: `first_file'.dta"
            exit 601
        }
        
        use "$era_derived/`country_name'/`first_file'.dta", clear
        
        qui count
        local first_obs = r(N)
        di "      Loaded first file: `first_obs' observations"
        
        local remaining_files: list temp_files - first_file
        local remaining_count = wordcount("`remaining_files'")
        
        if `remaining_count' > 0 {
            di "      Appending `remaining_count' additional files..."
            foreach file in `remaining_files' {
                capture append using "$era_derived/`country_name'/`file'.dta"
                if _rc {
                    di "        WARNING: Could not append `file'.dta"
                }
            }
            qui count
            di "      Combined total: `r(N)' observations"
        }
        
        local before_cleaning = r(N)
        drop if missing(cohort) | missing(census) | missing(SMALLEST)
        qui count
        local after_cleaning = r(N)
        local dropped = `before_cleaning' - `after_cleaning'
        
        if `dropped' > 0 {
            di "      Dropped `dropped' observations with missing key variables"
        }
        di "      Clean observations: `after_cleaning'"
        
        sort SMALLEST cohort census
        
        di "    Applying census selection criteria..."
        CensusSelector, country_code("`country_code'")
        
        qui count
        local final_obs = r(N)
        
        if `final_obs' == 0 {
            di as error "    ERROR: No observations after census selection"
            exit 459
        }
        
        qui tab census
        local n_census_final = r(r)
        qui levelsof census, local(final_census_list)
        di "      Final dataset: `final_obs' observations across `n_census_final' census years"
        di "      Census years: `final_census_list'"
        
        local final_file "$era_derived/`country_name'/`country_name'_allcensus-cohort-temperature.dta"
        capture save "`final_file'", replace
        if _rc != 0 {
            di as error "      ERROR: Cannot save final dataset to `final_file'"
            di as error "      Check directory permissions and disk space"
            exit 603
        }
        di "    Saved final dataset: `country_name'_allcensus-cohort-temperature.dta"
    }

    else if "`method'" == "cleanup_temp_files" {
        local files_deleted = 0
        
        di "      Deleting temporary cohort metric files..."
        foreach file in `temp_files' {
            local file_path "$era_derived/`country_name'/`file'.dta"
            capture erase "`file_path'"
            if _rc == 0 {
                local ++files_deleted
            }
        }
        
        di "      Deleting intermediate temperature file..."
        local temp_file "$era_derived/`country_name'/`country_name'_temperature_annual.dta"
        capture erase "`temp_file'"
        if _rc == 0 {
            local ++files_deleted
        }
        
        di "      Cleanup complete: `files_deleted' temporary files removed"
    }
end

********************************************************************************
* Main Execution
********************************************************************************

di ""
di "==============================================="
di "Multi-Country Temperature Analysis Pipeline"
di "==============================================="

* Initialize configuration first
ClimateAnalysisConfig, method("init")

* Test with a small subset first
local test_codes "RWA"  // Start with just one country for testing

* For full run, use:
*local country_codes "BEN BWA BFA CMR ETH GHA GIN IVC KEN LSO LBR MLI MWI MOZ RWA SLE ZAF SSD SEN SDN TZA TGO UGA ZMB ZWE"

local country_codes "`test_codes'"

local total_countries : word count `country_codes'
di "Processing `total_countries' countries"
di ""

local country_num = 0
local countries_completed ""
local countries_failed ""

foreach country_code in `country_codes' {
    local ++country_num
    
    * Get country name for display
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"
    
    di "============================================================================"
    di "Country `country_num'/`total_countries': `country_name' (`country_code')"
    di "============================================================================"
    
    capture noisily {
        TemperatureAnalysisWorkflow, ///
            method("run_full_analysis") ///
            country_code("`country_code'") ///
            target_age(45) ///
            cleanup("true")
    }
    
    if _rc == 0 {
        local countries_completed "`countries_completed' `country_code'"
        di "✓ SUCCESS: `country_name' (`country_code')"
    }
    else {
        local countries_failed "`countries_failed' `country_code'"
        di "✗ FAILED: `country_name' (`country_code') - Error code: `_rc'"
        
        * Provide specific error messages
        if _rc == 601 {
            di "  > File not found error - check data paths"
        }
        else if _rc == 459 {
            di "  > No observations/data error"
        }
        else if _rc == 111 {
            di "  > Variable not found error"
        }
        else if _rc == 198 {
            di "  > Invalid country code"
        }
        else {
            di "  > Unknown error - check log for details"
        }
    }
    di ""
}

local completed_count : word count `countries_completed'
local failed_count : word count `countries_failed'

di "==============================================="
di "Pipeline Complete"
di "==============================================="
di "Successful: `completed_count'/`total_countries'"
di "Failed: `failed_count'/`total_countries'"

if `completed_count' > 0 {
    di ""
    di "Successfully completed countries:"
    foreach country_code in `countries_completed' {
        GetCountryName, code("`country_code'")
        di "  ✓ `r(name)' (`country_code')"
    }
}

if `failed_count' > 0 {
    di ""
    di "Failed countries:"
    foreach country_code in `countries_failed' {
        GetCountryName, code("`country_code'")
        di "  ✗ `r(name)' (`country_code')"
    }
    
    di ""
    di "Troubleshooting tips:"
    di "1. Check that all required data files exist:"
    di "   - ERA data: $era_derived/00-subunit-day-data/[country]-era-subunit-day-1940-2019.dta"
    di "   - Phenology: $cf/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv"
    di "   - IPUMS: $cf/data/derived/ipums/country-census-years-mapping.dta"
    di "   - Cohort: $cf/data/derived/cohort_year.dta"
    di "2. Verify country names match exactly in phenology and IPUMS files"
    di "3. Check that directories exist and have write permissions"
    di "4. Run with a single country first to identify specific issues"
}

********************************************************************************
* Optional: Diagnostic Check Program
********************************************************************************

capture program drop DiagnosticCheck
program define DiagnosticCheck
    syntax, country_code(string)
    
    di ""
    di "==============================================="
    di "Diagnostic Check for Country Code: `country_code'"
    di "==============================================="
    
    * Get country name
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"
    
    if "`country_name'" == "" {
        di as error "ERROR: Invalid country code `country_code'"
        exit
    }
    
    di "Country Name: `country_name'"
    di ""
    
    * Check configuration
    di "1. Configuration:"
    di "   Base directory: $cf"
    di "   ERA derived: $era_derived"
    
    * Check ERA data file
    di ""
    di "2. ERA Data File:"
    local era_file "$era_derived/00-subunit-day-data/`country_name'-era-subunit-day-1940-2019.dta"
    capture confirm file "`era_file'"
    if _rc {
        di "   ✗ NOT FOUND: `era_file'"
    }
    else {
        di "   ✓ Found: `era_file'"
        preserve
            use "`era_file'", clear
            qui count
            di "     Observations: `r(N)'"
            qui ds
            di "     Variables: `r(varlist)'"
        restore
    }
    
    * Check phenology file
    di ""
    di "3. Phenology File:"
    local phenology_file "$cf/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv"
    capture confirm file "`phenology_file'"
    if _rc {
        di "   ✗ NOT FOUND: `phenology_file'"
    }
    else {
        di "   ✓ Found: `phenology_file'"
        preserve
            import delimited using "`phenology_file'", clear
            replace country = proper(country)
            qui count if country == "`country_name'"
            di "     Records for `country_name': `r(N)'"
        restore
    }
    
    * Check IPUMS file
    di ""
    di "4. IPUMS Mapping File:"
    local ipums_file "$cf/data/derived/ipums/country-census-years-mapping.dta"
    capture confirm file "`ipums_file'"
    if _rc {
        di "   ✗ NOT FOUND: `ipums_file'"
    }
    else {
        di "   ✓ Found: `ipums_file'"
        preserve
            use "`ipums_file'", clear
            keep if COUNTRY == "`country_name'"
            qui count
            if r(N) == 0 {
                di "     ✗ No records for `country_name'"
            }
            else {
                levelsof census, local(census_years)
                di "     Census years for `country_name': `census_years'"
            }
        restore
    }
    
    * Check cohort file
    di ""
    di "5. Cohort Year File:"
    local cohort_file "$cf/data/derived/cohort_year.dta"
    capture confirm file "`cohort_file'"
    if _rc {
        di "   ✗ NOT FOUND: `cohort_file'"
    }
    else {
        di "   ✓ Found: `cohort_file'"
        preserve
            use "`cohort_file'", clear
            qui count
            di "     Total cohort-year combinations: `r(N)'"
            qui sum year
            di "     Year range: `r(min)' - `r(max)'"
            qui sum cohort
            di "     Cohort range: `r(min)' - `r(max)'"
        restore
    }
    
    * Check output directory
    di ""
    di "6. Output Directory:"
    local output_dir "$era_derived/`country_name'"
    capture confirm file "`output_dir'/nul"
    if _rc {
        di "   Directory does not exist: `output_dir'"
        di "   Attempting to create..."
        capture mkdir "`output_dir'"
        if _rc {
            di "   ✗ Failed to create directory"
        }
        else {
            di "   ✓ Directory created successfully"
        }
    }
    else {
        di "   ✓ Directory exists: `output_dir'"
    }
    
    di ""
    di "==============================================="
    di "End of Diagnostic Check"
    di "==============================================="
end

* Example: Run diagnostic check for a specific country
* DiagnosticCheck, country_code("KEN")
