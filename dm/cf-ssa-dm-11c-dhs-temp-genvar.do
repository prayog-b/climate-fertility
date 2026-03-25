********************************************************************************
* PROJECT:              Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:               Prayog Bhattarai
* DATE MODIFIED:        2025-10-14
* DESCRIPTION:          DHS: Generate temperature variables using country code mapping
* DEPENDENCIES:         
* 
* NOTES: We use the subunit-day data created in previous steps to generate 
*        subunit-cohort-dhsyear level temperature data
*        Output data is in data/derived/dhs/05-dhs-region-cohort-temperature/`country_name'-dhs-cohort-temperature.dta
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
            global cf "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
        }
        else if "`username'" == "prayog" {
            global cf "D:/`username'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
        }
        else if inlist("`c(username)'", "yogita") {
            global cf "/Users/`c(username)'/NUS Dropbox/`c(username)'/Climate_Change_and_Fertility_in_SSA"
        }
        else if inlist("`c(username)'", "celin", "CE.4875") {
            global cf "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
        }
        else if inlist("`c(username)'", "yun") {
            global cf "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
        }
        else {
            di as error "Unknown user: `c(username)'. Set global cf manually."
            exit 198
        }
        
        di "Configuration successful:"
        di "  Base directory: $cf"
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
    else if "`code'" == "BFA" {
        local country_name "Burkina Faso"
    }
    else if "`code'" == "BDI" {
        local country_name "Burundi"
    }
    else if "`code'" == "CMR" {
        local country_name "Cameroon"
    }
    else if "`code'" == "CAF" {
        local country_name "Central African Republic"
    }
    else if "`code'" == "TCD" {
        local country_name "Chad"
    }
    else if "`code'" == "COM" {
        local country_name "Comoros"
    }
    else if "`code'" == "COG" {
        local country_name "Congo"
    }
    else if "`code'" == "DRC" {
        local country_name "Congo Democratic Republic"
    }
    else if "`code'" == "ETH" {
        local country_name "Ethiopia"
    }
    else if "`code'" == "GAB" {
        local country_name "Gabon"
    }
    else if "`code'" == "GMB" {
        local country_name "The Gambia"
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
    else if "`code'" == "MDG" {
        local country_name "Madagascar"
    }
    else if "`code'" == "MWI" {
        local country_name "Malawi"
    }
    else if "`code'" == "MLI" {
		local country_name "Mali"
	}
    else if "`code'" == "MOZ" {
        local country_name "Mozambique"
    }
    else if "`code'" == "NMB" {
        local country_name "Namibia"
    }
    else if "`code'" == "NER" {
        local country_name "Niger"
    }
    else if "`code'" == "NGA" {
        local country_name "Nigeria"
    }
    else if "`code'" == "RWA" {
        local country_name "Rwanda"
    }
    else if "`code'" == "SEN" {
        local country_name "Senegal"
    }
    else if "`code'" == "SLE" {
        local country_name "Sierra Leone"
    }
    else if "`code'" == "ZAF" {
        local country_name "South Africa"
    }
    else if "`code'" == "SWZ" {
        local country_name "Swaziland"
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
    
    * Calculate degree days for each threshold (scaled by 100)
    * Note: unscaled = scaled * 100, so compute on the fly if needed
    * Uses capture to skip thresholds that already exist (allows multiple calls)
    forvalues threshold = `lowerbound'(`intervals')`upperbound' {
        foreach stat in max mean {
            capture confirm variable temp_`stat'_dd_`threshold'
            if _rc {
                qui gen temp_`stat'_dd_`threshold' = max(temp_`stat' - `threshold', 0) / 100
                label variable temp_`stat'_dd_`threshold' "Degree days above `threshold'C (`stat' temp), scaled by 100"
            }
        }
    }
    
    * Clean up original temperature variables
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
    * ── Fine bins: six 5-year bins (configurable) ──
    local bin_starts "15 20 25 30 35 40"
    local bin_ends   "19 24 29 34 39 44"
    local bin_labels "15_19 20_24 25_29 30_34 35_39 40_44"
    local n_bins : word count `bin_starts'

    * ── Broad bins: two halves of the reproductive span ──
    local broad_starts "15 30"
    local broad_ends   "29 44"
    local broad_labels "15_29 30_44"
    local n_broad : word count `broad_starts'

    cap drop age
    qui gen age = year - cohort

    * Create fine-bin indicators
    forvalues i = 1/`n_bins' {
        local s : word `i' of `bin_starts'
        local e : word `i' of `bin_ends'
        local lbl : word `i' of `bin_labels'
        qui gen age_`lbl' = inrange(age, `s', `e')
        label variable age_`lbl' "Ages `s'-`e' indicator"
    }

    * Create broad-bin indicators
    forvalues i = 1/`n_broad' {
        local s : word `i' of `broad_starts'
        local e : word `i' of `broad_ends'
        local lbl : word `i' of `broad_labels'
        qui gen age_`lbl' = inrange(age, `s', `e')
        label variable age_`lbl' "Ages `s'-`e' indicator"
    }

    label variable age "Age in year"

    * Create age-specific variants for all degree day variables
    qui ds temp_*_dd_*
    local dd_vars "`r(varlist)'"

    foreach var of local dd_vars {
        * Fine bins
        forvalues i = 1/`n_bins' {
            local s : word `i' of `bin_starts'
            local e : word `i' of `bin_ends'
            local lbl : word `i' of `bin_labels'
            qui gen `var'_age_`lbl' = `var' * age_`lbl'
            label variable `var'_age_`lbl' "`var' exposure ages `s'-`e'"
        }
        * Broad bins
        forvalues i = 1/`n_broad' {
            local s : word `i' of `broad_starts'
            local e : word `i' of `broad_ends'
            local lbl : word `i' of `broad_labels'
            qui gen `var'_age_`lbl' = `var' * age_`lbl'
            label variable `var'_age_`lbl' "`var' exposure ages `s'-`e'"
        }
    }
    
    di "  Age-specific exposure variables created"
end

********************************************************************************
* Temperature Data Processing
********************************************************************************
capture program drop TemperatureDataProcessor
program define TemperatureDataProcessor
    syntax, method(string) country_code(string) [dhs_years(string) max_cohort(string)]

    * Get country name from code
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"

    if "`country_name'" == "" {
        di as error "ERROR: Unknown country code `country_code'"
        exit 198
    }

    if "`method'" == "load_and_aggregate" {
        di "    Loading and aggregating temperature data for `country_name' (`country_code')"
        * Check if the country ERA data file exists
        local data_file "$cf/data/derived/dhs/02-dhs-region-daily-data/`country_name'-era-dhs-region-day-1940-2023.dta"
        di "     Looking for data file: `data_file'"

        capture confirm file "`data_file'"
        if _rc {
            di as error "    ERROR: Data file for `country_name' not found."
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
        local required_vars "DHSREGEN DHS_code temp_max temp_mean day month year"
        foreach var of local required_vars {
            capture confirm variable `var'
            if _rc {
                di as error "    ERROR: Required variable `var' not found in data"
                exit 111
            }
        }

        di "   Keeping variables we need"
        keep DHSREGEN DHS_code temp_max temp_mean day month year
        drop if missing(temp_max) | missing(temp_mean)

        di "   Preparing to merge with crop phenology data"
        decode DHSREGEN, gen(dhsreg_decoded)
        destring dhsreg_decoded, replace
        drop DHSREGEN
        rename dhsreg_decoded DHSREGEN

        preserve
            local phenology_file "$cf/data/derived/dhs/03-dhs-fao-growing-season/dhs-crop-phenology-summary-stats.csv"
            di "    Looking for phenology file: `phenology_file'"
            
            capture confirm file "`phenology_file'"
            if _rc {
                di as error "    ERROR: Phenology file not found: `phenology_file'"
                exit 601
            }

            import delimited using "`phenology_file'", clear
            qui count
            di "    Loaded `r(N)' observations from phenology data"

            di "   Looking for phenology data for: `country_name'"
            ren (cntrynamee dhsregen dhs_code) (COUNTRY DHSREGEN DHS_code)
            
            keep if COUNTRY == "`country_name'"

            qui count
            if r(N) == 0 {
                di as error "    ERROR: No phenology data found for `country_name'"
                di          "    Creating empty phenology dataset"
                clear 
                gen DHS_code = .
                forvalues m = 1/12 {
                    gen growing_month_`m' = 0
                }
            }
            else {
                di as text "    Found phenology data for `country_name' with `r(N)' observations"
                keep DHSREGEN DHS_code growing_month_*
            }

            tempfile crop_phenology
            save "`crop_phenology'"
        restore

        di "   Merging ERA and phenology data ..."
        merge m:1 DHS_code using `crop_phenology'
        di " Examining merge success ..."
        tab _merge
        drop _merge 

        di "   Creating growing and non-growing season indicators ..."
        gen byte gs = 0 
        gen byte ngs = 0 
        forvalues m = 1/12 {
            capture confirm variable growing_month_`m'
            if !_rc {
                replace gs = 1 if growing_month_`m' == 1 & month == `m'
                replace ngs = 1 if growing_month_`m' == 0 & month == `m'
            }
        }    

        * Ensure DHS_code is numeric 
        capture confirm string variable DHS_code
        if !_rc {
            destring DHS_code, replace force
        }


        ** Create temperature bins 
        foreach var in temp_max {
            * First bin: < 18
            gen `var'_less_18 = (`var' < 18)
            
            * 3-degree bins: 18-21, 21-24, 24-27, 27-30, 30-33
            forvalues lower = 18(3)30 {
                local upper = `lower' + 3
                gen `var'_`lower'_`upper' = (`var' >= `lower' & `var' < `upper')
            }
            
            * Last bin: >= 33
            gen `var'_more_33 = (`var' >= 33)
        }

        egen temp_max_sum = rowtotal(temp_max_less_18 temp_max_18_21 temp_max_21_24 ///
                              temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33)

        * Should equal 1 for all non-missing temp_max
        assert temp_max_sum == 1 if !missing(temp_max)
        
        * Check for any missing temperatures
        count if missing(temp_max)
        if r(N) > 0 {
            di as error "Warning: `r(N)' observations have missing temp_max"
        }

        drop temp_max_sum

        di "    Calculating degree days ..."
        calculate_degree_days, lowerbound(24) upperbound(38) intervals(2)
        calculate_degree_days, lowerbound(26) upperbound(33) intervals(1)

        di "    Creating growing/non-growing season variants ..."
        qui ds temp_*_dd_*
        local dd_vars "`r(varlist)'"

        foreach var of local dd_vars {
            qui gen `var'_gs = `var' * gs
            qui gen `var'_ngs = `var' * ngs

            label variable `var'_gs "`var'  growing season"
            label variable `var'_ngs "`var'  non-growing season"
        }

        di "      Collapsing to annual level ..."
        ds temp_*_dd_* temp_max_less_18 temp_max_18_21 temp_max_21_24 temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33
        local all_temp_vars "`r(varlist)'"

        collapse (sum) `all_temp_vars' (mean) temp_mean (max) temp_max, by(DHS_code DHSREGEN year)
        recast float year 

        egen days_per_year = rowtotal(temp_max_less_18 temp_max_18_21 temp_max_21_24 ///
                              temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33)

        * Summary statistics
        sum days_per_year
        tab days_per_year

        * Check for years with < 365 days
        count if days_per_year < 365
        list year days_per_year if days_per_year < 365


        qui count 
        di "   Annual data has `r(N)' observations"

        * Create directory if it doesn't exist
        capture mkdir "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/01-annual-data"
        local save_file "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/01-annual-data/`country_name'-temperature_annual.dta"

        di "    Saving to: `save_file' ..."
        save "`save_file'", replace 
        di "Annual temperature data saved successfully."
    }       
end 

********************************************************************************
* Cohort Metrics Calculator (Temperature)
********************************************************************************

capture program drop CohortMetricsCalculator
program define CohortMetricsCalculator
    syntax, country_code(string) [reproductive_span(real 30) dhs_year(real 2020) save_file(string)]

    di "     Calculating cohort metrics for DHS year: `dhs_year' with reproductive span `reproductive_span' years"

    * Get country name from code 
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"

    if "`country_name'" == "" {
        di as error "ERROR: Unknown country code `country_code'"
        exit 198
    }

    * Check current dataset
    qui count
    di "    Starting with `r(N)' observations"
    di "    Generating age variables"

    generate_age_range_exposure

    di "    Collapsing to cohort level ..."

    * Get all temperature variables
    qui ds temp_*_dd_* temp_max_less_18 temp_max_18_21 temp_max_21_24 temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33
    local temp_vars "`r(varlist)'"

    if "`temp_vars'" == "" {
        di as error "    ERROR: No temperature variables found for cohort aggregation"
        exit 459
    }

    collapse (sum) `temp_vars' (mean) temp_mean (max) temp_max, by(DHS_code DHSREGEN cohort)

    * After second collapse to cohort level
    egen total_days = rowtotal(temp_max_less_18 temp_max_18_21 temp_max_21_24 temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33)
    di "Summary of total days"
    sum total_days if cohort <= 1976

    * Diagnostic checks at cohort level:
    scatter total_days cohort
    gen years_coverage = total_days / 365.25
    sum years_coverage
    sum cohort if years_coverage < 30
    local min_cohort = r(min)
    list cohort total_days years_coverage if cohort == `min_cohort'

    gen int_year = `dhs_year'

    local save_file "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/02-dhs-year-metrics/`country_code'-dhs-metrics-`dhs_year'.dta"

    local output_path "`save_file'"
    di "    Saving cohort metrics to: `output_path' ..."

    save "`output_path'", replace
    qui count
    di "Cohort metrics saved successfully with `r(N)' observations."
end 

********************************************************************************
* Census Selection
********************************************************************************

capture program drop CensusSelector
program define CensusSelector
    syntax, country_code(string) [log_file(string)]

    gen age_in_dhs_year = int_year - cohort 

    bys DHS_code cohort: egen max_age_observed = max(age_in_dhs_year)
    gen cohort_reaches_reproductive = (max_age_observed >= 15)

    gen exposure_years = max(0, min(age_in_dhs_year, 44) - 15 + 1)
    replace exposure_years = 0 if age_in_dhs_year < 15

    gen full_exposure = (exposure_years >= 30)

    drop max_age_observed

end 


********************************************************************************
* Main Temperature Workflow
********************************************************************************
capture program drop TemperatureAnalysisWorkflow
program define TemperatureAnalysisWorkflow
    syntax, method(string) country_code(string) [dhs_years(string) max_cohort(string) reproductive_span(real 30) cleanup(string) temp_files(string) target_age(real 45)]

    * Get country name from code 
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"

    if "`country_name'" == "" {
        di as error "ERROR: Unknown country code `country_code'"
        exit 198
    }

    if "`method'" == "run_full_analysis" {
        di ""
        di "==== Starting temperature analysis for `country_name' (`country_code') ===="

        ClimateAnalysisConfig, method(init)

        * Check for DHS country to years mapping file
        preserve 
            local mapping_file "$cf/data/derived/dhs/00-dhs-country-surveyyear-mappings/surveycd_reg_year.dta"
            di "   Checking DHS mapping file: `mapping_file' ..."

            capture confirm file "`mapping_file'"
            if _rc {
                di as error "   ERROR: DHS country to years mapping file not found."
                restore 
                exit 601
            }

            use "`mapping_file'", clear
            qui count
            di "   Loaded `r(N)' observations from DHS mapping file"
            * Some countries have different names in the mapping file vs. the ERA5 data files
            local mapping_cname "`country_name'"
            if "`country_name'" == "Ivory Coast" local mapping_cname "Cote d'Ivoire"
            di "   Keeping only the records for `mapping_cname' ..."
            keep if COUNTRY == "`mapping_cname'"
            di "Filtered to `country_name'"

            qui count 
            if r(N) == 0 {
                di as error "   ERROR: No DHS survey years found for `country_name' in mapping file."
                restore 
                exit 459
            }
            ren year int_year 
            levelsof int_year , local(dhs_years) 
            local n_dhsyears = wordcount("`dhs_years'")
            di "   Found `n_dhsyears' DHS survey years for `country_name'"
        restore 

        * Step 1: Load and aggregate temperature data
        capture noisily TemperatureDataProcessor, method("load_and_aggregate") country_code("`country_code'")
        if _rc {
            di as error "   ERROR: Temperature data processing failed for `country_name'"
            exit _rc
        }

        * Step 2: Process cohort metrics by DHS year 
        di "   Processing cohort metrics by DHS year ..."
        local temp_files ""
        local dhs_num = 0 
        foreach dhs_year in `dhs_years' { 
            local ++dhs_num
            di   "   DHS year `dhs_num'/`n_dhsyears': `dhs_year' ..."

            * Check cohort file
            local cohort_file "$cf/data/derived/dhs/00-dhs-country-surveyyear-mappings/dhs-cohort-year.dta"
            capture confirm file "`cohort_file'"
            if _rc {
                di as error "   ERROR: Cohort year file not found: `cohort_file'"
                continue
            }

            use "`cohort_file'", clear
            keep if year <= `dhs_year'
            keep if cohort <= `dhs_year' - 15

            su cohort
            list if cohort == 2005 & year == 2020

            qui count 
            di "      Cohort-year combinations: `r(N)'"
            
            if r(N) == 0 {
                di "      WARNING: No valid cohort-year combinations for DHS `dhs_year'"
                continue
            }
            
            local temp_file "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/01-annual-data/`country_name'-temperature_annual.dta"

            capture confirm file "`temp_file'"
            if _rc {
                di as error "   ERROR: Temperature file not found: `temp_file'"
                continue
            }

            di "    Joining with temperature data ..."
            capture noisily joinby year using "`temp_file'"
            if _rc {
                di as error "   ERROR: Failed to join temperature data"
                continue 
            }
            local temp_file_name "`country_code'-dhs-metrics-`dhs_year'"

            count if cohort == 2005

            capture noisily CohortMetricsCalculator, ///
                country_code("`country_code'") ///
                dhs_year(`dhs_year') ///
                reproductive_span(`reproductive_span') ///
                save_file("`temp_file_name'")

            if _rc == 0 { 
                local temp_files "`temp_files' `temp_file_name'"
            }
            else {
                di "   WARNING: Failed to calculate metrics for DHS `dhs_year' "
            }
        }

        if "`temp_files'" == "" {
            di as error "   ERROR: No temperature metric files were created for `country_name'"
            exit 459
        }

        * Step 3: Combine DHS year files 
        di "   Combining DHS years ..."
        TemperatureAnalysisWorkflow, ///
            method("combine_and_select_dhs") ///
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

    else if "`method'" == "combine_and_select_dhs" { 
        di "   Combining DHS datasets ..."
        local first_file: word 1 of `temp_files'

        capture confirm file "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/02-dhs-year-metrics/`first_file'.dta"
        if _rc {
            di as error "   ERROR: First DHS metrics file not found: `first_file'"
            exit 601
        }

        use "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/02-dhs-year-metrics/`first_file'.dta", clear
        qui count 
        local first_obs = r(N)
        di "    Loaded first file: `first_obs' observations"

        local remaining_files: list temp_files - first_file
        local remaining_count = wordcount("`remaining_files'")

        if `remaining_count' > 0 {
            di "   Appending `remaining_count' remaining files ..."
            foreach file in `remaining_files' { 
                capture append using "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/02-dhs-year-metrics/`file'.dta"
                if _rc {
                    di as error "   ERROR: Failed to append file: `file'"
                }
            }
            qui count 
            di "    Combined total: `r(N)' observations"
        }

        local before_cleaning = r(N)
        drop if missing(cohort) | missing(int_year) | missing(DHS_code)
        qui count 
        local after_cleaning = r(N)
        local dropped = `before_cleaning' - `after_cleaning'
        if `dropped' > 0 {
            di "    Dropped `dropped' observations with missing key variables"
        }
        di " Clean observations: `after_cleaning' observations"

        sort DHS_code cohort int_year
        di "   Applying DHS year selection criteria ..."
        CensusSelector, country_code("`country_code'")

        qui count 
        local final_obs = r(N)

                if `final_obs' == 0 {
            di as error "    ERROR: No observations after DHS year selection"
            exit 459
        }
        
        qui tab int_year
        local n_dhsyear_final = r(r)
        qui levelsof int_year, local(final_dhsyear_list)
        di "      Final dataset: `final_obs' observations across `n_dhsyear_final' DHS years"
        di "      DHS years: `final_dhsyear_list'"

        ** Label all of the degree day variables
        * Define age bins (must match generate_age_range_exposure)
        local bin_starts "15 20 25 30 35 40"
        local bin_ends   "19 24 29 34 39 44"
        local bin_labels "15_19 20_24 25_29 30_34 35_39 40_44"
        local n_bins : word count `bin_starts'

        local broad_starts "15 30"
        local broad_ends   "29 44"
        local broad_labels "15_29 30_44"
        local n_broad : word count `broad_starts'

        foreach measure in max mean {
        foreach thres in 24 26 27 28 29 30 31 32 33 34 36 38 {
            label variable temp_`measure'_dd_`thres' "Degree days (`measure' temp) `thres'C, scaled by 100"

            foreach season in gs ngs {
                label variable temp_`measure'_dd_`thres'_`season' "Degree days (`measure' temp) `thres'C (`season'), scaled by 100"
            }

            * Fine bins
            forvalues i = 1/`n_bins' {
                local s : word `i' of `bin_starts'
                local e : word `i' of `bin_ends'
                local lbl : word `i' of `bin_labels'
                label variable temp_`measure'_dd_`thres'_age_`lbl' "Degree days (`measure' temp) `thres'C ages `s'-`e', scaled by 100"
                foreach season in gs ngs {
                    label variable temp_`measure'_dd_`thres'_`season'_age_`lbl' "Degree days (`measure' temp) `thres'C ages `s'-`e', (`season'), scaled by 100"
                }
            }

            * Broad bins
            forvalues i = 1/`n_broad' {
                local s : word `i' of `broad_starts'
                local e : word `i' of `broad_ends'
                local lbl : word `i' of `broad_labels'
                label variable temp_`measure'_dd_`thres'_age_`lbl' "Degree days (`measure' temp) `thres'C ages `s'-`e', scaled by 100"
                foreach season in gs ngs {
                    label variable temp_`measure'_dd_`thres'_`season'_age_`lbl' "Degree days (`measure' temp) `thres'C ages `s'-`e', (`season'), scaled by 100"
                }
            }
        }
        }

        label variable temp_mean "Mean temperature, Celsius"
        label variable temp_max "Maximum temperature, Celsius"

        label variable temp_max_less_18 "Max Temperature bin, less than 18C"
        label variable temp_max_18_21 "Max Temperature bin, 18 - 21C"
        label variable temp_max_21_24 "Max Temperature bin, 21 - 24C"
        label variable temp_max_24_27 "Max Temperature bin, 24 - 27C"
        label variable temp_max_27_30 "Max Temperature bin, 27 - 30C"
        label variable temp_max_30_33 "Max Temperature bin, 30 - 33C"
        label variable temp_max_more_33 "Max Temperature bin, more than 33C"

        label variable int_year "DHS survey year"
        label variable age_in_dhs_year "Age in DHS survey year"
        label variable cohort_reaches_reproductive "Cohort reaches reproductive age"
        label variable exposure_years "Years of exposure"
        label variable full_exposure "Full exposure"

        label variable cohort "Cohort"
        label variable DHS_code "DHS code"
        label variable DHSREGEN "DHS region"

        
        local final_file "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/`country_name'-dhs-cohort-temperature.dta"
        capture save "`final_file'", replace 
        if _rc != 0 {
            di as error "   ERROR: Cannot save final dataset to `final_file'"
            di as error "   Check directory permissions and disk space."
            exit 603 
        }
        di "   Saved final dataset: `country_name'-dhs-cohort-temperature.dta"
    }

    else if "`method'" == "cleanup_temp_files" {
        local files_deleted = 0 
        di "    Deleting temporary cohort metric files ..."

        foreach file in `temp_files' {
            local file_path "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/02-dhs-year-metrics/`file'.dta"
            capture erase "`file_path'"
            if _rc == 0 {
                local ++files_deleted
            }
        }

        // di "    Deleting intermediate temperature file ..."
        // local temp_file "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/01-annual-data/`country_name'-temperature_annual.dta"
        // capture erase "`temp_file'"
        // if _rc == 0 {
        //     local ++files_deleted 
        // }

        di "   Cleanup complete: `files_deleted' temporary files removed"
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
local test_codes "IVC" 
local country_codes "`test_codes'"

* For full run, use: 
//local country_codes "BEN BFA BDI CMR CAF TCD COM COG DRC ETH GAB GMB GHA GIN IVC KEN LSO LBR MDG MWI MLI MOZ NMB NER NGA RWA SEN SLE ZAF SWZ TZA TGO UGA ZMB ZWE"

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
local failed_count: word count `countries_failed'

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
    di "   - ERA data: $cf/data/derived/dhs/02-dhs-region-daily-data/[country]-era-dhs-region-day-1940-2023.dta"
    di "   - Phenology: $cf/data/derived/dhs/03-dhs-fao-growing-season/dhs-crop-phenology-summary-stats.csv"
    di "   - Cohort: $cf/data/derived/dhs/00-dhs-country-surveyyear-mappings/dhs-cohort-year.dta"
    di "2. Verify country names match exactly in phenology and DHS files"
    di "3. Check that directories exist and have write permissions"
    di "4. Run with a single country first to identify specific issues"
}