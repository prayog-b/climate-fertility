********************************************************************************
* PROJECT:              Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:               Prayog Bhattarai
* DATE MODIFIED:        2025-10-09
* DESCRIPTION:          Generate precipitation variables
* DEPENDENCIES:         
* 
* NOTES: We use the SMALLEST-day data created in previous steps to generate SMALLEST-cohort-census level precipitation data
*        Input data is in data/derived/era/00-SMALLEST-day-data
*        Output data is in data/derived/era/`country_name'/`country_name'-allcensus-cohort-precip.dta
********************************************************************************

********************************************************************************
* Setup and Configuration
********************************************************************************

    capture program drop ClimateAnalysisConfig
    program define ClimateAnalysisConfig
        syntax, method(string)

        /*
        Program: ClimateAnalysisConfig
        Description: Sets up global macros and paths based on the user executing the script.
        Parameters:
            method: "init" to initialize configuration
        Output:
            Sets global macros:
                - cf: Base directory for the project
                - cf_overleaf: Directory for Overleaf outputs
                - $cf/data/derived/era: Directory for derived ERA data
                - $cf/data/raw/era: Directory for raw ERA data
        */
    
        if "`method'" == "init" {
            di "Initializing configuration for user: `c(username)'"
            
            if "`c(username)'" == "prayogbhattarai" {
                global cf "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
            }
			else if "`c(username)'" == "prayog" {
                global cf "D:/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "D:/`c(username)'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
            }
            else if inlist("`c(username)'", "yogita") {
                global cf "/Users/`c(username)'/NUS Dropbox/`c(username)'/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "/Users/`c(username)'/NUS Dropbox/`c(username)'/Apps/Overleaf/climate-fertility-ssa/output"
            }
            else if inlist("`c(username)'", "celin", "CE.4875") {
                global cf "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "/Users/`c(username)'/Apps/Overleaf/climate-fertility-ssa/output"
            }
            else if inlist("`c(username)'", "yun") {
                global cf "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "/Users/`c(username)'/Apps/Overleaf/climate-fertility-ssa/output"
            }
            else {
                di as error "Unknown user: `c(username)'. Set global cf manually."
                exit 198
            }
            
            di "Configuration successful:"
            di "  Base directory: $cf"
            di "  ERA derived: $cf/data/derived/era"
        }
    end

    ********************************************************************************
    * Country Code to Name Mapping
    ********************************************************************************
    capture program drop GetCountryName
    program define GetCountryName, rclass
        syntax, code(string)

        /*
        Program: GetCountryName
        Description: Maps a three-letter country code to its full country name.
        Parameters:
            code: Three-letter country code (e.g., "BEN", "BWA")
        Output:
            Returns local macro 'name' with the full country name.
        */
        
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
* Data Processing Functions
********************************************************************************

    capture program drop load_and_process_era
    program define load_and_process_era
        syntax, country_code(string)

        /*
        Program: load_and_process_era
        Description: Loads and validates ERA precipitation data for a specific country
        Parameters: 
            country_code: Three-letter country code (e.g. "BEN", "BWA")
        Output:
            Loads ERA daily precipitation data for the specified country into memory
        */
        * Get country name from code
        GetCountryName, code("`country_code'")
        local country_name "`r(name)'"
        
        di "  Loading ERA data for `country_name' (`country_code')..."
        
        * Check if ERA data file exists
        local data_file "$cf/data/derived/era/00-subunit-day-data/`country_name'-era-subunit-day-1940-2023.dta"
        //di "    Looking for: `data_file'"
        
        capture confirm file "`data_file'"
        if _rc {
            di as error "    ERROR: ERA data file not found: `data_file'"
            exit 601
        }
        
        use "`data_file'", clear
        
        qui count
        di "    Loaded `r(N)' daily observations"
        
        if r(N) == 0 {
            di as error "    ERROR: ERA data file is empty"
            exit 459
        }
        
        * Validate required variables
        local required_vars "SMALLEST precip day month year"
        foreach var of local required_vars {
            capture confirm variable `var'
            if _rc {
                di as error "    ERROR: Required variable `var' not found in ERA data"
                exit 111
            }
        }
        
        * Handle special country cases
        if "`country_code'" == "BWA" | "`country_code'" == "SDN" {
            di "    Special handling for `country_code' - removing invalid regions"
            qui drop if inlist(floor(SMALLEST/10000), 728, 729)
        }
        
        * Keep only essential variables
        keep SMALLEST precip day month year
        
        * Clean SMALLEST variable
        qui decode SMALLEST, gen(smallest_decoded)
        if "`country_code'" == "BWA" {
            drop if inlist(substr(smallest_decoded, 1, 3), "728", "729")
        }
        if "`country_code'" == "SDN" {
            drop if inlist(substr(smallest_decoded, 1, 3), "728")
        }
        qui destring smallest_decoded, replace  
        drop SMALLEST 
        rename smallest_decoded SMALLEST
        
        di "  ERA data loaded successfully"
    end

    ** Merging phenology data
    capture program drop merge_phenology_data
    program define merge_phenology_data
        syntax, country_name(string)
        /*
        Program: MergePhenologyData
        Description: Merges crop phenology data with the current ERA dataset
        Parameters:
            country_name: Full country name (e.g., "Benin", "Botswana")
        Output:
            Merges growing season indicators into the current dataset
        */
    
        di "  Merging crop phenology data..."
        
        preserve 
            local phenology_file "$cf/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv"
            di "    Looking for phenology file: `phenology_file'"
            
            capture confirm file "`phenology_file'"
            if _rc {
                di as error "    ERROR: Phenology file not found: `phenology_file'"
                exit 601
            }
            
            qui import delimited using "`phenology_file'", clear
            
            qui count
            di "    Phenology data has `r(N)' total observations"
            
            ren smallest SMALLEST
            qui replace country = proper(country)
            
            di "    Filtering for: `country_name'"
            qui keep if country == "`country_name'"
            ren gs1_has_data gs1_valid
            ren gs2_has_data gs2_valid
            gen no_valid_pixels = (gs1_valid == 0 & gs2_valid == 0)
            label var no_valid_pixels "Indicator for no valid phenology pixels"
            
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
                qui keep SMALLEST no_valid_pixels growing_month_* 
            }
            
            * Handle special country cases in phenology data
            if "`country_name'" == "Sudan" {
                qui tostring SMALLEST, replace
                drop if inlist(substr(SMALLEST, 1, 3), "728")
                destring SMALLEST, replace
            }
            
            tempfile crop_phenology
            save `crop_phenology'
        restore

        di "    Merging ERA and phenology data..."
        qui merge m:1 SMALLEST using `crop_phenology'
        drop _merge
        
        di "  Phenology data merged successfully"
    end

    ** Create Growing Season Variables
    capture program drop create_growing_season
    program define create_growing_season
        /*
        Program: CreateSeasonalVariables
        Description: Creates growing season and non-growing season precipitation variables
        Parameters: None (works on current dataset)
        Output:
            Generates gs, ngs, precip_gs, precip_ngs variables
        */
        
        di "  Creating seasonal precipitation variables..."
        
        * Create growing and non-growing season indicators
        qui gen byte gs = 0
        qui gen byte ngs = 0
        
        forvalues m = 1/12 {
            capture confirm variable growing_month_`m'
            if !_rc {
                qui replace gs = 1 if growing_month_`m' == 1 & month == `m'
                qui replace ngs = 1 if growing_month_`m' == 0 & month == `m'
            }
        }
        
        * Create seasonal precipitation variables
        gen precip_gs = precip * gs 
        gen precip_ngs = precip * ngs

        * Label variables
        la var precip_gs "Growing season precipitation"
        la var precip_ngs "Non-growing season precipitation"
        
        * Validate seasonal variables
        qui sum precip_gs
        di "    Growing season precip - Mean: `r(mean)', SD: `r(sd)'"
        qui sum precip_ngs
        di "    Non-growing season precip - Mean: `r(mean)', SD: `r(sd)'"
        
        di "  Seasonal variables created successfully"
    end

    ** Aggregate to annual level
    capture program drop aggregate_precip_yearly
    program define aggregate_precip_yearly
        syntax, country_name(string)
        /*
        Program: AggregateToAnnual
        Description: Aggregates daily precipitation data to annual level
        Parameters:
            country_name: Full country name for saving output
        Output:
            Saves annual precipitation dataset
        */
        
        di "  Aggregating to annual level..."
        
        * Ensure SMALLEST is numeric
        capture confirm string variable SMALLEST
        if !_rc {
            destring SMALLEST, replace force
        }
        
        * Collapse to annual level
        collapse (sum) precip precip_gs precip_ngs (first) no_valid_pixels, by(SMALLEST year)
        recast float year
        
        * Validate annual data
        qui count
        di "    Annual data has `r(N)' observations"
        
        qui count if missing(precip)
        if r(N) > 0 {
            di "    WARNING: Annual data has `r(N)' missing values for precip"
        }
        
        qui count if missing(precip_gs)
        if r(N) > 0 {
            di "    WARNING: Annual data has `r(N)' missing values for precip_gs"
        }
        
        qui count if missing(precip_ngs)
        if r(N) > 0 {
            di "    WARNING: Annual data has `r(N)' missing values for precip_ngs"
        }
        
        * Create directory if it doesn't exist
        capture mkdir "$cf/data/derived/era/`country_name'"
        
        * Save annual data
        local save_file "$cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
        di "    Saving annual data to: `save_file'"
        save "`save_file'", replace
        
        di "  Annual aggregation completed successfully"
    end

    capture program drop create_gammafit_dataset
    program define create_gammafit_dataset
        syntax, country_code(string)
        /*
        Program: create_gammafit_dataset
        Description: Creates dataset with gamma distribution fits for precipitation variables
        Parameters:
            country_code: Three-letter country code
        Output:
            Saves dataset with gamma distribution parameters and SPI values
        */
        
        di "  Creating gamma fit dataset with scaling..."
        
        * Get country name
        GetCountryName, code("`country_code'")
        local country_name "`r(name)'"
        
        local annual_file "$cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
        capture confirm file "`annual_file'"
        if _rc {
            di as error "    ERROR: Annual precipitation file not found: `annual_file'"
            exit 601
        }
        
        use "`annual_file'", clear
        
        qui count
        di "    Loaded `r(N)' annual observations for gamma fitting"
        
        sort SMALLEST year
        
        * Fit gamma distributions 
        GammaDistributionFitter, variables("precip precip_gs precip_ngs")
        
        * Keep only essential variables for gamma fit results
        local vars_to_keep "SMALLEST year no_valid_pixels abn_precip* precip*_spi alpha beta P spi i_gammafit_scaled"
        keep `vars_to_keep'
        
        * Save gamma fit results
        local save_file "$cf/data/derived/era/`country_name'/`country_code'_gammafit.dta"
        di "    Saving gamma fit results to: `save_file'"
        save "`save_file'", replace
        
        di "  Gamma fit dataset created successfully"
    end

    ** Cohort Metrics Calculator
    capture program drop cohort_metrics_calculator
    program define cohort_metrics_calculator
        syntax, country_code(string) [reproductive_span(real 30) census_year(real 2023) save_file(string)]
        /*
        Program: cohort_metrics_calculator
        Description: Calculates cohort-level precipitation metrics including scaling indicators
        Parameters:
            country_code: Three-letter country code
            reproductive_span: Number of years representing reproductive span
            census_year: Census year to consider
            save_file: File path to save cohort-level precipitation data
        */
        
        di "    Calculating cohort metrics for census `census_year'"
        
        * Get country name from code
        GetCountryName, code("`country_code'")
        local country_name "`r(name)'"
        
        * Check current dataset
        qui count
        di "      Starting with `r(N)' observations"
        * Define age bins (configurable: update bin_starts, bin_ends, bin_labels to change bins)
        * ── Fine bins: six 5-year bins covering ages 15-44 ──
        local bin_starts "15 20 25 30 35 40"
        local bin_ends   "19 24 29 34 39 44"
        local bin_labels "15_19 20_24 25_29 30_34 35_39 40_44"
        local n_bins : word count `bin_starts'

        * ── Broad bins: two halves of the reproductive span ──
        local broad_starts "15 30"
        local broad_ends   "29 44"
        local broad_labels "15_29 30_44"
        local n_broad : word count `broad_starts'

        di "      Generating age variables for `n_bins' fine + `n_broad' broad age bins..."
        forvalues i = 1/`n_bins' {
            local s : word `i' of `bin_starts'
            local e : word `i' of `bin_ends'
            local lbl : word `i' of `bin_labels'
            qui gen age_`lbl' = inrange(age, `s', `e')
        }
        forvalues i = 1/`n_broad' {
            local s : word `i' of `broad_starts'
            local e : word `i' of `broad_ends'
            local lbl : word `i' of `broad_labels'
            qui gen age_`lbl' = inrange(age, `s', `e')
        }

        // display "Variable list before gammafit merge:"
        // ds
        
        local gammafit_file "$cf/data/derived/era/`country_name'/`country_code'_gammafit.dta"
        
        capture confirm file "`gammafit_file'"
        if _rc {
            di "      WARNING: Gamma fit file not found: `gammafit_file'"
            di "      Proceeding without SPI variables"
        }
        else {
            di "      Merging with gamma fit results (including scaling indicators)"
            qui merge m:1 SMALLEST year using "`gammafit_file'"
            drop _merge
            
            * Report on scaling usage
            qui sum i_gammafit_scaled
            if r(N) > 0 & r(mean) > 0 {
                di "      Scaling was used for `=round(r(mean)*100, 0.1)'% of observations"
            }
        }

        // display "Variable list after gammafit merge:"
        // ds
        
        * Base variables including the new scaling indicator
        local base_vars "precip abn_precip_dry abn_precip_wet abn_precip precip_spi precip_gs_spi precip_ngs_spi precip_gs precip_ngs abn_precip_gs abn_precip_ngs abn_precip_gs_dry abn_precip_ngs_dry abn_precip_gs_wet abn_precip_ngs_wet i_gammafit_scaled"
        
        local vars_created = 0
        foreach var of local base_vars {
            capture confirm variable `var'
            if _rc == 0 {
                * Don't create age-specific versions for the scaling indicator
                if "`var'" != "i_gammafit_scaled" {
                    forvalues i = 1/`n_bins' {
                        local lbl : word `i' of `bin_labels'
                        qui gen `var'_`lbl' = `var' * age_`lbl'
                    }
                    forvalues i = 1/`n_broad' {
                        local lbl : word `i' of `broad_labels'
                        qui gen `var'_`lbl' = `var' * age_`lbl'
                    }
                }
                local ++vars_created
            }
        }

        
        di "      Collapsing to cohort level..."

        * Build age-bin-specific collapse specs dynamically
        local coll_sum_bins ""
        local coll_mean_bins ""
        * Fine bins
        forvalues i = 1/`n_bins' {
            local lbl : word `i' of `bin_labels'
            local coll_sum_bins "`coll_sum_bins' precip_total_`lbl' = precip_`lbl' precip_gs_total_`lbl' = precip_gs_`lbl' precip_ngs_total_`lbl' = precip_ngs_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_`lbl' = abn_precip_`lbl' tot_abn_precip_dry_`lbl' = abn_precip_dry_`lbl' tot_abn_precip_wet_`lbl' = abn_precip_wet_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_gs_`lbl' = abn_precip_gs_`lbl' tot_abn_precip_ngs_`lbl' = abn_precip_ngs_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_gs_dry_`lbl' = abn_precip_gs_dry_`lbl' tot_abn_precip_gs_wet_`lbl' = abn_precip_gs_wet_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_ngs_dry_`lbl' = abn_precip_ngs_dry_`lbl' tot_abn_precip_ngs_wet_`lbl' = abn_precip_ngs_wet_`lbl'"
            local coll_mean_bins "`coll_mean_bins' avg_precip_spi_`lbl' = precip_spi_`lbl' avg_precip_gs_spi_`lbl' = precip_gs_spi_`lbl' avg_precip_ngs_spi_`lbl' = precip_ngs_spi_`lbl'"
        }
        * Broad bins
        forvalues i = 1/`n_broad' {
            local lbl : word `i' of `broad_labels'
            local coll_sum_bins "`coll_sum_bins' precip_total_`lbl' = precip_`lbl' precip_gs_total_`lbl' = precip_gs_`lbl' precip_ngs_total_`lbl' = precip_ngs_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_`lbl' = abn_precip_`lbl' tot_abn_precip_dry_`lbl' = abn_precip_dry_`lbl' tot_abn_precip_wet_`lbl' = abn_precip_wet_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_gs_`lbl' = abn_precip_gs_`lbl' tot_abn_precip_ngs_`lbl' = abn_precip_ngs_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_gs_dry_`lbl' = abn_precip_gs_dry_`lbl' tot_abn_precip_gs_wet_`lbl' = abn_precip_gs_wet_`lbl'"
            local coll_sum_bins "`coll_sum_bins' tot_abn_precip_ngs_dry_`lbl' = abn_precip_ngs_dry_`lbl' tot_abn_precip_ngs_wet_`lbl' = abn_precip_ngs_wet_`lbl'"
            local coll_mean_bins "`coll_mean_bins' avg_precip_spi_`lbl' = precip_spi_`lbl' avg_precip_gs_spi_`lbl' = precip_gs_spi_`lbl' avg_precip_ngs_spi_`lbl' = precip_ngs_spi_`lbl'"
        }

        qui collapse ///
        (first) no_valid_pixels = no_valid_pixels ///
        (sum) precip_total = precip ///
            precip_gs_total = precip_gs ///
            precip_ngs_total = precip_ngs ///
            tot_abn_precip = abn_precip ///
            tot_abn_precip_gs = abn_precip_gs ///
            tot_abn_precip_ngs = abn_precip_ngs ///
            tot_abn_precip_dry = abn_precip_dry ///
            tot_abn_precip_wet = abn_precip_wet ///
            tot_abn_precip_gs_dry = abn_precip_gs_dry ///
            tot_abn_precip_gs_wet = abn_precip_gs_wet ///
            tot_abn_precip_ngs_dry = abn_precip_ngs_dry ///
            tot_abn_precip_ngs_wet = abn_precip_ngs_wet ///
            `coll_sum_bins' ///
            tot_gammafit_scaled = i_gammafit_scaled ///
        (mean) avg_precip_spi = precip_spi ///
            avg_precip_gs_spi = precip_gs_spi ///
            avg_precip_ngs_spi = precip_ngs_spi ///
            `coll_mean_bins' ///
        , by(SMALLEST cohort)
        
        qui gen census = `census_year'

        * Calculate shares (assuming reproductive_span is defined)
        di "Calculating shares..."

        quietly {
        * Overall shares (denominator = full reproductive span)
        gen share_abn_precip = tot_abn_precip / `reproductive_span'
        gen share_abn_precip_dry = tot_abn_precip_dry / `reproductive_span'
        gen share_abn_precip_wet = tot_abn_precip_wet / `reproductive_span'

        * Growing season overall shares
        gen share_abn_precip_gs = tot_abn_precip_gs / `reproductive_span'
        gen share_abn_precip_gs_dry = tot_abn_precip_gs_dry / `reproductive_span'
        gen share_abn_precip_gs_wet = tot_abn_precip_gs_wet / `reproductive_span'

        * Non-growing season overall shares
        gen share_abn_precip_ngs = tot_abn_precip_ngs / `reproductive_span'
        gen share_abn_precip_ngs_dry = tot_abn_precip_ngs_dry / `reproductive_span'
        gen share_abn_precip_ngs_wet = tot_abn_precip_ngs_wet / `reproductive_span'

        * Age-bin-specific shares (denominator = number of years in each bin)
        * Fine bins
        forvalues i = 1/`n_bins' {
            local s : word `i' of `bin_starts'
            local e : word `i' of `bin_ends'
            local lbl : word `i' of `bin_labels'
            local bin_years = `e' - `s' + 1

            gen share_abn_precip_`lbl'         = tot_abn_precip_`lbl'         / `bin_years'
            gen share_abn_precip_dry_`lbl'     = tot_abn_precip_dry_`lbl'     / `bin_years'
            gen share_abn_precip_wet_`lbl'     = tot_abn_precip_wet_`lbl'     / `bin_years'

            gen share_abn_precip_gs_`lbl'      = tot_abn_precip_gs_`lbl'      / `bin_years'
            gen share_abn_precip_gs_dry_`lbl'  = tot_abn_precip_gs_dry_`lbl'  / `bin_years'
            gen share_abn_precip_gs_wet_`lbl'  = tot_abn_precip_gs_wet_`lbl'  / `bin_years'

            gen share_abn_precip_ngs_`lbl'     = tot_abn_precip_ngs_`lbl'     / `bin_years'
            gen share_abn_precip_ngs_dry_`lbl' = tot_abn_precip_ngs_dry_`lbl' / `bin_years'
            gen share_abn_precip_ngs_wet_`lbl' = tot_abn_precip_ngs_wet_`lbl' / `bin_years'
        }
        * Broad bins
        forvalues i = 1/`n_broad' {
            local s : word `i' of `broad_starts'
            local e : word `i' of `broad_ends'
            local lbl : word `i' of `broad_labels'
            local bin_years = `e' - `s' + 1

            gen share_abn_precip_`lbl'         = tot_abn_precip_`lbl'         / `bin_years'
            gen share_abn_precip_dry_`lbl'     = tot_abn_precip_dry_`lbl'     / `bin_years'
            gen share_abn_precip_wet_`lbl'     = tot_abn_precip_wet_`lbl'     / `bin_years'

            gen share_abn_precip_gs_`lbl'      = tot_abn_precip_gs_`lbl'      / `bin_years'
            gen share_abn_precip_gs_dry_`lbl'  = tot_abn_precip_gs_dry_`lbl'  / `bin_years'
            gen share_abn_precip_gs_wet_`lbl'  = tot_abn_precip_gs_wet_`lbl'  / `bin_years'

            gen share_abn_precip_ngs_`lbl'     = tot_abn_precip_ngs_`lbl'     / `bin_years'
            gen share_abn_precip_ngs_dry_`lbl' = tot_abn_precip_ngs_dry_`lbl' / `bin_years'
            gen share_abn_precip_ngs_wet_`lbl' = tot_abn_precip_ngs_wet_`lbl' / `bin_years'
        }
        }

        * ASSERTION 2: tot_abn_precip should not exceed 30 for any cohort
        di "      Validating tot_abn_precip bounds..."
        qui sum tot_abn_precip
        if r(max) > 30 {
            di as error "      ASSERTION FAILED: tot_abn_precip exceeds 30 (max found: `r(max)')"
            di as error "      This violates the reproductive span assumption of 30 years"
            exit 459
        }
        di "      ✓ tot_abn_precip validation passed (max: `r(max)')"

        * ASSERTION 3: share_abn_precip should not exceed 1 for any cohort
        di "      Validating share_abn_precip bounds..."
        qui sum share_abn_precip
        if r(max) > 1 {
            di as error "      ASSERTION FAILED: share_abn_precip exceeds 1 (max found: `r(max)')"
            di as error "      Share should be a proportion between 0 and 1"
            exit 459
        }
        di "      ✓ share_abn_precip validation passed (max: `r(max)')"

        local save_file "$cf/data/derived/era/`country_name'/`country_code'_metrics_`census_year'.dta"
        
        local output_path "`save_file'"
        //di "      Saving cohort metrics to: `output_path'"
        qui save "`output_path'", replace
        
        qui count
        di "    Cohort metrics saved (`r(N)' observations)"
    end

    
********************************************************************************
* Statistical Analysis Functions
********************************************************************************

    ** Validate gamma distribution inputs
    capture program drop validate_gamma_inputs
    program define validate_gamma_inputs
        syntax, variables(string) 
        /*
        Program: ValidateGammaInputs
        Description: Validates inputs for gamma distribution fitting
        Parameters:
            variables: Space-separated list of precipitation variables
            smallest_list: List of SMALLEST geographic units
        Output:
            Returns error if validation fails
        */
        di " Validating inputs for gamma distribution fitting ..."
        * Check if variables exist
        foreach var in `variables' {
            capture confirm variable `var'
            if _rc {
                di as error "  ERROR: variable `var' not found in dataset."
            }
            
            di "Checking for missing values"
            qui count if missing(`var')
            if r(N) > 0 {
                di "  WARNING: There are `r(N)' missing values in variable `var'"
            }

            di "Checking for negative values"
            qui count if `var'  <  0 
            if r(N) > 0 {
                di "  WARNING: There are `r(N)' negative values in variable `var'"
            }
        }


        di "  Validation passed"
    end 

    capture program drop initialize_gamma_variables
    program define initialize_gamma_variables
        syntax, variables(string)

        /*
        Program: initialize_gamma_variables
        Description: Creates variables for storing gamma distribution results
        Parameters:
            variables: Space-separated list of precipitation variables
        Output:
            Creates alpha, beta, P, spi variables and abnormal precipitation indicators
        */
        
        di "  Initializing gamma distribution variables..."
        capture drop alpha beta P spi i_gammafit_scaled
        foreach var in `variables' {
            capture drop abn_`var'_wet abn_`var'_dry abn_`var' `var'_spi
        }
        
        * Create base gamma parameter variables
        gen alpha = .
        gen beta = .
        gen P = .
        gen spi = .

        * Create scaling indicator variable
        gen i_gammafit_scaled = 0
        label var i_gammafit_scaled "Gamma fit scaling level (0=original, 1=scaled/10, 2=scaled/100)"

        * Create abnormal precipitation indicators for each variable
        foreach var in `variables' {
            di "    Creating variables for: `var'"
            
            gen abn_`var'_wet = .
            gen abn_`var'_dry = .
            gen abn_`var' = .
            gen `var'_spi = .
            
            * Add variable labels
            label var abn_`var'_wet "Abnormally wet year for `var' (P ≥ 0.85)"
            label var abn_`var'_dry "Abnormally dry year for `var' (P ≤ 0.15)"
            label var abn_`var' "Abnormal precipitation year for `var'"
            label var `var'_spi "Standardized Precipitation Index for `var'"
        }
        
        label var alpha "Gamma distribution shape parameter"
        label var beta "Gamma distribution scale parameter"
        label var P "Cumulative probability from gamma distribution"
        label var spi "Standardized Precipitation Index"
        
        di "  Gamma variables initialized successfully"
    end

    capture program drop fit_gamma
    program define fit_gamma, rclass
        syntax, variable(string) smallest(real) [max_iter(integer 3)]

        /*
        Program: fit_gamma
        Description: Fits zero-inflated gamma distribution with iterative scaling for convergence.
                     Separates zero probability (q) from gamma fit on positive values,
                     following the standard SPI methodology (McKee et al. 1993).
        Parameters:
            variable: Precipitation variable name
            smallest: SMALLEST geographic unit ID
            max_iter: Maximum scaling iterations (default: 3)
        Output:
            Returns gamma parameters (alpha, beta), zero probability (q),
            scaling factor, and scale level used
        */

        //di "        Fitting gamma for `variable' in SMALLEST `smallest'"

        * --- Zero-value handling (zero-inflated gamma) ---
        * Count zeros and positive observations for this unit
        qui count if `variable' == 0 & SMALLEST == `smallest'
        local n_zeros = r(N)
        qui count if `variable' > 0 & SMALLEST == `smallest'
        local n_positive = r(N)
        local n_total = `n_zeros' + `n_positive'

        * If fewer than 2 positive observations, gamma fitting is impossible
        if `n_positive' < 2 {
            if `n_total' > 0 {
                di "          SKIP: `variable' has `n_positive' positive obs out of `n_total' for SMALLEST `smallest' (need >= 2)"
            }
            return scalar converged = 0
            return scalar scale_level = -1
            return scalar scaling_factor = 0
            return scalar q = 1
            exit
        }

        * Calculate zero probability q = P(X = 0)
        local q = `n_zeros' / `n_total'

        local scaling_factor 1
        local iteration 1
        local success 0
        local scale_level 0
        local original_var "`variable'"
        local temp_var "`variable'_temp"

        * Create temporary variable for scaling
        gen `temp_var' = `variable'

        while `iteration' <= `max_iter' & `success' == 0 {

            if `iteration' == 1 {
                //di "          Attempt 1: Using original scale"
                local scale_level = 0
            }
            else if `iteration' == 2 {
                //di "          Attempt 2: Scaling to 1/10 of original"
                replace `temp_var' = `variable' / 10
                local scaling_factor = 10
                local scale_level = 1
            }
            else if `iteration' == 3 {
                //di "          Attempt 3: Scaling to 1/100 of original"
                replace `temp_var' = `variable' / 100
                local scaling_factor = 100
                local scale_level = 2
            }

            * Attempt gamma fit on POSITIVE values only
            capture gammafit `temp_var' if SMALLEST == `smallest' & `temp_var' > 0

            if _rc == 0 {
                local success 1
                //di "          SUCCESS: Gamma fit converged with scaling factor 1/`scaling_factor'"

                * Store parameters (adjust beta back to original scale)
                local alpha_val = e(alpha)
                local beta_val = e(beta) * `scaling_factor'

                return scalar alpha = `alpha_val'
                return scalar beta = `beta_val'
                return scalar scaling_factor = `scaling_factor'
                return scalar scale_level = `scale_level'
                return scalar converged = 1
                return scalar q = `q'
            }
            else {
                di "          Attempt `iteration' failed"
                local ++iteration
            }
        }

        * Clean up temporary variable
        drop `temp_var'

        if `success' == 0 {
            di "          ERROR: Gamma fitting failed at all scaling levels for `variable' in SMALLEST `smallest'"
            return scalar converged = 0
            return scalar scale_level = -1
            return scalar scaling_factor = 0
            return scalar q = `q'
        }
    end

    capture program drop create_scaling_histograms
    program define create_scaling_histograms
        syntax, variable(string) smallest(real) scaling_factor(real) alpha(real) beta(real)
        
        /*
        Program: create_scaling_histograms
        Description: Creates diagnostic comparison plot showing scaled vs unscaled variables
        Parameters:
            variable: Precipitation variable name
            smallest: SMALLEST geographic unit ID
            scaling_factor: Scaling factor used
            alpha: Gamma shape parameter
            beta: Gamma scale parameter
        Output:
            Saves comparison plot to figures folder
        */
        
        preserve
            keep if SMALLEST == `smallest'
            
            * Create figure directory if it doesn't exist
            capture mkdir "$cf/figures"
            capture mkdir "$cf/figures/gammafit-scaling-logs"
            
            * Create scaled version of the variable for comparison
            gen `variable'_scaled = `variable' * `scaling_factor'
            
            * Generate theoretical gamma distribution values for the UNSCALED variable
            local xmax = max(50, `beta' * 3)  // Reasonable x-axis range
            range x_theoretical 0 `xmax' 100
            gen gamma_density = (x_theoretical^(`alpha' - 1) * exp(-x_theoretical/`beta')) / ///
                            (exp(lngamma(`alpha')) * `beta'^`alpha')
            
            * Create comparison plot: scaled vs unscaled histograms with gamma density
            local graph_title "Data Scaling Comparison - SMALLEST `smallest', `variable'"
            local graph_note "Scaling factor: 1/`=1/`scaling_factor'' | Alpha=`=round(`alpha', 0.001)', Beta=`=round(`beta', 0.001)'"
            
            twoway (histogram `variable', fraction color(blue%30) bin(20)) ///
                (histogram `variable'_scaled, fraction color(red%30) bin(20)) ///
                title("`graph_title'") ///
                subtitle("`graph_note'") ///
                legend(order(1 "Unscaled Data" 2 "Scaled Data (1/`=1/`scaling_factor'')")) ///
                xtitle("Precipitation (mm)") ///
                ytitle("Density")
            
            local filename "$cf/figures/gammafit-scaling-logs/`variable'_smallest_`smallest'_scaling_comparison.png"
            graph export "`filename'", replace width(2000) height(1500)
            di "          Scaling comparison plot saved: `filename'"
        restore
    end

    capture program drop fit_gamma_by_smallest
    program define fit_gamma_by_smallest
        syntax, variables(string)

        di "  Fitting gamma distributions by geographic unit..."
        di "  Using zero-inflated gamma: H(x) = q + (1-q)*G(x) where q = P(X=0)"

        qui levelsof SMALLEST, local(smallests)
        local n_smallests = wordcount("`smallests'")
        di "    Processing `n_smallests' geographic units"

        foreach var in `variables' {
            di "      Variable: `var'"

            * Initialize variable-specific counters
            local var_success_original = 0
            local var_success_scale10 = 0
            local var_success_scale100 = 0
            local var_fail_all = 0
            local var_skip_allzero = 0

            foreach smallest in `smallests' {

                * Fit gamma with scaling (handles zeros internally)
                fit_gamma, variable("`var'") smallest(`smallest') max_iter(3)

                if `r(converged)' == 1 {
                    local alpha_est = r(alpha)
                    local beta_est = r(beta)
                    local scale_level = r(scale_level)
                    local q_est = r(q)

                    * Track success by scale level
                    if `scale_level' == 0 {
                        local ++var_success_original
                    }
                    else if `scale_level' == 1 {
                        local ++var_success_scale10
                        replace i_gammafit_scaled = max(i_gammafit_scaled, 1) if SMALLEST == `smallest'
                    }
                    else if `scale_level' == 2 {
                        local ++var_success_scale100
                        replace i_gammafit_scaled = 2 if SMALLEST == `smallest'
                    }

                    * Store gamma parameters

                    quietly {
                    replace alpha = `alpha_est' if SMALLEST == `smallest'
                    replace beta = `beta_est' if SMALLEST == `smallest'

                    * Calculate CDF using zero-inflated gamma: H(x) = q + (1-q)*G(x)
                    * For positive values: mixed CDF
                    replace P = `q_est' + (1 - `q_est') * gammap(`alpha_est', `var'/`beta_est') if SMALLEST == `smallest' & `var' > 0
                    * For zero values: P = q (the probability mass at zero)
                    replace P = `q_est' if SMALLEST == `smallest' & `var' == 0

                    * Clamp P away from 0 and 1 to avoid infinite SPI values
                    replace P = max(0.001, min(0.999, P)) if SMALLEST == `smallest'

                    replace spi = invnormal(P) if SMALLEST == `smallest'

                    * Create abnormal precipitation indicators
                    replace abn_`var'_wet = (P >= 0.85) if SMALLEST == `smallest'
                    replace abn_`var'_dry = (P <= 0.15) if SMALLEST == `smallest'
                    replace abn_`var' = (P >= 0.85 | P <= 0.15) if SMALLEST == `smallest'
                    replace `var'_spi = spi if SMALLEST == `smallest'
                    }
                }
                else {
                    * Check if failure was due to all-zero data
                    qui count if `var' > 0 & SMALLEST == `smallest'
                    if r(N) < 2 {
                        local ++var_skip_allzero
                    }
                    else {
                        local ++var_fail_all
                    }
                }
            }
            
            * Store statistics for EACH variable in globals
            global gamma_`var'_total_units = `n_smallests'
            global gamma_`var'_fit_unsc = `var_success_original'
            global gamma_`var'_fit_scale10 = `var_success_scale10'
            global gamma_`var'_fit_scale100 = `var_success_scale100'
            global gamma_`var'_fail_all = `var_fail_all'
            global gamma_`var'_skip_allzero = `var_skip_allzero'

            * Report variable-specific results
            di "        Original scale: `var_success_original'/`n_smallests'"
            di "        Scale/10:       `var_success_scale10'/`n_smallests'"
            di "        Scale/100:      `var_success_scale100'/`n_smallests'"
            di "        Skipped (all zeros/insufficient positive obs): `var_skip_allzero'/`n_smallests'"
            di "        Failed all:     `var_fail_all'/`n_smallests'"
        }
        
        * Store mean alpha and beta for summary statistics
        preserve
            bysort SMALLEST: keep if _n == 1
            qui sum alpha if !missing(alpha)
            global alpha_mean = r(mean)
            qui sum beta if !missing(beta)
            global beta_mean = r(mean)
        restore
        
        di ""
        di "  ============================================"
        di "  GAMMA FITTING SUMMARY"
        di "  ============================================"
        di "  Total geographic units: `n_smallests'"
        foreach var in `variables' {
            di "  --- `var' ---"
            di "    Success (original):  ${gamma_`var'_fit_unsc}"
            di "    Success (scale/10):  ${gamma_`var'_fit_scale10}"
            di "    Success (scale/100): ${gamma_`var'_fit_scale100}"
            di "    Skipped (all zeros): ${gamma_`var'_skip_allzero}"
            di "    Failed at all:       ${gamma_`var'_fail_all}"
        }
        di "  ============================================"
    end

    capture program drop GammaDistributionFitter
    program define GammaDistributionFitter 
        syntax, [variables(string)]
        /*
        Main function that orchestrates gamma distribution fitting process. 
        Parameters:
            variables: space-separated list of precipitation variables
            
        */

        di "Starting gamma distribution fitting process with scaling"

        * Set default variables if not specified
        local varlist "`variables'"
        if "`varlist'" == "" {
            local varlist "precip precip_gs precip_ngs"
        }

        * Step 1: Validate inputs
        validate_gamma_inputs, variables("`varlist'")

        * Step 2: Initialise variables
        initialize_gamma_variables, variables("`varlist'")

        * Step 3: Fit distributions with scaling
        fit_gamma_by_smallest, variables("`varlist'")
        
        di " Gamma distribution fitting with scaling completed successfully."
    end

    capture program drop SaveGammaScalingStatistics
    program define SaveGammaScalingStatistics
        syntax, country(string) output_file(string) [append]
        
        preserve
            clear
            set obs 1
            
            gen str50 country = "`country'"
            gen n_smallests = ${gamma_precip_total_units}
            
            * Store stats for each variable
            foreach var in precip precip_gs precip_ngs {
                quietly {
                gen `var'_success_original = ${gamma_`var'_fit_unsc}
                gen `var'_success_scale10 = ${gamma_`var'_fit_scale10}
                gen `var'_success_scale100 = ${gamma_`var'_fit_scale100}
                gen `var'_fail_all = ${gamma_`var'_fail_all}
                
                * Calculate percentages
                gen `var'_pct_original = 100 * `var'_success_original / n_smallests
                gen `var'_pct_scale10 = 100 * `var'_success_scale10 / n_smallests
                gen `var'_pct_scale100 = 100 * `var'_success_scale100 / n_smallests
                gen `var'_pct_failed = 100 * `var'_fail_all / n_smallests
                }
            }
            
            if "`append'" != "" {
                capture confirm file "`output_file'"
                if _rc == 0 {
                    append using "`output_file'"
                }
            }
            
            save "`output_file'", replace
        restore
        
        di "  Gamma scaling statistics saved for `country'"
    end

    capture program drop LogGammaFittingFailures
    program define LogGammaFittingFailures
        syntax, country(string) output_file(string) [append]
        
        /*
        Program: LogGammaFittingFailures
        Description: Creates a dataset of SMALLEST units that failed gamma fitting
        Parameters:
            country: Country name
            output_file: Path to output file
            append: If specified, appends to existing file
        Output:
            Dataset with SMALLEST units and which variables failed
        */
        
        preserve
            * Keep one observation per SMALLEST
            bysort SMALLEST: keep if _n == 1
            
            * Create failure indicators for each variable
            quietly {
            gen byte failed_precip = missing(abn_precip)
            gen byte failed_precip_gs = missing(abn_precip_gs)
            gen byte failed_precip_ngs = missing(abn_precip_ngs)
            }
            * Keep only units with at least one failure
            keep if failed_precip == 1 | failed_precip_gs == 1 | failed_precip_ngs == 1
            
            qui count
            if r(N) > 0 {
                * Add country identifier
                gen str50 country = "`country'"
                
                * Keep relevant variables
                keep country SMALLEST failed_precip failed_precip_gs failed_precip_ngs
                
                * Add labels
                label var country "Country name"
                label var SMALLEST "Geographic unit ID"
                label var failed_precip "Failed to fit gamma for total precip"
                label var failed_precip_gs "Failed to fit gamma for growing season precip"
                label var failed_precip_ngs "Failed to fit gamma for non-growing season precip"
                
                if "`append'" != "" {
                    capture confirm file "`output_file'"
                    if _rc == 0 {
                        append using "`output_file'"
                    }
                }
                
                save "`output_file'", replace
                
                qui count
                di "  Logged `r(N)' SMALLEST units with gamma fitting failures for `country'"
            }
            else {
                di "  No gamma fitting failures to log for `country'"
            }
        restore
    end

    capture program drop GenerateGammaScalingTable
    program define GenerateGammaScalingTable
        
        * Load gamma scaling statistics
        use "$cf/data/derived/era/gamma_scaling_statistics.dta", clear
        duplicates drop country, force
        sort country
        
        * Open file for writing
        file open latexfile using "$cf_overleaf/table/gamma_scaling_summary.tex", write replace
        
        * Write table header - showing failures at each stage
        file write latexfile "\begin{tabular}{l r | rrr | rrr | rrr}" _n
        file write latexfile "\toprule" _n
        file write latexfile "& & \multicolumn{3}{c|}{Total Precip} & \multicolumn{3}{c|}{Growing Season} & \multicolumn{3}{c}{Non-Growing Season} \\" _n
        file write latexfile "Country & Units & Fail 1x & Fail /10 & Fail /100 & Fail 1x & Fail /10 & Fail /100 & Fail 1x & Fail /10 & Fail /100 \\" _n
        file write latexfile "& (1) & (2) & (3) & (4) & (5) & (6) & (7) & (8) & (9) & (10) \\" _n
        file write latexfile "\midrule" _n
        
        * Initialize totals for each variable
        local total_units = 0
        foreach var in precip precip_gs precip_ngs {
            local total_`var'_fail1x = 0
            local total_`var'_fail10 = 0
            local total_`var'_fail100 = 0
        }
        
        * Write data rows
        local N = _N
        forval i = 1/`N' {
            local cntry = country[`i']
            local n_sub = n_smallests[`i']
            
            * Accumulate totals
            local total_units = `total_units' + `n_sub'
            
            * Build the row string
            local row_string "`cntry' & `n_sub'"
            
            foreach var in precip precip_gs precip_ngs {
                local orig = `var'_success_original[`i']
                local s10 = `var'_success_scale10[`i']
                local s100 = `var'_success_scale100[`i']
                local fail = `var'_fail_all[`i']
                
                * Calculate cumulative failures
                * Fail 1x = units that didn't succeed at original scale
                local fail_1x = `n_sub' - `orig'
                * Fail /10 = units that didn't succeed at original OR /10
                local fail_10 = `n_sub' - `orig' - `s10'
                * Fail /100 = units that failed all attempts (same as fail_all)
                local fail_100 = `fail'
                
                local row_string "`row_string' & `fail_1x' & `fail_10' & `fail_100'"
                
                * Accumulate totals
                local total_`var'_fail1x = `total_`var'_fail1x' + `fail_1x'
                local total_`var'_fail10 = `total_`var'_fail10' + `fail_10'
                local total_`var'_fail100 = `total_`var'_fail100' + `fail_100'
            }
            
            file write latexfile "`row_string' \\" _n
        }
        
        * Write totals row
        file write latexfile "\midrule" _n
        local total_row "Total & `total_units'"
        foreach var in precip precip_gs precip_ngs {
            local total_row "`total_row' & `total_`var'_fail1x' & `total_`var'_fail10' & `total_`var'_fail100'"
        }
        file write latexfile "`total_row' \\" _n
        
        * Write table footer
        file write latexfile "\bottomrule" _n
        file write latexfile "\end{tabular}" _n
        file close latexfile
        
        di "Gamma scaling LaTeX table saved to: $cf_overleaf/table/gamma_scaling_summary.tex"
        
        * Display failure rate summary
        di "  Cumulative Failure Rates:"
        foreach var in precip precip_gs precip_ngs {
            di "  --- `var' ---"
            local pct_fail1x = round(100 * `total_`var'_fail1x' / `total_units', 0.1)
            local pct_fail10 = round(100 * `total_`var'_fail10' / `total_units', 0.1)
            local pct_fail100 = round(100 * `total_`var'_fail100' / `total_units', 0.1)
            di "    Failed at 1x:    `pct_fail1x'%"
            di "    Failed at /10:   `pct_fail10'%"
            di "    Failed at /100:  `pct_fail100'%"
        }
    end



********************************************************************************
* Precipitation Data Processing Workflow
********************************************************************************

    capture program drop PrecipitationDataProcessor
    program define PrecipitationDataProcessor
        syntax, method(string) country_code(string) [census_years(string) max_cohort(string)]
        
        /*
        Program: PrecipitationDataProcessor
        Description: Orchestrates the precipitation data processing pipeline
        Parameters:
            method: "load_and_aggregate" or "fit_gamma_and_save"
            country_code: Three-letter country code (e.g., "BEN", "BWA")
            census_years: Space-separated list of census years (optional)
            max_cohort: Maximum cohort year (optional)
        Output:
            Generates annual precipitation datasets and fits gamma distributions as needed.
        */
        
        * Get country name from code
        GetCountryName, code("`country_code'")
        local country_name "`r(name)'"
        
        if "`country_name'" == "" {
            di as error "ERROR: Unknown country code: `country_code'"
            exit 198
        }
        
        if "`method'" == "load_and_aggregate" {
            di "=== Starting precipitation data processing for `country_name' (`country_code') ==="
            
            * STEP 1: Load and clean ERA data
            di _n "STEP 1: Loading and processing ERA data..."
            load_and_process_era, country_code("`country_code'")
            
            * STEP 2: Merge with phenology data
            di _n "STEP 2: Merging crop phenology data..."
            merge_phenology_data, country_name("`country_name'")
            
            * STEP 3: Create seasonal variables
            di _n "STEP 3: Creating growing season variables..."
            create_growing_season
            
            * STEP 4: Aggregate to annual level
            di _n "STEP 4: Aggregating to annual level..."
            aggregate_precip_yearly, country_name("`country_name'")
            
            * Report summary
            qui count
            di _n "=== Precipitation data processing completed successfully ==="
            di "    Final annual dataset: `r(N)' observations"
            di "    Country: `country_name'"
            di "    Output: $cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
            
            * Display variable summary
            foreach var in precip precip_gs precip_ngs {
                qui sum `var'
                di "    `var': mean=`=round(r(mean), 0.1)', sd=`=round(r(sd), 0.1)'"
            }
        }
        
        else if "`method'" == "fit_gamma_and_save" {
            di "=== Starting gamma distribution fitting for `country_name' (`country_code') ==="
            di "=== Using robust scaling (1x → 1/10 → 1/100) ==="
            
            * STEP: Create gamma fit dataset with robust scaling
            create_gammafit_dataset, country_code("`country_code'")
            
            * Load and display gamma fit results
            local gamma_file "$cf/data/derived/era/`country_name'/`country_code'_gammafit.dta"
            use "`gamma_file'", clear
            
            * Report gamma fitting summary
            di _n "=== Gamma distribution fitting completed successfully ==="
            di "    Output: `gamma_file'"
            
            * Count observations with scaling
            qui sum i_gammafit_scaled
            local scaled_count = r(N) * r(mean)
            local total_obs = r(N)
            // local scaled_percent = (r(mean)) * 100
            
            di "    Total observations: `total_obs'"
            // di "    Observations requiring scaling: `=round(`scaled_count', 1)' (`=round(`scaled_percent', 0.1)'%)"
            
            * Display gamma parameter summary
            preserve
                * Keep unique SMALLEST units for parameter summary
                bysort SMALLEST: keep if _n == 1
                qui sum alpha
                di "    Alpha (shape): mean=`=round(r(mean), 0.003)', sd=`=round(r(sd), 0.003)'"
                qui sum beta
                di "    Beta (scale): mean=`=round(r(mean), 0.1)', sd=`=round(r(sd), 0.1)'"
            restore
            
            * Display SPI summary
            foreach var in precip_spi precip_gs_spi precip_ngs_spi {
                capture confirm variable `var'
                if !_rc {
                    qui sum `var'
                    di "    `var': mean=`=round(r(mean), 0.003)', sd=`=round(r(sd), 0.003)'"
                }
            }
            
            * Report on abnormal precipitation
            foreach type in wet dry {
                capture confirm variable abn_precip_`type'
                if !_rc {
                    qui sum abn_precip_`type'
                    di "    Abnormal `type' years: `=round(r(mean)*100, 0.1)'%"
                }
            }
        }
        
        else if "`method'" == "validate_inputs" {
            * Validation method to check data quality
            di "=== Validating precipitation data for `country_name' (`country_code') ==="
            
            * Check if annual file exists
            local annual_file "$cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
            capture confirm file "`annual_file'"
            if _rc {
                di as error "  ERROR: Annual precipitation file not found: `annual_file'"
                exit 601
            }
            
            use "`annual_file'", clear
            
            di "  Data validation results:"
            di "    Total observations: `c(N)'"
            di "    Time range: `=year[1]' to `=year[_N]'"
            
            * Check for missing values
            foreach var in precip precip_gs precip_ngs {
                qui count if missing(`var')
                if r(N) > 0 {
                    di "    WARNING: `var' has `r(N)' missing values"
                }
                else {
                    di "    ✓ `var': No missing values"
                }
            }
            
            * Check value ranges
            foreach var in precip precip_gs precip_ngs {
                qui sum `var'
                di "    `var' range: `=round(r(min), 0.1)' to `=round(r(max), 0.1)'"
                if r(min) < 0 {
                    di "    WARNING: `var' has negative values"
                }
            }
            
            * Check SMALLEST units
            qui levelsof SMALLEST
            local n_units = wordcount("`r(levels)'")
            di "    SMALLEST units: `n_units'"
            
            di "  ✓ Data validation completed"
        }
        
        else if "`method'" == "quick_gamma_test" {
            * Quick test of gamma fitting on a subset of data
            di "=== Quick gamma fitting test for `country_name' (`country_code') ==="
            
            local annual_file "$cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
            capture confirm file "`annual_file'"
            if _rc {
                di as error "  ERROR: Annual precipitation file not found: `annual_file'"
                exit 601
            }
            
            use "`annual_file'", clear
            
            * Test on first 5 SMALLEST units
            levelsof SMALLEST, local(all_smallests)
            local test_smallests ""
            local count = 0
            foreach smallest in `all_smallests' {
                local ++count
                local test_smallests "`test_smallests' `smallest'"
                if `count' >= 5 continue, break
            }
            
            di "  Testing gamma fitting on `=wordcount("`test_smallests'")' SMALLEST units..."
            
            * Initialize variables for test
            gen alpha_test = .
            gen beta_test = .
            gen scaling_used = 0
            
            local success_count = 0
            local fail_count = 0
            local scaled_count = 0
            
            foreach smallest in `test_smallests' {
                di "    Testing SMALLEST `smallest'..."
                
                capture fit_gamma, variable("precip") smallest(`smallest') max_iter(3)
                if `r(converged)' == 1 {
                    local ++success_count
                    if `r(scaling_factor)' > 1 {
                        local ++scaled_count
                    }
                    di "      ✓ Success (scaling: `r(scaling_factor)')"
                }
                else {
                    local ++fail_count
                    di "      ✗ Failed"
                }
            }
            
            di _n "  Quick test results:"
            di "    Successful fits: `success_count'"
            di "    Failed fits: `fail_count'"
            di "    Required scaling: `scaled_count'"
            di "    Success rate: `=round((`success_count'/(`success_count' + `fail_count'))*100, 0.1)'%"
        }
        
        else {
            di as error "ERROR: Unknown method: `method'"
            di "Available methods:"
            di "  - load_and_aggregate: Process ERA data and create annual dataset"
            di "  - fit_gamma_and_save: Fit gamma distributions with robust scaling"
            di "  - validate_inputs: Check data quality and completeness"
            di "  - quick_gamma_test: Test gamma fitting on a subset of data"
            exit 198
        }
    end

********************************************************************************
* Main code workflow
********************************************************************************
    capture program drop PrecipitationAnalysisWorkflow
    program define PrecipitationAnalysisWorkflow
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
            di "=== Starting precipitation analysis for `country_name' (`country_code') ==="
            di "=== With Robust Gamma Fitting (Automatic Scaling) ==="
            local start_time = clock("$S_TIME", "hms")
            
            * Create directories for outputs and diagnostics
            capture mkdir "$cf/data/derived/era/`country_name'"
            capture mkdir "$cf/figures"
            capture mkdir "$cf/figures/gammafit-scaling-logs"
            
            * Check IPUMS mapping file for census years
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
                di "  Keeping only the records for `country_name'"
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
            
            * STEP 1: Load and process ERA data with phenology
            di _n "STEP 1: Loading and processing ERA precipitation data..."
            capture noisily PrecipitationDataProcessor, method("load_and_aggregate") country_code("`country_code'") 
            if _rc {
                di as error "  FAILED at load_and_aggregate step"
                exit _rc
            }
            di "  ✓ STEP 1 completed successfully"
            
            * STEP 2: Fit gamma distributions with robust scaling
            di _n "STEP 2: Fitting gamma distributions with automatic scaling..."
            capture noisily PrecipitationDataProcessor, method("fit_gamma_and_save") country_code("`country_code'") 
            if _rc {
                di as error "  FAILED at gamma fitting step"
                exit _rc
            }
            di "  ✓ STEP 2 completed successfully"
            SaveGammaScalingStatistics, ///
                country("`country_name'") ///
                output_file("$cf/data/derived/era/gamma_scaling_statistics.dta") ///
                append

            * Log any gamma fitting failures for manual examination
            LogGammaFittingFailures, ///
                country("`country_name'") ///
                output_file("$cf/data/derived/era/gamma_fitting_failures.dta") ///
                append
            
            * STEP 3: Process cohort metrics by census year
            di _n "STEP 3: Processing cohort metrics by census year..."
            local temp_files ""
            local census_num = 0
            
            * Check cohort file exists
            local cohort_file "$cf/data/derived/cohort_year.dta"
            capture confirm file "`cohort_file'"
            if _rc {
                di as error "    ERROR: Cohort year file not found: `cohort_file'"
                exit 601
            }
            
            foreach census_year in `census_years' {
                local ++census_num
                di "    Census `census_num'/`n_census': `census_year'"
                
                use "`cohort_file'", clear
                
                * Filter to relevant years and cohorts
                keep if year <= `census_year'
                keep if cohort <= `census_year' - 15
                
                qui count
                di "      Cohort-year combinations: `r(N)'"
                
                if r(N) == 0 {
                    di "      WARNING: No valid cohort-year combinations for census `census_year'"
                    continue
                }
                
                local precip_file "$cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
                
                capture confirm file "`precip_file'"
                if _rc {
                    di as error "      ERROR: Precipitation file not found: `precip_file'"
                    continue
                }
                
                di "      Joining with precipitation data"
                capture noisily joinby year using "`precip_file'"
                if _rc {
                    di as error "      ERROR: Failed to join precipitation data"
                    continue
                }

                * ASSERTION 1: Each cohort should have maximum 30 year observations
                di "      Verifying cohort-year counts..."
                preserve
                    bys SMALLEST cohort: gen cohort_year_count = _N
                    qui sum cohort_year_count
                    if r(max) > 30 {
                        di as error "      ASSERTION FAILED: Found cohort with `r(max)' year observations (max should be 30)"
                        di as error "      This violates the reproductive span assumption"
                        restore
                        exit 459
                    }
                    di "      ✓ Cohort-year count validation passed (max: `r(max)' years)"
                restore

                * Calculate cohort metrics
                local temp_file "`country_code'_metrics_`census_year'"
                
                capture noisily cohort_metrics_calculator, ///
                    country_code("`country_code'") ///
                    reproductive_span(`reproductive_span') ///
                    census_year(`census_year') ///
                    save_file("`temp_file'")
                
                if _rc == 0 {
                    local temp_files "`temp_files' `temp_file'"
                    di "      ✓ Census `census_year' processed successfully"
                }
                else {
                    di "      WARNING: Failed to calculate metrics for census `census_year'"
                }
            }

            if "`temp_files'" == "" {
                di as error "  ERROR: No census years successfully processed"
                exit 459
            }
            
            di "  ✓ STEP 3 completed: `=wordcount("`temp_files'")' census years processed"
            
            * STEP 4: Combine census datasets and apply selection criteria
            di _n "STEP 4: Combining census datasets and applying selection criteria..."
            PrecipitationAnalysisWorkflow, method("combine_and_select_census") ///
                country_code("`country_code'") ///
                temp_files("`temp_files'") ///
                target_age(`target_age')
            di "  ✓ STEP 4 completed successfully"
             
            * Cleanup temporary files if requested
            if "`cleanup'" == "true" {
                di _n "STEP 5: Cleaning up temporary files..."
                PrecipitationAnalysisWorkflow, method("cleanup_temp_files") ///
                    country_code("`country_code'") ///
                    temp_files("`temp_files'")
                di "  ✓ Temporary files cleaned up"
            }
            
            di _n "=== Analysis complete for `country_name' (`country_code') ==="
        }
        
        else if "`method'" == "combine_and_select_census" {
            di "    Combining census datasets..."
            
            local first_file: word 1 of `temp_files'
            
            capture confirm file "$cf/data/derived/era/`country_name'/`first_file'.dta"
            if _rc {
                di as error "    ERROR: First temp file not found: `first_file'.dta"
                exit 601
            }
            
            use "$cf/data/derived/era/`country_name'/`first_file'.dta", clear
            
            qui count
            local first_obs = r(N)
            di "      Loaded first file: `first_obs' observations"
            
            local remaining_files: list temp_files - first_file
            local remaining_count = wordcount("`remaining_files'")
            
            if `remaining_count' > 0 {
                di "      Appending `remaining_count' additional files..."
                foreach file in `remaining_files' {
                    capture append using "$cf/data/derived/era/`country_name'/`file'.dta"
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
            
            * Create census selection variables
            gen age_in_census = census - cohort
            
            bys SMALLEST cohort: egen max_age_observed = max(age_in_census)
            gen cohort_reaches_reproductive = (max_age_observed >= 15)
            gen exposure_years = max(0, min(age_in_census, 44) - 15 + 1)
            replace exposure_years = 0 if age_in_census < 15
            gen full_exposure = (exposure_years == 30)
            
            drop max_age_observed
            
            qui count if full_exposure == 1
            local full_exposure_count = r(N)
            di "      Cohorts with full 30-year exposure: `full_exposure_count'"
            
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
            
            * Display variable summary
            di "      Key variables in final dataset:"
            foreach var in share_abn_precip avg_precip_spi {
                capture confirm variable `var'
                if !_rc {
                    qui sum `var'
                    di "        `var': mean=`=round(r(mean), 0.001)', sd=`=round(r(sd), 0.001)'"
                }
            }
            
            * Define age bins (must match cohort_metrics_calculator)
            local bin_starts "15 20 25 30 35 40"
            local bin_ends   "19 24 29 34 39 44"
            local bin_labels "15_19 20_24 25_29 30_34 35_39 40_44"
            local n_bins : word count `bin_starts'

            local broad_starts "15 30"
            local broad_ends   "29 44"
            local broad_labels "15_29 30_44"
            local n_broad : word count `broad_starts'

            * Apply variable labels
            label variable precip_total "Precipitation"
            label variable precip_gs_total "Precipitation (growing season)"
            label variable precip_ngs_total "Precipitation (non-growing season)"
            label variable tot_abn_precip "Total years of abnormal precipitation"
            label variable avg_precip_spi "Precipitation SPI"
            label variable share_abn_precip "Share of abnormal precipitation"

            foreach condition in dry wet {
                label variable tot_abn_precip_`condition' "Total number of abnormally `condition' years"
                label variable share_abn_precip_`condition' "Share of abnormally `condition' years"
                foreach season in gs ngs {
                    label variable tot_abn_precip_`season' "Total years of abnormal precipitation in `season'"
                    label variable tot_abn_precip_`season'_`condition' "Total number of abnormally `condition' years in `season'"
                    label variable share_abn_precip_gs_`condition' "Share of abnormally `condition' years, growing season"
                    label variable share_abn_precip_ngs_`condition' "Share of abnormally `condition' years, non-growing season"
                }
            }

            foreach season in gs ngs {
                label variable avg_precip_`season'_spi "Precipitation SPI, (`season')"
                label variable share_abn_precip_`season' "Share of abnormal precipitation, (`season')"
            }

            * Age-bin-specific labels — fine bins
            forvalues i = 1/`n_bins' {
                local s : word `i' of `bin_starts'
                local e : word `i' of `bin_ends'
                local lbl : word `i' of `bin_labels'

                label variable precip_total_`lbl'     "Precipitation (ages `s'-`e')"
                label variable precip_gs_total_`lbl'  "Precipitation (growing season), ages `s'-`e'"
                label variable precip_ngs_total_`lbl' "Precipitation (non-growing season), ages `s'-`e'"

                label variable avg_precip_spi_`lbl'    "Precipitation SPI, ages: (`lbl')"
                label variable share_abn_precip_`lbl'  "Share of abnormal precipitation, ages: (`lbl')"
                label variable tot_abn_precip_`lbl'    "Total years of abnormal precipitation, ages (`lbl')"

                foreach condition in dry wet {
                    label variable tot_abn_precip_`condition'_`lbl'     "Total number of abnormally `condition' years, ages (`lbl')"
                    label variable share_abn_precip_`condition'_`lbl'   "Share of abnormally `condition' years, ages: (`lbl')"
                }
                foreach season in gs ngs {
                    label variable avg_precip_`season'_spi_`lbl'         "Precipitation SPI, (`season'), ages: (`lbl')"
                    label variable share_abn_precip_`season'_`lbl'       "Share of abnormal precipitation, (`season'), ages: (`lbl')"
                    label variable tot_abn_precip_`season'_`lbl'         "Total years of abnormal precipitation, (`season'), ages (`lbl')"
                    foreach condition in dry wet {
                        label variable tot_abn_precip_`season'_`condition'_`lbl'   "Total number of abnormally `condition' years, (`season'), ages (`lbl')"
                        label variable share_abn_precip_`season'_`condition'_`lbl' "Share of abnormally `condition' years, (`season'), ages: (`lbl')"
                    }
                }
            }

            * Age-bin-specific labels — broad bins
            forvalues i = 1/`n_broad' {
                local s : word `i' of `broad_starts'
                local e : word `i' of `broad_ends'
                local lbl : word `i' of `broad_labels'

                label variable precip_total_`lbl'     "Precipitation (ages `s'-`e')"
                label variable precip_gs_total_`lbl'  "Precipitation (growing season), ages `s'-`e'"
                label variable precip_ngs_total_`lbl' "Precipitation (non-growing season), ages `s'-`e'"

                label variable avg_precip_spi_`lbl'    "Precipitation SPI, ages: (`lbl')"
                label variable share_abn_precip_`lbl'  "Share of abnormal precipitation, ages: (`lbl')"
                label variable tot_abn_precip_`lbl'    "Total years of abnormal precipitation, ages (`lbl')"

                foreach condition in dry wet {
                    label variable tot_abn_precip_`condition'_`lbl'     "Total number of abnormally `condition' years, ages (`lbl')"
                    label variable share_abn_precip_`condition'_`lbl'   "Share of abnormally `condition' years, ages: (`lbl')"
                }
                foreach season in gs ngs {
                    label variable avg_precip_`season'_spi_`lbl'         "Precipitation SPI, (`season'), ages: (`lbl')"
                    label variable share_abn_precip_`season'_`lbl'       "Share of abnormal precipitation, (`season'), ages: (`lbl')"
                    label variable tot_abn_precip_`season'_`lbl'         "Total years of abnormal precipitation, (`season'), ages (`lbl')"
                    foreach condition in dry wet {
                        label variable tot_abn_precip_`season'_`condition'_`lbl'   "Total number of abnormally `condition' years, (`season'), ages (`lbl')"
                        label variable share_abn_precip_`season'_`condition'_`lbl' "Share of abnormally `condition' years, (`season'), ages: (`lbl')"
                    }
                }
            }


            local final_file "$cf/data/derived/era/`country_name'/`country_name'_allcensus-cohort-precip.dta"
            capture save "`final_file'", replace
            if _rc != 0 {
                di as error "      ERROR: Cannot save final dataset to `final_file'"
                di as error "      Check directory permissions and disk space"
                exit 603
            }
            di "    ✓ Saved final dataset: `country_name'_allcensus-cohort-precip.dta"
            di "    ✓ File location: `final_file'"
        }

        else if "`method'" == "cleanup_temp_files" {
            local files_deleted = 0
            
            di "      Deleting temporary cohort metric files..."
            foreach file in `temp_files' {
                local file_path "$cf/data/derived/era/`country_name'/`file'.dta"
                capture erase "`file_path'"
                if _rc == 0 {
                    local ++files_deleted
                    di "        Deleted: `file'.dta"
                }
                else {
                    di "        WARNING: Could not delete `file'.dta"
                }
            }
            
            di "      Deleting intermediate precipitation files..."
            local precip_file "$cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
            capture erase "`precip_file'"
            if _rc == 0 {
                local ++files_deleted
                di "        Deleted: `country_name'_precip_annual.dta"
            }
            
            local gammafit_file "$cf/data/derived/era/`country_name'/`country_code'_gammafit.dta"
            capture erase "`gammafit_file'"
            if _rc == 0 {
                local ++files_deleted
                di "        Deleted: `country_code'_gammafit.dta"
            }
            
            di "      ✓ Cleanup complete: `files_deleted' temporary files removed"
        }
        
        else if "`method'" == "diagnostic_report" {
            * Diagnostic method to report on current state
            di "=== Diagnostic Report for `country_name' (`country_code') ==="
            
            * Check existence of key files
            local key_files = ""
            local key_files "`key_files' $cf/data/derived/era/`country_name'/`country_name'_precip_annual.dta"
            local key_files "`key_files' $cf/data/derived/era/`country_name'/`country_code'_gammafit.dta"
            local key_files "`key_files' $cf/data/derived/era/`country_name'/`country_name'_allcensus-cohort-precip.dta"
            
            foreach file in `key_files' {
                capture confirm file "`file'"
                if _rc == 0 {
                    di "  ✓ Found: `file'"
                    use "`file'", clear
                    qui count
                    di "      Observations: `r(N)'"
                    di "      Variables: `c(k)'"
                }
                else {
                    di "  ✗ Missing: `file'"
                }
            }
            
            * Check gamma scaling diagnostics
            capture mkdir "$cf/figures/gammafit-scaling-logs"
            local png_files: dir "$cf/figures/gammafit-scaling-logs" files "*.png"
            local n_diagnostics: word count `png_files'
            di "  Gamma scaling diagnostic plots: `n_diagnostics'"
        }
    end


******************************
* EXECUTION
******************************
    capture program drop RunPrecipitationAnalysis
    program define RunPrecipitationAnalysis
        syntax, country_codes(string) [cleanup(string) test_mode(string)]
        
        /*
        Program: RunPrecipitationAnalysis
        Description: Main execution function that runs the complete precipitation analysis pipeline
        Parameters:
            country_codes: Space-separated list of country codes to process
            cleanup: "true" to remove temporary files after processing (optional)
            test_mode: "true" to run in test mode with limited countries (optional)
        */
        
        di ""
        di "==============================================="
        di "PRECIPITATION ANALYSIS PIPELINE EXECUTION"
        di "==============================================="
        di "Start time: $S_TIME, $S_DATE"
        di ""
        
        * Initialize configuration
        ClimateAnalysisConfig, method("init")
        
        * Create necessary directories
        capture mkdir "$cf/data/derived/era"
        capture mkdir "$cf/figures"
        capture mkdir "$cf/figures/gammafit-scaling-logs"
        capture mkdir "$cf_overleaf/table"
        capture erase "$cf/data/derived/era/gamma_scaling_statistics.dta"
        capture erase "$cf/data/derived/era/gamma_fitting_failures.dta"
        
        * Process countries
        local total_countries : word count `country_codes'
        di "Processing `total_countries' countries"
        di "Countries: `country_codes'"
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
            di "COUNTRY `country_num'/`total_countries': `country_name' (`country_code')"
            di "============================================================================"
            
            * Run the full analysis pipeline
            capture noisily {
                PrecipitationAnalysisWorkflow, ///
                    method("run_full_analysis") ///
                    country_code("`country_code'") ///
                    cleanup("`cleanup'")
            }
            
            if _rc == 0 {
                local countries_completed "`countries_completed' `country_code'"
                di "✓ SUCCESS: `country_name' (`country_code') completed"
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
                    di "  > Invalid syntax or country code"
                }
                else {
                    di "  > Unknown error - check log for details"
                }
            }
            di ""
        }
        
        * Generate summary report
        local completed_count : word count `countries_completed'
        local failed_count : word count `countries_failed'
        
        di "==============================================="
        di "PIPELINE EXECUTION COMPLETE"
        di "==============================================="
        di "Completion time: $S_TIME, $S_DATE"
        di ""
        di "SUMMARY:"
        di "  Successful: `completed_count'/`total_countries' countries"
        di "  Failed:     `failed_count'/`total_countries' countries"
        di ""
        
        if `completed_count' > 0 {
            di "SUCCESSFULLY COMPLETED COUNTRIES:"
            foreach country_code in `countries_completed' {
                GetCountryName, code("`country_code'")
                di "  ✓ `r(name)' (`country_code')"
            }
            di ""
            
            * Generate Gamma Scaling Summary Table
            di "Generating gamma scaling summary table..."
            capture noisily GenerateGammaScalingTable
            if _rc == 0 {
                di "✓ Gamma scaling table saved to: $cf_overleaf/table/gamma_scaling_summary.tex"
            }
            else {
                di "WARNING: Could not generate gamma scaling table"
            }
            
        }

        * Report on gamma fitting failures
            capture confirm file "$cf/data/derived/era/gamma_fitting_failures.dta"
            if _rc == 0 {
                preserve
                    use "$cf/data/derived/era/gamma_fitting_failures.dta", clear
                    qui count
                    di "  Total SMALLEST units with fitting failures: `r(N)'"
                    di "  Failure log saved to: $cf/data/derived/era/gamma_fitting_failures.dta"
                restore
            }
            else {
                di "  No gamma fitting failures across all countries"
            }
        
        if `failed_count' > 0 {
            di "FAILED COUNTRIES:"
            foreach country_code in `countries_failed' {
                GetCountryName, code("`country_code'")
                di "  ✗ `r(name)' (`country_code')"
            }
            di ""
            
            di "TROUBLESHOOTING TIPS:"
            di "1. Check that all required data files exist:"
            di "   - ERA data: $cf/data/derived/era/00-subunit-day-data/[country]-era-subunit-day-1940-2023.dta"
            di  "   - Phenology: $cf/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv"
            di "   - IPUMS: $cf/data/derived/ipums/country-census-years-mapping.dta"
            di "   - Cohort: $cf/data/derived/cohort_year.dta"
            di "2. Verify country names match exactly in phenology and IPUMS files"
            di "3. Check that directories exist and have write permissions"
            di "4. Run individual countries to identify specific issues"
        }
        
        di ""
        di "OUTPUTS GENERATED:"
        di "  - Country-level precipitation datasets: $cf/data/derived/era/[country]/"
        di "  - Gamma scaling statistics: $cf/data/derived/era/gamma_scaling_statistics.dta"
        di "  - Gamma scaling LaTeX table: $cf_overleaf/table/gamma_scaling_summary.tex"
        di ""
        di "==============================================="
    end


* MAIN EXECUTION - UNCOMMENT TO RUN
********************************************************************************



* Example 1: Full production run
RunPrecipitationAnalysis, ///
    country_codes("BEN BWA BFA CMR ETH GHA GIN IVC KEN LSO LBR MWI MLI MOZ RWA SLE ZAF SSD SEN SDN TZA TGO UGA ZMB ZWE") ///
    cleanup("true")

// * Example 2: Test run with 3 countries  
// RunPrecipitationAnalysis, ///
//     country_codes("BWA GHA KEN") ///
//     cleanup("true")

* Example 3: Single country for debugging
// RunPrecipitationAnalysis, ///
//     country_codes("SEN") ///
//     cleanup("true")
