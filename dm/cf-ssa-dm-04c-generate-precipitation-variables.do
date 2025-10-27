********************************************************************************
* PROJECT:              Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:               Prayog Bhattarai
* DATE MODIFIED:        2025-10-09
* DESCRIPTION:          Generate precipitation variables
* DEPENDENCIES:         
* 
* NOTES: We use the subunit-day data created in previous steps to generate subunit-cohort-census level precipitation data
*        Input data is in data/derived/era/00-subunit-day-data
*        Output data is in data/derived/era/`country_name'/`country_name'-allcensus-cohort-precip.dta
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
                global cf_overleaf "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
            }
            else if inlist("`c(username)'", "yogita") {
                global dir "/Users/`c(username)'/NUS Dropbox/`c(username)'/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "/Users/`c(username)'/NUS Dropbox/`c(username)'/Apps/Overleaf/climate-fertility-ssa/output"
            }
            else if inlist("`c(username)'", "celin", "CE.4875") {
                global dir "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "/Users/`c(username)'/Apps/Overleaf/climate-fertility-ssa/output"
            }
            else if inlist("`c(username)'", "yun") {
                global dir "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
                global cf_overleaf "/Users/`c(username)'/Apps/Overleaf/climate-fertility-ssa/output"
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
* Gamma Distribution Fitting (for Precipitation)
********************************************************************************

capture program drop GammaDistributionFitter
program define GammaDistributionFitter
    syntax, method(string)  [variables(string)]    
    
    if "`method'" == "fit_distributions" {
        di "  Fitting gamma distributions..."
        
        local varlist "`variables'"
        if "`varlist'" == "" local varlist "precip precip_gs precip_ngs"

        capture drop alpha beta P spi *_spi *_wet *_dry
        
        gen alpha = .
        gen beta = .
        gen P = .
        gen spi = .
        
        levelsof SMALLEST, local(subunits)
        local n_subunits = wordcount("`subunits'")
        di "    Processing `n_subunits' geographic units"
        
        if `n_subunits' == 0 {
            di as error "    ERROR: No geographic units found"
            exit 459
        }
        
        foreach var in `varlist' {
            di "      Variable: `var'"
            
            * Check if variable exists
            capture confirm variable `var'
            if _rc {
                di "      WARNING: Variable `var' not found, skipping"
                continue
            }
            
            di "Generating abnormal variables ..."
            gen abn_`var'_wet = .
            gen abn_`var'_dry = .
            gen abn_`var' = .
            gen `var'_spi = .
            
            local success_count = 0
            local fail_count = 0
            foreach subunit in `subunits' {
                capture gammafit `var' if SMALLEST == `subunit'
                if _rc == 0 {
                    local ++success_count
                    replace alpha = e(alpha) if SMALLEST == `subunit'
                    replace beta = e(beta) if SMALLEST == `subunit'
                    
                    replace P = gammap(e(alpha), `var'/e(beta)) if SMALLEST == `subunit'
                    replace spi = invnormal(P) if SMALLEST == `subunit'
                    
                    replace abn_`var'_wet = (P >= 0.85) if SMALLEST == `subunit'
                    replace abn_`var'_dry = (P <= 0.15) if SMALLEST == `subunit'
                    replace abn_`var' = (P >= 0.85 | P <= 0.15) if SMALLEST == `subunit'
                    replace `var'_spi = spi if SMALLEST == `subunit'
                }
                else {
                    local ++fail_count
                }
            }
            di "      Successful fits: `success_count'/`n_subunits' (Failed: `fail_count')"
            
            * Store subunit-level parameters for this variable
            preserve
                bysort SMALLEST: keep if _n == 1
                keep SMALLEST alpha beta
                keep if !missing(alpha) & !missing(beta)
                
                tempfile gamma_params_`var'
                save `gamma_params_`var''
            restore
        }
        * Calculate country-level summaries from subunit parameters
        preserve
            use `gamma_params_precip', clear
            qui sum alpha
            global alpha_mean = r(mean)
            global alpha_sd = r(sd)
            qui sum beta
            global beta_mean = r(mean)
            global beta_sd = r(sd)
        restore
        
        di "  Gamma fitting complete"
    }
end

********************************************************************************
* Summary Statistics Collection
********************************************************************************

capture program drop CollectSummaryStats
program define CollectSummaryStats, rclass
    syntax, country_code(string) elapsed_time(real)
    
    * Get country name
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"
    
    di "  Collecting summary statistics for `country_name'..."
    
    * 1. Number of SMALLEST units
    qui count if !missing(SMALLEST)
    if r(N) > 0 {
        qui levelsof SMALLEST, local(subunits)
        local n_subunits = wordcount("`subunits'")
    }
    else {
        local n_subunits = 0
    }
    
    * 2. Zero precipitation count
    qui count if precip_total == 0 & !missing(precip_total)
    local zero_precip = r(N)
    
    * 3-5. Abnormal precipitation statistics
    qui sum tot_abn_precip if !missing(tot_abn_precip)
    local abn_precip_mean = r(mean)
    
    qui sum tot_abn_precip_wet if !missing(tot_abn_precip_wet)
    local wet_mean = r(mean)
    
    qui sum tot_abn_precip_dry if !missing(tot_abn_precip_dry)
    local dry_mean = r(mean)
    
    * 6. SPI Extreme count
    qui count if (avg_precip_spi < -3 | avg_precip_spi > 3) & !missing(avg_precip_spi)
    // local spi_extreme_obs = r(N)
    
    * Count unique subunits with extreme SPI
    qui sum avg_precip_spi if !missing(avg_precip_spi)
    local spi_mean = r(mean)
    // tempvar extreme_flag
    // gen `extreme_flag' = (avg_precip_spi < -3 | avg_precip_spi > 3) & !missing(avg_precip_spi)
    // qui bysort SMALLEST: egen temp_has_extreme = max(`extreme_flag')
    // qui count if temp_has_extreme == 1 & !missing(SMALLEST)
    // local spi_extreme = r(N)
    // qui drop temp_has_extreme
    
    * 7-8. Alpha and Beta parameters (mean across subunits)
    local alpha_mean = $alpha_mean
    local beta_mean = $beta_mean

    
    * 9. Processing time
    local time_sec = round(`elapsed_time', 1)
    
    * Return all statistics
    return local country_name "`country_name'"
    return scalar n_subunits = `n_subunits'
    return scalar zero_precip = `zero_precip'
    return scalar abn_precip = `abn_precip_mean'
    return scalar wet_mean = `wet_mean'
    return scalar dry_mean = `dry_mean'
    return scalar spi_mean = `spi_mean'
    return scalar alpha = `alpha_mean'
    return scalar beta = `beta_mean'
    return scalar time_sec = `time_sec'
    
    di "  Statistics collected successfully"
end

********************************************************************************
* Precipitation Data Processing
********************************************************************************

capture program drop PrecipitationDataProcessor
program define PrecipitationDataProcessor
    syntax, method(string) country_code(string) [census_years(string) max_cohort(string)]
    
    * Get country name from code
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"
    
    if "`country_name'" == "" {
        di as error "ERROR: Unknown country code: `country_code'"
        exit 198
    }
    
    if "`method'" == "load_and_aggregate" {
        di "  Loading and aggregating precipitation data for `country_name' (`country_code')"
        
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
        local required_vars "SMALLEST precip day month year"
        foreach var of local required_vars {
            capture confirm variable `var'
            if _rc {
                di as error "    ERROR: Required variable `var' not found in ERA data"
                exit 111
            }
        }
        

        if "`country_code'" == "BWA" | "`country_code'" == "SDN" {
            di "Botswana has a two-number country code that overlaps with Sudan and South Sudan. "
            di "This needs special handling."
            di "    Special handling for `country_code' - removing invalid regions"
            drop if inlist(floor(SMALLEST/10000), 728, 729)
        }
        
        di "Keeping only the variables we need"
        keep SMALLEST precip day month year
        di "Generating a scaled version of precipitation to allow gamma distribution fitting."
        generate precip_100 = precip / 100
        la var precip_100 "Precipitation (scaled by 100)"
        drop precip
        rename precip_100 precip
        di "The old variable precip has been replaced by the scaled version."

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
        di "Examining merge success ..."
        tab _merge
        drop _merge

        di "Creating growing and non-growing season indicators ..."
        gen byte gs = 0
        gen byte ngs = 0
        forvalues m = 1/12 {
            capture confirm variable growing_month_`m'
            if !_rc {
                replace gs = 1 if growing_month_`m' == 1 & month == `m'
                replace ngs = 1 if growing_month_`m' == 0 & month == `m'
            }
        }

        di "Creating precip_gs and precip_ngs"
        gen precip_gs = precip * gs 
        gen precip_ngs = precip * ngs

        la var precip_gs "Growing season precipitation"
        la var precip_ngs "Non-growing season precipitation"
        
        capture confirm string variable SMALLEST
        if !_rc {
            destring SMALLEST, replace force
        }
        
        di "    Collapsing to annual level..."
        collapse (sum) precip precip_gs precip_ngs, by(SMALLEST year)
        recast float year
        
        qui count
        di "    Annual data has `r(N)' observations"
        qui count if missing(precip)
        di "    Annual data has `r(N)' missing values for precip."
        qui count if missing(precip_gs)
        di "    Annual data has `r(N)' missing values for precip_gs"
        qui count if missing(precip_ngs)
        di "    Annual data has `r(N)' missing values for precip_ngs"


        * Create directory if it doesn't exist
        capture mkdir "$era_derived/`country_name'"
        
        local save_file "$era_derived/`country_name'/`country_name'_precip_annual.dta"
        di "    Saving to: `save_file'"
        save "`save_file'", replace
        di "  Annual precipitation data saved successfully"
    }
    
    else if "`method'" == "fit_gamma_and_save" {
        di "  Loading annual precipitation data for gamma fitting..."
        
        local annual_file "$era_derived/`country_name'/`country_name'_precip_annual.dta"
        capture confirm file "`annual_file'"
        if _rc {
            di as error "    ERROR: Annual precipitation file not found: `annual_file'"
            exit 601
        }
        
        use "`annual_file'", clear
        
        qui count
        di "    Loaded `r(N)' observations for gamma fitting"
        
        sort SMALLEST year
        
        di "Fitting gamma distributions ..."
        GammaDistributionFitter, method("fit_distributions") variables("precip precip_gs precip_ngs")
        ds
    
        local vars_to_keep "SMALLEST year abn_precip* precip*_spi alpha beta P spi"
        * Keep only variables that exist
        // local keep_vars ""
        // foreach var of local vars_to_keep {
        //     capture confirm variable `var'
        //     if !_rc {
        //         local keep_vars "`keep_vars' `var'"
        //     }
        // }
        // if "`keep_vars'" != "" {
        //     keep `keep_vars'
        // }
        // else {
        //     di as error "    ERROR: No gamma fit variables to save"
        //     exit 459
        // }
        local save_file "$era_derived/`country_name'/`country_code'_gammafit.dta"
        di "    Saving gamma fit results to: `save_file'"
        ds
        save "`save_file'", replace
        di "  Gamma fit results saved successfully"
    }
end

********************************************************************************
* Cohort Metrics Calculator (Precipitation)
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
    gen age_15_29 = inrange(age, 15, 29)
    gen age_30_44 = inrange(age, 30, 44)
    
    local gammafit_file "$era_derived/`country_name'/`country_code'_gammafit.dta"
    
    capture confirm file "`gammafit_file'"
    if _rc {
        di "      WARNING: Gamma fit file not found: `gammafit_file'"
        di "      Proceeding without SPI variables"
    }
    else {
        di "      Merging with gamma fit results"
        merge m:1 SMALLEST year using "`gammafit_file'"
        tab _merge
        drop _merge
    }
    
    local base_vars "precip abn_precip_dry abn_precip_wet abn_precip precip_spi precip_gs precip_ngs abn_precip_gs abn_precip_ngs abn_precip_gs_dry abn_precip_ngs_dry abn_precip_gs_wet abn_precip_ngs_wet precip_gs_spi precip_ngs_spi"
    
    local vars_created = 0
    foreach var of local base_vars {
        // capture confirm variable `var'
        // if _rc == 0 {
            gen `var'_15_29 = `var' * age_15_29
            gen `var'_30_44 = `var' * age_30_44
            local ++vars_created
        //}
    }
    
    di "      Created `vars_created' age-specific variables"
    
    di "      Collapsing to cohort level..."
    collapse ///
    (sum) precip_total = precip ///
          precip_gs_total = precip_gs ///
          precip_ngs_total = precip_ngs ///
          precip_total_15_29 = precip_15_29 ///
          precip_total_30_44 = precip_30_44 ///
          tot_abn_precip = abn_precip ///
          tot_abn_precip_gs = abn_precip_gs ///
          tot_abn_precip_ngs = abn_precip_ngs ///
          tot_abn_precip_dry = abn_precip_dry ///
          tot_abn_precip_wet = abn_precip_wet ///
          tot_abn_precip_gs_dry = abn_precip_gs_dry ///
          tot_abn_precip_gs_wet = abn_precip_gs_wet ///
          tot_abn_precip_ngs_dry = abn_precip_ngs_dry ///
          tot_abn_precip_ngs_wet = abn_precip_ngs_wet ///
          tot_abn_precip_15_29 = abn_precip_15_29 ///
          tot_abn_precip_30_44 = abn_precip_30_44 ///
          tot_abn_precip_dry_15_29 = abn_precip_dry_15_29 ///
          tot_abn_precip_dry_30_44 = abn_precip_dry_30_44 ///
          tot_abn_precip_wet_15_29 = abn_precip_wet_15_29 ///
          tot_abn_precip_wet_30_44 = abn_precip_wet_30_44 ///
          tot_abn_precip_gs_15_29 = abn_precip_gs_15_29 ///
          tot_abn_precip_gs_30_44 = abn_precip_gs_30_44 ///
          tot_abn_precip_ngs_15_29 = abn_precip_ngs_15_29 ///
          tot_abn_precip_ngs_30_44 = abn_precip_ngs_30_44 ///
          tot_abn_precip_gs_dry_15_29 = abn_precip_gs_dry_15_29 ///
          tot_abn_precip_gs_dry_30_44 = abn_precip_gs_dry_30_44 ///
          tot_abn_precip_ngs_dry_15_29 = abn_precip_ngs_dry_15_29 ///
          tot_abn_precip_ngs_dry_30_44 = abn_precip_ngs_dry_30_44 ///
          tot_abn_precip_gs_wet_15_29 = abn_precip_gs_wet_15_29 ///
          tot_abn_precip_gs_wet_30_44 = abn_precip_gs_wet_30_44 ///
          tot_abn_precip_ngs_wet_15_29 = abn_precip_ngs_wet_15_29 ///
          tot_abn_precip_ngs_wet_30_44 = abn_precip_ngs_wet_30_44 ///
          precip_gs_total_15_29 = precip_gs_15_29 ///
          precip_gs_total_30_44 = precip_gs_30_44 ///
          precip_ngs_total_15_29 = precip_ngs_15_29 ///
          precip_ngs_total_30_44 = precip_ngs_30_44 ///
    (mean) avg_precip_spi = precip_spi ///
           avg_precip_gs_spi = precip_gs_spi ///
           avg_precip_ngs_spi = precip_ngs_spi ///
           avg_precip_spi_15_29 = precip_spi_15_29 ///
           avg_precip_spi_30_44 = precip_spi_30_44 ///
           avg_precip_gs_spi_15_29 = precip_gs_spi_15_29 ///
           avg_precip_gs_spi_30_44 = precip_gs_spi_30_44 ///
           avg_precip_ngs_spi_15_29 = precip_ngs_spi_15_29 ///
           avg_precip_ngs_spi_30_44 = precip_ngs_spi_30_44 ///
    , by(SMALLEST cohort)
    
    gen census = `census_year'

    * Calculate shares (assuming reproductive_span is defined)
    di "Calculating shares..."

    * Basic shares
    gen share_abn_precip = tot_abn_precip / `reproductive_span'
    gen share_abn_precip_dry = tot_abn_precip_dry / `reproductive_span'
    gen share_abn_precip_wet = tot_abn_precip_wet / `reproductive_span'

    * Age-specific shares (15-29)
    gen share_abn_precip_15_29 = tot_abn_precip_15_29 / (`reproductive_span' / 2)
    gen share_abn_precip_dry_15_29 = tot_abn_precip_dry_15_29 / (`reproductive_span' / 2)
    gen share_abn_precip_wet_15_29 = tot_abn_precip_wet_15_29 / (`reproductive_span' / 2)

    * Age-specific shares (30-44)
    gen share_abn_precip_30_44 = tot_abn_precip_30_44 / (`reproductive_span' / 2)
    gen share_abn_precip_dry_30_44 = tot_abn_precip_dry_30_44 / (`reproductive_span' / 2)
    gen share_abn_precip_wet_30_44 = tot_abn_precip_wet_30_44 / (`reproductive_span' / 2)

    * Growing season shares
    gen share_abn_precip_gs = tot_abn_precip_gs / `reproductive_span'
    gen share_abn_precip_gs_dry = tot_abn_precip_gs_dry / `reproductive_span'
    gen share_abn_precip_gs_wet = tot_abn_precip_gs_wet / `reproductive_span'

    * Growing season age-specific shares (15-29)
    gen share_abn_precip_gs_15_29 = tot_abn_precip_gs_15_29 / (`reproductive_span' / 2)
    gen share_abn_precip_gs_dry_15_29 = tot_abn_precip_gs_dry_15_29 / (`reproductive_span' / 2)
    gen share_abn_precip_gs_wet_15_29 = tot_abn_precip_gs_wet_15_29 / (`reproductive_span' / 2)

    * Growing season age-specific shares (30-44)
    gen share_abn_precip_gs_30_44 = tot_abn_precip_gs_30_44 / (`reproductive_span' / 2)
    gen share_abn_precip_gs_dry_30_44 = tot_abn_precip_gs_dry_30_44 / (`reproductive_span' / 2)
    gen share_abn_precip_gs_wet_30_44 = tot_abn_precip_gs_wet_30_44 / (`reproductive_span' / 2)

    * Non-growing season shares
    gen share_abn_precip_ngs = tot_abn_precip_ngs / `reproductive_span'
    gen share_abn_precip_ngs_dry = tot_abn_precip_ngs_dry / `reproductive_span'
    gen share_abn_precip_ngs_wet = tot_abn_precip_ngs_wet / `reproductive_span'

    * Non-growing season age-specific shares (15-29)
    gen share_abn_precip_ngs_15_29 = tot_abn_precip_ngs_15_29 / (`reproductive_span' / 2)
    gen share_abn_precip_ngs_dry_15_29 = tot_abn_precip_ngs_dry_15_29 / (`reproductive_span' / 2)
    gen share_abn_precip_ngs_wet_15_29 = tot_abn_precip_ngs_wet_15_29 / (`reproductive_span' / 2)

    * Non-growing season age-specific shares (30-44)
    gen share_abn_precip_ngs_30_44 = tot_abn_precip_ngs_30_44 / (`reproductive_span' / 2)
    gen share_abn_precip_ngs_dry_30_44 = tot_abn_precip_ngs_dry_30_44 / (`reproductive_span' / 2)
    gen share_abn_precip_ngs_wet_30_44 = tot_abn_precip_ngs_wet_30_44 / (`reproductive_span' / 2)
        
        
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
* Main Precipitation Workflow
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
        local start_time = clock("$S_TIME", "hms")
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
            di "Keeping only the records of `country_name'"
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
        capture noisily PrecipitationDataProcessor, method("load_and_aggregate") country_code("`country_code'") 
        if _rc {
            di as error "  Failed at load_and_aggregate step"
            exit _rc
        }
        
        * Step 2: Fit gamma distributions
        capture noisily PrecipitationDataProcessor, method("fit_gamma_and_save") country_code("`country_code'") 
        if _rc {
            di as error "  Failed at gamma fitting step"
            exit _rc
        }
        
        * Step 3: Process cohort metrics by census year
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
            
            local precip_file "$era_derived/`country_name'/`country_name'_precip_annual.dta"
            
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
            
            local temp_file "`country_code'_metrics_`census_year'"
            
            capture noisily CohortMetricsCalculator, ///
                country_code("`country_code'") ///
                reproductive_span(`reproductive_span') ///
                census_year(`census_year') ///
                save_file("`temp_file'")
            
            if _rc == 0 {
                local temp_files "`temp_files' `temp_file'"
            }
            else {
                di "      WARNING: Failed to calculate metrics for census `census_year'"
            }
        }

        ds

        if "`temp_files'" == "" {
            di as error "  ERROR: No census years successfully processed"
            exit 459
        }
        
        di "  Combining census years..."
        PrecipitationAnalysisWorkflow, method("combine_and_select_census") ///
            country_code("`country_code'") ///
            temp_files("`temp_files'") ///
            target_age(`target_age')

        * Calculate elapsed time
        local end_time = clock("$S_TIME", "hms")
        local elapsed = (`end_time' - `start_time') / 1000  // Convert to seconds
        
        * Collect summary statistics before cleanup
        CollectSummaryStats, country_code("`country_code'") elapsed_time(`elapsed')
        
        * Store results in a global or save to file
        * Option 1: Append to a results file
        preserve
            clear
            set obs 1
            gen str20 country = "`r(country_name)'"
            gen n_subunits = `r(n_subunits)'
            gen zero_precip = `r(zero_precip)'
            gen abn_precip = `r(abn_precip)'
            gen wet_mean = `r(wet_mean)'
            gen dry_mean = `r(dry_mean)'
            gen spi_mean = `r(spi_mean)'
            gen alpha = `r(alpha)'
            gen beta = `r(beta)'
            gen time_sec = `r(time_sec)'
            
            * Append to summary file
            capture confirm file "$era_derived/summary_statistics.dta"
            if _rc {
                save "$era_derived/summary_statistics.dta", replace
            }
            else {
                append using "$era_derived/summary_statistics.dta"
                save "$era_derived/summary_statistics.dta", replace
            }
        restore
        
        if "`cleanup'" == "true" {
            di "  Cleaning up temporary files..."
            //PrecipitationAnalysisWorkflow, method("cleanup_temp_files") ///
            //    country_code("`country_code'") ///
            //    temp_files("`temp_files'")
        }
        
        di "=== Analysis complete for `country_name' (`country_code') ==="
    }
    
    else if "`method'" == "combine_and_select_census" {
        di "    Combining census datasets..."
        
        *if inlist("`country_code'", "IVC", "BFA", "CPV", "SLE", "ZAF", "SSD") {
//             local first_files: word 1 of `temp_files'
//             local second_files: word 2 of `temp_files'
//             local first_file "`first_files' `second_files'"
// 			di "File nom: `first_file'"
        *}
//         else {
            local first_file: word 1 of `temp_files'
//         }
        
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
		
		
        di "Remaining file names: `remaining files'"
		

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
        di "      Variables: "
        ds
        
        local final_file "$era_derived/`country_name'/`country_name'_allcensus-cohort-precip.dta"
        capture save "`final_file'", replace
        if _rc != 0 {
            di as error "      ERROR: Cannot save final dataset to `final_file'"
            di as error "      Check directory permissions and disk space"
            exit 603
        }
        di "    Saved final dataset: `country_name'_allcensus-cohort-precip.dta"
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
        
        di "      Deleting intermediate precipitation file..."
        local precip_file "$era_derived/`country_name'/`country_name'_precip_annual.dta"
        capture erase "`precip_file'"
        if _rc == 0 {
            local ++files_deleted
        }
        
        di "      Cleanup complete: `files_deleted' temporary files removed"
    }
end



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


********************************************************************************
* Generate LaTeX Table from Summary Statistics
********************************************************************************

capture program drop GenerateLaTeXTable
program define GenerateLaTeXTable
    
    * Load summary statistics
    use "$era_derived/summary_statistics.dta", clear
    duplicates drop country, force
    * Sort by country name
    sort country
    
    * Open file for writing
    file open latexfile using "$cf_overleaf/table/precip_summary_table.tex", write replace
    
    * Write table header
    file write latexfile "\begin{tabular}{lrrrrrrr}" _n
    file write latexfile "\toprule" _n
    file write latexfile "Country & SMALLEST units & Abn Precip Years & Abn Rainy Years & Abn Dry Years & Avg SPI & Alpha & Beta  \\" _n
    file write latexfile " & (1) & (2) & (3) & (4) & (5) & (6) & (7) \\" _n
    file write latexfile "\midrule" _n
    
    * Write data rows
    local N = _N
    forval i = 1/`N' {
        local cntry = country[`i']
        local n_sub = n_subunits[`i']
        local zero_p = zero_precip[`i']
        local abn_p = string(abn_precip[`i'], "%5.3f")
        local wet_m = string(wet_mean[`i'], "%5.3f")
        local dry_m = string(dry_mean[`i'], "%5.3f")
        local spi_m = string(spi_mean[`i'], "%5.3f")
        local alph = string(alpha[`i'], "%6.3f")
        local bet = string(beta[`i'], "%5.3f")
        local time_s = time_sec[`i']
        
        * Format zero_precip with thousands separator
        local zero_formatted = string(`zero_p', "%12.0fc")
        local zero_formatted = subinstr("`zero_formatted'", ",", "{,}", .)
        local zero_formatted = trim("`zero_formatted'")
        
        file write latexfile "`cntry' & `n_sub' & `abn_p' & `wet_m' & `dry_m' & `spi_m' & `alph' & `bet'  \\" _n
        di "`cntry' & `n_sub' & `abn_p' & `wet_m' & `dry_m' & `spi_m' & `alph' & `bet'"
    }
    
    * Write table footer
    file write latexfile "\bottomrule" _n
    file write latexfile "\end{tabular}" _n
    file close latexfile
    
    di "LaTeX table saved to: $cf_overleaf/table/precip_summary_table.tex"
end

********************************************************************************
* Main Execution
********************************************************************************

di ""
di "==============================================="
di "Multi-Country Climate Analysis Pipeline"
di "==============================================="

* Initialize configuration first
ClimateAnalysisConfig, method("init")

* Test with a small subset first
local test_codes "RWA"  // Start with just one country for testing
local country_codes "`test_codes'"

* For full run, use:
local country_codes "BEN BWA BFA CMR ETH GHA GIN IVC KEN LSO LBR MWI MLI MOZ RWA SLE ZAF SEN SDN TZA TGO UGA ZMB ZWE"

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
        PrecipitationAnalysisWorkflow, ///
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

* Generate LaTeX table if any countries completed successfully
if `completed_count' > 0 {
    di ""
    di "==============================================="
    di "Generating LaTeX Summary Table"
    di "==============================================="
    GenerateLaTeXTable
}

* Example: Run diagnostic check for a specific country
* DiagnosticCheck, country_code("BFA")

