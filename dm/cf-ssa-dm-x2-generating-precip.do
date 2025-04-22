* Project:      Climate Fertility in sub-Saharan Africa
* Author:       Prayog Bhattarai
* Date:         2025-04-02
* Description:  Stata demonstration of the code for generating precipitation variables
                * precipitation data is fitted to a gamma distribution.
                * current code is for Kenya only. plan is to scale this process to all IPUMS/DHS countries


********************************************************************************
* 0. setup
********************************************************************************
* ssc install gammafit
* user-dependent directory. add yourself in this condition if you want to run the code
if "`c(username)'" == "prayog" {
    global dir "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
}
else if inlist("`c(username)'", "yogita") {
    global dir "/Users/`c(username)'/Climate_Change_and_Fertility_in_SSA"
}
else if inlist("`c(username)'", "celin") {
    global dir "/Users/`c(username)'/Climate_Change_and_Fertility_in_SSA"
}
else if inlist("`c(username)'", "yun") {
    global dir "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
}
global era_intermediate "$dir/data/derived/era"

* load daily precipitation data from 1940 to 2019
use "$era_intermediate/Kenya/ERA_subcounty_day_precipitation_1940_2019_compiled.dta", clear  // source: cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
keep GEO_CONCAT precip_area day month year
ren precip_area precip


********************************************************************************
* 1. functions
********************************************************************************

cap drop program fit_gamma_dist
program define fit_gamma_dist 
    /*
    Program: fit_gamma_dist
    Description: This program fits gamma distributions to precipitation data and calculates 
    standardized precipitation indices (SPI) for different geographic sub-units. For each 
    precipitation variable (total, growing season, and non-growing season):

    1. Fits a gamma distribution to the data for each geographic sub-unit
    2. Calculates shape (alpha) and scale (beta) parameters 
    3. Computes cumulative distribution function (CDF) values
    4. Converts CDF to standardized precipitation index (SPI) using inverse normal
    5. Creates binary indicators for:
        - Wet seasons (P >= 0.85)
        - Dry seasons (P <= 0.15) 
        - Abnormal seasons (either wet or dry)

    Required variables:
    - precip: Total precipitation
    - precip_gs: Growing season precipitation 
    - precip_ngs: Non-growing season precipitation
    - GEO_CONCAT: Geographic sub-unit identifier

    Generated variables (for each precipitation type):
    - alpha: Shape parameter of gamma distribution
    - beta: Scale parameter of gamma distribution 
    - P: Cumulative probability from gamma distribution
    - spi: Standardized precipitation index
    - abn_*_wet: Binary indicator for wet season
    - abn_*_dry: Binary indicator for dry season
    - abn_*: Binary indicator for abnormal season
    - *_spi: SPI value for each precipitation type

    Version: Stata 16.0
    */
    version 16.0
    cap drop alpha beta P spi precip_wet precip_dry precip_spi ///
         precip_gs_wet precip_gs_dry precip_ngs_wet precip_ngs_dry ///
         precip_gs_spi precip_ngs_spi

    gen alpha = .
    gen beta = .
    gen P = .
    gen spi = .

    levelsof GEO_CONCAT, local(unique_values) // store names of sub-units here
    
    foreach var of varlist precip precip_gs precip_ngs {
        gen abn_`var'_wet = .
        gen abn_`var'_dry = .
        gen `var'_spi = .
        gen abn_`var' = .

        di "`var' variables for wetness, dryness, and SPI created."

        foreach sc in `unique_values' {
            di "Subunit: `sc'"
            * fit the gamma distribution
            qui gammafit `var' if GEO_CONCAT == "`sc'"
            if _rc == 0 {
                di "1. Gamma fit successful"
                replace alpha = e(alpha) if GEO_CONCAT == "`sc'"                        // shape parameter
                replace beta = e(beta) if GEO_CONCAT == "`sc'"                          // scale parameter
				replace P = gammap(e(alpha), `var'/e(beta)) if GEO_CONCAT == "`sc'"     // probability density function
                di "2. Shape and scale parameters calculated. CDF value calculated."
            }
            else di "Subunit `sc' gamma fit failed. Skipping this subunit." _rc 
            replace spi = invnormal(P) if GEO_CONCAT == "`sc'" // calculate SPI
            di "3. SPI calculated."
            * calculate SPI for wet and dry seasons
            replace abn_`var'_wet = (P >= 0.85) if GEO_CONCAT == "`sc'" // wet season
            replace abn_`var'_dry = (P <= 0.15) if GEO_CONCAT == "`sc'" // dry season
            replace abn_`var' = (P >= 0.85 | P <= 0.15) if GEO_CONCAT == "`sc'" // abnormal season
            replace `var'_spi = spi if GEO_CONCAT == "`sc'" // SPI for the variable
            di "4. Indicators updated."
        }
    }
end


cap program drop calculate_precip_metrics
program define calculate_precip_metrics
    /*
    This program calculates various precipitation metrics for demographic analysis in Kenya.
    It processes precipitation data by age groups (14-29 and 30-44) and generates:

    1. Total precipitation metrics:
        - Overall, growing season (gs), and non-growing season (ngs) precipitation
        - Abnormal precipitation measures (dry/wet periods)
        
    2. Age-specific precipitation metrics:
        - Separate calculations for age groups 14-29 and 30-44
        - Includes growing and non-growing season measures
        
    3. Standardized Precipitation Index (SPI) averages:
        - Overall, growing season, and non-growing season
        - Age-group specific calculations

    4. Share calculations:
        - Proportion of abnormal precipitation years over reproductive span
        - Separated by growing/non-growing seasons and age groups

    Parameters:
        reproductive_span - Length of reproductive period in years
        save_file        - Name of output file to save results
        census_year      - Optional: Census year (default 2019)

    Output:
        Saves calculated metrics to specified file in ERA intermediate Kenya directory
    */
    version 16.0
    syntax, reproductive_span(real) save_file(string) [census_year(real 2019)]
		
    g age_14_29 = inrange(age, 14, 29)
    g age_30_44 = inrange(age, 30, 44)
    merge m:1 GEO_CONCAT year using "$era_intermediate/Kenya/gammafit_result.dta"

    * create age-group interactions
    local varlist precip precip_gs precip_ngs abn_precip_dry abn_precip_wet ///
        abn_precip abn_precip_gs_dry abn_precip_gs_wet abn_precip_gs ///
        abn_precip_ngs_dry abn_precip_ngs_wet abn_precip_ngs precip_spi precip_gs_spi precip_ngs_spi
   
    foreach var of local varlist {
        g `var'_14_29 = `var' * age_14_29
        g `var'_30_44 = `var' * age_30_44
    }

    * FIXME: confirming that we use the 14-44 age range
    collapse (sum) precip_total = precip ///
        precip_total_14_29 = precip_14_29 ///
        precip_total_30_44 = precip_30_44 ///
        precip_gs_total = precip_gs ///
        precip_ngs_total = precip_ngs ///
        tot_abn_precip = abn_precip ///
        tot_abn_precip_gs = abn_precip_gs ///
        tot_abn_precip_ngs = abn_precip_ngs ///
        tot_abn_dry = abn_precip_dry ///
        tot_abn_wet = abn_precip_wet ///
        tot_abn_dry_gs = abn_precip_gs_dry ///
        tot_abn_wet_gs = abn_precip_gs_wet ///
        tot_abn_dry_ngs = abn_precip_ngs_dry ///
        tot_abn_wet_ngs = abn_precip_ngs_wet ///
        tot_abn_precip_gs_14_29 = abn_precip_gs_14_29 ///
        tot_abn_precip_gs_30_44 = abn_precip_gs_30_44 ///
        tot_abn_precip_ngs_14_29 = abn_precip_ngs_14_29 ///
        tot_abn_precip_ngs_30_44 = abn_precip_ngs_30_44 ///
        tot_abn_dry_gs_14_29 = abn_precip_gs_dry_14_29 ///
        tot_abn_dry_gs_30_44 = abn_precip_gs_dry_30_44 ///
        tot_abn_wet_gs_14_29 = abn_precip_gs_wet_14_29 ///
        tot_abn_wet_gs_30_44 = abn_precip_gs_wet_30_44 ///
        tot_abn_dry_ngs_14_29 = abn_precip_ngs_dry_14_29 ///
        tot_abn_dry_ngs_30_44 = abn_precip_ngs_dry_30_44 ///
        tot_abn_wet_ngs_14_29 = abn_precip_ngs_wet_14_29 ///
        tot_abn_wet_ngs_30_44 = abn_precip_ngs_wet_30_44 ///
        (mean) avg_precip_spi = precip_spi ///
            avg_precip_spi_14_29 = precip_spi_14_29 ///
            avg_precip_spi_30_44 = precip_spi_30_44 ///
            avg_precip_gs_spi = precip_gs_spi ///
            avg_precip_gs_spi_14_29 = precip_gs_spi_14_29 ///
            avg_precip_gs_spi_30_44 = precip_gs_spi_30_44 ///
            avg_precip_ngs_spi = precip_ngs_spi ///
            avg_precip_ngs_spi_14_29 = precip_ngs_spi_14_29 ///
            avg_precip_ngs_spi_30_44 = precip_ngs_spi_30_44 ///
        , by(GEO_CONCAT cohort)

    gen census = `census_year'

    * calculate shares
    foreach var in precip dry wet {
        gen share_abnormal_`var' = tot_abn_`var' / `reproductive_span'
        foreach cond in gs ngs {
            gen share_abnormal_`var'_`cond' = tot_abn_`var'_`cond' / `reproductive_span'
            foreach age in 14_29 30_44 {
                gen share_abnormal_`var'_`cond'_`age' = tot_abn_`var'_`cond'_`age' / (`reproductive_span' / 2)
            }
        }
    }

    * save results
    save "$era_intermediate/Kenya/`save_file'.dta", replace
    di "Results saved to $era_intermediate/Kenya/`save_file'.dta"
end


********************************************************************************
* 2. collapse data to annual level
********************************************************************************

* generate decade variable
g decade = floor((year - 1940)/10) * 10 + 1940

* create growing season and non-growing season variables
g growing_season = inlist(month, 3, 4, 5, 10, 11)
g non_growing = !inlist(month, 3, 4, 5, 10, 11)

foreach var of varlist precip {
    g `var'_gs = `var' * growing_season
    g `var'_ngs = `var' * non_growing
}

collapse (sum) precip_*, by(GEO_CONCAT year)
recast float year
tempfile annual_precipitation
save `annual_precipitation', replace

********************************************************************************
* 3. fit the gamma distribution
********************************************************************************

use `annual_precipitation', clear
fit_gamma_dist

keep GEO_CONCAT year abn_precip abn_precip_gs abn_precip_ngs ///
    abn_precip_gs_wet abn_precip_gs_dry abn_precip_ngs_wet abn_precip_ngs_dry ///
    precip_spi precip_gs_spi precip_ngs_spi ///
    abn_precip_wet abn_precip_dry ///
    alpha beta P spi ///

tempfile `gammafit_result'
save "$era_intermediate/Kenya/gammafit_result.dta", replace


********************************************************************************
* 4. create share of abnormal precipitation variables 
********************************************************************************

* 2019
use "$era_intermediate/cohort_year.dta", clear
joinby year using `annual_precipitation'
calculate_precip_metrics, reproductive_span(30) save_file("total_precip_2019") census_year(2019)

* 2009
use "$era_intermediate/cohort_year.dta", clear
keep if cohort <= 1995
keep if year <= 2009
joinby year using `annual_precipitation'
calculate_precip_metrics, reproductive_span(30) save_file("total_precip_2009") census_year(2009)

* 1999
use "$era_intermediate/cohort_year.dta", clear
keep if cohort <= 1985
keep if year <= 1999
joinby year using `annual_precipitation'
calculate_precip_metrics, reproductive_span(30) save_file("total_precip_1999") census_year(1999)

* 1989
use "$era_intermediate/cohort_year.dta", clear
keep if cohort <= 1975
keep if year <= 1989
joinby year using `annual_precipitation'
calculate_precip_metrics, reproductive_span(30) save_file("total_precip_1989") census_year(1989)

* append the data
append using "$era_intermediate/Kenya/total_precip_1999.dta"
append using "$era_intermediate/Kenya/total_precip_2009.dta"
append using "$era_intermediate/Kenya/total_precip_2019.dta"
drop if missing(cohort)
sort GEO_CONCAT cohort census
order GEO_CONCAT cohort census

bys GEO_CONCAT cohort census: keep if  _n == 1 

save "$era_intermediate/Kenya/census-cohort-precipitation.dta", replace
