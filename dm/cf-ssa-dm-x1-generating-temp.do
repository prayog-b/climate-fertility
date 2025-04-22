* Project:      Climate Fertility in sub-Saharan Africa
* Author:       Prayog Bhattarai
* Date:         2025-04-02
* Description:  Stata demonstration of the degree days generating code. 
                * degree days are generated for growing and non-growing seasons
                * code for generating degree day variables is in lines 34-45. 
                * current code is for Kenya only
				* plan is to scale this process to all IPUMS/DHS countries

********************************************************************************
* 0. setup
********************************************************************************
if `c(username)' == "prayog" {
    global dir "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
}
else if inlist(`c(username)', "yogita") {
    global dir "/Users/`c(username)'/Climate_Change_and_Fertility_in_SSA"
}
else if inlist(`c(username)', "celin") {
    global dir "/Users/`c(username)'/Climate_Change_and_Fertility_in_SSA"
}
global era_intermediate "$dir/data/derived/era"

* load daily temperature data from 1940 to 2019
use "data/derived/era/Kenya/ERA_subcounty_day_temperature_1940_2019_compiled.dta", clear
keep GEO_CONCAT temp_max_area temp_avg_area day month year 
ren (temp_max_area temp_avg_area) (temp_max temp_avg)


********************************************************************************
* 1. generate degree days 
********************************************************************************
* create subunit-year level degree day variables
forvalues i = 24/28 {
    * Create a new variable for degree days above temperature threshold `i`
    gen temp_max_dd_`i' = temp_max 
    * Set temperature to threshold if it's below the threshold
    replace temp_max_dd_`i' = `i' if temp_max_dd_`i' < `i'
    * Calculate degrees above threshold by subtracting threshold from temperature
    replace temp_max_dd_`i' = temp_max_dd_`i' - `i' 
    
    gen temp_avg_dd_`i' = temp_avg 
    replace temp_avg_dd_`i' = `i' if temp_avg_dd_`i' < `i'
    replace temp_avg_dd_`i' = temp_avg_dd_`i' - `i' 
}

drop temp_max temp_avg

* generate growing season DD variables
gen growing_season = inlist(month, 3, 4, 5, 10, 11)
gen non_growing = !inlist(month, 3, 4, 5, 10, 11)
* question: need to update this based on team decision

foreach var of varlist temp_* {
    gen `var'_gs = `var' * growing_season
    gen `var'_ngs = `var' * non_growing
}

* collapse daily data to subunit-year level. result is total degree days in a year
collapse (sum) temp_*, by(GEO_CONCAT year)
recast float year

save "$era_intermediate/Kenya/temperature_data.dta", replace

********************************************************************************
* 2. generate cohort-year level pairs
********************************************************************************
* generate cohorts
clear
set obs 80
gen cohort = 1926 
forvalues i = 2/80 {
    replace cohort = 1926 + `i' - 1 in `i'
}

gen key = 1
tempfile cohort
save `cohort'

* generate years
clear
set obs 80
gen year = 1940
forvalues i = 2/80 {
    replace year = 1940 + `i' - 1 in `i'
}

gen key = 1

* create cohort-year combinations
joinby key using `cohort'
sort cohort year
drop key

* generate age (how old a cohort is in a given year) 
gen age = year - cohort

* keep cohorts where age is between 14 and 44
keep if age >= 14 & age <= 44
// question: team needs to figure out the age range for analysis
save "$era_intermediate/cohort_year.dta", replace 	// cohort-year level dataset. 


********************************************************************************
* 3. merge cohort-year pairs with temperature data
********************************************************************************

cap program drop process_census_year
program define process_census_year
    args censusyear maxcohort

    /*
    Function to process temperature data for a given year
        
    Required parameters:
    -   censusyear: year of the census (1989, 1999, 2009, 2019)
        
    Optional parameters:
    -   maxcohort: latest cohort year to include in the analysis
    Example:
        process_census_year, censusyear(2009) maxcohort(1995)
    Notes:
        - Requires "cohort_year.dta" and "temperature_data.dta"
    Actions:
        - Creates age-specific temperature exposure variables
        - Collapses data by subcounty and cohort    
    */

    di "Census year: `censusyear'"
    use "$era_intermediate/cohort_year", clear
    di "- Step 0: Cohort year data opened"
    if "`maxcohort'" != "" {
        di "Maximum cohort: `maxcohort'"
        keep if cohort <= `maxcohort'
        keep if year <= `censusyear'
    }
    di "- Step 1. Data filtered to cohort and census."
    joinby year using "$era_intermediate/temperature_data"
    di "- Step 2. Joined with temperature data"
    foreach range in 14_29 30_44 {
        local start = real(substr("`range'", 1, 2))
        local end = real(substr("`range'", 4, 5))
        gen age_`range' = inrange(age, `start', `end')
    }
    di "- Step 3. Generated age variables."
    
    local temp_vars
    forvalues temp = 22/28 {
        local temp_vars `temp_vars' temp_max_dd_`temp' temp_max_dd_`temp'_gs temp_max_dd_`temp'_ngs
    }
    
    * Generate temperature exposure by age range
    foreach var of local temp_vars {
        foreach range in 14_29 30_44 {
            gen `var'_age_`range' = `var' * age_`range'
        }
    }
    di "- Step 4. Generated degree day variables."
    
    * Collapse data
    collapse (sum) temp_*, by(GEO_CONCAT cohort)
    di "- Step 5. Collapsed at the subcounty cohort level."
    gen census = `censusyear'
    di "Variables for "
end

* Process each census year
process_census_year 2019
tempfile census_2019
save `census_2019'

process_census_year 2009 1995
tempfile census_2009
save `census_2009'

process_census_year 1999 1985
tempfile census_1999
save `census_1999'

process_census_year 1989 1975

* append all census years
append using `census_1999'
append using `census_2009'
append using `census_2019'
isid cohort GEO_CONCAT census     // data uniquely identifiable at the cohort, sub-unit, census year level

la var census "Census year"
la var cohort "Cohort (birth year)"

save "$era_intermediate/census-cohort-temperature.dta", replace