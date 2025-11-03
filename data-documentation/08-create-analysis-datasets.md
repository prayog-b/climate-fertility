# Creating analysis datasets
This file documents the creation of the analysis datasets that we use for statistical analysis. The main script for this is:
`cf-ssa-dm-07a-append-smallest-at-geolev2.do`

- Inputs: `$cf/data/derived/census-climate-merged/`country_name'/`country_name'-census-climate-merged-by-birthplace.dta`
- Output: `$cf/data/derived/analysis/smallest-at-geolev2.dta`

## Script overview
For preliminary analysis, we restrict our analysis dataset to the following countries, which have SMALLEST units available at the GEOLEV2 level.
 - Benin, Botswana, Burkina Faso, Cameroon, Ivory Coast, Kenya, Mali, Mozambique, Senegal, Sierra Leone, South Africa, Zambia

**Note:** Zambia is unique in the sense that they have some respondents with SMALLEST values at the GEOLEV2 level, and some at the GEOLEV1 level.

We then load one country's merged dataset, and append other countries to it. The implementation is shown below: 

```stata
local country_codes "BEN BWA BFA CMR IVC KEN MLI MOZ SEN SLE ZAF ZMB"
ClimateAnalysisConfig, method("init")

local total_countries : word count `country_codes'
di "Processing `total_countries' countries"
di ""

local country_num = 0
local countries_completed ""
local countries_failed ""

* Create output directory if it doesn't exist
capture mkdir "$cf/data/derived/analysis"

foreach country_code in `country_codes' {
    local ++country_num
    
    * Get country name for display
    GetCountryName, code("`country_code'")
    local country_name "`r(name)'"

    di "Processing country `country_num' of `total_countries': `country_name' (`country_code')"
    
    * Try to load and append the country dataset
    capture confirm file "$cf/data/derived/census-climate-merged/`country_name'/`country_name'-census-climate-merged-by-birthplace.dta"
    if _rc == 0 {
        * File exists
        if `country_num' == 1 {
            * First country: load the data
            use "$cf/data/derived/census-climate-merged/`country_name'/`country_name'-census-climate-merged-by-birthplace.dta", clear
            di "  -> Loaded (first country)"
        }
        else {
            * Subsequent countries: append the data
            append using "$cf/data/derived/census-climate-merged/`country_name'/`country_name'-census-climate-merged-by-birthplace.dta"
            di "  -> Appended"
        }
        local countries_completed "`countries_completed' `country_name'"
    }
    else {
        * File doesn't exist
        di as error "  -> ERROR: File not found for `country_name'"
        local countries_failed "`countries_failed' `country_name'"
    }
    di ""
}
save "$cf/data/derived/analysis/smallest-at-geolev2.dta", replace
```
