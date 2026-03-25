********************************************************************************
* PROJECT:            Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:             Prayog Bhattarai
* DATE CREATED:       05-08-2025
* DATE MODIFIED:      2025-10-09
* DESCRIPTION:        Create a mapping of countries to their census years
* DEPENDENCIES:      
* INPUT:              $cf/data/source/census/africa_women.dta
* OUTPUT:             $cf/data/derived/ipums/country-census-years-mapping.dta
* NOTES:              
********************************************************************************

// Load the source dataset
use "$cf/data/source/census/africa_women.dta", clear

// Check the structure of COUNTRY variable
describe COUNTRY
tab COUNTRY, missing

// Create CNTRY_CD from original numeric COUNTRY values
gen CNTRY_CD = COUNTRY

// Decode COUNTRY to create string variable with country names
decode COUNTRY, gen(COUNTRY_temp)
drop COUNTRY
rename COUNTRY_temp COUNTRY
replace COUNTRY = "Ivory Coast" if COUNTRY == "Cote d'Ivoire"

// Rename YEAR to census for clarity
rename YEAR census

// Keep only the variables we need
keep COUNTRY CNTRY_CD census

// Remove any missing values
drop if missing(COUNTRY) | missing(CNTRY_CD) | missing(census)

// Create unique country-census year combinations
duplicates drop COUNTRY CNTRY_CD census, force

// Sort for clean output
sort COUNTRY census

// Display summary statistics
di "Summary of country-census year mapping:"
di "Number of unique country-census combinations: " _N
di ""
di "Countries and their census years:"
tab COUNTRY census, missing

// Create output directory if it doesn't exist
cap mkdir "$cf/data/derived"
cap mkdir "$cf/data/derived/ipums"

// Save the mapping dataset
save "$cf/data/derived/ipums/country-census-years-mapping.dta", replace

// Display final confirmation
di ""
di "Dataset saved to: $cf/data/derived/ipums/country-census-years-mapping.dta"
di "Variables: COUNTRY (string), CNTRY_CD (numeric), census (numeric)"
di "Observations: " _N
