********************************************************************************
* PROJECT:            Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:             Prayog Bhattarai
* DATE CREATED:       30 October, 2025
* DATE MODIFIED:      30 October, 2025
* DESCRIPTION:        Append datasets where SMALLEST units are identified at GEOLEV2 level.
* DEPENDENCIES:       reghdfe
* INPUTS:             Country-specific merged climate-census datasets (merged on BIRTH_SMALLEST)

* NOTES: This script appends data for countries identified at GEOLEV2. 
********************************************************************************


** STEP 1: Append country-specific datasets at GEOLEV2 level **
/* NOTE: We have the following countries available at the GEOLEV2 level:
Benin, Burkina Faso, Cameroon, Ivory Coast, Kenya, Mali, Mozambique, Senegal, Sierra Leone, South Africa, and Zambia
Together they have 622 of the 825 SMALLEST units in our IPUMS sample. */

clear all
* datasets are in data/derived/census-climate-merged/`country'/`country'-census-climate-merged-by-birthplace.dta"


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
                - dir: Base directory for the project
                - cf_overleaf: Directory for Overleaf outputs
                - cf: Base directory for data management
                - era_derived: Directory for derived ERA data
                - era_raw: Directory for raw ERA data
        */
    
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
* Main Workflow: Append Country Datasets
********************************************************************************

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

* Display summary
di "********************************************************************************"
di "SUMMARY"
di "********************************************************************************"
di "Countries successfully processed: `countries_completed'"
if "`countries_failed'" != "" {
    di as error "Countries failed (file not found): `countries_failed'"
}
di ""
di "Total observations in combined dataset: " _N

* Save the combined dataset
save "$cf/data/derived/analysis/smallest-at-geolev2.dta", replace
di ""
di "Combined dataset saved to:"
di "  $cf/data/derived/analysis/smallest-at-geolev2.dta"
di "********************************************************************************"
