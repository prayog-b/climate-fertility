/*******************************************************************************
* Program: dhs_climate_merge.do
* Purpose: Merge DHS survey data with temperature and precipitation data
*          for sub-Saharan African countries
* Author:  Generated for Prayog Bhattarai
* Date:    December 2024
*
* Description:
*   This program processes DHS datasets for 36 sub-Saharan African countries,
*   merging them with historical temperature and precipitation data at the
*   region-cohort-survey year level.
*
* Input datasets:
*   1. DHS individual recode (IR) datasets
*   2. Region-cohort temperature datasets
*   3. Region-cohort precipitation datasets
*
* Output:
*   Country-specific merged DHS-climate datasets
*   LaTeX summary tables of merge results
*
*******************************************************************************/

clear all
set more off
cap log close

local username = c(username)

if "`username'" == "prayogbhattarai" {
    global cf "/Users/`username'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
    global cf_overleaf "/Users/`username'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
}
else if "`username'" == "prayog" {
    global cf "D:/`username'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
    global cf_overleaf "D:/`username'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
}
else if "`username'" == "yogita" {
    global cf "/Users/`username'/NUS Dropbox/`username'/Climate_Change_and_Fertility_in_SSA"
    global cf_overleaf "/Users/`username'/NUS Dropbox/`username'/Apps/Overleaf/climate-fertility-ssa/output"
}
else if inlist("`username'", "celin", "CE.4875") {
    global cf "/Users/`username'/Dropbox/Climate_Change_and_Fertility_in_SSA"
    global cf_overleaf "/Users/`username'/Apps/Overleaf/climate-fertility-ssa/output"
}
else if "`username'" == "yun" {
    global cf "/Users/`username'/Dropbox/Climate_Change_and_Fertility_in_SSA"
    global cf_overleaf "/Users/`username'/Apps/Overleaf/climate-fertility-ssa/output"
}

* Define path for LaTeX tables (adjust as needed)
global latex_output "$cf_overleaf/table"

* Start log file
log using "$cf/data/derived/dhs/06-dhs-survey-climate-merge/dhs_climate_merge_log.txt", text replace

* Display start time
di as txt "========================================================================"
di as txt "DHS-Climate Data Merge Program"
di as txt "Started: " c(current_date) " " c(current_time)
di as txt "========================================================================"

********************************************************************************
* PROGRAM DEFINITIONS
********************************************************************************

* Program to get country name from code
capture program drop GetCountryName
program define GetCountryName, rclass
    syntax, code(string)
    
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

* Program to get DHS folder name from country name
* Handles the three exceptions: Ivory Coast, Congo Democratic Republic, The Gambia
capture program drop GetDHSFolder
program define GetDHSFolder, rclass
    syntax, country(string)
    
    * Default: convert to lowercase and replace spaces with underscores
    local folder_name = lower(subinstr("`country'", " ", "_", .))
    
    * Handle exceptions
    if "`country'" == "Ivory Coast" {
        local folder_name "cote_divoire"
    }
    else if "`country'" == "Congo Democratic Republic" {
        local folder_name "drc"
    }
    else if "`country'" == "The Gambia" {
        local folder_name "gambia"
    }
    
    return local folder "`folder_name'"
end

********************************************************************************
* MAIN PROCESSING LOOP
********************************************************************************

* Define list of country codes
local country_codes "BEN BFA BDI CMR CAF TCD COM COG DRC ETH GAB GMB GHA GIN IVC KEN LSO LBR MDG MWI MLI MOZ NMB NER NGA RWA SEN SLE ZAF SWZ TZA TGO UGA ZMB ZWE"
*local country_codes "LBR"


* Initialize tracking locals
local success_count = 0
local fail_count = 0
local low_merge_countries ""
local failed_countries ""
local success_countries ""

* Process each country
foreach code of local country_codes {
    
    * Get country name and folder name
    GetCountryName, code("`code'")
    local country_name "`r(name)'"
    
    GetDHSFolder, country("`country_name'")
    local folder_name "`r(folder)'"
    
    di as txt ""
    di as txt "========================================================================"
    di as txt "Processing: `country_name' (`code')"
    di as txt "========================================================================"
    
    * Define file paths
    local dhs_file "$cf/data/source/dhs/`folder_name'/`folder_name'_dhs_IR.dta"
    local temp_file "$cf/data/derived/dhs/05-dhs-region-cohort-temperature/`country_name'-dhs-cohort-temperature.dta"
    local precip_file "$cf/data/derived/dhs/04-dhs-region-cohort-precipitation/`country_name'-dhs-cohort-precipitation.dta"
    local output_file "$cf/data/derived/dhs/06-dhs-survey-climate-merge/`country_name'-dhs-ir-climate-merged.dta"
    
    * Check if source files exist
    capture confirm file "`dhs_file'"
    local dhs_exists = (_rc == 0)
    
    capture confirm file "`temp_file'"
    local temp_exists = (_rc == 0)
    
    capture confirm file "`precip_file'"
    local precip_exists = (_rc == 0)
    
    * Report file existence
    di as txt "  DHS file:    `dhs_file'"
    di as txt "               Exists: " cond(`dhs_exists', "Yes", "NO - MISSING")
    di as txt "  Temp file:   `temp_file'"
    di as txt "               Exists: " cond(`temp_exists', "Yes", "NO - MISSING")
    di as txt "  Precip file: `precip_file'"
    di as txt "               Exists: " cond(`precip_exists', "Yes", "NO - MISSING")
    
    * Skip if any file is missing
    if !`dhs_exists' | !`temp_exists' | !`precip_exists' {
        di as err "  ERROR: One or more source files missing. Skipping `country_name'."
        local fail_count = `fail_count' + 1
        local failed_countries "`failed_countries' `country_name'"
        
        * Store failure status in global for table
        global `code'_status "Failed"
        global `code'_failure_reason "Missing files"
        
        continue
    }
    
    **************************************************************************
    * STAGE 1: Load and prepare datasets
    **************************************************************************
    
    di as txt ""
    di as txt "  STAGE 1: Loading and preparing datasets"
    di as txt "  ----------------------------------------"
    
    * Load DHS data
    use "`dhs_file'", clear
    local dhs_n_initial = _N
    di as txt "    DHS observations loaded: `dhs_n_initial'"
    
    * ASSERTION: Check caseid uniquely identifies observations
    capture isid caseid int_year
    if _rc != 0 {
        di as err "    ASSERTION FAILED: caseid and int_year do not uniquely identify observations"
        di as err "    Skipping `country_name'"
        local fail_count = `fail_count' + 1
        local failed_countries "`failed_countries' `country_name'"
        
        * Store failure status in global for table
        global `code'_status "Failed"
        global `code'_failure_reason "Duplicate IDs"
        
        continue
    }
    di as txt "    ASSERTION PASSED: caseid uniquely identifies observations"
    
    * Decode region variable to create DHSREGEN
    decode reg, gen(DHSREGEN)

    * Clean: trim whitespace and convert to proper case
    if inlist("`country_name'", "Burundi","The Gambia", "Sierra Leone", "Swaziland", "Mozambique") {
        gen DHSREGEN_clean = proper(strtrim(DHSREGEN))
        count if DHSREGEN != DHSREGEN_clean
        if r(N) > 0 {
            di as txt "    NOTE: Cleaned `r(N)' DHSREGEN values (trimmed/proper case)"
        }
        drop DHSREGEN
        rename DHSREGEN_clean DHSREGEN
    }
    levelsof DHSREGEN

    * Clean: Manual fixes for known issues
    if "`country_name'" == "Sierra Leone" {
        replace DHSREGEN = "Western" if DHSREGEN == "North Western"
    }
    label var DHSREGEN "DHS region identifier"
    
    
    * ASSERTION: Check decode was successful (no empty strings)
    count if DHSREGEN == "" | missing(DHSREGEN)
    local empty_regions = r(N)
    if `empty_regions' > 0 {
        di as err "    WARNING: `empty_regions' observations have missing/empty DHSREGEN"
    }
    else {
        di as txt "    ASSERTION PASSED: All observations have valid DHSREGEN values"
    }
    
    * Rename mom_yob to cohort
    rename mom_yob cohort

    * ASSERTION: Check no missing values in merge keys
    count if missing(DHSREGEN) | missing(cohort) | missing(int_year)
    local missing_keys = r(N)
    if `missing_keys' > 0 {
        di as txt "    WARNING: `missing_keys' observations have missing merge key values"
        di as txt "      - These will not merge with weather data"
    }
    else {
        di as txt "    ASSERTION PASSED: No missing values in merge keys"
    }

	capture {
	// Replace the missing code with actual missings for whether respondent is currently working
	replace mom_work = . if mom_work == 9

	// Replace the non-numeric responses for ideal number of children with missings
	replace mom_ideal =. if inlist(mom_ideal, 94, 95, 96, 99)

	// Replace the missing codes for mom_edyears with missings
	replace mom_edyears =. if inlist(mom_edyears, 99)

	// Marital status variable has some issues
	replace marital = "Divorced" if marital == "divorced"
	replace marital = "Married" if marital == "married"
	replace marital = "Living with partner" if marital == "living with partner"
	replace marital = "Never in union" if marital == "never in union"
	replace marital = "Not living together" if inlist(marital, "No longer living together/separated", "no longer living together/separated", "not living together")
	replace marital = "Never married" if marital == "never married"
	replace marital = "Widowed" if marital == "widowed"
	replace marital = "Living together" if marital == "living together"

	// Replace missing codes for cowives with missings
	replace cowives =. if inlist(cowives, 98, 99)

	// Replace missing codes for husband education years with missings
	replace husb_edyears =. if inlist(husb_edyears, 98, 99)

	// Replace missing codes and DKs for husband's desire for children with missings
	replace husb_desire =. if inlist(husb_desire, 8, 9)

	// Fix inconsistent coding for religion
	replace religion = "Celestes" if inlist(religion, ""celestes"", "celestes")
	replace religion = "Vodoun" if inlist(religion, "vodoun", "traditional (vodoun)")
	replace religion = "Traditional" if inlist(religion, "taditional", "traditional", "traditional/spiritualist")
	replace religion = "Islam" if inlist(religion, "moslem", "Muslim", "musulman")
	replace religion = "Islam" if inlist(religion, "muslin", "muslim", "islamic", "islam", "muslim/islam")
	replace religion = "Catholic" if inlist(religion, "catholic", "catholique")
	replace religion = "Other Christian" if inlist(religion, "other christian", "other christians", "other chritians")
	replace religion = "Other Protestant" if inlist(religion, "other protestant", "other protestants")
	replace religion = "Protestant" if inlist(religion, "protestant", "protestants", "prostestant")
	replace religion = "Protestant" if inlist(religion, "protestant/ other christian", "protestant/other christian")
	replace religion = "None" if inlist(religion, "none",  "no religion", "no religion + other")
	replace religion = "Other" if inlist(religion, "other", "other religions", "Other religion")
	replace religion = "Other" if inlist(religion, "salvation army", "zephirin/matsouaniste/ngunza")
	replace religion = "Other" if inlist(religion, "zephirrin/matsouanist/ngunza", "sect", "jehovah witness") 
	replace religion = "Other" if inlist(religion, "nature worship", "new religions (eglises rebeillees)") 
	replace religion = "Other" if inlist(religion, "eglise de r�veil", "arm�e du salut", "traditional")
	replace religion = "Other" if inlist(religion, "other/missing", "roman catholic", "pentecostal/charismatic") 
	replace religion = "Other" if inlist(religion, "presbyterian", "protest /oth cristian")
	replace religion = "Other" if inlist(religion, "bundu dia kongo", "vuvamu", "spiritualist")
	replace religion = "Other" if inlist(religion, "apostolic sect", "pentecostal", "spiritual")
	replace religion = "Animist" if inlist(religion, "animist", "animiste", "traditional / animist")
	replace religion = "Animist" if inlist(religion, "traditionnal/animist", "tradition/animist")
	replace religion = "Adventist" if inlist(religion, "Adventist", "Adventiste", "adventist/jehova", "adventiste/jehova")
	replace religion = "Animalist" if religion == "animalist"
	replace religion = "Kimbanguism" if inlist(religion, "kibanguist", "kimbanguist", "kimbanguiste")
	replace religion = "Orthodox" if religion == "orthodox"
	replace religion = "Atheist" if religion == "atheist"
	replace religion = "Christianity" if inlist(religion, "Christian", "christianity", "christian")
	replace religion = "Anglican" if inlist(religion, "anglican") 
	replace religion = "Methodist" if inlist(religion, "methodist")
	}

    * Create indicator for not having any children
    gen no_child = .
    replace no_child = 1 if total_babies == 0 & !missing(total_babies)
    replace no_child = 0 if total_babies >= 0 & !missing(total_babies)
    label variable no_child "Respondent does not have children"

    * Create indicator for no surviving children
    gen no_surv_child = .
    replace no_surv_child = 1 if no_child == 0 & living_babies == 0 & !missing(living_babies)
    replace no_surv_child = 0 if no_child == 0 & living_babies >= 0 & !missing(living_babies)
    label variable no_surv_child "Respondent does not have surviving children"

    * Create indicator for having had at least one child
    gen has_child = .
    replace has_child = 1 if total_babies >= 1 & !missing(total_babies)
    replace has_child = 0 if total_babies == 0 & !missing(total_babies)
    label variable has_child "Respondent has at least one child"

    * Report merge key value ranges
    qui sum cohort
    di as txt "    Cohort range: `r(min)' - `r(max)'"
    qui sum int_year
    di as txt "    Survey year range: `r(min)' - `r(max)'"
    qui tab DHSREGEN
    di as txt "    Number of regions: `r(r)'"
    
    * Save as temp file
    tempfile dhs_temp
    save `dhs_temp', replace
    
    **************************************************************************
    * STAGE 2: Merge weather datasets
    **************************************************************************
    
    di as txt ""
    di as txt "  STAGE 2: Merging weather datasets"
    di as txt "  ----------------------------------"
    
    * Load temperature data
    use "`temp_file'", clear
    local temp_n = _N
    di as txt "    Temperature observations: `temp_n'"
    
    * ASSERTION: Check temperature data is uniquely identified
    capture isid DHSREGEN cohort int_year
    if _rc != 0 {
        di as err "    WARNING: Temperature data not uniquely identified by DHSREGEN cohort int_year"
        * Try to make it unique by keeping first observation
        bysort DHSREGEN cohort int_year: keep if _n == 1
        di as txt "    Kept first observation per group. New N = `=_N'"
    }
    else {
        di as txt "    ASSERTION PASSED: Temperature data uniquely identified"
    }
    
    * Save temp temperature file
    tempfile temp_weather
    save `temp_weather', replace
    
    * Load precipitation data
    use "`precip_file'", clear
    local precip_n = _N
    di as txt "    Precipitation observations: `precip_n'"
    
    * ASSERTION: Check precipitation data is uniquely identified
    capture isid DHSREGEN cohort int_year
    if _rc != 0 {
        di as err "    WARNING: Precipitation data not uniquely identified by DHSREGEN cohort int_year"
        bysort DHSREGEN cohort int_year: keep if _n == 1
        di as txt "    Kept first observation per group. New N = `=_N'"
    }
    else {
        di as txt "    ASSERTION PASSED: Precipitation data uniquely identified"
    }
    
    * Merge temperature with precipitation
    merge 1:1 DHSREGEN cohort int_year using `temp_weather'
    
    * Check merge results for weather data
    tab _merge
    
    * Count merge outcomes
    count if _merge == 1
    local precip_only = r(N)

    ta int_year if _merge == 1
    ta cohort if _merge == 1
    levelsof DHSREGEN if _merge == 1


    count if _merge == 2
    local temp_only = r(N)

    count if _merge == 3
    local both_weather = r(N)
    
    di as txt ""
    di as txt "    Weather merge results:"
    di as txt "      Precipitation only (_merge=1): `precip_only'"
    di as txt "      Temperature only (_merge=2):   `temp_only'"
    di as txt "      Both matched (_merge=3):       `both_weather'"
    
    * ASSERTION: Perfect merge for weather data
    local total_weather = `precip_only' + `temp_only' + `both_weather'
    local weather_merge_rate = (`both_weather' / `total_weather') * 100
    
    if `precip_only' > 0 | `temp_only' > 0 {
        di as err "    ASSERTION FAILED: Temperature-Precipitation merge is not perfect"
        di as err "    Weather merge rate: " %5.2f `weather_merge_rate' "%"
        di as err "    Continuing with matched observations only..."
        keep if _merge == 3
    }
    else {
        di as txt "    ASSERTION PASSED: Perfect merge between temperature and precipitation"
    }
    
    drop _merge
    
    * Save combined weather data
    tempfile weather_combined
    save `weather_combined', replace
    local weather_n = _N
    di as txt "    Combined weather observations: `weather_n'"
    
    **************************************************************************
    * STAGE 2b: Merge DHS with weather data
    **************************************************************************
    
    di as txt ""
    di as txt "  STAGE 2b: Merging DHS with weather data"
    di as txt "  ----------------------------------------"
    
    * Load DHS data
    use `dhs_temp', clear
    
    * Merge with weather data
    merge m:1 DHSREGEN cohort int_year using `weather_combined'
    
    * Detailed merge diagnostics
    tab _merge
    
    count if _merge == 1
    local dhs_only = r(N)
    count if _merge == 2
    local weather_only = r(N)
    count if _merge == 3
    local matched = r(N)
    
    local total_dhs = `dhs_only' + `matched'
    local merge_rate = (`matched' / `total_dhs') * 100
    
    di as txt ""
    di as txt "    DHS-Weather merge results:"
    di as txt "      DHS only (unmatched, _merge=1):     `dhs_only'"
    di as txt "      Weather only (_merge=2):            `weather_only'"
    di as txt "      Matched (_merge=3):                 `matched'"
    di as txt "      -----------------------------------------"
    di as txt "      Total DHS observations:             `total_dhs'"
    di as txt "      Merge rate:                         " %5.2f `merge_rate' "%"
    
    * Store merge statistics in globals for LaTeX table
    global `code'_status "Success"
    global `code'_total_obs "`total_dhs'"
    global `code'_matched "`matched'"
    global `code'_unmatched "`dhs_only'"
    global `code'_merge_rate: di %5.2f `merge_rate'
    global `code'_weather_only "`weather_only'"
    
    * Count unique regions
    qui tab DHSREGEN if _merge == 3
    global `code'_regions_matched "`r(r)'"
    qui tab DHSREGEN if _merge == 1
    global `code'_regions_unmatched "`r(r)'"
    
    * Check for weather-only observations (shouldn't happen in normal case)
    if `weather_only' > 0 {
        di as txt ""
        di as txt "    NOTE: `weather_only' weather observations have no matching DHS records"
        di as txt "    These may represent region-cohort combinations not in the survey"
    }
    
    * Flag low merge rate countries (< 90%)
    if `merge_rate' < 90 {
        di as err ""
        di as err "    *** WARNING: LOW MERGE RATE (<90%) ***"
        di as err "    `country_name' has only " %5.2f `merge_rate' "% of observations merged"
        local low_merge_countries "`low_merge_countries' `country_name'(`=round(`merge_rate',0.1)'%)"
        global `code'_low_merge "Yes"
    }
    else {
        global `code'_low_merge "No"
    }
    
    * Analyze unmatched DHS observations
    if `dhs_only' > 0 {
        di as txt ""
        di as txt "    Analysis of unmatched DHS observations:"
        
        preserve
        keep if _merge == 1
        
        * By region
        di as txt "      By region:"
        tab DHSREGEN, sort
        
        * By cohort range
        qui sum cohort
        di as txt "      Cohort range of unmatched: `r(min)' - `r(max)'"
        
        * By survey year
        di as txt "      By survey year:"
        tab int_year
        
        restore
    }
    
    * Keep all DHS observations (matched and unmatched)
    keep if _merge == 1 | _merge == 3

    di as text "    Regions with unmatched DHS observations: "
    levelsof DHSREGEN if _merge == 1

	levelsof cohort if _merge == 1

	levelsof int_year if _merge == 1
    
    
    * Create indicator for successful merge
    gen byte climate_data_merged = (_merge == 3)
    label var climate_data_merged "Successfully merged with climate data (1=yes)"
    
    drop _merge
    
    **************************************************************************
    * STAGE 3: Final checks and save
    **************************************************************************
    
    di as txt ""
    di as txt "  STAGE 3: Final checks and save"
    di as txt "  -------------------------------"
    
    * ASSERTION: Final dataset still uniquely identified by caseid
    capture isid caseid int_year
    if _rc != 0 {
        di as err "    ASSERTION FAILED: Final dataset not uniquely identified by caseid"
        di as err "    Checking for duplicates..."
        duplicates report caseid
        di as err "    Skipping save for `country_name'"
        local fail_count = `fail_count' + 1
        local failed_countries "`failed_countries' `country_name'"
        
        * Update failure status
        global `code'_status "Failed"
        global `code'_failure_reason "Duplicate final IDs"
        
        continue
    }
    di as txt "    ASSERTION PASSED: Final dataset uniquely identified by caseid"
    
    * Report final counts
    local final_n = _N
    count if climate_data_merged == 1
    local merged_n = r(N)
    count if climate_data_merged == 0
    local unmerged_n = r(N)
    
    di as txt ""
    di as txt "    Final dataset summary:"
    di as txt "      Initial DHS observations:     `dhs_n_initial'"
    di as txt "      Final observations:           `final_n'"
    di as txt "      With climate data:            `merged_n'"
    di as txt "      Without climate data:         `unmerged_n'"
    
    * ASSERTION: No observations lost
    if `final_n' != `dhs_n_initial' {
        di as err "    WARNING: Observation count changed from `dhs_n_initial' to `final_n'"
    }
    else {
        di as txt "    ASSERTION PASSED: All DHS observations retained"
    }
    
    * Add metadata
    label data "DHS-Climate merged data for `country_name' - Created `c(current_date)'"
    notes: Created by dhs_climate_merge.do on `c(current_date)' `c(current_time)'
    notes: Source DHS file: `dhs_file'
    notes: Source temperature file: `temp_file'
    notes: Source precipitation file: `precip_file'
    notes: Merge rate: `=round(`merge_rate', 0.01)'%
    
    * Save final dataset
    save "`output_file'", replace
    di as txt ""
    di as txt "    SAVED: `output_file'"
    
    * Update success counter
    local success_count = `success_count' + 1
    local success_countries "`success_countries' `country_name'"
    
    * Clear temporary files from memory
    clear
}

********************************************************************************
* GENERATE LATEX SUMMARY TABLES
********************************************************************************

di as txt ""
di as txt "========================================================================"
di as txt "GENERATING LATEX SUMMARY TABLES"
di as txt "========================================================================"

* Create output directory for LaTeX tables if it doesn't exist
cap mkdir "$cf_overleaf/table"

********************************************************************************
* TABLE 1: Overall Merge Results
********************************************************************************

di as txt ""
di as txt "Creating Table 1: Overall Merge Results..."

file open merge_table using "$cf_overleaf/table/dhs_climate_merge_results.tex", write replace

* Write table header
file write merge_table "\begin{tabular}{lrrrr}" _n
file write merge_table "\toprule" _n
file write merge_table "Country & Total Obs & Matched & Unmatched & Merge Rate (\%) \\" _n
file write merge_table " & (1) & (2) & (3) & (4) \\" _n
file write merge_table "\midrule" _n

* Initialize accumulators for totals
local sum_total = 0
local sum_matched = 0
local sum_unmatched = 0

* Write country rows
foreach code of local country_codes {
    GetCountryName, code("`code'")
    local country_name "`r(name)'"
    
    * Only include if processing was successful
    if "${`code'_status}" == "Success" {
        * Accumulate totals
        local sum_total = `sum_total' + ${`code'_total_obs}
        local sum_matched = `sum_matched' + ${`code'_matched}
        local sum_unmatched = `sum_unmatched' + ${`code'_unmatched}
        
        * Write row
        file write merge_table "`country_name' & ${`code'_total_obs} & ${`code'_matched} & ${`code'_unmatched} & ${`code'_merge_rate} \\" _n
    }
}

* Calculate overall merge rate
if `sum_total' > 0 {
    local overall_rate: di %5.2f (`sum_matched' / `sum_total') * 100
}
else {
    local overall_rate = 0
}

* Write total row
file write merge_table "\midrule" _n
file write merge_table "\textbf{Total} & \textbf{`sum_total'} & \textbf{`sum_matched'} & \textbf{`sum_unmatched'} & \textbf{`overall_rate'} \\" _n
file write merge_table "\bottomrule" _n
file write merge_table "\end{tabular}" _n

* Add table notes
file write merge_table "\end{table}" _n

file close merge_table

di as txt "  SUCCESS: Table saved to $cf_overleaf/table/dhs_climate_merge_results.tex"

********************************************************************************
* TABLE 2: Merge Quality by Region Coverage
********************************************************************************

di as txt ""
di as txt "Creating Table 2: Region Coverage Statistics..."

file open region_table using "$cf_overleaf/table/dhs_climate_region_coverage.tex", write replace

* Write table header
// file write region_table "\begin{table}[htbp]" _n
// file write region_table "\centering" _n
// file write region_table "\caption{Regional Coverage in DHS-Climate Merge}" _n
// file write region_table "\label{tab:dhs_climate_regions}" _n
file write region_table "\begin{tabular}{lrrr}" _n
file write region_table "\toprule" _n
file write region_table "Country & Total Regions & Regions Matched & Regions Unmatched \\" _n
file write region_table " & (1) & (2) & (3) \\" _n
file write region_table "\midrule" _n

* Initialize accumulators
local sum_matched_regions = 0
local sum_unmatched_regions = 0

* Write country rows
foreach code of local country_codes {
    GetCountryName, code("`code'")
    local country_name "`r(name)'"
    
    * Only include if processing was successful
    if "${`code'_status}" == "Success" {
        local total_regions = ${`code'_regions_matched} + ${`code'_regions_unmatched}
        
        * Accumulate totals
        local sum_matched_regions = `sum_matched_regions' + ${`code'_regions_matched}
        local sum_unmatched_regions = `sum_unmatched_regions' + ${`code'_regions_unmatched}
        
        * Write row
        file write region_table "`country_name' & `total_regions' & ${`code'_regions_matched} & ${`code'_regions_unmatched} \\" _n
    }
}

* Calculate totals
local sum_total_regions = `sum_matched_regions' + `sum_unmatched_regions'

* Write total row
file write region_table "\midrule" _n
file write region_table "\textbf{Total} & \textbf{`sum_total_regions'} & \textbf{`sum_matched_regions'} & \textbf{`sum_unmatched_regions'} \\" _n
file write region_table "\bottomrule" _n
file write region_table "\end{tabular}" _n

* Add table notes
file write region_table "\end{table}" _n

file close region_table

di as txt "  SUCCESS: Table saved to $cf_overleaf/table/dhs_climate_region_coverage.tex"

********************************************************************************
* TABLE 3: Countries with Issues
********************************************************************************

di as txt ""
di as txt "Creating Table 3: Processing Issues Summary..."

file open issues_table using "$cf_overleaf/table/dhs_climate_merge_issues.tex", write replace

* Write table header
file write issues_table "\begin{tabular}{lp{6cm}r}" _n
file write issues_table "\toprule" _n
file write issues_table "Country & Issue & Merge Rate (\%) \\" _n
file write issues_table "\midrule" _n

* Count issues
local has_issues = 0

* Write rows for failed countries
foreach code of local country_codes {
    GetCountryName, code("`code'")
    local country_name "`r(name)'"
    
    if "${`code'_status}" == "Failed" {
        local has_issues = 1
        file write issues_table "`country_name' & ${`code'_failure_reason} & -- \\" _n
    }
}

* Write rows for low merge rate countries
foreach code of local country_codes {
    GetCountryName, code("`code'")
    local country_name "`r(name)'"
    
    if "${`code'_status}" == "Success" & "${`code'_low_merge}" == "Yes" {
        local has_issues = 1
        file write issues_table "`country_name' & Low merge rate (< 90\%) & ${`code'_merge_rate} \\" _n
    }
}

* If no issues, add a note
if !`has_issues' {
    file write issues_table "\multicolumn{3}{c}{\textit{No countries with merge issues}} \\" _n
}

file write issues_table "\bottomrule" _n
file write issues_table "\end{tabular}" _n

// * Add table notes
file write issues_table "\end{table}" _n

file close issues_table

di as txt "  SUCCESS: Table saved to $cf_overleaf/table/dhs_climate_merge_issues.tex"

********************************************************************************
* FINAL SUMMARY
********************************************************************************

di as txt ""
di as txt "========================================================================"
di as txt "PROCESSING COMPLETE"
di as txt "========================================================================"
di as txt ""
di as txt "Summary:"
di as txt "  Countries processed successfully: `success_count'"
di as txt "  Countries failed:                 `fail_count'"
di as txt "  Total observations merged:        `sum_matched'"
di as txt "  Overall merge rate:               `overall_rate'%"
di as txt ""

if "`success_countries'" != "" {
    di as txt "Successfully processed countries:"
    di as txt "  `success_countries'"
}

if "`failed_countries'" != "" {
    di as err ""
    di as err "Failed countries (missing files or assertion failures):"
    di as err "  `failed_countries'"
}

if "`low_merge_countries'" != "" {
    di as err ""
    di as err "Countries with low merge rates (<90%):"
    di as err "  `low_merge_countries'"
}

di as txt ""
di as txt "Output files:"
di as txt "  Data directory:   $cf/data/derived/dhs/06-dhs-survey-climate-merge"
di as txt "  LaTeX tables:     $cf_overleaf/table"
di as txt "    - dhs_climate_merge_results.tex"
di as txt "    - dhs_climate_region_coverage.tex"
di as txt "    - dhs_climate_merge_issues.tex"
di as txt ""
di as txt "Finished: " c(current_date) " " c(current_time)
di as txt "========================================================================"

log close

* End of program
