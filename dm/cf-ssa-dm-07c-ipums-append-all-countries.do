********************************************************************************
* PROJECT:              Climate Change and Fertility in SSA
* AUTHOR:               Celine Zipfel 
* DATE CREATED:         January 09, 2026
* DATE MODIFIED:        January 09, 2026
* DESCRIPTION:          Append merged datasets for all countries
* DEPENDENCIES: 
* 
* NOTES: 
********************************************************************************
	clear all 
	cap log close 

    capture program drop ClimateAnalysisConfig
    program define ClimateAnalysisConfig
        syntax, method(string)
    
        if "`method'" == "init" {
            local username "`c(username)'"
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
                else {
                    di as error "Unknown user. Set global cf manually."
                    exit 198
                }       
        }
    end

    ClimateAnalysisConfig, method("init")

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

    * Define list of countries to append
    local country_codes "BEN BWA BFA CMR ETH GHA GIN IVC KEN LSO LBR MWI MLI MOZ RWA SLE ZAF SEN SDN TZA TGO UGA ZMB ZWE"
    
    * Define variables to keep
	local keepvars "final_merge CH* avg_precip_gs_spi* avg_precip_ngs_spi* temp_max_dd_27* temp_max_dd_28* temp_max_dd_29* temp_max_dd_30* temp_max_dd_31* temp_max_dd_32* temp_max_dd_33* temp_max_dd_34* temp_max_dd_36* temp_mean_dd_36* AGE BPL* BIRTH_SMALLEST COHORT URBAN PERWT EDATTAIN YRSCHOOL EMPSTAT OCCISCO INDGEN MARST EDATTAIN_SP YRSCHOOL_SP EMPSTAT_SP OCCISCO_SP INDGEN_SP africa_region RES_SMALLEST no_valid_pixels temp_mean avg_precip_spi avg_precip_spi_15_29 avg_precip_spi_30_44 share_abn_precip_gs_15_29 share_abn_precip_gs_dry_15_29 share_abn_precip_gs_wet_15_29 share_abn_precip_gs_30_44 share_abn_precip_gs_dry_30_44 share_abn_precip_gs_wet_30_44 share_abn_precip_ngs_15_29 share_abn_precip_ngs_dry_15_29 share_abn_precip_ngs_wet_15_29 share_abn_precip_ngs_30_44 share_abn_precip_ngs_dry_30_44 share_abn_precip_ngs_wet_30_44 temp_max_less_18 temp_max_18_21 temp_max_21_24 temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33"

    * Initialize counter
    local counter = 0 

    foreach country_code in `country_codes' {
        * Get country name for display
        GetCountryName, code("`country_code'")
        local country_name "`r(name)'"

        local counter = `counter' + 1

        di as text _newline "Processing country `counter': `country_name'"

        * Load the data
        capture use "$cf/data/derived/census-climate-merged/`country_name'/`country_name'-census-climate-merged-by-birthplace.dta", clear

        * Check if file loaded successfully 
        if _rc != 0 {
            di as error "    - Could not load data for `country_name', skipping ..."
            continue
        }

        * Keep only necessary variables
        keep `keepvars'

        * Add country identifier
        gen str country = "`country_name'"

        tempfile temp`counter'
        save `temp`counter'', replace

        di as result "    - Successfully processed `country'"
    }

    * Merge all the temperary files together
    clear
    local first = 1
    forvalues i = 1/`counter' {
        cap confirm file `temp`i''
        if _rc == 0 {
            if `first' == 1 {
                use `temp`i'', clear 
                local first = 0 
            }
            else {
                append using `temp`i''
            }
        }
    }

    * Check if appending operation was successful
    count
    display as result _newline "    Total observations in merged dataset: " _N 

    * Label the country variable
    label variable country "Country name"

    * Sort the data
    sort country COHORT BIRTH_SMALLEST 

    * Save the merged dataset
    save "$cf/data/derived/census-climate-merged/merged_all_countries_temperature.dta", replace

    di as result _newline "    - Merged dataset saved to $cf/data/derived/census-climate-merged/merged_all_countries_temperature.dta"

    
    * Load data
    use "$cf/data/derived/census-climate-merged/merged_all_countries_temperature.dta", clear 

    * Display number of observations by country
    tab country
    egen countryid = group(country)

    * Generate regional indicators
    gen region = ""

    * West Africa
	replace region = "West" if inlist(country, "Benin", "Burkina Faso", "Cote d'Ivoire", "Ghana")
	replace region = "West" if inlist(country, "Guinea", "Liberia", "Mali")
	replace region = "West" if inlist(country, "Senegal", "Sierra Leone", "Togo")

	* East Africa
	replace region = "East" if inlist(country, "Ethiopia", "Kenya", "Rwanda", "Tanzania", "Uganda")

	* Central Africa
	replace region = "Central" if inlist(country, "Cameroon")

	* Southern Africa
	replace region = "South" if inlist(country, "Botswana", "Lesotho", "Malawi", "Mozambique")
	replace region = "South" if inlist(country, "South Africa", "South Sudan", "Sudan", "Zambia", "Zimbabwe")

	egen regionid = group(region)

    * Label the region variable
	label variable region "Geographic region in Africa"

	* Create dummy variables for each region
	gen west = (region == "West")
	gen east = (region == "East")
	gen central = (region == "Central")
	gen south = (region == "South")
	
	** Generate a country-specific threshold for temperature:
	
	* First quartile
	
	gen country_dd_q1 = .
	replace country_dd_q1 = 29  if country == "Benin"
	replace country_dd_q1 = 26  if country == "Botswana"
	replace country_dd_q1 = 32  if country == "Burkina Faso"
	replace country_dd_q1 = 28  if country == "Cameroon"
	replace country_dd_q1 = 30  if country == "Ethiopia"
	replace country_dd_q1 = 30  if country == "Ghana"
	replace country_dd_q1 = 30  if country == "Guinea"
	replace country_dd_q1 = 30  if country == "Cote d'Ivoire"
	replace country_dd_q1 = 27  if country == "Kenya"
	replace country_dd_q1 = 21  if country == "Lesotho"
	replace country_dd_q1 = 29  if country == "Liberia"
	replace country_dd_q1 = 27  if country == "Malawi"
	replace country_dd_q1 = 33  if country == "Mali"
	replace country_dd_q1 = 27  if country == "Mozambique"
	replace country_dd_q1 = 26  if country == "Rwanda"
	replace country_dd_q1 = 32  if country == "Senegal"
	replace country_dd_q1 = 29  if country == "Sierra Leone"
	replace country_dd_q1 = 26  if country == "South Africa"
	replace country_dd_q1 = 32  if country == "South Sudan"
	replace country_dd_q1 = 35  if country == "Sudan"
	replace country_dd_q1 = 25  if country == "Tanzania"
	replace country_dd_q1 = 28  if country == "Togo"
	replace country_dd_q1 = 26  if country == "Uganda"
	replace country_dd_q1 = 27  if country == "Zambia"
	replace country_dd_q1 = 30  if country == "Zimbabwe"	
	
	* anything below 28 becomes 28 (FIXME later, once we have added 24 & 26 DD thresholds back in)
	replace country_dd_q1 = 28 if country_dd_q1 < 28
	* for values >= 28, round to nearest even number
	replace country_dd_q1 = 2 * round(country_dd_q1 / 2) if country_dd_q1 >= 28

	* Generate a country-specific DD variable = DD above Q1 threshold for that country
	gen temp_max_dd_Q1=temp_max_dd_28 if country_dd_q1==28
	replace temp_max_dd_Q1=temp_max_dd_30 if country_dd_q1==30
	replace temp_max_dd_Q1=temp_max_dd_32 if country_dd_q1==32
	replace temp_max_dd_Q1=temp_max_dd_34 if country_dd_q1==34
	replace temp_max_dd_Q1=temp_max_dd_36 if country_dd_q1==36
	
	foreach XXXX in 15_29 30_44 {
		gen temp_max_dd_Q1_`XXXX' = temp_max_dd_28_age_`XXXX' if country_dd_q1==28
		replace temp_max_dd_Q1_`XXXX' = temp_max_dd_30_age_`XXXX' if country_dd_q1==30
		replace temp_max_dd_Q1_`XXXX' = temp_max_dd_32_age_`XXXX' if country_dd_q1==32
		replace temp_max_dd_Q1_`XXXX' = temp_max_dd_34_age_`XXXX' if country_dd_q1==34
		replace temp_max_dd_Q1_`XXXX' = temp_max_dd_36_age_`XXXX' if country_dd_q1==36		
	}
	
	** Median:
	
	gen country_dd_q2 = .
	replace country_dd_q2 = 32  if country == "Benin"
	replace country_dd_q2 = 30  if country == "Botswana"
	replace country_dd_q2 = 35  if country == "Burkina Faso"
	replace country_dd_q2 = 30  if country == "Cameroon"
	replace country_dd_q2 = 40  if country == "Ethiopia"
	replace country_dd_q2 = 33  if country == "Ghana"
	replace country_dd_q2 = 32  if country == "Guinea"
	replace country_dd_q2 = 31  if country == "Cote d'Ivoire"
	replace country_dd_q2 = 30  if country == "Kenya"
	replace country_dd_q2 = 26  if country == "Lesotho"
	replace country_dd_q2 = 30  if country == "Liberia"
	replace country_dd_q2 = 28  if country == "Malawi"
	replace country_dd_q2 = 36  if country == "Mali"
	replace country_dd_q2 = 30  if country == "Mozambique"
	replace country_dd_q2 = 26  if country == "Rwanda"
	replace country_dd_q2 = 34  if country == "Senegal"
	replace country_dd_q2 = 30  if country == "Sierra Leone"
	replace country_dd_q2 = 31  if country == "South Africa"
	replace country_dd_q2 = 35  if country == "South Sudan"
	replace country_dd_q2 = 38  if country == "Sudan"
	replace country_dd_q2 = 30  if country == "Tanzania"
	replace country_dd_q2 = 35  if country == "Togo"
	replace country_dd_q2 = 28  if country == "Uganda"
	replace country_dd_q2 = 29  if country == "Zambia"
	replace country_dd_q2 = 33  if country == "Zimbabwe"

	* anything below 28 becomes 28
	replace country_dd_q2 = 28 if country_dd_q2 < 28

	* for values >= 28, round to nearest even number (so we land on 28/30/32/34/36/38/40)
	replace country_dd_q2 = 2 * round(country_dd_q2 / 2) if country_dd_q2 >= 28

	* Generate a country-specific DD variable = DD above q2 threshold for that country
	gen temp_max_dd_q2=temp_max_dd_28 if country_dd_q2==28
	replace temp_max_dd_q2=temp_max_dd_30 if country_dd_q2==30
	replace temp_max_dd_q2=temp_max_dd_32 if country_dd_q2==32
	replace temp_max_dd_q2=temp_max_dd_34 if country_dd_q2==34
	replace temp_max_dd_q2=temp_max_dd_36 if country_dd_q2==36
	*replace temp_max_dd_q2=temp_max_dd_38 if country_dd_q2==38
	*replace temp_max_dd_q2=temp_max_dd_40 if country_dd_q2==40
	
	foreach XXXX in 15_29 30_44 {
		gen temp_max_dd_q2_`XXXX' = temp_max_dd_28_age_`XXXX' if country_dd_q2==28
		replace temp_max_dd_q2_`XXXX' = temp_max_dd_30_age_`XXXX' if country_dd_q2==30
		replace temp_max_dd_q2_`XXXX' = temp_max_dd_32_age_`XXXX' if country_dd_q2==32
		replace temp_max_dd_q2_`XXXX' = temp_max_dd_34_age_`XXXX' if country_dd_q2==34
		replace temp_max_dd_q2_`XXXX' = temp_max_dd_36_age_`XXXX' if country_dd_q2==36		
		*replace temp_max_dd_q2_`XXXX' = temp_max_dd_38_age_`XXXX' if country_dd_q2==38		
		*replace temp_max_dd_q2_`XXXX' = temp_max_dd_40_age_`XXXX' if country_dd_q2==40		
	}
	
	* Country-year census identifier:
	gen YEAR = COHORT + AGE 
	tostring YEAR, gen(year_str)
	gen census_name = country+ " " + year_str
	*tab census_name if temp_max_dd_Q1_15_29==. 

	* Other outcome variables than fertility:
	* Employment
	foreach var in OCCISCO EMPSTAT {
		gen flag_`var'_miss = `var' ==. 
		bys census_name: egen tot_`var'_miss = sum(flag_`var'_miss)
		bys census_name: gen share_`var'_miss = tot_`var'_miss / _N		
	}
	
	cap drop farming
	gen farming = OCCISCO == 6 if OCCISCO < 97
	replace farming = 0 if OCCISCO >= 97 & share_OCCISCO_miss<1 // will assign farming=0 to observations from censuses that did collect OCCISCO, but where OCCISCO is recorded as Unknown/Response Suppressed/NIU/. (note: only 501 obs have value ., all from Sudan 2008)

	gen in_lforce = inlist(EMPSTAT,1,2)
	replace in_lforce = . if inlist(EMPSTAT,0,9,.)
		
	gen primary_edu = inlist(EDATTAIN,2,3,4)
	replace primary_edu = . if inlist(EDATTAIN,0,9,.)
	
	gen secondary_edu = inlist(EDATTAIN,3,4)
	replace secondary_edu = . if inlist(EDATTAIN,0,9,.)
	
	gen moved = RES_SMALLEST!=BIRTH_SMALLEST & BIRTH_SMALLEST!=. & RES_SMALLEST!=. 
	
	drop share_*_miss flag_*_miss

    save "$cf/data/derived/census-climate-merged/merged_all_countries_temperature.dta", replace