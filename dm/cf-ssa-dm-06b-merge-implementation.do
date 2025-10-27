/********************************************************************************
* PROJECT:        Climate Fertility
* AUTHOR:         Prayog
* DATE MODIFIED:  19 September 2025
* DESCRIPTION:    Merging ERA weather data with IPUMS census data, creating diagnostics for unmatched observations
* DEPENDENCIES:   Needs overleaf - dropbox integration. Need to run dm-06a first.
* 
* NOTES: 
********************************************************************************/

/**********************************************************************/
/*  SECTION 1:  Setup	
    Notes: */
/**********************************************************************/
	capture program drop ClimateAnalysisConfig
	program define ClimateAnalysisConfig
	    syntax, method(string) [generate_tables(string)]
	  
	    if "`method'" == "init" {
	        di "Initializing configuration for user: `c(username)'"
	        
	        if "`c(username)'" == "prayogbhattarai" {
	            global dir "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
	        }
			else if "`c(username)'" == "prayog" {
				global dir "D:/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "D:/`c(username)'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
				di "    User detected: prayog"
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
				di as error "Unknown user. Set global dir manually."
				exit 198
			}
			global cf "$dir"
			global era_derived "$dir/data/derived/era"
			global era_raw "$dir/data/raw/era"

			* Switch to generate LaTeX summary output
			if "`generate_tables'" == "" {
				local run_latex_tables = "Yes"
			}
			else {
				local run_latex_tables = "No"
			}

			di "Configuration successful:"
			di "  Base directory: $cf"
			di "  ERA derived: $era_derived"
	    }
	end
/*------------------------------------ End of SECTION 1 - Setup ------------------------------*/


/**********************************************************************/
/*  SECTION 2:  Implementation  			
    Notes: */
/**********************************************************************/
	
	ClimateAnalysisConfig, method("init") generate_tables("Yes")

	// Run the merge setup do file, which contains the building blocks and the main workflow function
	do "$cf/dm/cf-ssa-dm-06a-merge-setup.do"

	// Set up the testing subset
	local test_codes "SDN"   // Start with just one country for testing
	local country_codes "`test_codes'"

	* For full run, use:
	local country_codes "BEN BWA BFA CMR ETH GHA GIN IVC KEN LSO LBR MWI MLI MOZ RWA SLE ZAF SEN SDN TZA TGO UGA ZMB ZWE"

	local total_countries : word count `country_codes'
	di "Processing `total_countries' countries"
	di ""

	local country_num = 0
	local countries_completed ""
	local countries_failed ""
	local successful_countries = 0
	local failed_countries = 0 
	local success_list = ""
	local total_obs = 0
	local total_matched = 0

	foreach country_code in `country_codes' {
	    local ++country_num
	    
	    * Get country name for display
	    GetCountryName, code("`country_code'")
	    local country_name "`r(name)'"
		local merge_var "RES_SMALLEST"
	    
	    di "============================================================================"
	    di "Country `country_num'/`total_countries': `country_name' (`country_code')"
	    di "============================================================================"

		capture noisily process_country, country_code("`country_code'") merge_var("`merge_var'")
		if `country_success' {
			local ++successful_countries
			local success_list "`success_list' `country_name'"
			local total_obs = `total_obs' + `country_final_obs'
			local total_matched = `total_matched' + `country_matched_obs'
			di "✓ `country_name' completed successfully"
		}
		else {
			local failed_countries "`failed_countries' `country_name'"
			di "✗ `country_name' failed"
		}
	}

/*------------------------------------ End of SECTION 2 - Implementation------------------------------*/


/**********************************************************************/
/*  SECTION 3:  Summary of implementation  			
    Notes:      Evaluation of merge quality */
/**********************************************************************/

	*if "`run_latex_tables'" == "Yes" {
		*************************************
		* Table 1: Merge results 
		*************************************
		// Initialize accumulators
		local sum_allobs = 0
		local sum_matched = 0
		local sum_unmatched = 0
		file open merge_table using "$cf_overleaf/table/merge-results-table-`merge_var'.tex", write replace
		file write merge_table "\begin{tabular}{lrrr}" _n
		file write merge_table "\toprule" _n
		file write merge_table "Country & Total in census & Matched & Unmatched \\" _n
		file write merge_table " & (1) & (2) & (3) \\" _n
		file write merge_table "\midrule" _n
		foreach country_code in `country_codes' {
			GetCountryName, code("`country_code'")
			local country_name "`r(name)'"
			if "${`country_code'_total_obs}" != "" {

				// Accumulate totals
				local sum_allobs = `sum_allobs' + ${`country_code'_allobs}
				local sum_matched = `sum_matched' + ${`country_code'_matched_count}
				local sum_unmatched = `sum_unmatched' + ${`country_code'_unmatched_census_count}
				
				// Format numbers with commas for readability in console
				local allobs_fmt: di %12.0fc ${`country_code'_allobs}
				local matched_fmt: di %12.0fc ${`country_code'_matched_count}
				local unmatched_fmt: di %12.0fc ${`country_code'_unmatched_census_count}
				
				di "`country_name' & `allobs_fmt' & `matched_fmt' & `unmatched_fmt' \\"
				
				// Write to file (LaTeX will handle number formatting)
				file write merge_table "`country_name' & ${`country_code'_allobs} & ${`country_code'_matched_count} & ${`country_code'_unmatched_census_count} \\" _n
				file write merge_table " & & (${`country_code'_matched_share}\%) & (${`country_code'_unmatched_census_share}\%) \\" _n
				file write merge_table "\addlinespace" _n
			}
		}
		// Calculate percentages for totals
		local sum_matched_pct: di %9.2fc (`sum_matched' / `sum_allobs') * 100
		local sum_unmatched_pct: di %9.2fc (`sum_unmatched' / `sum_allobs') * 100
		file write merge_table "\midrule" _n
		file write merge_table "\textbf{Total} & \textbf{`sum_allobs'} & \textbf{`sum_matched'} & \textbf{`sum_unmatched'} \\" _n
		file write merge_table " & & \textbf{(`sum_matched_pct'\%)} & \textbf{(`sum_unmatched_pct'\%)} \\" _n


		file write merge_table "\bottomrule" _n
		file write merge_table "\end{tabular}" _n
		file close merge_table

		di "LaTeX table created successfully: $cf_overleaf/table/merge-results-table.tex"


		*----------------------------------------------
		* Table 2: Unmatched SMALLEST analysis 
		*----------------------------------------------
		// Initialize accumulators
		local sum_weather = 0
		local sum_census = 0
		local sum_unmatched = 0
		local sum_matched = 0

		local total_countries : word count `country_codes'
		di "Processing `total_countries' countries"
		di ""


		file open smallest_table using "$cf_overleaf/table/merge-unmatched-smallest-analysis-`merge_var'.tex", write replace
		file write smallest_table "\begin{tabular}{lrrrr}" _n
		file write smallest_table "\toprule" _n
		file write smallest_table "Country & \shortstack[l]{Total in\\weather} & \shortstack[l]{Total in\\census} & \shortstack[l]{Total Unmatched\\units} & \shortstack[l]{Total matched \\ units} \\" _n
		file write smallest_table " & (1) & (2) & (3) & (4) \\" _n
		file write smallest_table "\midrule" _n

		foreach country_code in `country_codes' {
			GetCountryName, code("`country_code'")
			local country_name "`r(name)'"
			
			// Accumulate totals
			local sum_weather = `sum_weather' + ${`country_code'_n_weather}
			local sum_census = `sum_census' + ${`country_code'_n_census}
			local sum_unmatched = `sum_unmatched' + ${`country_code'_n_unmatched_census}
			local sum_matched = `sum_matched' + ${`country_code'_n_matched}

			
			di "`country_name' & ${`country_code'_n_weather} & ${`country_code'_n_census} & ${`country_code'_n_unmatched_census} & ${`country_code'_n_matched} \\"
			file write smallest_table "`country_name' & ${`country_code'_n_weather} & ${`country_code'_n_census} & ${`country_code'_n_unmatched_census} & ${`country_code'_n_matched} \\" _n
		
		}

		// Write sum row
		file write smallest_table "\midrule" _n
		file write smallest_table "\textbf{Total} & \textbf{`sum_weather'} & \textbf{`sum_census'} & \textbf{`sum_unmatched'} & \textbf{`sum_matched'} \\" _n

		file write smallest_table "\bottomrule" _n
		file write smallest_table "\end{tabular}" _n
		file close smallest_table

		di "LaTeX tables created successfully:"
		di "  - Merge results: $cf_overleaf/merge_results_table.tex"
		di "  - SMALLEST analysis: $cf_overleaf/merge-unmatched-smallest-analysis.tex.tex"
	*}


/*------------------------------------ End of SECTION 3 - Summary of implementation------------------------------*/



