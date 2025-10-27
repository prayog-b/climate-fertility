/********************************************************************************
* PROJECT:        Climate Fertility
* AUTHOR:         Prayog
* DATE MODIFIED:  09 October 2025
* DESCRIPTION:    Utility functions for merging weather and census datasets
* DEPENDENCIES:    
* 
* NOTES: 
********************************************************************************/


/**********************************************************************/
/*  SECTION 1:  Building blocks   			
    Notes: */
/**********************************************************************/

	/*
		Program: GetCountryName
		Purpose: Based on a country code input, generates a local with the full name of the country as output.
		Usage:   GetCountryName, code(string)
		Arguments:
			code - String. Country code used to identify the country.
		Details:
			- Retrieves the country name using the provided country code.
	*/
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
		    else if "`code'" == "IVC" {
		    	local country_name "Ivory Coast"
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
		    else if "`code'" == "KEN" {
		        local country_name "Kenya"
		    }
		    else if "`code'" == "LSO" {
		        local country_name "Lesotho"
		    }
		    else if "`code'" == "LBR" {
		        local country_name "Liberia"
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


	/*
	Program: validate_dataset
	Purpose: Check for existence of required variables, and show counts of the dataset
	Usage:   validate_dataset, required_vars(string) dataset_name(string) [count_by(string)]
	Arguments: 
		- required_vars: String - existence specified variables will be checked
		- dataset_name: String - specifies the dataset that we will be validating
		- (Optional) count_by: String - at the end, the variable specified here will be tabulated using tab
	*/
		capture program drop validate_dataset
		program define validate_dataset
			syntax, required_vars(string) dataset_name(string) [count_by(string)]
			local missing_vars ""
			local validation_passed = 1
			
			foreach var in `required_vars' {
			    capture confirm variable `var'
				if _rc {
				    local missing_vars "`missing_vars' `var'"
					local validation_passed = 0
				}
			}
			
			if !`validation_passed' {
			    di as error "   ERROR: Missing required variables in `dataset_name': `missing_vars'"
				c_local validation_error = 1
				exit
			}
			
			// basic dataset characteristics
			qui count
			local obs_count = r(N)
			di "   SUCCESS: `dataset_name' validation passed"
			di "   Observation count: `obs_count'"
			
			if "`count_by'" != "" {
			    qui tab `count_by'
				di "   Unique `count_by': `r(r)'"
			}
			
			c_local validation_error = 0 
			c_local obs_count = `obs_count'
		end


	/*
		Program: load_weather_data
		Purpose: Loads weather data (precipitation or temperature) for a specified country, 
				 performs standard variable renaming, and checks for required variables.
		Usage:   load_weather_data, country_code(string) data_type(string)
		Arguments:
			country_code - String. Country code used to identify the country.
			data_type    - String. Type of weather data to load ("precip" or "temperature").
		Details:
			- Retrieves the country name using the provided country code.
			- Constructs the file path based on the country name and data type.
			- Attempts to load the specified .dta file.
			- Renames key variables to standard names for merging.
			- Checks for the existence of required variables (MERGE_SMALLEST, COHORT, CENSUS).
			- Sets the local macro `load_success` to 1 if successful, 0 otherwise.
	*/
		capture program drop load_weather_data
		program define load_weather_data
			syntax, country_code(string) data_type(string) 
			GetCountryName, code("`country_code'")
			local country_name "`r(name)'"
			local country "`r(name)'"
			di "Loading precipitation data: `country_name'"
			
			// Setting up file path based on data type (precipitation or temperature)
			if "`data_type'" == "precip" {
			    local file_path "$cf/data/derived/era/`country_name'/`country_name'_allcensus-cohort-precip.dta"
			}
			else if "`data_type'" == "temperature" {
			    local file_path "$cf/data/derived/era/`country_name'/`country_name'_allcensus-cohort-temperature.dta"
			}

			// Validating file path exists
			capture confirm file "`file_path'"
			if _rc {
			    di as error "    ERROR: `data_type' file not found: `file_path'"
			}
			
			// Load weather data file
			capture use "`file_path'", clear
			if _rc {
			    di as error "     ERROR: `file_path' could not be loaded. Please check file path name in directory."
			}
			
			// Standard renaming and reformatting
			capture rename SMALLEST MERGE_SMALLEST
			capture rename smallest MERGE_SMALLEST
			capture rename year YEAR
			capture rename cohort COHORT
			capture rename age AGE
			capture rename census CENSUS
			format %15.0f MERGE_SMALLEST
			
			
			// Ensure required variables exist
			foreach var in MERGE_SMALLEST COHORT CENSUS {
			    capture confirm variable `var'
				if _rc {
					di as error "  ERROR: Required variable `var' not found in `data_type' data."
					c_local load_success = 0
					exit
				}		
			}
			
			c_local load_success = 1
			di "    - SUCCESS: `data_type' data loaded for `country'"
		end

	/*
	Program: load_dropped_geom_report
	Purpose: Load the csv file that contains a list of all geometries 
			dropped during cleaning and the reason they were dropped from the shapefile. 
	Usage:   load_dropped_geom_report, country_code(string)
	Arguments:
		- country_code: String - specifies which country is being processed

	Procedure:
		- load the dropped geometries report generated from the shapefile cleaning in dm-01a-clean-shapefile.py
		- Confirm that the variables we need exist
		- Keep observations (if any) that are from the country whose code is specified in the function argument
		- If there are dropped geometries for the country:
			- ensure the variable SMALLEST is in a variable type consistent with weather and census data.
			- rename the variable SMALLEST to MERGE_SMALLEST for the merging procedure. 
	Notes: 
		- when loading the dropped geometries report csv, 
			we use the global $cf for the filepath. ensure that the global is defined.
	*/
		capture program drop load_dropped_geom_report
		program define load_dropped_geom_report
		    syntax, country_code(string) 
		    GetCountryName, code("`country_code'")
		    local country_name "`r(name)'"
		    local country "`r(name)'"
		    
		    di "Loading dropped geometries report"
		    capture import delimited using "$cf/data/source/shapefiles/cleaned_ssa_boundaries_all_dropped_records.csv", clear
		    if _rc {
		        di as error "    ERROR: Could not load dropped geometries report data"
		        c_local geom_load_success = 0
		        c_local has_dropped = 0
		        exit
		    }

		    di "Checking whether required variables exist"
		    local vars_to_keep "country smallest drop_reason drop_details"
		    foreach var of local vars_to_keep {
		        capture confirm variable `var'
		        if _rc {
		                di as error "ERROR: Could not find `var'"
		                c_local geom_load_success = 0
		                c_local has_dropped = 0
		                exit
		        }
		    }
		    
		    /*
			Since this dataset will only have countries whose geometries were dropped,
			we check whether the selected country has observations. 
			If yes, continue. If no, exit.
		    */
			di "Restricting observations to `country_name'"
		    capture keep if country == "`country_name'"
		    quietly count
		    if r(N) == 0 {
		        di "No geometries were dropped for `country_name' during the shapefile processing, continue."
		        c_local has_dropped = 0
		        c_local geom_load_success = 1  // Add this line
		        exit  
		    }
		    else if r(N) > 0 {
		        di "Found `r(N)' geometries dropped for `country_name' during the shapefile processing."
		        c_local has_dropped = 1
		    }
		    
			di "Checking variable type of smallest ..."
		    local vartype: type smallest
		    if "`vartype'" == "long" {
		        di "- Smallest variable is in long format. no recasting needed"
		    }
		    else if "`vartype'" == "float" | "`vartype'" == "double" {
		        di "smallest variable is float or double. recasting to long."
		        cap recast long smallest
		        if _rc {
		            di as error "Could not recast smallest as long. please check data type to diagnose."
		            c_local geom_load_success = 0 
		            exit
		        }
		    }
		    else if substr("`vartype'",1,3) == "str" {
		        di as error "smallest variable is a string (type: `vartype'). recasting not possible."
		        di as text "Attempting destring"
		        cap destring smallest, replace
		        if _rc {
		            di as error "Could not destring smallest."
		            c_local geom_load_success = 0 
		            exit
		        }
		    }
		    else {
		        di as error "Unexpected type (`vartype') for variable smallest. Please check."
		        c_local geom_load_success = 0 
		        exit
		    }
			di "- Renaming smallest to MERGE_SMALLEST"
		    rename smallest MERGE_SMALLEST
		    
		    c_local geom_load_success = 1
		end
		
		
	/*
	Program: load_shapefile_names
	Purpose: Load cleaned shapefile and extract SMALLEST unit names for merging
	Usage:   load_shapefile_names, country_code("string")
	Arguments:
		- country_code: String - specifies the country to process 
	Workflow:
		- Load the cleaned shapefile
		- Filter to selected country
		- Validate and prepare MERGE_SMALLEST variable
		- Check uniqueness of SMALLEST - BPL_NAME combinations
		- Keep only necessary variables
	*/	
		capture program drop load_shapefile_names
		program define load_shapefile_names
			syntax, country_code(string)
			
			GetCountryName, code("`country_code'")
			local country_name "`r(name)'"
			
			di "Loading shapefile for BPL_NAME data ..."
		    capture {
				qui shp2dta using "$cf/data/derived/shapefiles/cleaned_ssa_boundaries.shp", ///
				data("$cf/data/temp/shapefile_names_data.dta") ///
				coor("$cf/data/temp/shapefile_names_coord.dta") ///
				genid(id) replace
				use "$cf/data/temp/shapefile_names_data.dta", clear
			}
			if _rc {
				di as error "    ERROR: Could not load shapefile"
				c_local shapefile_load_success = 0
				exit 
			}
			
			// Variables in shapefile 
			di "Variables in shapefile: "
			ds
			
			
			// Validate and prepare COUNTRY variable
			di "Validating COUNTRY variable ..."
			capture confirm string variable COUNTRY 
			if _rc {
				// COUNTRY variable needs decoding
				capture decode COUNTRY, gen(country_string)
				if _rc {
					di as error "    ERROR: Could not decode COUNTRY variable."
					c_local shapefile_load_success = 0
					exit
				}
				drop COUNTRY
				rename country_string COUNTRY
			}
			
			// Standardize to proper names
			di "    Converting COUNTRY to proper case ..."
			replace COUNTRY = proper(COUNTRY)
			
			
			// Filter to selected country(ies)
			if "`country_code'" == "SDN" {
				di "Filtering to Sudan and South Sudan"
				keep if inlist(COUNTRY, "Sudan", "South Sudan")
			}
			else {
				di "Filtering to `country_name'"
				keep if COUNTRY == "`country_name'"
			}
			
			
			qui count 
			if r(N) == 0 {
				di as error "    ERROR: No records found for `country_name' in shapefile"
				c_local shapefile_load_success = 0
				exit
			}
			
			di "    Found `r(N)' records for `country_name'"
			
			// Check and prepare SMALLEST variable 
			di "Preparing SMALLEST variable ..."
			local vartype: type SMALLEST
			di "    SMALLEST type in shapefile: `vartype'"
		
			if substr("`vartype'", 1, 3) == "str" {
				di "    SMALLEST is string, converting to numeric..."
				capture destring SMALLEST, replace
				if _rc {
					di as error "    ERROR: Could not destring SMALLEST"
					c_local shapefile_load_success = 0
					exit
				}
			}
			
			capture recast long SMALLEST
			if _rc {
				di as error "    ERROR: Could not recast SMALLEST to long"
				c_local shapefile_load_success = 0
				exit
			}
				
			format %15.0f SMALLEST
			rename SMALLEST MERGE_SMALLEST
			
			// If country is Mali, the Bamako district has several BPL_NAME units assigned to same SMALLEST
			replace BPL_NAME = "district bamako" if MERGE_SMALLEST == 466009001
			replace BPL_NAME = "kidal" if MERGE_SMALLEST == 466007005
			
			// Check for duplicates in SMALLEST - BPL_NAME combinations
			di "Checking uniqueness of MERGE_SMALLEST - BPL_NAME combinations ..."
			duplicates report MERGE_SMALLEST 
			
			qui duplicates tag MERGE_SMALLEST, gen(dup_tag)
			qui count if dup_tag > 0
			
			if r(N) > 0 {
				local n_dups = r(N)
				di as text "    WARNING: Found `n_dups' duplicate MERGE_SMALLEST"
				di as text "    Dropping duplicates and keeping first occurrence..."
				duplicates drop MERGE_SMALLEST, force
			}

			drop dup_tag 
			
			// Keep only necessary variables
			keep MERGE_SMALLEST BPL_NAME 
			qui count 
			
			local final_count = r(N)
			qui unique MERGE_SMALLEST 
			local unique_smallest = r(unique)
			
			di "    SUCCESS: Shapefile names data prepared"
			di "      Records: `final_count'"
			di "      Unique MERGE_SMALLEST: `unique_smallest'"
			
			c_local shapefile_load_success = 1
		end
			
	

	/*
	Program: load_census_data
	Purpose: Prepare census data for merger
	Usage:   load_census_data, country_code("ZAF") [merge_var(BIRTH_SMALLEST)]
	Arguments:
		- country_code: String - specified to restrict the census data to the country's observations. 
		- merge_var: String - specify whether we will merge based on a woman's birthplace SMALLEST value or place of residence 
	Workflow:
		- Load the cleaned census data
		- Restrict the census data to the country selected
		- Check if selected merging variable exists and recast it to a different variable type if needed. 
		- Drop observations that have missing values for the merging variable
	Notes:
	*/
		capture program drop load_census_data
		program define load_census_data
			syntax, country_code(string) [merge_var(string)]
			
			GetCountryName, code("`country_code'")
			local country_name "`r(name)'"
			
			// Default is using BIRTH_SMALLEST for merger
			if "`merge_var'" == "" local merge_var "BIRTH_SMALLEST"
			
			// Validate merge variable choice
		    if !inlist("`merge_var'", "BIRTH_SMALLEST", "RES_SMALLEST") {
		        di as error "  ERROR: Invalid merge_var. Must be BIRTH_SMALLEST or RES_SMALLEST"
		        c_local census_load_success = 0
		        exit
		    }
			
			di as text "Loading census data ..."
			capture use "$dir/data/derived/census/africa-women-genvar.dta", clear
			if _rc {
				di as error "    ERROR: Could not load census data"
				c_local census_load_success = 0
				exit
			}
			
			di "Validating COUNTRY variable ..."
			capture confirm numeric variable COUNTRY
			if _rc {
				quietly generate country_name = COUNTRY
			}
			else {
				capture decode COUNTRY, gen(country_name)
				if _rc {
					di as error "ERROR: Could not decode COUNTRY variable"
					c_local census_load_success = 0
					exit
				}
			}
			capture replace country_name = "Ivory Coast" if country_name == "Cote d'Ivoire"
			capture drop COUNTRY
			capture rename country_name COUNTRY
			
			di "Filtering to keep observations in `country_name' ..."
			capture keep if COUNTRY == "`country_name'"
			count
			di "Number of observations after filtering; `r(N)'"
			
			// merging variable preparation
			if "`merge_var'" == "BIRTH_SMALLEST" {
		        di "  Using BIRTH_SMALLEST as merge variable (with missing value imputation)"
		        
		        // Check if BIRTH_SMALLEST exists
		        capture confirm variable BIRTH_SMALLEST
		        if _rc {
		            di as error "  ERROR: BIRTH_SMALLEST variable not found in census data"
		            c_local census_load_success = 0
		            exit
		        }
		        
				qui count if BIRTH_SMALLEST == . & COUNTRY == "Ethiopia" & inlist(YEAR, 1994, 2007)
		        local imputed_count = r(N)
		        if `imputed_count' > 0 {
					di "Apply special case imputation for BIRTH_SMALLEST in Ethiopia"

		            di "replacing BIRTH_SMALLEST with RES_SMALLEST for `imputed_count' observations"
		            replace BIRTH_SMALLEST = RES_SMALLEST if BIRTH_SMALLEST == . & COUNTRY == "Ethiopia" & inlist(YEAR, 1994, 2007)
		            di "    Imputed `imputed_count' missing BIRTH_SMALLEST values using RES_SMALLEST"
					}
				// Create standardized merge variable
				gen MERGE_SMALLEST = BIRTH_SMALLEST
			 }
			 else if "`merge_var'" == "RES_SMALLEST" {
		        di "  Using RES_SMALLEST as merge variable (no imputation needed)"
		        
		        // Check if RES_SMALLEST exists
		        capture confirm variable RES_SMALLEST
		        if _rc {
		            di as error "  ERROR: RES_SMALLEST variable not found in census data"
		            c_local census_load_success = 0
		            exit
		        }
		        
		        // Create standardized merge variable
		        gen MERGE_SMALLEST = RES_SMALLEST
		    }
			
			local vartype: type MERGE_SMALLEST
			di "Variable type of MERGE_SMALLEST in census: `vartype'"
			
			
			di "Validate merge variable has non-missing values"
			qui count if missing(MERGE_SMALLEST)
			local missing_merge = r(N)
			qui count
			local total_obs = r(N)
			
			if `missing_merge' == `total_obs' {
				di as error "    ERROR: All values missing in `merge_var'"
				c_local census_load_success = 0
				exit
			}
			
			if `missing_merge' > 0 {
				local pct_missing  = round(100 * (`missing_merge'/`total_obs'), 0.1)
				di as text "   Warning: `missing_merge' (`pct_missing'%) missing values in `merge_var'"
				
				// Drop observations with missing merge variable 
				drop if missing(MERGE_SMALLEST)
				qui count
				local final_obs = r(N)
				di       " Proceeding with `final_obs' observations after dropping missing values."
			}
			
			rename YEAR CENSUS 
			
			di "Dropping people born before 1926"
			qui drop if COHORT < 1926
			
			qui count 
			if r(N) == 0 {
				di as error "    ERROR: No census data found for `country_name'"
				c_local census_load_success = 0
				exit
			}
			
			di "Final validation and statistics."
			qui count 
			local final_obs = r(N)
			qui tab MERGE_SMALLEST
			local unique_subunits = r(r)
			qui sum COHORT 
			local cohort_range "`=r(min)'-`=r(max)'"
			
			c_local census_load_success = 1
			c_local census_obs = `final_obs'
			c_local merge_variable_used = "`merge_var'"
			global `country_code'_allobs = `final_obs'
			di "   SUCCESS: Census data loaded for `country'"
			di "    - Merge variable: `merge_var'"
			di "    - Observations:   `final_obs'"
			di "    - Unique subunits: `unique_subunits'"
			di "    - Cohort range: `cohort_range'"
		end


	/*
	Program: merge_datasets
	Purpose: Bring in loading functions and merge the datasets. 
	Usage:   merge_datasets, country_code("ZAF") merge_var("BIRTH_SMALLEST")
	Arguments:
		- country_code: String - specifies the country to use 
		- merge_var: String - specifies the merge mapping variable
	Notes:
	*/
	capture program drop merge_datasets
	program define merge_datasets
		syntax, country_code(string) [merge_var(string)]
		
		GetCountryName, code("`country_code'")
		local country_name "`r(name)'"
		di "Country name: `country_name'"
		if "`merge_var'" == "" local merge_var "BIRTH_SMALLEST"
	    
	    di "Merge variable: `merge_var'"

	    di "===================================================================="
	    di "STEP 1: Loading precipitation data ..."
		di "===================================================================="
	    load_weather_data, country_code("`country_code'") data_type("precip")
	    if !`load_success' {
	        c_local merge_strategy_success = 0
	        exit
	    }
		
		validate_dataset, required_vars("MERGE_SMALLEST COHORT CENSUS") dataset_name("precip") count_by("MERGE_SMALLEST")
		if `validation_error' {
			c_local merge_strategy_success = 0
			exit
		}
		
		tempfile precip_data
		save `precip_data'
		di "    Saved precipitation data to tempfile"
		
		di "===================================================================="
	    di "STEP 2: Loading temperature data ..."
		di "===================================================================="
		load_weather_data, country_code("`country_code'") data_type("temperature")
		if _rc {
			c_local merge_strategy_success = 0
			exit
		}
		
		validate_dataset, required_vars("MERGE_SMALLEST COHORT CENSUS") dataset_name("temperature") count_by("MERGE_SMALLEST")
	    if `validation_error' {
	        c_local merge_strategy_success = 0
	        exit
	    }
		di "    Temperature data prepared"
		
	    di "===================================================================="
	    di "STEP 3: Merging weather datasets..."
		di "===================================================================="
	    capture quietly merge 1:1 COHORT MERGE_SMALLEST CENSUS using `precip_data', gen(weather_merge)
		if _rc != 0 {
			di as error "Could not merge weather datasets."
			exit
		}
		
		* Count the number of unique subunits in the weather data
		qui unique MERGE_SMALLEST 
		local num_subunits_weather = r(unique)
		di "There are `num_subunits_weather' subunits in the weather data"
		// save the number of unique subunits in weather data to a global 
		global `country_code'_n_weather = "`num_subunits_weather'"
		* levelsof MERGE_SMALLEST
		
	    // Keep only matched weather observations
	    keep if weather_merge == 3
	    quietly drop weather_merge
		
	    // Keep original name for weather data
	    qui count
	    local weather_obs = r(N)
	    di "- Proceeding with `weather_obs' weather observations"
		
		// Identifying the range of cohorts in the weather data
		qui summarize COHORT
		local min_cohort_weather = r(min)
		local max_cohort_weather = r(max)
		di "- Range of cohorts in weather data: `min_cohort_weather' - `max_cohort_weather'"

		local vartype: type MERGE_SMALLEST 
		di "- MERGE_SMALLEST variable type: `vartype'"
		    
	    quietly {
			tempfile weather_data
			save `weather_data'
		}
		
	    di "   ===================================================================="
		di "    Intermediary step: Load the dropped geometries report data"
	    di "   ===================================================================="
			load_dropped_geom_report, country_code("`country_code'")
				if !`geom_load_success' {
						c_local merge_strategy_success = 0
						exit
				}
				tempfile dropped_geom_report
				save `dropped_geom_report'
		
	    di "===================================================================="
	    di "STEP 4: Loading census data..."
		di "===================================================================="
	    load_census_data, country_code("`country_code'") merge_var("`merge_var'")
			if !`census_load_success' {
				c_local merge_strategy_success = 0
				exit
			}
			
			// Identify the number of SMALLEST units in the census data
			qui unique MERGE_SMALLEST 
			local num_subunits_census = r(unique)
			di "There are `num_subunits_census' subunits in the census data"
			global `country_code'_n_census = "`num_subunits_census'"
			
			// Count observations with missing values for MERGE_SMALLEST
			qui count if missing(MERGE_SMALLEST)
			local missing_smallest_census = r(N)
			di "There are `r(N)' observations with missing MERGE_SMALLEST"

			// Identify the range of cohorts in the census data
			qui summarize COHORT 
			local min_cohort_census = r(min)
			local max_cohort_census = r(max)
			di "Range of cohorts in census data: `min_cohort_census' - `max_cohort_census'"
			
			// Estimate the number of missing values we should expect 
			local gap_min = `min_cohort_census' - `min_cohort_weather'
			local gap_max = `max_cohort_weather' - `max_cohort_census'
			di "Gap in minimum cohort: `gap_min'"
			di "Gap in maximum cohort: `gap_max'"
			local min_max = `gap_min' + `gap_max'

			qui unique CENSUS
			local num_census = r(unique)
			di "Number of unique census observations: `num_census'"
			local num_unmatch = `num_census' * `min_max' * `num_subunits_weather'
			di "We should expect around `num_unmatch' non-matching observations from weather data."
			
		if `has_dropped' == 1 {
			di "  ===================================================================="
			di "   Intermediary step: Merge the dropped geometries report data"
			di "  ===================================================================="
				capture noisily merge m:1 MERGE_SMALLEST using `dropped_geom_report', gen(dropped_geom_merge)
				if _rc {
					di as error " WARNING: merge between census and dropped geometries report failed"
					c_local merge_strategy_success = 0
					exit
				}	
				drop if dropped_geom_merge == 2
				*list MERGE_SMALLEST drop_details if !missing(drop_details)
		}
		else {
			di "There were no dropped units from the shapefile, so we skip the merge with the dropped geometries report."
		}
			
		di "===================================================================="
	    di "STEP 4: Final merge with weather data..."
		di "===================================================================="	
		// Update merge diagnostic to show which variable was used 
		di "Country: `country_name'"
		di "   Merging on: `merge_variable_used' + COHORT + CENSUS ↔ WEATHER_SMALLEST + COHORT + CENSUS"
		merge m:1 COHORT MERGE_SMALLEST CENSUS using `weather_data', gen(final_merge) ///
			keepusing(*) update replace
		* For Sudan, there are BIRTH_SMALLEST entries being brought over from South Sudan so need to account for that
		if "`country_code'" == "SDN" {
			di "Country is Sudan. Need to bring South Sudan data"
			preserve
				di "Loading South Sudan precipitation"
				load_weather_data, country_code("SSD") data_type("precip")
				if !`load_success' {
					c_local merge_strategy_success = 0
					exit
				}
				di "Creating tempfile"
				tempfile precip_data_ssd
				save `precip_data_ssd'
				di "Loading South Sudan temperature"
				load_weather_data, country_code("SSD") data_type("temperature")
				if _rc {
					c_local merge_strategy_success = 0
					exit
				}
			
				merge 1:1 COHORT MERGE_SMALLEST CENSUS using `precip_data_ssd', gen(weather_merge)
				tempfile weather_data_ssd
				save `weather_data_ssd'
			restore
			merge m:1 COHORT MERGE_SMALLEST CENSUS using `weather_data_ssd', gen(final_merge_ssd) update
		}
		qui count 
		local total_obs = r(N)
	
		if "`country_code'" == "SDN" {
			di "Updating the final_merge variable to account for South Sudan data ..."
			replace final_merge = 3 if final_merge_ssd == 4 & final_merge != 3 
		}
		
		di " MERGE RESULTS:"
		di "  Total observations: `total_obs'"
		local match_count = 0
		forvalues i = 1/3 {
			qui count if final_merge == `i'
			local merge_`i' = r(N)
			local pct_`i' = round(100 * `merge_`i'' / `total_obs', 0.1)
			
			if `i' == 1 {
				di "    Master only: `merge_`i'' (`pct_`i''%)"
			}
			else if `i' == 2 {
				di "    Using only:  `merge_`i'' (`pct_`i''%)"
			}
			else if `i' == 3 {
				di "    Matched: `merge_`i'' (`pct_`i''%)"
				local match_count = r(N)
			}
		}
		
		// Identify 
		qui count if final_merge == 1 
		local master_not_mergeds = r(N)

		di "SMALLEST units that got matched"
		levelsof MERGE_SMALLEST if final_merge == 3

		di "SMALLEST units from census not getting matched."
		levelsof MERGE_SMALLEST if final_merge == 1 
		
		di "COHORT units from census not getting matched"
		levelsof COHORT if final_merge == 1 & COHORT <= `max_cohort_census'
		
		count if final_merge == 1 & COHORT >= 1926 & COHORT <= `max_cohort_census'
		di "`r(N)' observations that are after cohort 1926 not getting matched."
		
		qui unique MERGE_SMALLEST if final_merge == 3
		local merged_smallest_units = r(unique)
		di "`merged_smallest_units' subunits were matched between weather and census datasets."
		global `country_code'_n_matched = `merged_smallest_units'  

		qui unique MERGE_SMALLEST if final_merge == 2
		local unmerged_smallest_weather = r(unique)
		di "`unmerged_smallest_weather' subunits represented in the `merge_2' unmerged observations in weather data."
		
		qui unique MERGE_SMALLEST if final_merge == 1
		local unmerged_smallest_census = r(unique)
		di "`unmerged_smallest_census' subunits represented in the `merge_1' unmerged observations in census data"
		global `country_code'_n_unmatched_census = `unmerged_smallest_census'
		
		local difference_unmerged_weather = `unmerged_smallest_weather' - `num_unmatch'
		if `difference_unmerged_weather' > 0 {
			di "There are `difference_unmerged_weather' observations that are unexpectedly unmerged."
		}
		
		di "There should be noone in the census who does not have weather data assigned to them."
		capture assert final_merge != 1
		if _rc {
			di as error "WARNING: There are people in the census data without weather data for them."
			quietly count if inlist(NATIVITY,2,3) | missing(NATIVITY) 
			di as text "      - `r(N)' observations have foreign or unknown or missing origins (based on NATIVITY).'"
			quietly count if final_merge == 1
			local num_census_unmatched = r(N)
			di as text "     - `num_census_unmatched' observations in census without weather data found."
			
			if `has_dropped' == 1 {
				quietly count if drop_details == "Geometry column contains null/NaN values"
				di as text "     - Of these people `r(N)' observations are listed in null/invalid geometries in the shapefile."
				levelsof MERGE_SMALLEST if drop_details == "Geometry column contains null/NaN values"
				quietly count if drop_details == "Geometry became invalid after overlap resolution or operation failed"
				di as text "     - `r(N)' observations are listed as Geometry became invalid after overlap resolution or operation failed"
				levelsof MERGE_SMALLEST if drop_details == "Geometry became invalid after overlap resolution or operation failed"
				if "`country_code'" == "SDN" {
				quietly count if drop_details == "SMALLEST code doesn't start with CNTRY_CD (Sudan/South Sudan boundary issues)"
				di as text "      - `(N)' observations have the Sudan/South Sudan boundary issue"
				}
				di as text "      - The rest of these observations cannot be explained by shapefile cleaning. It is likely they were never in the shapefile."
			}
			
		}
		
		di "===================================================================="
		di "STEP 5: Merging BPL_NAME from shapefile..."
		di "===================================================================="
		
		// Load shapefile names
		preserve
			load_shapefile_names, country_code("`country_code'")
			if !`shapefile_load_success' {
				di as error "    ERROR: Failed to load shapefile names"
				restore 
				c_local merge_strategy_success = 0 
				exit
			}
			tempfile shapefile_names
			save `shapefile_names'
		restore 
		
		// Merge shapefile names
		di "Merging BPL_NAME with census-climate data ..."
		qui merge m:1 MERGE_SMALLEST using `shapefile_names', gen(name_merge) ///
			keepusing(BPL_NAME)
			
		// Report merge results
		qui count if name_merge == 1
		local unmatched_census_climate = r(N)
		qui count if name_merge == 2
		local unmatched_shapefile = r(N)
		qui count if name_merge == 3
		local matched_names = r(N)
		
		di "BPL_NAME merge results: "
		di "    - Matched: `matched_names'"
		di "    - Unmatched from census-climate data: `unmatched_census_climate'"
		di "    - Unmatched from shapefile: `unmatched_shapefile'"
		
		// Flag unmatched census-climate observations
		if `unmatched_census_climate' > 0 {
			di as text "WARNING: `unmatched_census_climate' observations from census-climate data without BPL_NAME"
			
			qui unique MERGE_SMALLEST if name_merge == 1
			local unmatched_subunits = r(unique)
			di as text "    Number of subunits wihout names: `unmatched_subunits'"
			
			di as text "    MERGE_SMALLEST values without BPL_NAME"
			levelsof MERGE_SMALLEST if name_merge == 1, local(unmatched_list)
			foreach val in `unmatched_list' {
				di as text "      `val'"
			}
		}
		
		// Drop unmatched shapefile records
		if `unmatched_shapefile' > 0 {
			di "Dropping `unmatched_shapefile' unmatched records from shapefile ..."
			drop if name_merge == 2
		}
		
		drop name_merge
		
		
		di "===================================================================="
		di "STEP 6: Wrapping up merge function ..."
		di "===================================================================="
		local match_rate = (`match_count' / `total_obs') * 100
		if `match_rate' < 90 {
			di as error "WARNING: Low match rate (`match_rate'%)"
			c_local low_match_warning = 1
		}
		else {
			c_local low_match_warning = 0
		}
		c_local match_rate = `match_rate'
		c_local matched_obs = `match_count'

		c_local merge_strategy_success = 1
	    c_local final_obs = `total_obs'
	    c_local final_matched = `match_count'
		di "Merging function completed"
	end


	/*
	Program: analyze_unmatched_smallest
	Purpose: diagnose reasons why some SMALLEST units are unmatched
	Usage:   analyze_unmatched_smallest, country_code("ZAF")
	Workflow: 
		- Keep the observations not matched from census
		- Load the shapefile and prepare it for a merger
		- Investigate whether the unmatched SMALLEST units were in the shapefile
		- Examine if the unmatched units were those that we dropped in dm-01a
	*/
	capture program drop analyze_unmatched_smallest
	program define analyze_unmatched_smallest
		syntax, country_code(string)
		local country_name "`r(name)'"
		
		di "We need the dropped geom report later. Generating it in the background"
		preserve 
			load_dropped_geom_report, country_code(`country_code')
			if !`geom_load_success' {
					c_local merge_strategy_success = 0
					exit
			}
			tempfile dropped_geom_report
			save `dropped_geom_report'
		restore
		
		di "We only want to work with the unmatched census observations at this point ..."
		preserve
			keep if final_merge == 1
			quietly count 
			if r(N) == 0 {
				di " No unmatched census observations for `country_name' `country_code'"
				restore 
				exit
			}
			
			di "Get unique SMALLEST values from unmatched census"
			keep MERGE_SMALLEST 
			duplicates drop 
			tempfile unmatched_smallest
			save `unmatched_smallest'
			
			di "Loading raw shapefile" 
			shp2dta using "$dir/data/source/shapefiles/ipums/bpl_data_smallest_unit_africa.shp", data("$dir/data/temp/shapefile_data.dta") coor("$dir/data/temp/shapefile_coord.dta") genid(id) replace 
			use "$dir/data/temp/shapefile_data.dta", clear
			
			di "Check and prepare SMALLEST variable ..."
			di " - Confirming SMALLEST exists."
			capture confirm variable SMALLEST 
			if _rc {
				di as error "SMALLEST variable not found in the shapefile."
				restore
				return 
			}
			
			di " - Ensure SMALLEST is same type as MERGE_SMALLEST "
			local vartype: type SMALLEST 
			di "Raw shapefile SMALLEST type: `vartype'"
			
			di " - Checking for missings."
			count if missing(SMALLEST)
			di " - Dropping `r(N)' observations with missing SMALLEST."
			capture drop if missing(SMALLEST)
			
			di " - Convert to same format as MERGE_SMALLEST "
			if substr("`vartype'", 1, 3) == "str" {
				di as text " SMALLEST is a string variable of type `vartype'"
				cap noisily destring SMALLEST, replace 
				if _rc!=0 {
					di as error " ERROR: Could not destring SMALLEST."
				}
				recast long SMALLEST
				format %15.0f SMALLEST 
				rename SMALLEST MERGE_SMALLEST
			}
			else {
				recast long SMALLEST 
				format %15.0f SMALLEST 
				rename SMALLEST MERGE_SMALLEST 
			}
				
			di " - Keep only unique SMALLEST values"
			keep MERGE_SMALLEST
			duplicates drop 
			tempfile raw_shapefile_smallest
			save `raw_shapefile_smallest'
			
			di " - Merging with unmatched SMALLEST values."
			use `unmatched_smallest', clear 
			quietly merge 1:1 MERGE_SMALLEST using `raw_shapefile_smallest', gen(raw_shapefile_merge)
			
			di "Count number of entries that are in the raw shapefile and those that aren't."
			quietly count if raw_shapefile_merge == 1
			local not_in_raw_shapefile = r(N)
			
			quietly count if raw_shapefile_merge == 3
			local in_raw_shapefile = r(N)
			
			di "Now we check which of those in raw shapefile were dropped during cleaning "
			if `in_raw_shapefile' > 0 {
				di "  - Restricting to perfect matches from the raw shapefile merge."
				keep if raw_shapefile_merge == 3
				di "  - Merging with the dropped geometries report"
				capture quietly merge 1:1 MERGE_SMALLEST using `dropped_geom_report', gen(cleaning_merge)
				
				quietly count if cleaning_merge == 3
				local dropped_during_cleaning = r(N)
				
				quietly count if cleaning_merge == 1
				local in_raw_but_not_dropped = r(N)
			}
			else {
				local dropped_during_cleaning = 0 
				local in_raw_but_not_dropped  = 0
			}
			
			// Store results in global macros
			global `country_code'_unmatched_total = `not_in_raw_shapefile' + `in_raw_shapefile'
			global `country_code'_not_in_raw = `not_in_raw_shapefile'
			global `country_code'_dropped_during_cleaning = `dropped_during_cleaning'
			global `country_code'_in_raw_not_dropped = `in_raw_but_not_dropped'
			
			di "Count of 'in raw not dropped': ${country_code}_in_raw_not_dropped"
			
			di "Unmatched SMALLEST analysis for `country_name': "
			di "    Total unmatched: ${`country_code'_unmatched_total}"
			di "    Not in raw shapefile: ${`country_code'_not_in_raw}"
			di "    Dropped during cleaning: ${`country_code'_dropped_during_cleaning}"
			di "    In raw but not dropped: ${`country_code'_in_raw_not_dropped}"
		restore
	end
 

/*------------------------------------ End of SECTION 1 - Building blocks ------------------------------*/



/**********************************************************************/
/*  SECTION 2:  Main workflow   			
    Notes: */
/**********************************************************************/
	* Main processing function
	capture program drop process_country
	program define process_country 
		syntax, country_code(string) [merge_var(string)]
		GetCountryName, code("`country_code'")
		local country_name "`r(name)'"
		di "Country name: `country_name'"
		if "`merge_var'" == "" local merge_var "BIRTH_SMALLEST"
		
		merge_datasets, country_code("`country_code'")
		if `merge_strategy_success' != 1 {
			di as error "Merge strategy failed for `country'"
			c_local country_success = 0 
			exit
		}
		
		// Store merge statistics for LaTeX table 
		quietly count if inlist(final_merge, 1, 3)
		local total_obs = r(N)
		quietly count if final_merge == 3
		local matched_count = r(N)
		quietly count if final_merge == 1
		local unmatched_census_count = r(N)
		quietly count if final_merge == 2
		local unmatched_weather_count = r(N)
		
		local matched_share: di %9.2fc (`matched_count' / `total_obs') * 100
		local unmatched_census_share: di %9.2fc (`unmatched_census_count' / `total_obs') * 100
		local unmatched_weather_share: di %9.2fc (`unmatched_weather_count' / `total_obs') * 100

		di "Dropping unmatched observations from the weather data."
		capture drop if final_merge == 2
		
		* Store these values in global macros for later table creation
		global `country_code'_matched_count "`matched_count'"
		global `country_code'_matched_share "`matched_share'"
		global `country_code'_unmatched_census_count "`unmatched_census_count'"
		global `country_code'_unmatched_census_share "`unmatched_census_share'"
		global `country_code'_unmatched_weather_count "`unmatched_weather_count'"
		global `country_code'_unmatched_weather_share "`unmatched_weather_share'"
		global `country_code'_total_obs "`total_obs'"
		
		// Analyse unmatched SMALLEST units
		analyze_unmatched_smallest, country_code("`country_code'")
		
		// Define output filenames based on merging variable
		di as text " Step 5: Saving final dataset ..."
		if "`merge_var'" == "BIRTH_SMALLEST" {
			local merge_name "by-birthplace"
		}
		else if "`merge_var'" == "RES_SMALLEST" {
			local merge_name "by-residence"
		}
		
		local output_file "$cf/data/derived/census-climate-merged/`country_name'/`country_name'-census-climate-merged-`merge_name'.dta"
		cap mkdir "$cf/data/derived/census-climate-merged/`country_name'/"
		
		capture save "`output_file'", replace
		if _rc {
			di as error "Could not save final dataset."
			c_local country_success = 0
			exit
		}
		di "    SUCCESS: Final dataset saved."
		di "       - File: `output_file'"
		di "       - Total observations: `final_obs'"
		di "       - Matched observations: `final_matched'"
		
		c_local country_success = 1
	    c_local country_final_obs = `final_obs'
	    c_local country_matched_obs = `final_matched'
	end

/*------------------------------------ End of SECTION 2 - Main workflow ------------------------------*/
