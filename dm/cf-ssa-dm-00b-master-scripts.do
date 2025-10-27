********************************************************************************
* PROJECT:           Climate Change and Fertility
* AUTHOR:            Prayog Bhattarai
* DATE CREATED:      2025-06-09
* DATE MODIFIED:     2025-06-09
* DESCRIPTION:       Master do file listing all dm and sa scripts
* DEPENDENCIES: 
* 
* NOTES: 
********************************************************************************


********************************************************************************
* Section:       Setup
* Description:   Set user-dependent globals
********************************************************************************

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

    ClimateAnalysisConfig, method("init")

    * Create a code logs directory if it doesn't exist
    cap mkdir "$cf/dm/logs"

    * Install required packages
	foreach package in carryforward rsource {
		 capture which `package'
		 if _rc==111 ssc install `package'  
	}

	* program to set up timestamp of analysis
    cap program drop _print_timestamp
	program define _print_timestamp 
		di "{hline `=min(79, c(linesize))'}"

		di "Date and time: $S_DATE $S_TIME"
		di "Stata version: `c(stata_version)'"
		di "Updated as of: `c(born_date)'"
		di "Variant:       `=cond( c(MP),"MP",cond(c(SE),"SE",c(flavor)) )'"
		di "Processors:    `c(processors)'"
		di "OS:            `c(os)' `c(osdtl)'"
		di "Machine type:  `c(machine_type)'"
		local hostname : env HOSTNAME
		if !mi("`hostname'") di "Hostname:      `hostname'"
		
		di "{hline `=min(79, c(linesize))'}"
	end

	* auxiliary function saving input in file called 'input.tex'
    cap program drop save_input
	program define save_input 
		/*
		description:	saves a number/statistic as a .tex file for use in overleaf
		usage:			save_input `number' "filepath"
		example:		save_input `coef' "number_of_cities"
		*/
		args number filename
		file open newfile using "$cf_overleaf/outnum/worker-survey/`filename'.tex", write replace
		file write newfile "`number'%"
		file close newfile
	end

	* auxiliary function to install any uninstalled ssc packages before using them
    cap program drop verify_package
	program verify_package
		/*
		description: 	checks whether a package is installed. if not, installs it from ssc or net.
		usage:			verify_package `package'
		example:		verify_package reghdfe
		*/
	    args package
	    capture which `package'
	    if _rc ssc install `package'
		if !_rc net install `package'.pkg
	    if !_rc di "Package `package' already installed in the system. Code can proceed."
	end

    * double check packages
	cap verify_package require
	foreach package in  rsource {
		require `package'
	}


********************************************************************************
* Section:      Data management (dm)
* Description:  
********************************************************************************

   ********************************************************************************
   * Cleaning shapefile data
   ********************************************************************************
        
        display "Cleaning shapefile data ..."
        shell python "dm/cf-ssa-01a-clean-shapefile.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-01a-clean-shapefile.py"
            exit 1
        }
        /* NOTES on cf-ssa-01a: The raw shapefile data has some null geometries and data entry issues. 
        This script cleans the shapefile data and creates a cleaned shapefile for use in the rest of the analysis. */

   ********************************************************************************
   * ERA temperature and precipitation data
   ********************************************************************************
        display "Starting ERA climate data processing pipeline..."

        display "Downloading CDS API data (Python) ... "
        shell python "dm/cf-ssa-02a-era-cdsapi-climate-data-download.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-02a-era-cdsapi-climate-data-download.py"
            exit 1
        }
        /* NOTES on cf-ssa-02a:
            - We use the Climate Data Store (CDS) API to download the raw temperature and precipitation data for Africa from 1940 to 2019. 
            - This file requires the replicator to create an account with the CDS and get API keys that they can use for downloads.
            - To speed up downloads, we used four API keys and used parallel programming to execute this program. 
            - The default script has been set at one API key and the parallel processor to one task at a time. 
            - If you wish to speed things up, you can add multiple API keys and add multiple parallel downloads. */


        
        display "Creating country-level data at the daily level (Python) ... "
        shell python "dm/cf-ssa-02b-era-create-country-year-parquet-data.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-02b-era-create-country-year-parquet-data.py"
            exit 1
        }
        /* NOTES on cf-ssa-02b: 
            - The data downloaded from the CDS is available in netCDF format and is available for all of Africa for each year from 1940 to 2019. 
            - Since we only look at a subset of countries, this script subsets the Africa data to country-level datasets. 
            - These country-level datasets are collapsed from the hourly level of the raw data to a daily level average and max temperature and total daily precipitation.
            - The outputs are saved as .parquet files. Parquet allows us to store these datasets more efficiently. */

        
        display "Creating subunit-day level data (Python) ..."
        shell python "dm/cf-ssa-era-01c-create-subunit-day-data.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-era-02c-create-subunit-day-data.py"
            exit 1
        }
        /* NOTES on cf-ssa-02c:
            - The outputs from Step 2 are at the gridpoint-year level. 
            - We want to append these year-specific data into one big country-level dataset. 
            - We then want to aggregate the gridpoints that fall within an administrative subunit and create a subunit-day level dataset.
            - The resulting output datasets are .dta files for each country from 1940-2019. */

        display "ERA data processing pipeline completed ..."

        di "Step 4: Quick cleaning of subunit-day data (Stata)"
        do "dm/cf-ssa-dm-02d-era-clean-subunit-day.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-02d-era-clean-subunit-day.do"
            exit 1
        }
        /* NOTES on cf-ssa-dm-02d:
            - This script labels variables in the subunit-day data. */

   ********************************************************************************
   * Subsection: FAO crop phenology data
   ********************************************************************************
        
        display "Starting FAO crop phenology data processing pipeline..."
        display "Step 1: Downloading crop phenology data."
        do "dm/cf-ssa-03a-fao-download-crop-phenology-data.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-03a-fao-download-crop-phenology-data.do"
            exit 1
        }
        /* NOTES on cf-ssa-03a:
            - I download crop phenology data from the Google Cloud platform using gsutil: 
            - https://console.cloud.google.com/storage/browser/fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE;tab=objects?pli=1&prefix=&forceOnObjectsSortingFiltering=false&inv=1&invt=AbzoWg
            - Information on installing gsutil can be found here:  https://cloud.google.com/storage/docs/gsutil_install
            - The shell script can only be executed once gsutil is installed and you have completed the setup using your Google account. 
            - The downloaded files will be .tiff and .json files. The .tiff files contain the raster data that we will use for processing the subunit level phenology data.  */


        display "Processing crop phenology raster files into a sub-unit level dataset"
        shell python "dm/cf-ssa-03b-crop-phenology-processing.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-03b-fao-crop-phenology-processing.py"
            exit 1
        }
        /* NOTES on cf-ssa-03b: 
            - The downloaded .tiff files are raster datasets at the global level. 
            - This script subsets the data for the countries of interest in Africa, and takes the grid cell-based info and aggregates it to subunit-level. 
            - The main output is a csv file producing summary statistics for phenology information for all selected countries. */


   ********************************************************************************
   * Generating cohort weather variables
   ********************************************************************************
        /* Step 1: Generate cohort weather variables */
        display " Step 1: Generating cohort weather variables and merging with DHS data"
        do "dm/cf-ssa-dm-04a-create-country-censusyear-mapping.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-04a-create-country-censusyear-mapping.do"
            exit 1
        }
        /* NOTES on cf-ssa-dm-04a:
            - This script uses the census dataset to create a mapping of countries and census years. 
            - The output is a .dta file with country and census year information. */

        
        display " Step 2: Generating cohort-year pairs"
        do "dm/cf-ssa-dm-04b-generate-cohort-year-pairs.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-04b-generate-cohort-year-pairs.do"
            exit 1
        }
        /* NOTES on cf-ssa-dm-04b: 
            - In this script, we create a mapping of cohorts and years to identify how old a cohort will be in a given year.
            - The output is a .dta file with cohort-year pairs. */

        /* Step 3: Generate precipitation variables */
        display "Generating subunit-cohort level precipitation variables"
        do "dm/cf-ssa-dm-04c-generate-precipitation-variables.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-04c-generate-precipitation-variables.do"
            exit 1
        }
        /* NOTES on cf-ssa-dm-04c:
            - This script generates precipitation variables at the subunit-cohort level. 
            - The output is a .dta file with subunit-cohort level precipitation variables.
            - We generate variables capturing the share of years in a woman's reproductive 
                period with abnormally dry or wet conditions. */

        /* Step 4: Generate temperature variables */
        display "Generating subunit-cohort level temperature variables"
        do "dm/cf-ssa-dm-04d-generate-temperature-variables.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-04d-generate-temperature-variables.do"
            exit 1
        }
        /*  NOTES on cf-ssa-dm-04d: 
            - This script generates temperature variables at the subunit-cohort level. 
            - The output is a .dta file with subunit-cohort level temperature variables.
            - We generate variables capturing the number of degree days above a range of 
                temperature thresholds that a woman is exposed to during her reproductive period. */


   ********************************************************************************
   * Cleaning census data
   ********************************************************************************
        
        display "Cleaning census data (women)"
        do "dm/cf-ssa-dm-05a-census-women-genvar.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-05a-census-women-genvar.do"
            exit 1
        }
        /*
        NOTES on cf-ssa-dm-05a:
            - This script cleans the women's census data and generates variables required for analysis. 
            - The output is a cleaned .dta file with all required variables for analysis.
            - Output path: "$dir/data/derived/census/africa-women-genvar.dta" */

        
        display "Cleaning census data (children)"
        do "dm/cf-ssa-dm-05b-census-children-genvar.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-05b-census-children-genvar.do"
            exit 1
        }
        /* NOTES on cf-ssa-dm-05b: 
            - This script cleans the children's census data and generates variables required for analysis. 
            - The output is a cleaned .dta file with all required variables for analysis.
            - Output path: "$dir/data/derived/census/africa-children-genvar.dta" */

   ********************************************************************************
   * Merging climate and census data
   ********************************************************************************
        
        // cf-ssa-dm-06a-merge-setup.do sets up the merging environment and is called from cf-ssa-dm-06b
        display "Merging climate and census data"
        do "dm/cf-ssa-dm-06b-merge-implementation.do"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-06b-merge-implementation.do"
            exit 1
        }


********************************************************************************
* Section:        Statistical analysis (sa)
* Description: 
********************************************************************************

   ********************************************************************************
   * Subsection:  Descriptive statistics
   ********************************************************************************





   ********************************************************************************
   * Subsection: Exploratory Regressions
   ********************************************************************************