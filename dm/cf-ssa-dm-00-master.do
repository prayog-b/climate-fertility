********************************************************************************
* PROJECT:           Climate Change and Fertility
* DATE CREATED:      2025-06-09
* DATE MODIFIED:     2025-11-14
* DESCRIPTION:       Master do file listing all dm scripts
********************************************************************************

********************************************************************************
* 00. Setup
********************************************************************************
    * Set user globals
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

    * Load helper functions 
    do "$cf/dm/cf-ssa-dm-00b-helper-functions.do"

    * Create logs directory if it doesn't exist
    cap mkdir "$cf/dm/logs"
            
    * Set environment variables for Python if needed
    if "`c(os)'" == "Windows" {
        di as text "Setting Windows environment variables (requires admin privileges)..."
        shell setx CF_DIR "`base_dir'"
        shell setx CF_OVERLEAF "`overleaf_dir'"
        di as text "Note: Restart Stata for changes to take effect"
    }
    else {
        di as text "Setting Unix/Mac environment variables..."
        * Determine which shell configuration file to use
        capture confirm file "~/.zshrc"
        if _rc == 0 {
            local shell_config "~/.zshrc"
            local shell_name "zsh"
        }
        else {
            local shell_config "~/.bash_profile"
            local shell_name "bash"
        }
        
        di as text "Detected shell: `shell_name'"
        di as text "Config file: `shell_config'"
        
        * Create backup of shell config
        shell cp "`shell_config'" "`shell_config'.backup.`c(current_date)'" 2>/dev/null || true
        
        di "Remove old Climate Fertility environment variables if they exist"
        shell sed -i.tmp '/# Climate Fertility Project - Auto-generated/,/# End Climate Fertility/d' "`shell_config'" 2>/dev/null || true
        
        * Append new environment variables
        shell echo "" >> "`shell_config'"
        shell echo "# Climate Fertility Project - Auto-generated on `c(current_date)'" >> "`shell_config'"
        shell echo "export CF_DIR=\"`base_dir'\"" >> "`shell_config'"
        shell echo "export CF_OVERLEAF=\"`overleaf_dir'\"" >> "`shell_config'"
        shell echo "# End Climate Fertility" >> "`shell_config'"
        
        * Also set for current Stata session
        quietly {
            shell export CF_DIR="$cf"
            shell export CF_OVERLEAF="$cf_overleaf"
        }
        di as result "✓ Environment variables written to `shell_config'"
        di as text "To activate immediately, run:"
        di as input "  source `shell_config'"
        di as text "Or restart your terminal/Stata"
    }
        
    /*
    Python scripts can now access these variables using:
        import os
        base_dir = os.environ['CF_DIR']
    */
              
    * Check if Python 3 is installed
    capture shell python3 --version
    if _rc != 0 {
        di as error "Please install Python first:"
        di as error "  macOS:   brew install python"
        di as error "  Windows: https://www.python.org/downloads/"
        exit 601
    }
        
    * Get Python version
    shell python3 --version
    
    * Setting up Python virtual environment...
    capture confirm file "$cf/dm/cf-ssa-dm-00a-setup-python-env.sh"
    if _rc != 0 {
        di as error "Setup script not found: $cf/dm/cf-ssa-dm-00a-setup-python-env.sh"
        exit 601
    }
    * Make executable and run
    shell chmod +x "$cf/dm/cf-ssa-dm-00a-setup-python-env.sh"
    shell bash "$cf/dm/cf-ssa-dm-00a-setup-python-env.sh" "$cf/dm"
    
    * Check if environment was created
    capture confirm file "$cf/dm/.venv/bin/python3"
    if _rc != 0 {
        di as error "Python environment creation failed!"
        exit 601
    }



********************************************************************************
* 01. Download / Create raw datasets 
********************************************************************************

* 01a. Download raw temperature and precipitation data for Africa from 1940 to 2023
    /* Uses the Climate Data Store (CDS) API. 
        * This file requires the replicator to create an account with the CDS and get 
            API keys that they can use for downloads. 
        * To speed up downloads, we used four API keys and used parallel programming 
            to execute this program. The default script has been set at one API key 
            and the parallel processor to one task at a time. If you wish to speed 
            things up, you can add multiple API keys and add multiple parallel 
            downloads. */
        
    shell python "$cf/dm/cf-ssa-dm-01a-era-cdsapi-climate-data-download.py"
    if _rc != 0 {
        display as error "Error in cf-ssa-dm-01a-era-cdsapi-climate-data-download.py"
        exit 1
    }


* 01b. Download crop phenology data
    /* NOTES on dm-03a:
        - I download crop phenology data from the Google Cloud platform using gsutil: 
        - https://console.cloud.google.com/storage/browser/fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE;tab=objects?pli=1&prefix=&forceOnObjectsSortingFiltering=false&inv=1&invt=AbzoWg
        - Information on installing gsutil can be found here:  https://cloud.google.com/storage/docs/gsutil_install
        - The shell script can only be executed once gsutil is installed and you have completed the setup using your Google account. 
        - The downloaded files will be .tiff and .json files. 
        - The .tiff files contain the raster data that we will use for processing the subunit level phenology data.  */
    do "$cf/dm/cf-ssa-dm-01b-fao-download-crop-phenology-data.do"


********************************************************************************
* Section:     IPUMS Data Preparation
* Description: 
********************************************************************************

    ********************************************************************************
    * 02. Process IPUMS shapefile 
    ********************************************************************************

        * The raw shapefile data has some null geometries and data entry issues. This script cleans the shapefile data and creates a cleaned shapefile for use in the rest of the analysis.
        run_python "$cf/dm/cf-ssa-dm-02-clean-shapefile.py"
        if _rc {
            display as error "Error in cf-ssa-dm-02-clean-shapefile.py"
            exit 1
        }

    ********************************************************************************
    * 03. Process ERA temperature and precipitation data
    ********************************************************************************

    * 03a. Subset the Africa data to country-level datasets
        /* 
        - The data downloaded from the CDS is available in netCDF format and is available for all of Africa for each year from 1940 to 2019. 
        - Since we only look at a subset of countries, this script will extract data for each country from the Africa dataset and save the 
                precipitation and temperature data into separate files stored in .parquet format for ease of storage.
            * FIXME Prayog incomplete sentence
            * PB: Completed the sentence.
        - These country-level datasets are collapsed from the hourly level of the raw data to a daily level average and max temperature and total daily precipitation.
        - The outputs are saved as .parquet files. Parquet allows us to store these datasets more efficiently. */
        shell python "$cf/dm/cf-ssa-dm-03a-ipums-era-create-country-hour-parquet-data.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-03a-ipums-era-create-country-hour-parquet-data.py"
            exit 1
        }
        
    * 03b. Aggregate to IPUMS subunits
        /* 
        - The outputs from Step 2 are at the gridpoint-year level. 
        - We want to append these year-specific data into one big country-level dataset. 
        - We then want to aggregate the gridpoints that fall within an administrative subunit and create a subunit-day level dataset.
        - The resulting output datasets are .dta files for each country from 1940-2019. */
        shell python "$cf/dm/cf-ssa-dm-03b-ipums-era-create-subunit-day-data.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-03b-ipums-era-create-subunit-day-data.py"
            exit 1
            }
            
    * 03c. Label variables in subunit-day data 
        do "$cf/dm/cf-ssa-dm-03c-ipums-era-clean-subunit-day.do"

    ********************************************************************************
    * 04. Process FAO crop phenology data
    ********************************************************************************
        
    * 04. Process crop phenology raster files into a sub-unit level dataset
        /* The downloaded .tiff files are raster datasets at the global level. 
        - This script subsets the data for the countries of interest in Africa, 
            and takes the grid cell-based info and aggregates it to subunit-level. 
        - Output is a csv file producing summary statistics for phenology information 
            for all selected countries. */
        run_python "$cf/dm/cf-ssa-dm-04-ipums-fao-process-growing-season-data.py"

    ********************************************************************************
    * 05. Generate IPUMS weather variables
    ********************************************************************************
    * 05a. Generate cohort weather variables 
        do "$cf/dm/cf-ssa-dm-05a-ipums-create-country-censusyear-mapping.do"

    * 05b. Generate cohort-year pairs to identify how old a cohort will be in a given year
        do "$cf/dm/cf-ssa-dm-05b-ipums-generate-cohort-year-pairs.do"

    * 05c. Generate precipitation variables at the subunit-cohort level
        do "$cf/dm/cf-ssa-dm-05c-ipums-precip-genvar.do"

    * 05d. Generate temperature variables at the subunit-cohort level
        do "$cf/dm/cf-ssa-dm-05d-ipums-temp-genvar.do"
    
    ********************************************************************************
    * 06. Process census data
    ********************************************************************************
    * 06a. Clean census data (women) and generate variables required for analysis"
        do "$cf/dm/cf-ssa-dm-06a-ipums-women-genvar.do"

    * 06b. Clean census data (children) and generate variables required for analysis"
        do "$cf/dm/cf-ssa-dm-06b-ipums-children-genvar.do"

    ********************************************************************************
    * 07. Merge climate and census data
    ********************************************************************************
    * 07a. Merge IPUMS and climate datasets
    do "$cf/dm/cf-ssa-dm-07a-merge-census-climate.do"

    * 07b. Append country-specific merged datasets that have GEOLEV2-level data
    do "$cf/dm/cf-ssa-dm-07b-ipums-append-smallest-at-geolev2.do"

    * 07c. Append all countries' merged datasets
    do "$cf/dm/cf-ssa-dm-07c-ipums-append-all-countries.do"


********************************************************************************
* Section:     DHS Sample Creation
* Description: 
********************************************************************************
    ********************************************************************************
    * 08. Clean DHS Shapefile
    ********************************************************************************
        run_python "$cf/dm/cf-ssa-dm-08-dhs-clean-shapefile.py"
        if _rc {
            display as error "Error in cf-ssa-dm-08-dhs-clean-shapefile.py"
            exit 1
        }

    ********************************************************************************
    * 09. Process ERA temperature and precipitation data
    ********************************************************************************
    * 09a. Subset the Africa data to country-level datasets
        /* 
        - The data downloaded from the CDS is available in netCDF format and is available 
            for all of Africa for each year from 1940 to 2023. 
        - Since we only look at a subset of countries, this script will extract data for 
            each country from the Africa dataset and save the precipitation and temperature 
            data into separate files stored in .parquet format for ease of storage.
        - These country-level datasets are collapsed from the hourly level of the raw data 
            to a daily level average and max temperature and total daily precipitation.
        - The outputs are saved as .parquet files. Parquet allows us to store these datasets 
            more efficiently. */
        shell python "$cf/dm/cf-ssa-dm-09a-dhs-era-create-country-hour-parquet.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-09a-dhs-era-create-country-hour-parquet.py"
            exit 1
        }
        
    * 09b. Aggregate to DHS regions
        /* 
        - The outputs from Step 2 are at the gridpoint-year level. 
        - We want to append these year-specific data into one big country-level dataset. 
        - We then want to aggregate the gridpoints that fall within an administrative subunit and create a subunit-day level dataset.
        - The resulting output datasets are .dta files for each country from 1940-2019. */
        shell python "$cf/dm/cf-ssa-dm-09b-dhs-era-create-region-day.py"
        if _rc != 0 {
            display as error "Error in cf-ssa-dm-09b-dhs-era-create-region-day.py"
            exit 1
        }

    
    ********************************************************************************
    * 10. Process FAO Crop Phenology Data
    ********************************************************************************
    
        * 10. Process crop phenology raster files into a DHS region level dataset
        /* The downloaded .tiff files are raster datasets at the global level. 
        - This script subsets the data for the countries of interest in Africa, 
            and takes the grid cell-based info and aggregates it to subunit-level. 
        - Output is a csv file producing summary statistics for phenology information 
            for all 35 DHS countries. */
        run_python "$cf/dm/cf-ssa-dm-10-dhs-fao-process-growing-season-data.py"


    ********************************************************************************
    * 11. Generate DHS Weather Variables
    ********************************************************************************

    * 11a. Create a mapping of countries and their DHS survey years.
    do "$cf/dm/cf-ssa-dm-11a-dhs-country-surveyyear-mapping.do"

    * 11b. Create DHS cohort-year pairs
    do "$cf/dm/cf-ssa-dm-11b-dhs-cohort-year-pairs.do"

    * 11c. Create DHS precipitation variables
    do "$cf/dm/cf-ssa-dm-11c-dhs-precip-genvar.do"

    * 11d. Create DHS temperature variables
    do "$cf/dm/cf-ssa-dm-11d-dhs-temp-genvar.do"

    ********************************************************************************
    * 12. Merge 
    ********************************************************************************

    * 12. Merge ERA temperature and precipitation data with the DHS surveys
    do "$cf/dm/cf-ssa-dm-12-dhs-climate-merge.do"