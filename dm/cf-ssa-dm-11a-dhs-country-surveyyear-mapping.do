********************************************************************************
* PROJECT:               Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:                Prayog Bhattarai
* DATE CREATED:          January 14, 2026
* DATE MODIFIED:         January 14, 2026
* DESCRIPTION:           Create a mapping of country, DHS region, and years
* DEPENDENCIES:          data/source/dhs/surveycd_reg_year.dta
* 
* NOTES:                 We need a mapping of countries to survey years to 
*                        be able to identify the right survey years to examine.
*                        This script creates that mapping from an existing raw
*                        file                        
********************************************************************************

* Load the raw dataset containing survey codes, DHS regions, and survey years
    use "$cf/data/source/dhs/surveycd_reg_year.dta", clear
    /*
    NOTE: In this dataset, there are several instances where the same DHS region, 
        in the same year, will have multiple entries because the survey was 
        conducted in several months. As such, we collapse the data to keep only 
        one observation per year. 
    */
    bys cntry_name regname int_year: keep if _n == 1

    * Restricting the dataset to only the necessary variables
    drop surveycd int_month region 

    /* Our variables are now
    // ds
        obs:         6,004                          
        vars:             7                          26 Mar 2025 13:41
                                                    (_dta has notes)
        -----------------------------------------------------------------------------------------------------
                    storage   display    value
        variable name   type    format     label      variable label
        -----------------------------------------------------------------------------------------------------
        surveycd        str4    %9s                   country code and phase
        int_month       byte    %8.0g                 month of interview
        int_year        int     %8.0g                 year of interview
        region          byte    %8.0g                 Region (survey-specific code)
        cntry_name      str25   %25s                  country name
        regname         str30   %30s                  permanent region name
        DHS_code        int     %9.0g                 Unique Country-Region ID
        -----------------------------------------------------------------------------------------------------
        Sorted by: 
    */
    
    * Renaming for ease when merging in with the temperature and precipitation data
    rename int_year year
    rename cntry_name COUNTRY
    rename regname DHSREGEN

* Save the cleaned dataset
save "$cf/data/derived/dhs/00-dhs-country-surveyyear-mappings/surveycd_reg_year.dta", replace