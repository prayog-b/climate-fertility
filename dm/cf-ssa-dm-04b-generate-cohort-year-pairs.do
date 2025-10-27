********************************************************************************
* PROJECT:          Climate Change and Fertility in SSA
* AUTHOR:           Prayog Bhattarai
* DATE CREATED:     July 31, 2025
* DATE MODIFIED:    July 31, 2025
* DESCRIPTION:      Creates a comprehensive dataset of birth cohort and calendar year combinations
                    * for individuals aged 15-44. This forms the foundation for linking temperature
                    * exposure data to specific cohorts during their reproductive years.
* DEPENDENCIES:     - Global macro $cf needs to be defined from the master script
* INPUTS:           None (generates data internally)
* NOTES:            
********************************************************************************


/*
================================================================================
Function: generate_cohort_year_pairs
================================================================================
Parameters:
    None (uses hardcoded ranges)

Input Requirements:
    - Global macro $cf must be defined (directory path)
    - Write access to $cf directory

Output:
    - Creates file: $cf/data/derived/era/cohort_year.dta
    - Variables: cohort (birth year), year (calendar year), age (age in that year)
    - Only includes observations where age is between 15-44

Data Structure:
    - Cohorts: 1926-2005 (80 birth cohorts)
    - Years: 1940-2019 (80 calendar years)
    - Full cross-product creates 6,400 cohort-year combinations
    - Filtered to reproductive ages (15-44) reduces to ~2,480 observations

Algorithm:
    1. Check if output file already exists (skip if present)
    2. Create cohort dataset (1926-2005)
    3. Create year dataset (1940-2019)
    4. Cross-join cohorts with years using key variable
    5. Calculate age = year - cohort
    6. Keep only ages 15-44
    7. Save final dataset
================================================================================
*/

cap program drop generate_cohort_year_pairs 
program define generate_cohort_year_pairs
    * Check if cohort-year pairs already exist
    capture confirm file "$cf/data/derived/cohort_year.dta"
    // if _rc == 0 {
    //     di "Cohort-year pairs already exist, skipping generation"
    //     exit
    // }
    di "Generating cohort-year pairs ..."

    * Generate cohorts
    clear
    set obs 121
    gen cohort = 1926 
    forvalues i = 2/81 {
        replace cohort = 1926 + `i' - 1 in `i'
    }

    gen key = 1
    tempfile cohort
    save `cohort'

    * generate years
    clear
    set obs 81
    gen year = 1940
    forvalues i = 2/81 {
        replace year = 1940 + `i' - 1 in `i'
    }
    gen key = 1

    * create cohort-year combinations
    joinby key using `cohort'
    sort cohort year
    drop key

    * generate age (how old a cohort is in a given year) 
    gen age = year - cohort

    * keep cohorts where age is between 15 and 44
    keep if age >= 15 & age <= 44
    save "$cf/data/derived/cohort_year.dta", replace
    di "Cohort-year pairs generated and saved"
end

* Implementation
generate_cohort_year_pairs


cap program drop generate_cohort_year_pairs
clear