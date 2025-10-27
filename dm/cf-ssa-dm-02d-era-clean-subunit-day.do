********************************************************************************
* PROJECT:           Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:            Prayog
* DATE CREATED:      June 6, 2025
* DATE MODIFIED:     October 09, 2025
* DESCRIPTION:       Quick cleaning of subunit-day data
* DEPENDENCIES:      Input files are in the folder data/derived/era/00-subunit-day-data
* 
* NOTES: 
********************************************************************************

local datapath "$cf/data/derived/era/00-subunit-day-data"

local files: dir "`datapath'" files "*.dta"
di `files'

foreach file of local files {
    di "`file'"
    use "`datapath'/`file'", clear
    cap drop date decade
    di "Labelling variables ..."
    label var SMALLEST "Matching variable between weather and demographic data"
    label var precip   "Precipitation (mm) (daily measure derived from hourly data)"
    label var temp_mean "Mean temperature, Celsius (daily measure derived from hourly data)"
    label var temp_max  "Max temperature, Celsius (daily measure derived from hourly data)"
    label var year     "Year"
    label var month    "Month"
    label var day      "Day"
    di "Saving dataset ..."
    save  "`datapath'/`file'", replace
    di "Dataset saved to `datapath'/`file' successfully. Moving to next file"

    save "`file'", replace
    clear
}
