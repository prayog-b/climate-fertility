# Merging climate and census data

This file provides a walkthrough of the code that merges the census microdata with precipitation and temperature datasets. 
Specifically, there are two merge steps:
**Step 1:** Merge precipitation and temperature datasets to create a unified weather data. 
**Step 2:** Merge that unified weather data with census microdata. 

Because the code executing the merger is extremely long, we separate the merge operation into two steps:
- `cf-ssa-dm-06a-merge-setup.do`
- `cf-ssa-dm-06b-merge-implementation.do`

## Inputs
- Precipitation data: `$cf/data/derived/era/`country_name'/`country_name'_allcensus-cohort-precip.dta`
- Temperature data: `$cf/data/derived/era/`country_name'/`country_name'_allcensus-cohort-temperature.dta`
- Cleaned census dataset: `$cf/data/derived/census/africa-women-genvar.dta`
- Report of geometries dropped during shapefile cleaning: `$cf/data/source/shapefiles/cleaned_ssa_boundaries_all_dropped_records.csv`
- Shapefile: `$cf/data/derived/shapefiles/cleaned_ssa_boundaries.shp`

## High-level overview of merge workflow
1. Load temperature and precipitation datasets.
2. Merge the weather datasets
3. Load the census dataset
4. If we dropped geometries from a country during the shapefile cleaning, load and merge the report of dropped geometries.
5. Merge the census dataset with the merged weather dataset.
6. If there are SMALLEST units in the census that didn't get matched with weather data, analyse why.
7. Generate LaTeX tables summarising the merge quality for documentation purposes.

## Setting up the merge `cf-ssa-dm-06a-merge-setup.do`
This script contains the building blocks of the merger. 

### Building blocks
#### Load weather data `load_weather_data`
  ```stata
    * Syntax
    load_weather_data, country_code(string) data_type(string)
  ```
This function takes in two inputs that define which dataset to load. The argument `country_code` identifies the country to use and the `data_type` argument identifies whether we should load the precipitation data or the temperature data. While loading the dataset, it also standardizes the variable names and formats to ensure a clean merge between the precipitation and temperature datasets. The function also checks whether required variables exist.


#### Load census data 
```stata
  load_census_data, country_code(string) [merge_var(string)]
```
This function takes in the country code and merging variable as inputs. It then loads the cleaned census dataset created in `cf-ssa-dm-05x`. Based on the country code specified, it restricts the census data to the country's observations. It then checks whether the merging variable (`BIRTH_SMALLEST` or `RES_SMALLEST`) exists in the dataset, standardizes its formatting to match that of the SMALLEST variable in the weather datasets. We also filter the census dataset at this step in two ways:

1. We drop people born before 1926, as they would not have weather data throughout their reproductively active span.
2. We drop people who do not have a SMALLEST variable value, because this prevents us from being able to identify the local weather patterns that they were exposed to.


#### Validating datasets
This helper function checks for the existence of specified required variables, and shows counts of the dataset grouped by a specified variable.
```stata
validate_dataset, required_vars(string) dataset_name(string) [count_by(string)]
```

#### Merging datasets 
  ```stata
  * Syntax
  merge_datasets, country_code(string) [merge_var(string)]
  ```
**1: Merge precipitation and temperature datasets**
In this step, we load the precipitation and temperature datasets.
  ```stata
    load_weather_data, country_code("`country_code'") data_type("precip")
        tempfile precip_data
    		save `precip_data'
    load_weather_data, country_code("`country_code'") data_type("temperature")
    merge 1:1 COHORT MERGE_SMALLEST CENSUS using `precip_data', gen(weather_merge)
  ```

**2. Load census data**
We then load the census dataset:
  ```stata
  load_census_data, country_code("`country_code'") merge_var("`merge_var'")
  ```

**_Intermediary step: Check whether the country had any dropped geometries_**
  ```stata
    load_dropped_geom_report, country_code("`country_code'")
    tempfile dropped_geom_report
		save `dropped_geom_report'
    * The load_dropped_geom_report function creates a local called `has_dropped' which indicates whether the country had any dropped null geometries
    if `has_dropped' == 1 {
      merge m:1 MERGE_SMALLEST using `dropped_geom_report', gen(dropped_geom_merge)
    }
  ```
  If the country being processed had any null geometries in the raw shapefile that got dropped, we will merge this information with the merged dataset to check whether any unmatched observations in the merged dataset can be explained by us dropping those units because they had null geometries. 

**3: Merge census data with joint weather data**
  ```stata
  merge m:1 COHORT MERGE_SMALLEST CENSUS using `weather_data', gen(final_merge) keepusing(*) update replace
  ```
Note that because some units in Sudan have SMALLEST codes from South Sudan, we have a sub-workflow to merge in South Sudan weather data when we deal with data. 
```stata
if "`country_code'" == "SDN" {
			preserve
				load_weather_data, country_code("SSD") data_type("precip")
				tempfile precip_data_ssd
				save `precip_data_ssd'
				
				load_weather_data, country_code("SSD") data_type("temperature")
				merge 1:1 COHORT MERGE_SMALLEST CENSUS using `precip_data_ssd', gen(weather_merge)
				tempfile weather_data_ssd
				save `weather_data_ssd'
			restore
			merge m:1 COHORT MERGE_SMALLEST CENSUS using `weather_data_ssd', gen(final_merge_ssd) update
		}
		qui count 
		local total_obs = r(N)
  }
```

**4.: Merging in birthplace names**
To ensure that the final merged dataset has information on birthplace names, we also merge in birthplace name data that 
  ```stata
    load_shapefile_names, country_code("`country_code'")
    tempfile shapefile_names
    save `shapefile_names'
  
    merge m:1 MERGE_SMALLEST using `shapefile_names', gen(name_merge)	keepusing(BPL_NAME)
  ```
#### Analysing why some SMALLEST units don't have matching weather data
For countries where some SMALLEST units don't have matching weather data, we run a diagnostic check to understand whether it can be fully explained by the fact that we dropped null geometries in the raw shapefile data.
```stata
  * Syntax
  analyze_unmatched_smallest, country_code("`country_code'")
```
This process involves first restricting the data to observations not matched from the census
  ```stata
    keep if final_merge == 1
  ```
Then we load the raw shapefile data, which contains the full list of SMALLEST units that were considered for constructing the weather data. We then merge these datasets together to understand:

- Among SMALLEST units in the census that weren't matched to weather data, how many were dropped during the cleaning (we get this from the dropped geometries report that we had merged in earlier.
- How many unmatched SMALLEST units were never in the raw shapefile to begin with
- How many unmatched SMALLEST units were in the raw shapefile and not dropped, suggesting some other reason why the units may not have been matched to weather data. 

#### Processing a country
```stata

```

## Implementing the merge `cf-ssa-dm-06b-merge-implementation.do`
### Implementation workflow
This script brings in the building blocks from `cf-ssa-dm-06a` and simply creates a workflow to implement the merge for each country that we are studying. 
  ```stata
  local country_codes "BEN BWA BFA CMR ETH GHA GIN IVC KEN LSO LBR MWI MLI MOZ RWA SLE ZAF SEN SDN TZA TGO UGA ZMB ZWE"
  foreach country_code in `country_codes' {
  	    * Get country name for display
  	    GetCountryName, code("`country_code'")
  	    local country_name "`r(name)'"
  		  local merge_var "BIRTH_SMALLEST"
  	    
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
  ```

### Summarizing merge quality 
We create two summary tables to document the quality of the merge between the census and climate datasets.
```
Overall merge results: $cf_overleaf/table/merge-results-table-`merge_var'.tex
Breakdown of unmatched SMALLEST units: $cf_overleaf/table/merge-unmatched-smallest-analysis-`merge_var'.tex
```





