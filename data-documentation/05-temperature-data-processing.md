# Temperature data processing
`cf-ssa-dm-04d-generate-temperature-variables.do`
This Stata script uses the subunit-day dataset created using ERA5 data processing to generate a SMALLEST-cohort-census level dataset with degree day variables. 

## Inputs



## Core Functions
### A. Calculate Degree days `calculate_degree_days`
This Stata program calculates degree days (cumulative temperature exposure above specified thresholds) from temperature data. Degree days are commonly used in climate research to measure thermal stress or growing conditions, representing the accumulated temperature above a baseline threshold.
#### Formula
```stata
degree_days = max(temperature - threshold, 0)
```
**Interpretation:**
- If temperature > threshold → `degree_days = temperature - threshold`
- If temperature ≤ threshold → `degree_days = 0`

_Examples_
| Temperature | Threshold | Degree Days | Explanation |
|-------------|-----------|-------------|-------------|
| 32°C | 25°C | 7 | Temperature exceeds threshold by 7°C |
| 28°C | 30°C | 0 | Temperature below threshold |
| 30°C | 30°C | 0 | Temperature equals threshold |
| 22°C | 20°C | 2 | Temperature exceeds threshold by 2°C |

### B. Generate age-specific temperature exposure variables `generate_age_range_exposure()`
Creates age-specific variants of temperature degree day variables.

_Process_

Calculate age from cohort and year: `age = year - cohort`
Create age group indicators:
 - `age_15_29`: Binary indicator for ages 15-29
 - `age_30_44`: Binary indicator for ages 30-44
For each degree day variable (`temp_*_dd_*`):
  - Generate `[var]_age_15_29 = [var] * age_15_29`
  - Generate `[var]_age_30_44 = [var] * age_30_44`


### C. `TemperatureDataProcessor`
_Process_
**1. Load Daily Temperature Data**

Load: `$era_derived/00-subunit-day-data/[country]-era-subunit-day-1940-2019.dta`
Required variables: `SMALLEST`, `temp_max`, `temp_mean`, `day`, `month`, `year`

**2. Handle Special Cases**

Remove invalid regions for BWA and SDN (SMALLEST codes 728*, 729*)
Convert SMALLEST to numeric format

**3. Merge Crop Phenology Data**

Load: `$cf/data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv`
Filter to country
- Keep: `SMALLEST`, growing_month_1 through growing_month_12
- Merge with temperature data

**4. Create Season Indicators**

statagen gs = 1 if growing_month_[m] == 1 & month == m
gen ngs = 1 if growing_month_[m] == 0 & month == m

**5. Calculate Degree Days**

Call `calculate_degree_days, lowerbound(22) upperbound(28) intervals(1)`

**6. Create Growing Season Variants**

For each degree day variable:
```stata
[var]_gs = [var] * gs (growing season only)
[var]_ngs = [var] * ngs (non-growing season only)
```

**7. Aggregate to Annual Level**

```stata
collapse (sum) temp_*_dd_*, by(SMALLEST year)
```
**8. Save**

Output: `$era_derived/[country]/[country]_temperature_annual.dta`

**NOTE: The functions for calculating cohort metrics and census selection are identical to the precipitation data workflow. Please see details in file `04-precipitation-data-processing**

### D. `TemperatureAnalysisWorkflow`
#### Method: `run_full_analysis`
Complete pipeline for single country:

**Step 1: Detect Census Years**

Load IPUMS mapping: $cf/data/derived/ipums/country-census-years-mapping.dta
Filter to country
Extract census years

**Step 2: Load and Aggregate**

```stata
TemperatureDataProcessor, method("load_and_aggregate") country_code("[code]")
```

**Step 3: Process Each Census Year**
For each census year:

Load cohort-year combinations
- Filter: year <= census_year and cohort <= census_year - 15
- Join with temperature data
- Calculate cohort metrics
- Save: `[code]_metrics_[year].dta`

**Step 4: Combine Census Years**
```stata
TemperatureAnalysisWorkflow, method("combine_and_select_census") ...
```

**Step 5: Cleanup (Optional)**
```stata
TemperatureAnalysisWorkflow, method("cleanup_temp_files") ...
```

#### Method: combine_and_select_census
**_Process:_**

1. Load first census metrics file
2. Append remaining census years
3. Drop missing observations
4. Sort by SMALLEST, cohort, census
5. Apply census selection (call CensusSelector)
6. Save: `$era_derived/[country]/[country]_allcensus-cohort-temperature.dta`

#### Method: cleanup_temp_files
_**Process:**_
- Delete individual census metric files
- Delete intermediate annual temperature file

### Multi-Country Processing
The main execution section processes multiple countries:
  ```stata
  foreach country_code in [list] {
      TemperatureAnalysisWorkflow, ///
          method("run_full_analysis") ///
          country_code("`country_code'") ///
          target_age(45) ///
          cleanup("true")
  }
  ```
