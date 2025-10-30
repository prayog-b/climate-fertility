# Processing weather variables

## Preliminary scripts
### `cf-ssa-dm-04a-create-country-censusyear-mapping.do`
#### Overview
This Stata script creates a lookup table of country-census year combinations from the IPUMS Africa women's census dataset. It extracts unique country identifiers and census years, providing both numeric country codes and string country names.

#### Input
**Source Data:** `$cf/data/source/census/africa_women.dta`
- Original IPUMS Africa census microdata for women

#### Processing Steps
1. **Country Variable Processing:**
   - Creates `CNTRY_CD`: numeric country code (from original COUNTRY variable)
   - Converts COUNTRY to string with decoded country names
   - Standardizes country name: "Cote d'Ivoire" → "Ivory Coast"

2. **Variable Preparation:**
   - Renames `YEAR` to `census` for clarity
   - Keeps only: COUNTRY, CNTRY_CD, census

3. **Data Cleaning:**
   - Removes observations with missing values
   - Eliminates duplicate country-census year combinations
   - Sorts by country and census year

4. **Summary Statistics:**
   - Displays count of unique country-census combinations
   - Shows cross-tabulation of countries and census years

#### Output
**File:** `$cf/data/derived/ipums/country-census-years-mapping.dta`
**Variables:**
- `COUNTRY` (string): Country name
- `CNTRY_CD` (numeric): Country code
- `census` (numeric): Census year
**Structure:** One observation per unique country-census year combination

#### Purpose
Creates a clean reference table for linking census years to countries in subsequent analysis, enabling:
- Verification of available census data by country
- Merging census information into other datasets
- Tracking temporal coverage of the census data


### Generating cohort pairs `cf-ssa-dm-04b-generate-cohort-year-pairs.do`
This Stata program creates a comprehensive dataset of birth cohort and calendar year combinations for individuals aged 15-44 (reproductive ages). This forms the foundation for linking climate exposure data to specific cohorts during their reproductive years.
#### Input
**Requirements:**
- Global macro `$cf` must be defined (project directory path)
- No input data files needed (generates data internally)

#### Processing Logic

##### 1. Generate Birth Cohorts
```stata
    set obs 121
    gen cohort = 1926 
    forvalues i = 2/81 {
        replace cohort = 1926 + `i' - 1 in `i'
    }
```

##### 2. Generate Calendar Years
```stata
    set obs 81
    gen year = 1940
    forvalues i = 2/81 {
        replace year = 1940 + `i' - 1 in `i'
    }
```

##### 3. Cross-Join
- Creates all possible cohort-year combinations using `joinby` with key variable
  ```stata
      joinby key using `cohort'
  ``` 

##### 4. Calculate Age
  ```stata
  gen age = year - cohort
  ```

##### 5. Filter to Reproductive Ages
  ```stata
  keep if age >= 15 & age <= 44
  ```
- Retains only cohort-year pairs where individuals are 15-44 years old

#### Output
**File:** `$cf/data/derived/cohort_year.dta`
**Variables:**
- `cohort` (numeric): Birth year 
- `year` (numeric): Calendar year 
- `age` (numeric): Age in that year 
**Structure:** One observation per cohort-year-age combination where age is 15-44


## Precipitation Data Processing `cf-ssa-dm-04c-generate-precipitation-variables.do`
This Stata script uses the subunit-day dataset created during ERA5 data processing to generate a SMALLEST-cohort-census level precipitation dataset. 
### Inputs
- `data/derived/era/00-subunit-day-data/'country_name'-era-subunit-day-1940-2019.dta`
- `data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv`
- `data/derived/ipums/country-census-years-mapping.dta`
- `data/derived/cohort_year.dta`

### Major Components
#### `GammaDistributionFitter`
This Stata program fits gamma distributions to precipitation variables at the subunit (SMALLEST) geographic level and generates standardized precipitation indices (SPI) and abnormal precipitation indicators. 
##### Processing Steps
**1. Variable Preparation**
  ```stata
  gen alpha = .      // Shape parameter
  gen beta = .       // Scale parameter
  gen P = .          // Cumulative probability
  gen spi = .        // Standardized Precipitation Index
  ```
**2. Geographic Loop**
For each SMALLEST unit:
- Fits gamma distribution using gammafit command
- Extracts shape (α) and scale (β) parameters
- Stores parameters for that subunit

**3. SPI Calculation**
For each observation in each subunit:
```stata
P = gammap(α, precip/β)     // Cumulative probability
spi = invnormal(P)          // Convert to standard normal
```
_**Interpretation:**_
```
P = 0.50 → spi = 0.00 (median precipitation)
P = 0.85 → spi ≈ +1.04 (85th percentile, wet)
P = 0.15 → spi ≈ -1.04 (15th percentile, dry)
```

**4. Abnormal Precipitation Classification**
Creates binary indicators for each variable:
| Indicator | Condition | Interpretation |
|-----------|-----------|----------------|
| `abn_[var]_wet` | P ≥ 0.85 | Abnormally wet (top 15%) |
| `abn_[var]_dry` | P ≤ 0.15 | Abnormally dry (bottom 15%) |
| `abn_[var]` | P ≥ 0.85 OR P ≤ 0.15 | Any abnormal condition |

**5. Parameter Storage**
Saves SMALLEST-level α and β parameters to temporary files
Calculates country-level summary statistics (mean and SD of α and β)
Stores in global macros for later use

##### Output Variables
For Each Input Variable (e.g., precip):
| Variable | Type | Description |
|----------|------|-------------|
| `[var]_spi` | continuous | Standardized Precipitation Index (-3 to +3 typically) |
| `abn_[var]_wet` | binary | 1 if abnormally wet (P ≥ 0.85) |
| `abn_[var]_dry` | binary | 1 if abnormally dry (P ≤ 0.15) |
| `abn_[var]` | binary | 1 if any abnormal condition |

**Intermediate Variables:**
- `alpha`: Shape parameter of gamma distribution
- `beta`: Scale parameter of gamma distribution
- `P`: Cumulative probability (0-1)
- `spi`: Standardized index value

**Global Macros:**
```
$alpha_mean: Mean shape parameter across subunits
$alpha_sd: Standard deviation of shape parameter
$beta_mean: Mean scale parameter across subunits
$beta_sd: Standard deviation of scale parameter
```

##### Why Gamma Distribution?

Precipitation data is typically:
- **Right-skewed:** Few extreme wet events, many moderate events
- **Bounded at zero:** Cannot have negative precipitation
- **Continuous:** Can take any positive value

The gamma distribution is ideal for modeling such data.

###### Parameters:
- **α (alpha)**: Shape parameter - controls distribution skewness
- **β (beta)**: Scale parameter - controls distribution spread

###### Probability Calculation:
```
P(X ≤ x) = Γ(α, x/β) / Γ(α)
```
Where Γ is the gamma function.

#### Cohort-level precipitation calculation `CohortMetricsCalculator`
This Stata program calculates cohort-level precipitation metrics for a specific SMALLEST unit and census year, aggregating climate exposure data across the reproductive lifespan (ages 15-44). It creates comprehensive precipitation statistics by cohort, administrative unit, and age group for climate-fertility analysis.
It transforms SMALLEST-year precipitation data into cohort-level exposure metrics by:
- Aggregating precipitation across reproductive years 
- Creating age-specific exposure measures (15-29 vs 30-44)
- Calculating cumulative abnormal precipitation events
- Computing average standardized precipitation indices (SPI)
- Producing shares of years exposed to extreme precipitation

##### Processing steps
**1. Age group creation**
  ```stata
  gen age_15_29 = inrange(age, 15, 29)
  gen age_30_44 = inrange(age, 30, 44)
  ```

**2. Gamma Fit Merge**
  ```stata
  merge m:1 SMALLEST year using "$era_derived/[country]/[code]_gammafit.dta"
  ```

**3. Age-Specific Variable Generation**
We create age-specific versions of the precipitation variables as follows:
  ```stata
  gen [var]_15_29 = [var] * age_15_29
  gen [var]_30_44 = [var] * age_30_44
  ```

**4. Cohort-Level Collapse**
We collapse these variables to the SMALLEST-cohort level. 
  ```stata
    collapse ///
        (sum) precip_total = precip ///
              tot_abn_precip_dry = abn_precip_dry ///
              ... ///
        (mean) avg_precip_spi = precip_spi ///
               ... ///
        , by(SMALLEST cohort)
  ```

**5. Share Calculations**
We then calculate the share of years lived in an abnormal precipitation year. 
  ```stata
  gen share_abn_precip_dry = tot_abn_precip_dry / reproductive_span
  ```
##### Output structure
**File Location:** `$era_derived/[country_name]/[country_code]_metrics_[census_year].dta`

**Key Variables (78 total):**

_**1. Identifiers**_
- `SMALLEST`: Administrative unit
- `cohort`: Birth year
- `census`: Census year (added as constant)

_**2. Cumulative Totals (prefix: `tot_`)**_
| Variable | Description |
|----------|-------------|
| `precip_total` | Total precipitation (mm) across reproductive span |
| `precip_gs_total` | Total growing season precipitation |
| `precip_ngs_total` | Total non-growing season precipitation |
| `tot_abn_precip_dry` | Count of abnormally dry years |
| `tot_abn_precip_wet` | Count of abnormally wet years |
| `tot_abn_precip_gs_dry` | Count of dry growing seasons |
| `tot_abn_precip_gs_wet` | Count of wet growing seasons |
| ... (with `_15_29` and `_30_44` variants) |

_**3. Average Standardized Indices (prefix: `avg_`)**_

| Variable | Description |
|----------|-------------|
| `avg_precip_spi` | Mean SPI across reproductive span |
| `avg_precip_gs_spi` | Mean growing season SPI |
| `avg_precip_ngs_spi` | Mean non-growing season SPI |
| ... (with `_15_29` and `_30_44` variants) |

_**4. Exposure Shares (prefix: `share_`)**_

| Variable | Description |
|----------|-------------|
| `share_abn_precip_dry` | Proportion of years abnormally dry |
| `share_abn_precip_wet` | Proportion of years abnormally wet |
| `share_abn_precip_gs_dry` | Proportion of dry growing seasons |
| `share_abn_precip_gs_wet` | Proportion of wet growing seasons |
| ... (with `_15_29` and `_30_44` variants) |

_**Metric Categories**_
_By Precipitation Type:_
1. **Total:** All precipitation (`precip`)
2. **Growing Season:** During agricultural growing months (`precip_gs`)
3. **Non-Growing Season:** Outside growing months (`precip_ngs`)

_By Condition:_
1. **Total:** All conditions (`precip_total`)
2. **Dry:** Abnormally dry (P ≤ 0.15) (`abn_precip_dry`)
3. **Wet:** Abnormally wet (P ≥ 0.85) (`abn_precip_wet`)
4. **Any Abnormal:** Dry or wet (`abn_precip`)

_By Age Group:_
1. **Full Span (15-44):** No suffix
2. **Early Reproductive (15-29):** `_15_29` suffix
3. **Late Reproductive (30-44):** `_30_44` suffix

_Metric Types:_
1. **Totals:** Sum across years (`tot_*`)
2. **Shares:** Proportion of years (`share_*`)
3. **Averages:** Mean SPI values (`avg_*`)

#### `CensusSelector`
This Stata program identifies which birth cohorts have observable reproductive outcomes in census data by calculating their age at census and determining exposure completeness. It's used to filter cohorts for climate-fertility analysis based on data availability.
##### Generated Variables
| Variable | Type | Description | Formula |
|----------|------|-------------|---------|
| `age_in_census` | numeric | Age at census time | `census - cohort` |
| `cohort_reaches_reproductive` | binary | Whether cohort reached age 15 by census | `max_age_observed ≥ 15` |
| `exposure_years` | numeric | Number of reproductive years observable (15-44) | `min(age_in_census, 44) - 15 + 1` |
| `full_exposure` | binary | Whether full 30-year reproductive span is observable | `exposure_years == 30` |

##### Logic

**Age Calculation**
  ```stata
  age_in_census = census - cohort
  ```
_Example:_ Cohort born 1970, census 2019 → age_in_census = 49

##### Exposure Years Calculation
  ```stata
  exposure_years = max(0, min(age_in_census, 44) - 15 + 1)
  ```
##### Full Exposure Flag
  ```stata
  full_exposure = (exposure_years == 30)
  ```
Identifies cohorts where complete reproductive history (ages 15-44) is observable.

#### PrecipitationAnalysisWorkflow
##### Methods
**1. `run_full_analysis` (Main Workflow)**
Complete end-to-end analysis pipeline:
_Steps:_
1. **Initialize configuration** - Set up environment
2. **Detect census years** - Query IPUMS mapping file for available census years
3. **Load and aggregate** - Process raw ERA5 precipitation data
4. **Fit gamma distributions** - Calculate SPI and abnormal indicators
5. **Calculate cohort metrics** - For each census year separately
6. **Combine census years** - Merge all census datasets
7. **Apply selection criteria** - Identify valid cohorts
8. **Save final dataset** - Export combined results
9. **Generate summary statistics** - Record processing metadata
10. **Cleanup** (optional) - Remove temporary files

_Calls These Programs:_
- `GetCountryName` - Validate country code
- `ClimateAnalysisConfig` - Initialize environment
- `PrecipitationDataProcessor` - Load data and fit gamma
- `CohortMetricsCalculator` - Calculate cohort metrics
- `CensusSelector` - Filter cohorts
- `CollectSummaryStats` - Generate metadata

**2. `combine_and_select_census`**
Merges cohort metrics across multiple census years:
_Process:_
1. Load first census metrics file
2. Append remaining census years
3. Drop observations with missing identifiers
4. Sort by SMALLEST, cohort, census
5. Apply census selection criteria
6. Save combined dataset

**3. `cleanup_temp_files`**
Removes temporary files generated during analysis:
- Individual census metric files (`[code]_metrics_[year].dta`)
- Intermediate precipitation file (`[country]_precip_annual.dta`)
