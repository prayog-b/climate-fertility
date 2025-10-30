# Climate and Fertility in Sub-Saharan Africa

This repository contains data documentation for the project. The project combines high-resolution climate data (ERA5 reanalysis), crop phenology information (FAO), and census microdata from IPUMS to examine how temperature and precipitation patterns affect fertility decisions and human capital investment.

---

# Preliminaries
## Pre-requisites
- Python 3.8 or later
- Stata 16 or later
- Bash shell (Linux/macOS) or WSL (Windows)
- Google Cloud SDK (gsutil) - for downloading FAO crop phenology data

## External API Access
**1. Climate Data Store (CDS) API Key**
- Create account key [here](https://cds.climate.copernicus.eu/).
- Obtain API key from your profile
- Store credentials in ~/.cdsapirc file:
  - url: https://cds.climate.copernicus.eu/api/v2
  - key: YOUR_UID:YOUR_API_KEY

**2. Google Cloud Platform**
- Install gsutil: https://cloud.google.com/storage/docs/gsutil_install
- Authenticate with Google account for FAO data downloads

## Quick Start Guide
**Set Up Python Environment**
```
cd dm
bash cf-ssa-dm-00a-master-setup-python-environment.sh
```
**Note:** Before running, update the `PROJECT_DIR` variable in the script to match your local directory path.

This script will:
- Create a Python virtual environment at `.venv/`
- Install all required packages
- Register a Jupyter kernel named "Climate Fertility Env"
- Generate `requirements.txt`
- Create `.gitignore` with appropriate exclusions


**Configure User Settings**
Open `cf-ssa-dm-00b-master-scripts.do` and add your username to the `ClimateAnalysisConfig` program (lines 18-49) if you're not already listed.

**Run master script**
```
* In Stata:
do "dm/cf-ssa-dm-00b-master-scripts.do"
```
The master script will automatically execute the entire pipeline in sequence.


## Project Structure 
## Project Structure

<details>
<summary>📁 Click to expand full project directory structure</summary>

```
Climate_Change_and_Fertility_in_SSA/
│
├── dm/                                    # Data management scripts
│   ├── cf-ssa-dm-00a-master-setup-python-environment.sh
│   ├── cf-ssa-dm-00b-master-scripts.do   # Master orchestration script
│   │
│   ├── cf-ssa-01a-clean-shapefile.py     # Shapefile cleaning
│   │
│   ├── cf-ssa-02a-era-cdsapi-climate-data-download.py
│   ├── cf-ssa-02b-era-create-country-data.py
│   ├── cf-ssa-02c-era-create-subunit-day-data.py
│   ├── cf-ssa-dm-02d-era-clean-subunit-day.do
│   │
│   ├── cf-ssa-03a-fao-download-crop-phenology-data.do
│   ├── cf-ssa-03b-crop-phenology-processing.py
│   │
│   ├── cf-ssa-dm-04a-create-country-censusyear-mapping.do
│   ├── cf-ssa-dm-04b-generate-cohort-year-pairs.do
│   ├── cf-ssa-dm-04c-generate-precipitation-variables.do
│   ├── cf-ssa-dm-04d-generate-temperature-variables.do
│   │
│   ├── cf-ssa-dm-05a-census-women-genvar.do
│   ├── cf-ssa-dm-05b-census-children-genvar.do
│   │
│   ├── cf-ssa-dm-06a-merge-setup.do
│   ├── cf-ssa-dm-06b-merge-implementation.do
│   │
│   ├── logs/                              # Execution logs
│   └── requirements.txt                   # Python package versions
│
├── sa/                                    # Statistical analysis scripts
│   └── [analysis scripts]
│
├── data/
│   ├── source/
│   │   ├── era/                          # Raw ERA5 climate data (NetCDF)
│   │   ├── fao/                          # FAO crop phenology rasters
│   │   ├── census/                       # Census microdata
│   │   └── shapefiles/                   # Administrative boundary shapefiles
│   │
│   └── derived/
│       ├── era/                          # Processed climate data
│       ├── census/                       # Cleaned census data
│       ├── cohort_weather/               # Cohort-level weather variables
│       └── analysis/                     # Final merged datasets
│
└── output/                               # Tables and figures
    └── [LaTeX tables, graphs]
```

</details>

### Quick Navigation

| Directory | Purpose | Key Contents |
|-----------|---------|--------------|
| **`dm/`** | Data management pipeline | All data processing scripts (Python + Stata) |
| **`sa/`** | Statistical analysis | Regression scripts, tables, figures |
| **`data/raw/`** | Raw data (read-only) | ERA5, FAO, census, shapefiles |
| **`data/derived/`** | Processed data | Analysis-ready datasets |
| **`output/`** | Results | LaTeX tables, graphs, maps |


# Data Sources
### 1. ERA5 Climate Reanalysis Data
**Source:** Copernicus Climate Data Store (CDS)
**Spatial Resolution:** 0.25° × 0.25° (~25km at equator)
**Temporal Coverage:** 1940-2019, hourly
**Variables:**
- 2-meter temperature (Kelvin), mean and maximum
- Total precipitation (m)
**Processing:**
- Downloaded at hourly resolution
- Aggregated to daily level (mean/max temperature, total precipitation)
- Spatially aggregated to administrative subunit level using population-weighted averages
**Format:** NetCDF files.

### 2. FAO Crop Phenology Data
**Source:** FAO GIEWS Agricultural Stress Index System (ASIS)
**Spatial Resolution:** ~10km
**Variables:**
- Crop growing season start dates
- Crop growing season end dates
- Crop types (maize, wheat, rice, etc.)

**Access:** Google Cloud Platform bucket
**Format:** GeoTIFF rasters + JSON metadata

### 3. Census Microdata
**Source:** IPUMS International
**Countries:** Benin, Botswana, Cameroon, Côte d'Ivoire, Ethiopia, Ghana, Guinea, Kenya, Lesotho, Liberia, Malawi, Mali, Mozambique, Rwanda, Senegal, South Africa, South Sudan, Sudan, Tanzania, Togo, Uganda, Zambia, Zimbabwe
**Rounds:** Varies by country
**Sample Size:** 
**Key Variables:**
- Birth history (number of children ever born, number of dead children, number of surviving children)
- Age and cohort identifiers
- Geographic identifiers (matched to the `SMALLEST` administrative units)
- Socioeconomic characteristics (industry and occupation codes)
- Education levels (level of education completed and years of schooling)

### 4. Administrative Boundaries
Source: ??
Level: Varies by country


# Detailed script documentation
## Master script

`cf-ssa-dm-00-master.do`
- **Purpose:** Master orchestration script for entire pipeline
- **Key functions:**
  - `ClimateAnalysisConfig`: Sets user-specific directory paths
  - `print_timestamp`: Logs system information
  - `save_input`: Exports numbers to LaTeX
  - `verify_package`: Auto-installs missing Stata packages
- **Usage:** Run once to execute full pipeline
- **Required packages:** 

## Setup scripts
`cf-ssa-dm-00a-setup-python-env.sh`
- **Purpose:** Initialize virtual Python environment and install dependencies
- **Inputs:** None (creates new environment)
- **Outputs:**
  - `.venv/` directory
  - `requirements.txt`
  - `.gitignore`
- **Notes:** Must run `cf-ssa-dm-00-master.do` before running to set environment variables.

`cf-ssa-dm-00b-helper-functions.do`
- **Purpose:** Auxiliary functions used throughout project files.
- **Key functions:**
  - `print_timestamp`: Logs system information
  - `save_input`: Exports numbers to LaTeX
  - `verify_package`: Auto-installs missing Stata packages
- **Usage:** Run once to set up


## Data Processing Scripts:
### Stage 1: Shapefile cleaning 
#### Script 1a `cf-ssa-01a-clean-shapefile.py`
- **Purpose:** Clean administrative boundary shapefile. 
- **Issues addressed:**
  - Null geometries or null values for administrative codes
  - Inconsistent assignment of administrative codes
- **Inputs:**
  - Raw shapefile in `data/source/shapefiles/ipums/bpl_data_smallest_unit_africa.shp`
  - _Note:_ The raw shapefile available to us is identifiable at the birthplace name level `BPL_NAME`. The linking variable with the census data is the variable called `SMALLEST`, which represents the smallest administrative level for which a person's birthplace or place of residence is known. 
- **Outputs:**
  - Cleaned shapefile in `data/derived/shapefiles/cleaned-ssa-boundaries.shp`
  - Details on null geometries found in shapefile: `data/documentation/shapefile-cleaning/ssa-shapefile-null-geometries.csv`
  - Details on instances of a single `SMALLEST` unit has multiple birthplace names associated to it: `data/documentation/shapefile-cleaning/smallest-multiple-bpl-names.csv`
 
### Stage 2: ERA5 Climate Data
#### Script 2a: `cf-ssa-02a-era-cdsapi-climate-data-download.py`
- **Purpose:** Download hourly temperature and precipitation from CDS API
- **Spatial extent:** African continent bounding box.
- **Temporal coverage:** 1940-2019
- **Parallelization:** Supports multiple API keys for faster downloads
- **Configuration:**
  - Default: 1 API key, sequential downloads
  - Advanced: 4 API keys, parallel downloads
- **Rate limits:** CDS has download limits, so code execution may take long depending on their capabilities.

#### Script 2b: `cf-ssa-02b-era-create-country-data.py`
- **Purpose:** Subset Africa-wide data to country-specific datasets.
- **Processing:**
  - Converts hourly to daily aggregation:
    - Temperature: mean and maximum, and convert from Kelvin to degrees Celsius.
    - Preicpitation: total daily sum, and convert from meters to milimeters
  - Clip to country boundaries
- **Outputs:** Country-year `.parquet` files in `data/derived/era/country_level/`
- **Format:** Parquet for efficient storage
 
#### Script 2c: `cf-ssa-02c-era-create-subunit-data.py`
- **Purpose:** Aggregate grid cells to administrative units
- **Aggregation method:** Weighted average. Weighted by share of a grid cell that falls within a subunit.
- **Temporal resolution:** Daily level
- **Inputs:**
  - Country-level parquet files from 2b
- **Outputs:**
  - SMALLEST-day `.dta` files for each country (1940-2019)
- **File structure:**
  - Panel data (SMALLEST x day)
 
#### Script 2d: `cf-ssa-dm-02d-era-clean-subunit-day.do`
- **Purpose:** Add variable labels and perform basic data validation.
- **Operations:**
  - Label all variables with descriptive names
  - Check for missing values and outliers
  - Verify geographic identifiers.
- **Outputs:** `.dta` files from 2c updated with variable labels.


### Stage 3: FAO Crop Phenology
#### Script 3a: `cf-ssa-03a-fao-download-crop-phenology-data.do`
- **Purpose:** Download FAO's crop phenology rasters from Google Cloud
- **Tool:** `gsutil` command-line utility
- **Data Downloaded:**
  - GeoTIFF rasters (global coverage)
  - JSON metadata files
- **Indicators downloaded:** Phenology data for start, max, and end of growing season for up to two crop-growing seasons.
- **Pre-requisites:** Must configure `gsutil` with Google account.

#### Script 3b: `cf-ssa-03b-crop-phenology-processing.py`
- **Purpose:** Process global rasters to SMALLEST-level summaries
- **Processing steps:**
  1. Clip global rasters to African continent using shapefile from Script 1a.
  2. Extract crop growing season variables.
  3. For each SMALLEST unit, find the modal start and end of season.
  4. Create indicator variables to flag months of the year based on whether they fall within the growing season for a SMALLEST unit.

 
### Stage 4: Cohort Weather Variables
**Motivation:** We construct weather exposure measures for each birth cohort during their reproductive years (ages 15-44). This allows us to estimate how lifetime exposure to extreme weather affects fertility outcomes.

#### Script 4a: `cf-ssa-dm-04a-create-country-censusyear-mapping.do`
- **Purpose:** Create lookup table of countries and census years using the raw census microdata
- **Output:** Cartesian product of countries and census rounds in a `.dta` format file
- **Used for:** Identifying which cohorts to process for each census.

#### Script 4b: `cf-ssa-dm-04b-generate-cohort-year-pairs.do`
- **Purpose:** Create all relevant cohort-year combinations
- **Logic:** For each cohort born in year Y, generate records for ages 15-49
- **Example:** A woman born in 1950 would get records for 1965-1999
- **Output:** Cohort-year panel structure.

#### Script 4c: `cf-ssa-04c-generate-precipitation-variables.do`
- **Purpose:** Calculate precipitation exposure metrics by cohort
- **Variables created:**
  - Fit the precipitation distribution for each SMALLEST unit to a gamma distribution to identify years where precipitation was abnormally high or abnormally low. 
  - Share of years with abnormal precipitation in a woman's reproductive span (less than 15th percentile or above 85th percentile of the precipitation distribution in their birthplace SMALLEST unit)
  - Standardized precipitation index (SPI)
  - Precipitation variables disaggregated by growing season and non-growing season precipitation.
  - Precipitation variables disaggregated by early (ages 15-29) or late (ages 30-44) exposure.
- **Aggregation level:** SMALLEST-cohort-census

#### Script 4d: `cf-ssa-dm-04d-generate-temperature-variables.do`
- **Purpose:** Calculate temperature exposure metrics by cohort
- **Variables created:**
  - Degree days above various temperature thresholds from 22 to 28C
  - Degree days disaggregated by growing season and non-growing season.
  - Degree days disaggregated by early (ages 15-29) or late (ages 30-44) exposure.
- **Aggregation level:** Subunit-cohort


### Stage 5: Census Data Cleaning
#### Script 5a: `cf-ssa-dm-05a-census-women-genvar.do`
- **Purpose:** Clean women's census microdata and generate variables for analysis.
- **Variables generated:**
  - Total children ever born, number of dead children, number of surviving children
  - Education categories (primary, secondary, tertiary)
  - Marital status indicators
  - Occupation and industry code indicators
  - Agricultural and non-agricultural worker status
  - Spouse's education and employment indicators.
- **Outputs:** `data/derived/census/africa-women-genvar.do`

#### Script 5b: `cf-ssa-dm-05b-census-children-genvar.do`
- **Purpose:** Clean children's census samples for child-level analysis
- **Variables generated:**
  - Child age and birth cohort
  - Schooling status
  - Mother's birthplace
- **Output:** `data/derived/census/africa-children-genvar.dta`


### Stage 6: Data Integration
#### Script 6a: `cf-ssa-dm-06a-merge-setup.do`
- **Purpose:** Set up helper functions to create the merging environment (called by dm-06b)

#### Script 6b: `cf-ssa-dm-06b-merge-implementation.do`
- **Purpose:** Merge census data with weather exposure variables
- **Merge keys:**
  - SMALLEST unit (birthplace or place of residence)
  - Birth cohort (year)
  - Census year
- **Merge type:** Many-to-one (many census observations per SMALLEST-cohort-census)
- **Validation checks:**
  - All census records match to weather data
  - No duplicates created
  - Weather variables non-missing for matched sample
- **Final Output:** Analysis-ready dataset with individual-level census data linked to lifetime weather exposure.



## Output Files
### Intermediate Outputs
| File/Directory | Description | 
|-----------|---------|
| **`data/derived/shapefiles/cleaned_ssa_boundaries.shp`** | Cleaned shapefile |
| **`data/derived/era/country/*.parquet`** | Daily country-year level climate data | 
| **`data/derived/era/00-subunit-day/*.dta`** | Daily SMALLEST-day level climate data | 
| **`data/derived/fao/crop-phenology/crop-phenology-summary-stats.csv`** | SMALLEST-level dataset with growing season indicators for each month |
| **`data/derived/era/country/*-precip.dta`** | SMALLEST-cohort-census level precipitation exposure |
| **`data/derived/era/country/*-temperature.dta`** | SMALLEST-cohort-census level temperature exposure |
| **`data/derived/census/africa-women-genvar.dta`** | Cleaned women's census microdata | 


### Final Outputs
| File/Directory | Description |
|----------------|-------------|
| **`data/derived/census-climate-merged/country/country-census-climate-merged-by-birthplace.dta`** | Country-specific SMALLEST-cohort-census level dataset, merged on SMALLEST unit of a respondent's birthplace |
| **`data/derived/census-climate-merged/country/country-census-climate-merged-by-birthplace.dta`** | Country-specific SMALLEST-cohort-census level dataset, merged on SMALLEST unit of a respondent's place of residence |


## Troubleshooting
### Common issues
#### CDS API Authentication Fails
**Error:** `API key not found`
**Solution:**
- Verify whether `.cdsapirc` file exists in your home directory
- Check file format: must be `url` and `key` fields
- Confirm API key is active on CDS website

#### Out of memory errors (Python)
**Solution:**
- Process data in smaller chunks
- Close other applications to free RAM
- Use a machine with more memory

#### Stata Dataset Too Large
**Solution:**
- Increase Stata memory: `set max_memory 32g`
- Process countries separately

#### Python Package Conflicts
**Solution:**
```
# Remove existing environment and recreate
rm -rf .venv
bash cf-ssa-dm-00a-master-setup-python-environment.sh
```

#### Missing gsutil
**Error:** `gsutil: command not found`
**Solution:** 
- Install Google Cloud SDK:  https://cloud.google.com/sdk/docs/install
- Authenticate: `gcloud auth login`


## Acknowledgments
- ERA5 data: Copernicus Climate Change Service
- FAO phenology data: FAO GIEWS ASIS
- Census data: IPUMS International

**Last updated:** October 30, 2025

