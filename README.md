# climate-fertility

This repository contains replication code for analyzing the relationship between climate change and fertility outcomes in Sub-Saharan Africa. The project combines high-resolution climate data (ERA5 reanalysis), crop phenology information (FAO), and census microdata from IPUMS to examine how temperature and precipitation patterns affect fertility decisions and human capital investment.

# Preliminaries
## Pre-requisites
- Python 3.8 or later
- Stata 16 or later
- Bash shell (Linux/macOS) or WSL (Windows)
- Google Cloud SDK (gsutil) - for downloading FAO crop phenology data

## External API Access
**1. Climate Data Store (CDS) API Key**
- Create account key at https://cds.climate.copernicus.eu/
- Obtain API key from your profile
- Store credentials in ~/.cdsapirc file:
      url: https://cds.climate.copernicus.eu/api/v2
       key: YOUR_UID:YOUR_API_KEY

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
## Setup scripts
`cf-ssa-dm-00a-master-setup-python-environment.sh`
- **Purpose:** Initialize virtual Python environment and install dependencies
- **Inputs:** None (creates new environment)
- **Outputs:**
  - `.venv/` directory
  - `requirements.txt`
  - `.gitignore`
- **Notes:** Must update `PROJECT_DIR` variable before running

`cf-ssa-dm-00b-master-scripts.do`
- **Purpose:** Master orchestration script for entire pipeline
- **Key functions:**
  - `ClimateAnalysisConfig`: Sets user-specific directory paths
  - `print_timestamp`: Logs system information
  - `save_input`: Exports numbers to LaTeX
  - `verify_package`: Auto-installs missing Stata packages
- **Usage:** Run once to execute full pipeline
- **Required packages:** `reghdfe`

## Data Processing Scripts:
### Stage 1: Shapefile cleaning 
**Script:** `cf-ssa-01a-clean-shapefile.py`
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
 



