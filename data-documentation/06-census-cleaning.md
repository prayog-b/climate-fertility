# Cleaning census data
Note that we have two raw census microdata files. 

- `$cf/data/source/census/africa_women.dta`
- `$cf/data/source/census/africa_children.dta`

The `dm` folder contains two scripts that clean each of these files separately. 
```
dm/cf-ssa-dm-05a-census-women-genvar.do
dm/cf-ssa-dm-05b-census-children-genvar.do
```

## Cleaning women's census microdata
The code sits in `cf-ssa-dm-05a-census-women-genvar.do`. This code file generates and labels a set of new variables. 

### Basic demographic variables
We create the following variables:
**1. Country code:** `country_code` assigns a three-letter code based on the country name. Because some of the country names have spaces (ex: Burkina Faso) or special categories (ex: Côte d'Ivoire), using country codes makes it easier to iterate any data tasks over countries more easily using locals and loops.

**2. Region:** `africa_region` assigns each country to a geographical cluster: 
  
  - West Africa: Benin, Burkina Faso, Côte d'Ivoire, Ghana, Guinea, Liberia, Mali, Senegal, Sierra Leone, Togo
  - East Africa: Ethiopia, Kenya, Tanzania, Uganda, South Sudan, Sudan
  - Southern Africa: Botswana, Lesotho, Malawi, Mozambique, South Africa, Zambia, Zimbabwe
  - Central Africa: Cameroon

**3. Other demographic variables**
  
  - `rural` captures whether a household resides in a rural area
  - `has_bp_smallest` is an indicator that turns 1 if a respondent has birthplace SMALLEST
  - `has_res_smallest` is an indicator that turns 1 if respondent has residence SMALLEST
  - `no_children` turns 1 if respondent never had a child
  - `no_child_dead` turns 1 if respondent has no dead children
  - `no_child_surv` turns 1 if respondent has no surviving children
  - `cohort` is the difference between the census year and a respondent's age. It indicates a woman's birth cohort, which will be a crucial component in the merge operation.


### Education attainment
We create the following variables:

  - `no_educ`,  `primary_educ`, `secondary_educ`, `university_educ`: Indicators for educational attainment. Indicates whether respondent has completed no education, primary education, secondary education, or university education, respectively.
  - `spouse_primary_educ`, `spouse_secondary_educ` and `spouse_university_educ` capture whether a respondent's spouse has completed primary, secondary, or university education, respectively.

### Employment status
We created three indicators based on the raw variable `EMPSTAT` from the IPUMS microdata. 
   
   - `employed`: Respondent is employed
   - `unemployed`: Respondent is unemployed
   - `inactive_lfp`: Respondent is inactive in the labor force

#### Industry codes
For respondents who have the information, we classify respondents based on the industry in which they work using the `INDGEN` variable in the raw data. We create the following indicators:
  - `industry_none`:  No industry (NA)
  - `industry_agri`: Agriculture, fishing, forestry
  - `industry_mine`: Mining and extraction
  - `industry_manuf`: Manufacturing
  - `industry_utilities`: Electricity, water, gas, waste management
  - `industry_construct`: Construction
  - `industry_trade`: Wholesale or retail trade
  - `industry_hospitality`: Hotels and restaurants
  - `industry_logistics`: Transportation, storage, and communications
  - `industry_finsurance`: Financial services and insurance
  - `industry_public`: Public administration and defense
  - `industry_realestate`: Business services
  - `industry_education`: Education
  - `industry_health`: Health and social work
  - `industry_hhservice`: Private household services
  - `industry_other`: Other services
  - `nonag`: Non-agriculture industry
  - `sector_agriculture`: If `(industry_agri == 1) & !missing(INDGEN)`
  - `sector_industry`: If `(industry_mine == 1 | industry_manuf == 1 | industry_logistics == 1 | industry_trade == 1 | industry_construct == 1) & !missing(INDGEN)`
  - `sector_service`: If `industry_finsurance == 1 | industry_education == 1 | industry_health == 1 | industry_hospitality == 1 | industry_public == 1 | industry_realestate == 1 | industry_hhservice == 1 & !missing(INDGEN)`

#### Occupation codes
For respondents who have the information, we also classify respondents based on their ISCO Occupation codes, which is available in the variable `OCCISCO` in the raw data.
  
  - `occup_manager`: Legislators or senior officials and managers
  - `occup_professional`: Professionals
  - `occup_technic`: Technicians and associate professionals
  - `occup_clerk`: Clerks
  - `occup_service`: Service workers
  - `occup_agro`: Skilled agriculture and fishery
  - `occup_crafts`: Crafts and related trades
  - `occup_machineop`:  Machine operators
  - `occup_army`: Armed forces

  - `occup_employer` turns 1 if `occup_manager == 1`
  - `occup_employee` turns 1 if `(occup_service == 1 | occup_clerk == 1 | occup_technic == 1 | occup_machineop == 1)`

#### Spouse's employment status variables
We also created an equivalent set of variables with `spouse_*` prefix to capture the repsondent's spouse's employment status, industry code, and occupation labels. 

### Filtering census data
We drop people who were born before 1926, because we would not be able to observe their entire reproductively active life span in the weather data, which only begins from 1940. 
  ```stata
  	count if COHORT < 1926
  	di as text "   Dropping `r(N)' observations."
  	drop if COHORT < 1926
  ```


## Cleaning children's census microdata
We will now outline the processing of the children's census microdata. 
### Basic demographic variables
We create the following variables:
**1. Country code:** `country_code` assigns a three-letter code based on the country name. Because some of the country names have spaces (ex: Burkina Faso) or special categories (ex: Côte d'Ivoire), using country codes makes it easier to iterate any data tasks over countries more easily using locals and loops.

**2. Region:** `africa_region` assigns each country to a geographical cluster: 
  
  - West Africa: Benin, Burkina Faso, Côte d'Ivoire, Ghana, Guinea, Liberia, Mali, Senegal, Sierra Leone, Togo
  - East Africa: Ethiopia, Kenya, Tanzania, Uganda, South Sudan, Sudan
  - Southern Africa: Botswana, Lesotho, Malawi, Mozambique, South Africa, Zambia, Zimbabwe
  - Central Africa: Cameroon

**3. Other demographic variables**
  - `has_res_smallest`: Indicates that the child's residential SMALLEST unit is known
  - `has_mom_res_smallest`: Indicates that the child's mother's residential SMALLEST unit is known
  - `has_mom_birth_smallest`: Indicates that the child's mother's birthplace SMALLEST unit is known
  - `female`: Indicates that the child is female
  - `has_schooling`: Indicates that the child is attending schooling
  - `ever_schooling`: Indicates that the child is either attending, or has ever attended school
  - `never_school`: Indicates that the child has never attended school
  - `university_ed` indicates that the child has completed university education
  - `secondary_ed`: indicates that the child has completed secondary education
  - `primary_ed`: indicates that the child has completed primary education
  - `mom_native_born`: Mother is native-born
  - `mom_foreign_born`: Mother is foreign-born

### Employment status
We created three indicators based on the raw variable `EMPSTAT` from the IPUMS microdata, that capture the child's employment status. 
   
   - `employed`: Respondent is employed
   - `unemployed`: Respondent is unemployed
   - `inactive_lfp`: Respondent is inactive in the labor force
