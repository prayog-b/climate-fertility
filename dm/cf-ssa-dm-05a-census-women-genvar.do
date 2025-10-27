********************************************************************************
* PROJECT:       Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:        Prayog Bhattarai
* DATE CREATED:  09-09-2025
* DATE MODIFIED: 08-10-2025
* DESCRIPTION:   Sanity checks on census data for women
* DEPENDENCIES: 
* 
* NOTES: 
********************************************************************************

********************************************************************************
* Setup and Configuration
********************************************************************************
	clear all 
	cap log close 

	capture program drop ClimateAnalysisConfig
	program define ClimateAnalysisConfig
		syntax, method(string)
	  
		if "`method'" == "init" {
			if "`c(username)'" == "prayogbhattarai" {
				global dir "/Users/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
			}
			else if "`c(username)'" == "prayog" {
				global dir "D:/`c(username)'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "D:/`c(username)'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
				di "    User detected: prayog"
			}
			else if inlist("`c(username)'", "yogita") {
				global dir "/Users/`c(username)'/NUS Dropbox/`c(username)'/Climate_Change_and_Fertility_in_SSA"
			}
			else if inlist("`c(username)'", "celin", "CE.4875") {
				global dir "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
			}
			else if inlist("`c(username)'", "yun") {
				global dir "/Users/`c(username)'/Dropbox/Climate_Change_and_Fertility_in_SSA"
			}
			else {
				di as error "Unknown user. Set global dir manually."
				exit 198
			}
			
			global era_derived "$dir/data/derived/era"
			global era_raw "$dir/data/raw/era"
		}
	end

	global era_derived "$dir/data/derived/era"
	global era_raw "$dir/data/raw/era"

	* Create log file 
	*log using "$dir/dm/logs/census/census_sanity_checks.log", replace
	ClimateAnalysisConfig, method("init")

	* Load dataset
	use "$dir/data/source/census/africa_women.dta", replace


********************************************************************************
* Generate basic demographic variables
********************************************************************************
	/*
	codebook COUNTRY
	----------------------------------------------------------------------------------------------
	COUNTRY                                                                                Country
	----------------------------------------------------------------------------------------------

					  type:  numeric (double)
					 label:  COUNTRY

					 range:  [72,894]                     units:  1
			 unique values:  25                       missing .:  0/7,857,915

				  examples:  288   Ghana
							 466   Mali
							 646   Rwanda
							 710   South Africa

	tab COUNTRY
		  Country |      Freq.     Percent        Cum.
	--------------+-----------------------------------
		 Botswana |     27,874        0.35        0.35
		 Cameroon |    218,577        2.78        3.14
			Benin |    139,402        1.77        4.91
		 Ethiopia |    923,306       11.75       16.66
			Ghana |    384,799        4.90       21.56
		   Guinea |    141,400        1.80       23.36
	Cote d'Ivoire |    135,413        1.72       25.08
			Kenya |    767,976        9.77       34.85
		  Lesotho |     26,990        0.34       35.20
		  Liberia |     28,074        0.36       35.55
		   Malawi |    215,357        2.74       38.29
			 Mali |    193,909        2.47       40.76
	   Mozambique |    379,348        4.83       45.59
		   Rwanda |  1,227,344       15.62       61.21
		  Senegal |    183,387        2.33       63.54
	 Sierra Leone |     69,044        0.88       64.42
	 South Africa |  1,255,940       15.98       80.40
		 Zimbabwe |     40,893        0.52       80.93
	  South Sudan |     31,970        0.41       81.33
			Sudan |    277,968        3.54       84.87
			 Togo |     42,263        0.54       85.41
		   Uganda |    197,066        2.51       87.92
		 Tanzania |    635,038        8.08       96.00
	 Burkina Faso |    152,773        1.94       97.94
		   Zambia |    161,804        2.06      100.00
	--------------+-----------------------------------
			Total |  7,857,915      100.00
	*/
	
	
	
	* Create country code variable 
	decode COUNTRY, gen(CNTRY)
	replace CNTRY = "Ivory Coast" if CNTRY == "Cote d'Ivoire"
	gen str3 country_code = ""
	replace country_code = "BEN" if lower(trim(CNTRY)) == "benin"
	replace country_code = "BWA" if lower(trim(CNTRY)) == "botswana"
	replace country_code = "BFA" if lower(trim(CNTRY)) == "burkina faso"
	replace country_code = "CMR" if lower(trim(CNTRY)) == "cameroon"
	replace country_code = "ETH" if lower(trim(CNTRY)) == "ethiopia"
	replace country_code = "GHA" if lower(trim(CNTRY)) == "ghana"
	replace country_code = "GIN" if lower(trim(CNTRY)) == "guinea"
	replace country_code = "KEN" if lower(trim(CNTRY)) == "kenya"
	replace country_code = "IVC" if lower(trim(CNTRY)) == "ivory coast"
	replace country_code = "LSO" if lower(trim(CNTRY)) == "lesotho"
	replace country_code = "LBR" if lower(trim(CNTRY)) == "liberia"
	replace country_code = "MWI" if lower(trim(CNTRY)) == "malawi"
	replace country_code = "MLI" if lower(trim(CNTRY)) == "mali"
	replace country_code = "MOZ" if lower(trim(CNTRY)) == "mozambique"
	replace country_code = "SLE" if lower(trim(CNTRY)) == "sierra leone"
	replace country_code = "ZAF" if lower(trim(CNTRY)) == "south africa"
	replace country_code = "SSD" if lower(trim(CNTRY)) == "south sudan"
	replace country_code = "SEN" if lower(trim(CNTRY)) == "senegal"
	replace country_code = "SDN" if lower(trim(CNTRY)) == "sudan"
	replace country_code = "TZA" if lower(trim(CNTRY)) == "tanzania"
	replace country_code = "TGO" if lower(trim(CNTRY)) == "togo"
	replace country_code = "UGA" if lower(trim(CNTRY)) == "uganda"
	replace country_code = "ZMB" if lower(trim(CNTRY)) == "zambia"
	replace country_code = "ZWE" if lower(trim(CNTRY)) == "zimbabwe"
	label variable country_code "ISO3 country code derived from COUNTRY"

	* Create variable to capture region
	gen str20 africa_region = ""
	replace africa_region = "West Africa" if inlist(country_code, "BEN", "BFA", "GHA", "GIN") 
	replace africa_region = "West Africa" if inlist(country_code, "IVC", "LBR", "MLI", "SEN", "SLE", "TGO")
	replace africa_region = "East Africa" if inlist(country_code, "ETH", "KEN", "TZA", "UGA", "SSD", "SDN")
	replace africa_region = "Southern Africa" if inlist(country_code, "BWA", "LSO", "MWI", "MOZ", "ZAF", "ZMB", "ZWE")
	replace africa_region = "Central Africa" if inlist(country_code, "CMR")

	* Create variable to capture rural-urban status 
	gen rural = (URBAN == 1) & URBAN != 9
	label variable rural "Rural"
	
	
	* Create an indicator that turns 1 if respondent has birthplace SMALLEST
	gen has_bp_smallest = (!missing(BIRTH_SMALLEST))
	label variable has_bp_smallest "Has BIRTHPLACE SMALLEST"
	
	* Create an indicator that turns 1 if respondent has residence SMALLEST
	gen has_res_smallest = (!missing(RES_SMALLEST))
	label variable has_res_smallest "Has RESIDENTIAL SMALLEST"

	* Recode CHBORN categories to missing if unknown or not in universe
	replace CHBORN = . if inlist(CHBORN, 98, 99)
	label variable CHBORN "Children ever born"

	* Recode CHSURV categories to missing if unknown or not in universe
	replace CHSURV = . if inlist(CHSURV, 98, 99)
	label variable CHSURV "Children surviving"

	* Recode CHDEAD categories to missing if unknown or not in universe
	replace CHDEAD = . if inlist(CHDEAD, 98, 99)
	label variable CHDEAD "Children dead"

	* Indicator if respondent never had a child
	gen no_children = (CHBORN == 0)
	label variable no_children "No children"
	
	* Indicator if respondent ever had at least one child
	gen had_children = (CHBORN > 0)
	label variable had_children "Had at least one child"
	
	* Indicator if respondent has no dead children
	gen no_child_dead = (CHDEAD == 0)
	label variable no_child_dead "No children dead"
	
	
	* Indicator if respondent has no surviving children
	gen no_child_surv = (CHSURV == 0)
	label variable no_child_surv "No surviving children"

	* Create cohort variable 
	gen COHORT = YEAR - AGE 
	label variable COHORT "Birth cohort"

	*********************************************
	*IMPORTANT STEP: We're filtering out a bunch of the early cohorts before 1926
	count if COHORT < 1926
	di as text "   Dropping `r(N)' observations."
	drop if COHORT < 1926
	**********************************************


	/*
	codebook EDATTAIN
	----------------------------------------------------------------------------------------------
	EDATTAIN                        Educational attainment, international recode [general version]
	----------------------------------------------------------------------------------------------

					  type:  numeric (long)
					 label:  EDATTAIN

					 range:  [0,9]                        units:  1
			 unique values:  6                        missing .:  269,815/7,857,915

				tabulation:  Freq.   Numeric  Label
						   374,927         0  NIU (not in universe)
						 4,976,874         1  Less than primary completed
						 1,546,593         2  Primary completed
						   487,277         3  Secondary completed
						   115,921         4  University completed
							86,508         9  Unknown
						   269,815         .  
	*/
	* Create indicators for no education, primary education, secondary education, and university education
	gen no_educ = (EDATTAIN == 1)
	gen primary_educ = (EDATTAIN == 2)
	gen secondary_educ = (EDATTAIN == 3)
	gen university_educ = (EDATTAIN == 4)


	/*
	codebook MARST
	----------------------------------------------------------------------------------------------
	MARST                                                         Marital status [general version]
	----------------------------------------------------------------------------------------------

					  type:  numeric (long)
					 label:  MARST

					 range:  [0,9]                        units:  1
			 unique values:  6                        missing .:  78,522/7,857,915

				tabulation:  Freq.   Numeric  Label
						   366,350         0  NIU (not in universe)
						   593,551         1  Single/never married
						 5,129,397         2  Married/in union
						   516,493         3  Separated/divorced/spouse absent
						 1,142,182         4  Widowed
							31,420         9  Unknown/missing
							78,522         .  
	*/
	* Create indicators for single, married, divorced, and widowed
	gen marstat_single = (MARST == 1)
	gen marstat_married = (MARST == 2)
	gen marstat_divorced = (MARST == 3)
	gen marstat_widow = (MARST == 4)
	
		
	/* 
	codebook NATIVITY
	----------------------------------------------------------------------------------------------
	NATIVITY                                                                       Nativity status
	----------------------------------------------------------------------------------------------

					  type:  numeric (long)
					 label:  NATIVITY

					 range:  [0,9]                        units:  1
			 unique values:  4                        missing .:  1,118,934/7,857,915

				tabulation:  Freq.   Numeric  Label
							31,195         0  NIU (not in universe)
						 6,522,517         1  Native-born
						   170,529         2  Foreign-born
							14,740         9  Unknown/missing
						 1,118,934         .  

	*/
	* Create indicator variables for native born status 
	gen native_born = (NATIVITY == 1)
	label variable native_born "Native-born" 
	gen foreign_born = (NATIVITY == 2)
	label variable foreign_born "Foreign-born"
	
	
	** Spouse educational attainment
	gen spouse_primary_educ = (EDATTAIN_SP == 2)
	gen spouse_secondary_educ = (EDATTAIN_SP == 3)
	gen spouse_university_educ = (EDATTAIN_SP == 4)
	label variable spouse_primary_educ "Spouse: Completed primary education"
	label variable spouse_secondary_educ "Spouse: Completed secondary education"
	label variable spouse_university_educ "Spouse: Completed university education"



	
********************************************************************************
* Generate variables for employment status
********************************************************************************
	
	/*
	codebook EMPSTAT
	----------------------------------------------------------------------------------------------
	EMPSTAT                                  Activity status (employment status) [general version]
	----------------------------------------------------------------------------------------------

					  type:  numeric (long)
					 label:  EMPSTAT

					 range:  [0,9]                        units:  1
			 unique values:  5                        missing .:  999,171/7,857,915

				tabulation:  Freq.   Numeric  Label
						   371,492         0  NIU (not in universe)
						 4,258,058         1  Employed
						   331,808         2  Unemployed
						 1,878,359         3  Inactive
							19,027         9  Unknown/missing
						   999,171         .  
	*/

	* Create indicators for employed, unemployed, and inactive in the labor force
	gen employed = (EMPSTAT == 1)
	gen unemployed = (EMPSTAT == 2)
	gen inactive_lfp = (EMPSTAT == 3)


	/*
	codebook INDGEN
	----------------------------------------------------------------------------------------------
	INDGEN                                                                Industry, general recode
	----------------------------------------------------------------------------------------------

					  type:  numeric (long)
					 label:  INDGEN

					 range:  [0,999]                      units:  1
			 unique values:  20                       missing .:  2,868,010/7,857,915

				  examples:  0     NIU (not in universe)
							 10    Agriculture, fishing, and forestry
							 112   Education
							 .    
	*/
	* Create indicators for different industry codes
	gen industry_none = (INDGEN == 0 | missing(INDGEN))
	label variable industry_none "No industry (NA)"
	gen industry_agri = (INDGEN == 10)  & !missing(INDGEN)   // This is our ag variable 
	label variable industry_agri "Industry: Agriculture, fishing, forestry"
	gen industry_mine = (INDGEN == 20)  & !missing(INDGEN)
	label variable industry_mine "Industry: Mining and extraction"
	gen industry_manuf = (INDGEN == 30)  & !missing(INDGEN)
	label variable industry_manuf "Industry: Manufacturing"
	gen industry_utilities = (INDGEN == 40)  & !missing(INDGEN)
	label variable industry_utilities "Industry: Electricity, water, gas, waste mgmt"
	gen industry_construct = (INDGEN == 50)  & !missing(INDGEN)
	label variable industry_construct "Industry: Construction"
	gen industry_trade = (INDGEN == 60)  & !missing(INDGEN)
	label variable industry_trade "Industry: Wholesale or retail trade"
	gen industry_hospitality = (INDGEN == 70) & !missing(INDGEN)
	label variable industry_hospitality "Industry: Hotels and restaurants"
	gen industry_logistics = (INDGEN == 80) & !missing(INDGEN)
	label variable industry_logistics "Industry: Transportation, storage, and communications"
	gen industry_finsurance = (INDGEN == 90) & !missing(INDGEN)
	label variable industry_finsurance "Industry: Financial services and insurance"
	gen industry_public = (INDGEN == 100) & !missing(INDGEN)
	label variable industry_public "Industry: Public admin and defense"
	gen industry_realestate = (INDGEN == 111) & !missing(INDGEN)
	label variable industry_realestate "Industry: Business services"
	gen industry_education = (INDGEN == 112) & !missing(INDGEN)
	label variable industry_education "Industry: Education"
	gen industry_health = (INDGEN == 113) & !missing(INDGEN)
	label variable industry_health "Industry: Health and social work"
	gen industry_other = (INDGEN == 114) & !missing(INDGEN)
	label variable industry_other "Industry: Other services"
	gen industry_hhservice = (INDGEN == 120) & !missing(INDGEN)
	label variable industry_hhservice "Industry: Private household services"

	* Generate variables for ag and non-ag
	gen nonag = (industry_agri != 1) & !missing(INDGEN)
	label variable nonag "Non-Agriculture industry"
	
	* Generate variables for industry and service sector
	gen sector_agriculture = (industry_agri == 1) & !missing(INDGEN)
	gen sector_industry    = (industry_mine == 1 | industry_manuf == 1 | industry_logistics == 1 | industry_trade == 1 | industry_construct == 1) & !missing(INDGEN)
	gen sector_service     = industry_finsurance == 1 | industry_education == 1 | industry_health == 1 | industry_hospitality == 1 | industry_public == 1 | industry_realestate == 1 | industry_hhservice == 1 & !missing(INDGEN)
	
	label variable sector_agriculture "Sector: Agriculture"
	label variable sector_industry "Sector: Industry"
	label variable sector_service "Sector: Services"
	
	/*
	codebook OCCISCO 
	-------------------------------------------------------------------------------------------------------------------------------
	OCCISCO                                                                                                Occupation, ISCO general
	-------------------------------------------------------------------------------------------------------------------------------

					  type:  numeric (long)
					 label:  OCCISCO

					 range:  [1,99]                       units:  1
			 unique values:  14                       missing .:  2,082,098/7,857,915

				  examples:  6     Skilled agricultural and fishery workers
							 6     Skilled agricultural and fishery workers
							 99    NIU (not in universe)
							 .   
	*/
	* Create indicators for different occupation codes
	gen occup_manager = (OCCISCO == 1)
	gen occup_professional = (OCCISCO == 2)
	gen occup_technic = (OCCISCO == 3)
	gen occup_clerk = (OCCISCO == 4)
	gen occup_service = (OCCISCO == 5)
	gen occup_agro = (OCCISCO == 6)
	gen occup_crafts = (OCCISCO == 7)
	gen occup_machineop = (OCCISCO == 8)
	gen occup_elementary = (OCCISCO == 9)
	gen occup_army = (OCCISCO == 10)
	label variable occup_agro "Occupation: Skilled agriculture and fishery"
	label variable occup_crafts "Occupation: Crafts and related trades"
	label variable occup_service "Occupation: Service workers"
	label variable occup_clerk "Occupation: Clerks"
	label variable occup_professional "Occupation: Professionals"
	label variable occup_manager "Occupation: Legislators or senior officials and managers"
	label variable occup_army "Occupation: Armed forces"
	label variable occup_technic "Occupation: Technicians and associate professionals"
	label variable occup_machineop "Occupation: machine operators"

	// Generate indicator variables for whether someone is an employer or employee
	gen occup_employer = (occup_manager == 1)
	gen occup_employee = (occup_service == 1 | occup_clerk == 1 | occup_technic == 1 | occup_machineop == 1)

	label variable occup_employer "Occupation - Employer"
	label variable occup_employee "Occupation - Employee" 


	** Spouse employment status
	gen spouse_employed = (EMPSTAT_SP == 1)
	gen spouse_unemployed = (EMPSTAT_SP == 2)
	gen spouse_inactive_lfp = (EMPSTAT_SP == 3)
	label variable spouse_employed "Spouse: Employed "
	label variable spouse_unemployed "Spouse: Unemployed"
	label variable spouse_inactive_lfp "Spouse: Not active in labor force "

	** Spouse industry 
	gen spouse_industry_none = (INDGEN_SP == 0) & !missing(INDGEN_SP)
	label variable spouse_industry_none "Spouse No industry (NA)"
	gen spouse_industry_agri = (INDGEN_SP == 10) & !missing(INDGEN_SP)   // This is our ag variable for spouse
	label variable spouse_industry_agri "Spouse Industry: Agriculture, fishing, forestry"
	gen spouse_industry_mine = (INDGEN_SP == 20) & !missing(INDGEN_SP)
	label variable spouse_industry_mine "Spouse Industry: Mining and extraction"
	gen spouse_industry_manuf = (INDGEN_SP == 30) & !missing(INDGEN_SP)
	label variable spouse_industry_manuf "Spouse Industry: Manufacturing"
	gen spouse_industry_utilities = (INDGEN_SP == 40) & !missing(INDGEN_SP)
	label variable spouse_industry_utilities "Spouse Industry: Electricity, water, gas, waste mgmt"
	gen spouse_industry_construct = (INDGEN_SP == 50) & !missing(INDGEN_SP)
	label variable spouse_industry_construct "Spouse Industry: Construction"
	gen spouse_industry_trade = (INDGEN_SP == 60) & !missing(INDGEN_SP)
	label variable spouse_industry_trade "Spouse Industry: Wholesale or retail trade"
	gen spouse_industry_hospitality = (INDGEN_SP == 70) & !missing(INDGEN_SP)
	label variable spouse_industry_hospitality "Spouse Industry: Hotels and restaurants"
	gen spouse_industry_logistics = (INDGEN_SP == 80) & !missing(INDGEN_SP)
	label variable spouse_industry_logistics "Spouse Industry: Transportation, storage, and communications"
	gen spouse_industry_finsurance = (INDGEN_SP == 90) & !missing(INDGEN_SP)
	label variable spouse_industry_finsurance "Spouse Industry: Financial services and insurance"
	gen spouse_industry_public = (INDGEN_SP == 100) & !missing(INDGEN_SP)
	label variable spouse_industry_public "Spouse Industry: Public admin and defense"
	gen spouse_industry_realestate = (INDGEN_SP == 111) & !missing(INDGEN_SP)
	label variable spouse_industry_realestate "Spouse Industry: Business services"
	gen spouse_industry_education = (INDGEN_SP == 112) & !missing(INDGEN_SP)
	label variable spouse_industry_education "Spouse Industry: Education"
	gen spouse_industry_health = (INDGEN_SP == 113) & !missing(INDGEN_SP)
	label variable spouse_industry_health "Spouse Industry: Health and social work"
	gen spouse_industry_other = (INDGEN_SP == 114) & !missing(INDGEN_SP)
	label variable spouse_industry_other "Spouse Industry: Other services"
	gen spouse_industry_hhservice = (INDGEN_SP == 120) & !missing(INDGEN_SP)
	label variable spouse_industry_hhservice "Spouse Industry: Private household services"

	** Occupation label
	gen spouse_occup_manager = (OCCISCO_SP == 1)
	gen spouse_occup_professional = (OCCISCO_SP == 2)
	gen spouse_occup_technic = (OCCISCO_SP == 3)
	gen spouse_occup_clerk = (OCCISCO_SP == 4)
	gen spouse_occup_service = (OCCISCO_SP == 5)
	gen spouse_occup_agro = (OCCISCO_SP == 6)
	gen spouse_occup_crafts = (OCCISCO_SP == 7)
	gen spouse_occup_machineop = (OCCISCO_SP == 8)
	gen spouse_occup_elementary = (OCCISCO_SP == 9)
	gen spouse_occup_army = (OCCISCO_SP == 10)
	
	label variable spouse_occup_agro "Spouse Occupation: Skilled agriculture and fishery"
	label variable spouse_occup_crafts "Spouse Occupation: Crafts and related trades"
	label variable spouse_occup_service "Spouse Occupation: Service workers"
	label variable spouse_occup_clerk "Spouse Occupation: Clerks"
	label variable spouse_occup_professional "Spouse Occupation: Professionals"
	label variable spouse_occup_manager "Spouse Occupation: Legislators or senior officials and managers"
	label variable spouse_occup_army "Spouse Occupation: Armed forces"
	label variable spouse_occup_technic "Spouse Occupation: Technicians and associate professionals"
	label variable spouse_occup_machineop "Spouse Occupation: machine operators"
	label variable spouse_occup_elementary "Spouse Occupation: elementary occupations"



********************************************************************************
* Label variables for employment status
********************************************************************************
	label variable EDATTAIN "Educational attainment"
	label variable EMPSTAT  "Employment status"
	label variable OCCISCO  "Occupation"
	label variable MARST    "Marital status"
	label variable INDGEN   "Industry of work"
	label variable NATIVITY "Nativity status"
	label variable EDATTAIN_SP "Spouse's educational attainment"
	label variable YRSCHOOL_SP "Spouse's years of schooling"
	label variable EMPSTAT_SP  "Spouse's employment status"
	label variable OCCISCO_SP  "Occupation"
	label variable INDGEN_SP   "Spouse's industry of work"
	label variable employed  "Employed"
	label variable unemployed "Unemployed"
	label variable inactive_lfp "Inactive in labor force"
	label variable no_educ "No education completed"
	label variable primary_educ "Completed primary education"
	label variable secondary_educ "Completed secondary education"
	label variable university_educ "Completed university education"
	label variable marstat_single "Single or never married"
	label variable marstat_married "Married"
	label variable marstat_divorced "Divorced or separated or spouse absent"
	label variable marstat_widow "Widowed"


	save "$dir/data/derived/census/africa-women-genvar.dta", replace
