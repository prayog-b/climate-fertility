********************************************************************************
* PROJECT:       Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:        Prayog Bhattarai
* DATE CREATED:  09-09-2025
* DATE MODIFIED: 08-10-2025
* DESCRIPTION:   Cleaning and generating variables for census children's dataset
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
			if "`username'" == "prayogbhattarai" {
				global cf "/Users/`username'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "/Users/`username'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
			}
			else if "`username'" == "prayog" {
				global cf "D:/`username'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "D:/`username'/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
			}
			else if "`username'" == "yogita" {
				global cf "/Users/`username'/NUS Dropbox/`username'/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "/Users/`username'/NUS Dropbox/`username'/Apps/Overleaf/climate-fertility-ssa/output"
			}
			else if inlist("`username'", "celin", "CE.4875") {
				global cf "/Users/`username'/Dropbox/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "/Users/`username'/Apps/Overleaf/climate-fertility-ssa/output"
			}
			else if "`username'" == "yun" {
				global cf "/Users/`username'/Dropbox/Climate_Change_and_Fertility_in_SSA"
				global cf_overleaf "/Users/`username'/Apps/Overleaf/climate-fertility-ssa/output"
			}
			else {
				di as error "Unknown user. Set global cf manually."
				exit 198
			}
		}
	end	

	ClimateAnalysisConfig, method("init")

	use "$cf/data/source/census/africa_children.dta", replace

	

********************************************************************************
* Generate basic demographic variables 
********************************************************************************	
	
	/*
	-------------------------------------------------------------------------------------------------------------------------------
				  storage   display    value
	variable name   type    format     label      variable label
	-------------------------------------------------------------------------------------------------------------------------------
	COUNTRY         int     %13.0g     COUNTRY    Country
	YEAR            long    %12.0g                Year
	PERWT           double  %10.0g                Sampling weight
	GEOLEV1         double  %10.0g                Place of residence (1st level)
	GEOLEV2         double  %10.0g                Place of residence (2nd level)
	RES_SMALLEST    double  %10.0g                Place of residence (smallest unit)
	AGE             long    %12.0g     AGE        Age
	SEX             long    %12.0g     SEX        Sex
	SCHOOL          long    %12.0g     SCHOOL     School attendance
	EDATTAIN        long    %12.0g     EDATTAIN   Educational attainment, international recode [general version]
	YRSCHOOL        long    %12.0g     YRSCHOOL   Years of schooling
	EMPSTAT         long    %12.0g     EMPSTAT    Activity status (employment status) [general version]
	MOMLOC          double  %10.0g     MOMLOC     Mother's location in household
	AGE_MOM         long    %12.0g     AGE_MOM    Mother's age
	NATIVITY_MOM    long    %12.0g     NATIVITY_MOM
												  Mother's nativity status
	BPL1_MOM        double  %10.0g                Mother's place of birth (1st level)
	BPL2_MOM        double  %10.0g                Mother's place of birth (2nd level)
	BIRTH_SMALLES~M double  %10.0g                Mother's place of birth (smallest unit)
	-------------------------------------------------------------------------------------------------------------------------------
	*/
	
	* Create a country code variable
	decode COUNTRY, gen(CNTRY)
	gen str3 country_code = ""
	replace country_code = "BEN" if lower(trim(CNTRY)) == "benin"
	replace country_code = "BWA" if lower(trim(CNTRY)) == "botswana"
	replace country_code = "BFA" if lower(trim(CNTRY)) == "burkina faso"
	replace country_code = "CMR" if lower(trim(CNTRY)) == "cameroon"
	replace country_code = "ETH" if lower(trim(CNTRY)) == "ethiopia"
	replace country_code = "GHA" if lower(trim(CNTRY)) == "ghana"
	replace country_code = "GIN" if lower(trim(CNTRY)) == "guinea"
	replace country_code = "KEN" if lower(trim(CNTRY)) == "kenya"
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
	replace africa_region = "West Africa" if inlist(country_code, "BEN", "BFA", "GHA", "GIN", "LBR", "MLI", "SLE", "TGO", "SEN")
	replace africa_region = "East Africa" if inlist(country_code, "ETH", "KEN", "TZA", "UGA", "SSD", "SDN")
	replace africa_region = "Southern Africa" if inlist(country_code, "BWA", "LSO", "MWI", "MOZ", "ZAF", "ZMB", "ZWE")
	replace africa_region = "Central Africa" if inlist(country_code, "CMR")
	
	
	
	** Create indicator's for availablility of mother's data
	* Imputing RES_SMALLEST
	count if missing(RES_SMALLEST) & (!missing(GEOLEV1) | !missing(GEOLEV2))
	
	di "If RES_SMALLEST is not known, impute it from GEOLEV2"
	replace RES_SMALLEST = GEOLEV2 if missing(RES_SMALLEST) & !missing(GEOLEV2)
	replace RES_SMALLEST = GEOLEV1 if missing(RES_SMALLEST) & missing(GEOLEV2) & !missing(GEOLEV1)
	
	* Has RES_SMALLEST 
	gen has_res_smallest = (!missing(RES_SMALLEST))
	label variable has_res_smallest "Has RESIDENTIAL SMALLEST"
	
	* Has mom RES_SMALLEST
	gen has_mom_res_smallest = (!missing(RES_SMALLEST))
	label variable has_mom_res_smallest "Has mother's RESIDENTIAL SMALLEST"
	
	* Has mom's BIRTH SMALLEST
	gen has_mom_birth_smallest = (!missing(BIRTH_SMALLEST_MOM))
	label variable has_mom_birth_smallest "Has mother's BIRTHPLACE SMALLEST"
	
	* Female
	gen female = (SEX == 2) & !missing(SEX)
	label variable female "Female"
	
	* Indicator for child attending school  
	gen has_schooling = (SCHOOL == 1) & !missing(SCHOOL)
	label variable has_schooling "Attending school"
	
	* Indicator for child ever attended school
	gen ever_schooling  = inlist(SCHOOL, 1, 3) & !missing(SCHOOL)
	label variable ever_schooling "Attending or ever attended school"
	
	* Indicator for child never having attended school
	gen never_school = inlist(SCHOOL, 4) & !missing(SCHOOL)
	label variable never_school "Never attended school"
	
	* Indicators for primary, secondary, and university-level educational attainment
	gen university_ed = (EDATTAIN == 1)
	gen secondary_ed  = (EDATTAIN == 2)
	gen primary_ed = (EDATTAIN == 3)
	gen less_primary_ed = (EDATTAIN == 1)
	label variable university_ed "University education completed"
	label variable secondary_ed "Secondary education completed"
	label variable primary_ed "Primary education completed"
	label variable less_primary_ed "Less than primary completed"
	
	* Years of schooling 
	/*	
			  Years of schooling |      Freq.     Percent        Cum.
	-----------------------------+-----------------------------------
			  None or pre-school | 11,455,279       34.09       34.09
						  1 year |  2,763,484        8.22       42.31
						 2 years |  2,375,149        7.07       49.38
						 3 years |  2,239,526        6.66       56.04
						 4 years |  2,048,437        6.10       62.14
						 5 years |  1,844,817        5.49       67.63
						 6 years |  1,824,922        5.43       73.06
						 7 years |  1,870,628        5.57       78.63
						 8 years |  1,134,695        3.38       82.00
						 9 years |    932,267        2.77       84.78
						10 years |    720,152        2.14       86.92
						11 years |    487,029        1.45       88.37
						12 years |    331,136        0.99       89.36
						13 years |     51,888        0.15       89.51
						14 years |      9,677        0.03       89.54
						15 years |      3,420        0.01       89.55
						16 years |        958        0.00       89.55
						17 years |        176        0.00       89.55
				18 years or more |      1,577        0.00       89.56
				   Not specified |     69,103        0.21       89.76
					Some primary |     42,902        0.13       89.89
	Some technical after primary |      2,105        0.01       89.90
				  Some secondary |     11,777        0.04       89.93
				   Some tertiary |      2,366        0.01       89.94
				  Adult literacy |     17,637        0.05       89.99
				 Unknown/missing |    224,114        0.67       90.66
		   NIU (not in universe) |  3,139,146        9.34      100.00
	-----------------------------+-----------------------------------
						   Total | 33,604,367      100.00

	*/
	* I'm setting the vague categories to missings for now.
	replace YRSCHOOL = . if inlist(YRSCHOOL, 90, 91, 92, 93, 94, 95, 98, 99)
	
	
	/*
	MOMLOC


	--------------------------------------------------------------------------------------------------------
	MOMLOC                                                                    Mother's location in household
	--------------------------------------------------------------------------------------------------------

					  type:  numeric (double)
					 label:  MOMLOC, but 97 nonmissing values are not labeled

					 range:  [0,98]                       units:  1
			 unique values:  98                       missing .:  980,598/40,214,606

				  examples:  0     
							 1     or higher = The person number of this person's mother
							 2     
							 2    

							NOTE:  I don't quite understand what this variable is trying to capture. 
	*/


	/*
	codebook NATIVITY_MOM

	--------------------------------------------------------------------------------------------------------
	NATIVITY_MOM                                                                    Mother's nativity status
	--------------------------------------------------------------------------------------------------------

					  type:  numeric (long)
					 label:  NATIVITY_MOM

					 range:  [0,9]                        units:  1
			 unique values:  4                        missing .:  17,447,758/40,214,606

				tabulation:  Freq.   Numeric  Label
							21,520         0  NIU (not in universe)
						22,135,627         1  Native-born
						   576,271         2  Foreign-born
							33,430         9  Unknown/missing
						17,447,758         .  

	*/
		gen mom_native_born = (NATIVITY == 1)
		label variable mom_native_born "Mother: Native-born" 
		gen mom_foreign_born = (NATIVITY == 2)
		label variable mom_foreign_born "Mother: Foreign-born"
		

********************************************************************************
* Generate variables for employment status 
********************************************************************************	
	/*
	EMPSTAT 
	----------------------------------------------------------------------
	EMPSTAT          Activity status (employment status) [general version]
	----------------------------------------------------------------------

					  type:  numeric (long)
					 label:  EMPSTAT

					 range:  [0,9]                        units:  1
			 unique values:  5                        missing .:  5,019,278/40,214,606

				tabulation:  Freq.   Numeric  Label
						 9,469,351         0  NIU (not in universe)
						 5,907,329         1  Employed
						   717,853         2  Unemployed
						18,806,308         3  Inactive
						   294,487         9  Unknown/missing
						 5,019,278         .  

	*/

	gen employed = (EMPSTAT == 1) & !missing(EMPSTAT)
	gen unemployed = (EMPSTAT == 2) & !missing(EMPSTAT) 
	gen lfp_inactive = (EMPSTAT == 3) & !missing(EMPSTAT)

	label variable employed "Employed"
	label variable unemployed "Unemployed"
	label variable lfp_inactive "Inactive in the labor force"	
	
	save "$cf/data/derived/census/africa-children-genvar.dta", replace