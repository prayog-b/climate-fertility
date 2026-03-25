clear all
set more off
eststo clear
clear matrix

*Set root:

if "`c(username)'" == "Frances" {
	gl cf_data "/Users/Frances/Dropbox/Climate_Change_and_Fertility_in_SSA/data"
}
if "`c(username)'" == "tvogl" {
	gl cf_data "/Users/tvogl/Dropbox/research/Climate_Change_and_Fertility_in_SSA/data"
}
if "`c(username)'" == "yun" {
	gl cf_data "/Users/yun/Dropbox/climate_fertility/Climate_Change_and_Fertility_in_SSA/data"
}
if "`c(username)'" == "prayogbhattarai" {
	gl cf_data "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data"
}
 
*************************************************************************

*Set directory
cd "$cf_data/source/dhs"

set sortseed 20181110
local countries_all "benin burkina_faso burundi cameroon central_african_republic chad comoros congo cote_divoire drc ethiopia gabon gambia ghana guinea kenya lesotho liberia madagascar malawi mali mozambique namibia niger nigeria rwanda senegal sierra_leone  south_africa swaziland tanzania togo uganda zambia zimbabwe " //not available: angola 

//leaves out last country
local countries "benin burkina_faso burundi cameroon central_african_republic chad comoros congo cote_divoire drc ethiopia gabon gambia ghana guinea kenya lesotho liberia madagascar malawi mali mozambique namibia niger nigeria rwanda senegal sierra_leone south_africa swaziland tanzania togo uganda zambia" //not available: angola
/*
**********
* 0. (Optional) Re-run country-level cleaning files - commented out for now; can use if modifications to cleaning are needed
**********
foreach x of local countries_all {
	display "`x'"
	quietly: do "`x'/`x'.do"
}

*/
clear
**********
* 1. Clean IR for mother's fertility and education time series
**********


*Read in data for each country
foreach x of local countries_all {
	display "`x'"
	use "`x'/`x'_dhs_IR.dta", clear
	gen country="`x'"
	decode reg, gen(regname)
	label drop reg
	tempfile tmp_`x'
	save `tmp_`x''
}

foreach x of local countries {
	display "`x'"
	quietly: append using "`tmp_`x''",force
}



replace country=subinstr(country, "_", " ",.) 
replace country=proper(country)
replace regname=proper(regname)
replace country="Cote d'Ivoire" if country=="Cote Divoire"
replace regname="Cote d'Ivoire" if regname=="Cote D'Ivoire"
replace regname="Boucle du Mouhoun" if regname=="Boucle Du Mouhoun"
replace regname="Rio de Janeiro" if regname=="Rio De Janeiro"
replace country="Congo Democratic Republic" if country=="Drc"
replace country="The Gambia" if country=="Gambia"
replace country="Timor-Leste" if country=="Timor Leste"
replace regname="Outer Java-Bali II" if regname=="Outer Java-Bali Ii"
replace regname="Punjab/ICT" if regname=="Punjab/Ict"
replace regname="NWFP" if regname=="Nwfp"
replace regname="FANA/Azad J&K" if regname=="Fana/Azad J&K"
replace regname="FATA" if regname=="Fata"
replace regname="NayPyitaw" if regname=="Naypyitaw"
replace regname="GBAO" if regname=="Gbao"
replace regname="DRS" if regname=="Drs"
replace regname="KwaZulu Natal" if regname=="Kwazulu Natal"
replace regname="SNNP" if regname=="Snnp"
replace regname="Orinoquia and Amazon" if regname=="Orinoquia And Amazon"
replace regname="Gracias a Dios" if regname=="Gracias A Dios"
replace regname="Islas de la Bahia" if regname=="Islas De La Bahia"
replace regname="Jammu and Kashmir" if regname=="Jammu And Kashmir"
replace regname="Upper West, East and Northern" if regname=="Upper West, East And Northern"
replace regname="Orinoquia and Amazon" if regname=="Orinoquia And Amazon"
replace regname="North Atlantic Autonomous Region (RAAN)" if regname=="North Atlantic Autonomous Region (Raan)"
replace regname="South Atlantic Autonomous Region (RAAS)" if regname=="South Atlantic Autonomous Region (Raas)"
replace regname = "Western" if regname == "North Western" & country == "Sierra Leone"

gen mother_id=_n
label var mother_id "unique mother ID"

label var reg "permanent region code"
label var regname "permanent region name"
label var country "country name"

merge m:1 country regname using "$cf_data/source/dhs/dhs_code.dta" 
drop if _merge == 2 // Sierra Leone Western and North Western unmatched
drop _merge 
gen id = DHS_code
merge m:1 DHS_code using "$cf_data/source/dhs/growing_onset.dta" //merge in growing season onset
drop _merge
gen mom_gyob = mom_yob if mom_mob>=onset_month //mom's growing year of birth
replace mom_gyob = mom_yob-1 if mom_mob<onset_month
gen int_gyear = int_year if int_month>=onset_month //interview growing year
replace int_gyear = int_year-1 if int_month<onset_month

gen birth_year = mom_gyob

//preliminary cleaning 
tab mom_age //main DHS ages are 15-49
drop if mom_age<15|mom_age>=50
count if id==. //missing region identifier
drop if id==.
drop caseid surveycd smpwt cluster region snewzon v101 sregnew sregnat szone secozn sregion sreg1 //drop extraneous survey & geographic variables
drop b0_* b4_* b5_* b7_* //dropping child vars not reqd

rename b?_0? b?_? 
forvalues i=1/20 {
  replace b2_`i' = b2_`i'-1 if b1_`i'<onset_month //child's growing year of birth
}

//expand dataset to inclue one obs per year of woman's childbearing career
gen career = (int_gyear-1)-(mom_gyob+14) //childbearing career from 15 years after birth to 1 year before survey
sum career
keep if career>0 /*career<=0 is a 15-16 year old who had not aged through an entire growing year prior to the survey growing year*/
expand career
drop career
bysort mother_id: gen mother_age = 14+_n
gen gyear = mom_gyob+mother_age
gen birth=0
gen birth_month1=.
gen birth_month2=.
gen birth_month3=.
gen birth_month4=.
forvalues j=1/20 {
	replace birth=birth+1 if (b2_`j'==gyear&b1_`j'>=onset_month+9&b1_`j'<.)|(b2_`j'==gyear+1&b1_`j'<onset_month+9)
	replace birth_month4=b1_`j' if (birth_month3<.&birth_month4==.)&((b2_`j'==gyear&b1_`j'>=onset_month+9&b1_`j'<.)|(b2_`j'==gyear+1&b1_`j'<onset_month+9))
	replace birth_month3=b1_`j' if (birth_month2<.&birth_month3==.)&((b2_`j'==gyear&b1_`j'>=onset_month+9&b1_`j'<.)|(b2_`j'==gyear+1&b1_`j'<onset_month+9))
	replace birth_month2=b1_`j' if (birth_month1<.&birth_month2==.)&((b2_`j'==gyear&b1_`j'>=onset_month+9&b1_`j'<.)|(b2_`j'==gyear+1&b1_`j'<onset_month+9))
	replace birth_month1=b1_`j' if (birth_month1==.)&((b2_`j'==gyear&b1_`j'>=onset_month+9&b1_`j'<.)|(b2_`j'==gyear+1&b1_`j'<onset_month+9))
}
drop b1_* b2_*
tab birth_month1 birth_month2 //most of these seem to be twins and to a lesser extent miscodes. not many 'irish' twins

//label variables and save
label var mother_age "Age of mother in growing year"
label var gyear "Growing year"
label var birth "Number of births conceinved in growing year"
label var birth_month1 "Month of birth, 1st birth in growing year"
label var birth_month2 "Month of birth, 1st birth in growing year"
label var birth_month3 "Month of birth, 1st birth in growing year"
label var birth_month4 "Month of birth, 1st birth in growing year"
order mother_id year birth birth_month* mother_age int_month int_year id country regname

save "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta", replace

use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" if mother_age<45&onset_month<.,clear //ages up to 44, drop 3 regions without growing season data
codebook mother_id
collapse birth (count) n=birth,by(id mother_age gyear)
label var birth "births per woman, full sample"
label var n "number women, full sample"
save "$cf_data/derived/dhs/08-dhs-birth-panels/region_year_age_birth_weather.dta",replace
use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" if urban==1&mother_age<45&onset_month<.,clear
collapse birth_urban=birth (count) n_urban=birth,by(id mother_age gyear)
label var birth_urban "births per woman, urban sample"
label var n_urban "number women, urban sample"
merge 1:1 id mother_age gyear using "$cf_data/derived/dhs/08-dhs-birth-panels/region_year_age_birth_weather.dta"
drop _merge
save "$cf_data/derived/dhs/08-dhs-birth-panels/region_year_age_birth_weather.dta",replace
use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" if urban==0&mother_age<45&onset_month<.,clear
collapse birth_rural=birth (count) n_rural=birth,by(id mother_age gyear)
label var birth_rural "births per woman, rural sample"
label var n_rural "number women, rural sample"
merge 1:1 id mother_age gyear using "$cf_data/derived/dhs/08-dhs-birth-panels/region_year_age_birth_weather.dta"
drop _merge
ren id DHS_code
ren gyear g_year

* Save the renamed birth panel so we can reload it after building the weather files
capture mkdir "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge"
save "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/region_year_age_birth_weather.dta", replace

**********
* 2. Build combined annual precipitation file and merge into birth panel
**********

* Source: cf-ssa-dm-11b outputs, 04-dhs-region-cohort-precipitation/01-annual-data/
* Merge key: DHS_code x year (= g_year in the birth panel)

local precip_dir "$cf_data/derived/dhs/04-dhs-region-cohort-precipitation/01-annual-data"

local first_precip = 1
foreach x of local countries_all {
	* Map underscore-separated country name to title-case name used by cf-ssa-dm-11b
	local cname = proper(subinstr("`x'", "_", " ", .))
	if "`x'" == "cote_divoire" local cname "Ivory Coast"
	if "`x'" == "drc"          local cname "Congo Democratic Republic"
	if "`x'" == "gambia"       local cname "The Gambia"

	local precip_file "`precip_dir'/`cname'_precip_annual.dta"
	capture confirm file "`precip_file'"
	if _rc {
		di "WARNING: Precip file not found for `cname', skipping: `precip_file'"
		continue
	}
	if `first_precip' {
		use "`precip_file'", clear
		keep DHS_code year precip precip_gs precip_ngs
		local first_precip = 0
	}
	else {
		preserve
			use "`precip_file'", clear
			keep DHS_code year precip precip_gs precip_ngs
			tempfile tp_`x'
			save `tp_`x''
		restore
		append using `tp_`x''
	}
}

label var precip     "Annual total precipitation (mm)"
label var precip_gs  "Annual growing season precipitation (mm)"
label var precip_ngs "Annual non-growing season precipitation (mm)"

duplicates drop DHS_code year, force  // safety: one obs per region-year
save "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/combined_precip_annual.dta", replace

* Reload the birth panel and merge in precipitation
use "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/region_year_age_birth_weather.dta", clear
rename g_year year  // align key name with weather file
merge m:1 DHS_code year using "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/combined_precip_annual.dta"
tab _merge
* _merge==1: birth-panel obs outside ERA coverage range — expected for very old/recent gyears
* _merge==2: weather years with no birth-panel match — fine, ERA spans 1940-2023
drop if _merge == 2
drop _merge
rename year g_year  // restore original name
save "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/region_year_age_birth_weather.dta", replace

**********
* 3. Merge annual temperature data into birth panel
**********

* Build a single combined annual temperature file across all countries.
* Source: cf-ssa-dm-11c outputs, 05-dhs-region-cohort-temperature/01-annual-data/
* Variables kept: temp_mean, temp_max, degree days (max and mean, all thresholds),
*   growing/non-growing season degree days, temperature bins.

local temp_dir "$cf_data/derived/dhs/05-dhs-region-cohort-temperature/01-annual-data"

local first_temp = 1
local countries_all "benin burkina_faso burundi cameroon central_african_republic chad comoros congo cote_divoire drc ethiopia gabon gambia ghana guinea kenya lesotho liberia madagascar malawi mali mozambique namibia niger nigeria rwanda senegal sierra_leone  south_africa swaziland tanzania togo uganda zambia zimbabwe " //not available: angola 
foreach x of local countries_all {
	local cname = proper(subinstr("`x'", "_", " ", .))
	if "`x'" == "cote_divoire" local cname "Ivory Coast"
	if "`x'" == "drc"          local cname "Congo Democratic Republic"
	if "`x'" == "gambia"       local cname "The Gambia"

	local temp_file "`temp_dir'/`cname'-temperature_annual.dta"
	capture confirm file "`temp_file'"
	if _rc {
		di "WARNING: Temperature file not found for `cname', skipping: `temp_file'"
		continue
	}
	if `first_temp' {
		use "`temp_file'", clear
		keep DHS_code year temp_mean temp_max ///
		     temp_max_less_18 temp_max_18_21 temp_max_21_24 ///
		     temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33 ///
		     temp_max_dd_* temp_mean_dd_*
		local first_temp = 0
	}
	else {
		preserve
			use "`temp_file'", clear
			keep DHS_code year temp_mean temp_max ///
			     temp_max_less_18 temp_max_18_21 temp_max_21_24 ///
			     temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33 ///
			     temp_max_dd_* temp_mean_dd_*
			tempfile t_`x'
			save `t_`x''
		restore
		append using `t_`x''
	}
}
*gen year = g_year
duplicates drop DHS_code year, force  // safety: one obs per region-year
save "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/combined_temp_annual.dta", replace

* Reload the precip-merged birth panel and merge in temperature
use "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/region_year_age_birth_weather.dta", clear
rename g_year year
merge m:1 DHS_code year using "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/combined_temp_annual.dta"
tab _merge
drop if _merge == 2
drop _merge
rename year g_year  // restore

**********
* 4. Final labels and save
**********

label var DHS_code "DHS region code"
label var g_year   "Growing year"

save "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge/region_year_age_birth_weather.dta", replace