********************************************************************************
* PROJECT:       Climate Change and Fertility in Sub-Saharan Africa
* FILE:          cf-ssa-dm-13b-dhs-birth-panel-lags.do
* DATE CREATED:  2026-03-09
* DESCRIPTION:   Integrated script that (1) builds the DHS birth panel at the
*                region × growing-year × mother-age level, (2) assembles the
*                combined annual precipitation and temperature files from
*                dm-11b/dm-11c outputs, (3) creates t-1 and t-2 lag variables
*                for all precipitation and degree-day variables, and (4) merges
*                contemporaneous + lagged weather into the birth panel.
*
* PRE-REQUISITES (must be run first):
*   dm-11b  →  $cf_data/.../04-dhs-region-cohort-precipitation/01-annual-data/
*   dm-11c  →  $cf_data/.../05-dhs-region-cohort-temperature/01-annual-data/
*
* INPUTS:
*   DHS IR files : $cf_data/source/dhs/[country]/[country]_dhs_IR.dta
*   DHS codes    : $cf_data/source/dhs/dhs_code.dta
*   Growing onset: $cf_data/source/dhs/growing_onset.dta
*   Precip files : $cf_data/derived/dhs/04-.../01-annual-data/[Country]_precip_annual.dta
*   Gammafit     : $cf_data/derived/dhs/04-.../02-gammafit-data/[CODE]_gammafit.dta
*   Temp files   : $cf_data/derived/dhs/05-.../01-annual-data/[Country]-temperature_annual.dta
*
* OUTPUTS:
*   Woman-year file (intermediate):
*     $cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta
*   Combined weather files (intermediate):
*     $cf_data/derived/dhs/09-.../combined_precip_annual.dta
*     $cf_data/derived/dhs/09-.../combined_temp_annual.dta
*   Final analysis panel:
*     $cf_data/derived/dhs/09-.../region_year_age_birth_weather.dta
*     Key: DHS_code × g_year × mother_age
*     Weather: contemporaneous (t), one-year lag (_L1), two-year lag (_L2)
*     Lag variables: precip, precip_gs, precip_ngs,
*                    abn_precip (dry/wet, gs/ngs variants),
*                    precip_spi, precip_gs_spi, precip_ngs_spi,
*                    temp_max_dd_*, temp_mean_dd_*
*                    (including growing-season _gs and non-growing-season _ngs variants)
********************************************************************************

clear all
capture file close tex
set more off
set sortseed 20181110

********************************************************************************
* 00. Setup — path globals
********************************************************************************

local username "`c(username)'"

if "`username'" == "prayogbhattarai" {
    gl cf_data "/Users/`username'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data"
}
else if "`username'" == "prayog" {
    gl cf_data "D:/`username'/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data"
}
else if "`username'" == "yogita" {
    gl cf_data "/Users/`username'/NUS Dropbox/`username'/Climate_Change_and_Fertility_in_SSA/data"
}
else if inlist("`username'", "celin", "CE.4875") {
    gl cf_data "/Users/`username'/Dropbox/Climate_Change_and_Fertility_in_SSA/data"
}
else if "`username'" == "yun" {
    gl cf_data "/Users/`username'/Dropbox/Climate_Change_and_Fertility_in_SSA/data"
}
else {
    di as error "Unknown user: `username'. Please set global cf_data manually."
    exit 198
}

* Output directories
capture mkdir "$cf_data/derived/dhs/08-dhs-birth-panels"
capture mkdir "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge"
local outdir "$cf_data/derived/dhs/09-dhs-birth-panel-weather-merge"

* Country list: all 35 SSA countries.
local countries_all "benin burkina_faso burundi cameroon central_african_republic chad comoros congo cote_divoire drc ethiopia gabon gambia ghana guinea kenya lesotho liberia madagascar malawi mali mozambique namibia niger nigeria rwanda     senegal sierra_leone south_africa swaziland tanzania togo uganda zambia zimbabwe"

********************************************************************************
* 01. Read and stack DHS IR files
*     Produces one obs per woman in each country's most recent DHS survey.
********************************************************************************

cd "$cf_data/source/dhs"

local first = 1
foreach x of local countries_all {
    di "Reading `x' ..."
    if `first' {
        use "`x'/`x'_dhs_IR.dta", clear
        gen country = "`x'"
        decode reg, gen(regname)
        label drop reg
        local first = 0
    }
    else {
        preserve
            use "`x'/`x'_dhs_IR.dta", clear
            gen country = "`x'"
            decode reg, gen(regname)
            label drop reg
            tempfile tmp_`x'
            save `tmp_`x''
        restore
        quietly append using `tmp_`x'', force
    }
}

* ---- Name cleaning ----
replace country = subinstr(country, "_", " ", .)
replace country = proper(country)
replace regname = proper(regname)

replace country  = "Cote d'Ivoire"             if country  == "Cote Divoire"
replace regname  = "Cote d'Ivoire"             if regname  == "Cote D'Ivoire"
replace regname  = "Boucle du Mouhoun"         if regname  == "Boucle Du Mouhoun"
replace country  = "Congo Democratic Republic" if country  == "Drc"
replace country  = "The Gambia"                if country  == "Gambia"
replace regname  = "KwaZulu Natal"             if regname  == "Kwazulu Natal"
replace regname  = "SNNP"                      if regname  == "Snnp"
replace regname  = "GBAO"                      if regname  == "Gbao"
replace regname  = "DRS"                       if regname  == "Drs"
replace regname  = "NayPyitaw"                 if regname  == "Naypyitaw"
replace regname  = "FATA"                      if regname  == "Fata"
replace regname  = "NWFP"                      if regname  == "Nwfp"
replace regname  = "Western"                   if regname  == "North Western" & country == "Sierra Leone"
replace regname  = "Upper West, East and Northern" ///
                                               if regname  == "Upper West, East And Northern"

gen mother_id = _n
label var mother_id "Unique mother ID"
label var reg       "Permanent region code"
label var regname   "Permanent region name"
label var country   "Country name"

********************************************************************************
* 02. Merge in DHS codes and growing season onset
********************************************************************************

merge m:1 country regname using "$cf_data/source/dhs/dhs_code.dta"
drop if _merge == 2   // Sierra Leone Western/North Western unmatched — expected
drop _merge

gen id = DHS_code

merge m:1 DHS_code using "$cf_data/source/dhs/growing_onset.dta"
drop _merge

* Growing-year adjustments: a growing year starts at onset_month and ends just
* before onset_month in the following calendar year.
gen mom_gyob = mom_yob if mom_mob >= onset_month
replace mom_gyob = mom_yob - 1 if mom_mob < onset_month

gen int_gyear = int_year if int_month >= onset_month
replace int_gyear = int_year - 1 if int_month < onset_month

gen birth_year = mom_gyob

********************************************************************************
* 03. Preliminary cleaning
********************************************************************************

drop if mom_age < 15 | mom_age >= 50
drop if id == .

drop caseid surveycd smpwt cluster region snewzon v101 ///
     sregnew sregnat szone secozn sregion sreg1

drop b0_* b4_* b5_* b7_*
rename b?_0? b?_?

* Adjust children's birth years to growing-year basis.
forvalues i = 1/20 {
    replace b2_`i' = b2_`i' - 1 if b1_`i' < onset_month
}

********************************************************************************
* 04. Expand to one obs per year of each woman's childbearing career
*     Career = growing years from age 15 to one year before the survey.
********************************************************************************

gen career = (int_gyear - 1) - (mom_gyob + 14)
sum career
* Exclude women who have not aged through a full growing year before the survey.
keep if career > 0
expand career
drop career

bysort mother_id: gen mother_age = 14 + _n
gen gyear = mom_gyob + mother_age

********************************************************************************
* 05. Count births per growing year
*     A birth is attributed to growing year g if conception fell in g
*     (i.e. birth occurred between onset_month+9 of g and onset_month+9 of g+1).
********************************************************************************

gen birth = 0
gen birth_month1 = .
gen birth_month2 = .
gen birth_month3 = .
gen birth_month4 = .

forvalues j = 1/20 {
    replace birth = birth + 1 ///
        if (b2_`j' == gyear     & b1_`j' >= onset_month + 9 & b1_`j' < .) ///
        |  (b2_`j' == gyear + 1 & b1_`j' <  onset_month + 9)

    replace birth_month4 = b1_`j' ///
        if (birth_month3 < . & birth_month4 == .) ///
        & ((b2_`j' == gyear & b1_`j' >= onset_month + 9 & b1_`j' < .) ///
        |  (b2_`j' == gyear + 1 & b1_`j' < onset_month + 9))

    replace birth_month3 = b1_`j' ///
        if (birth_month2 < . & birth_month3 == .) ///
        & ((b2_`j' == gyear & b1_`j' >= onset_month + 9 & b1_`j' < .) ///
        |  (b2_`j' == gyear + 1 & b1_`j' < onset_month + 9))

    replace birth_month2 = b1_`j' ///
        if (birth_month1 < . & birth_month2 == .) ///
        & ((b2_`j' == gyear & b1_`j' >= onset_month + 9 & b1_`j' < .) ///
        |  (b2_`j' == gyear + 1 & b1_`j' < onset_month + 9))

    replace birth_month1 = b1_`j' ///
        if (birth_month1 == .) ///
        & ((b2_`j' == gyear & b1_`j' >= onset_month + 9 & b1_`j' < .) ///
        |  (b2_`j' == gyear + 1 & b1_`j' < onset_month + 9))
}

drop b1_* b2_*

label var mother_age    "Age of mother in growing year"
label var gyear         "Growing year"
label var birth         "Number of births conceived in growing year"
label var birth_month1  "Month of birth, 1st birth in growing year"
label var birth_month2  "Month of birth, 2nd birth in growing year"
label var birth_month3  "Month of birth, 3rd birth in growing year"
label var birth_month4  "Month of birth, 4th birth in growing year"

order mother_id gyear birth birth_month* mother_age int_month int_year id country regname

save "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta", replace
di as result "✓ Woman-year file saved."

********************************************************************************
* 06. Collapse to region × growing-year × mother-age panel
*     Three subsamples: full, urban, rural.
********************************************************************************

* Full sample
use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" ///
    if mother_age < 45 & onset_month < ., clear

collapse birth (count) n = birth, by(id country mother_age gyear)
label var birth "Births per woman, full sample"
label var n     "Number of women, full sample"
save "`outdir'/region_year_age_birth_weather.dta", replace

* Urban subsample
use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" ///
    if urban == 1 & mother_age < 45 & onset_month < ., clear
collapse birth_urban = birth (count) n_urban = birth, by(id country mother_age gyear)
label var birth_urban "Births per woman, urban sample"
label var n_urban     "Number of women, urban sample"
merge 1:1 id country mother_age gyear using "`outdir'/region_year_age_birth_weather.dta"
drop _merge
save "`outdir'/region_year_age_birth_weather.dta", replace

* Rural subsample
use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" ///
    if urban == 0 & mother_age < 45 & onset_month < ., clear
collapse birth_rural = birth (count) n_rural = birth, by(id country mother_age gyear)
label var birth_rural "Births per woman, rural sample"
label var n_rural     "Number of women, rural sample"
merge 1:1 id country mother_age gyear using "`outdir'/region_year_age_birth_weather.dta"
drop _merge

* Rename keys to match weather data conventions.
rename id    DHS_code
rename gyear g_year

save "`outdir'/region_year_age_birth_weather.dta", replace
di as result "✓ Birth panel saved (DHS_code × g_year × mother_age)."

********************************************************************************
* 06b. Age-bin birth rate variables
*      Computes weighted mean birth rates and woman counts for:
*        5-year bins : 15-19, 20-24, 25-29, 30-34, 35-39, 40-44
*        2-group split: 15-29, 30-44
*      birth_XX_YY = sum(births) / count(women) — weighted mean birth rate.
*      n_XX_YY     = total women in bin (denominator for collapsing).
*      Merged m:1 on DHS_code × g_year; constant within each age bin cell.
*      Also adds age_bin5 and age_bin2 identifier variables to the panel so
*      a researcher can collapse to either bin level as needed.
********************************************************************************

* ---- 06b-i. 5-year age bins ----
use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" ///
    if mother_age < 45 & onset_month < ., clear

gen age_bin5 = 1519 * inrange(mother_age, 15, 19) ///
             + 2024 * inrange(mother_age, 20, 24) ///
             + 2529 * inrange(mother_age, 25, 29) ///
             + 3034 * inrange(mother_age, 30, 34) ///
             + 3539 * inrange(mother_age, 35, 39) ///
             + 4044 * inrange(mother_age, 40, 44)

collapse (sum) birth_sum = birth (count) n_bin = birth, ///
    by(id age_bin5 gyear)

gen birth_bin = birth_sum / n_bin
drop birth_sum

rename (id gyear) (DHS_code g_year)

reshape wide birth_bin n_bin, i(DHS_code g_year) j(age_bin5)

rename (birth_bin1519 birth_bin2024 birth_bin2529 ///
        birth_bin3034 birth_bin3539 birth_bin4044) ///
       (birth_15_19   birth_20_24   birth_25_29   ///
        birth_30_34   birth_35_39   birth_40_44)

rename (n_bin1519 n_bin2024 n_bin2529 ///
        n_bin3034 n_bin3539 n_bin4044) ///
       (n_15_19   n_20_24   n_25_29   ///
        n_30_34   n_35_39   n_40_44)

label var birth_15_19 "Births per woman, age 15-19 (weighted mean)"
label var birth_20_24 "Births per woman, age 20-24 (weighted mean)"
label var birth_25_29 "Births per woman, age 25-29 (weighted mean)"
label var birth_30_34 "Births per woman, age 30-34 (weighted mean)"
label var birth_35_39 "Births per woman, age 35-39 (weighted mean)"
label var birth_40_44 "Births per woman, age 40-44 (weighted mean)"
label var n_15_19     "Number of women, age 15-19"
label var n_20_24     "Number of women, age 20-24"
label var n_25_29     "Number of women, age 25-29"
label var n_30_34     "Number of women, age 30-34"
label var n_35_39     "Number of women, age 35-39"
label var n_40_44     "Number of women, age 40-44"

tempfile bins5
save `bins5'

* ---- 06b-ii. 2-group split ----
use "$cf_data/derived/dhs/08-dhs-birth-panels/woman-year.dta" ///
    if mother_age < 45 & onset_month < ., clear

gen age_bin2 = 1529 * inrange(mother_age, 15, 29) ///
             + 3044 * inrange(mother_age, 30, 44)

collapse (sum) birth_sum = birth (count) n_bin = birth, ///
    by(id age_bin2 gyear)

gen birth_bin = birth_sum / n_bin
drop birth_sum

rename (id gyear) (DHS_code g_year)

reshape wide birth_bin n_bin, i(DHS_code g_year) j(age_bin2)

rename (birth_bin1529 birth_bin3044) (birth_15_29 birth_30_44)
rename (n_bin1529     n_bin3044)     (n_15_29     n_30_44)

label var birth_15_29 "Births per woman, age 15-29 (weighted mean)"
label var birth_30_44 "Births per woman, age 30-44 (weighted mean)"
label var n_15_29     "Number of women, age 15-29"
label var n_30_44     "Number of women, age 30-44"

tempfile bins2
save `bins2'

* ---- 06b-iii. Merge bin variables into birth panel ----
use "`outdir'/region_year_age_birth_weather.dta", clear

* Age bin identifier variables (enable toggling between granularities)
gen age_bin5 = 1519 * inrange(mother_age, 15, 19) ///
             + 2024 * inrange(mother_age, 20, 24) ///
             + 2529 * inrange(mother_age, 25, 29) ///
             + 3034 * inrange(mother_age, 30, 34) ///
             + 3539 * inrange(mother_age, 35, 39) ///
             + 4044 * inrange(mother_age, 40, 44)
label var age_bin5 "5-year mother age bin (1519 = ages 15-19, etc.)"

gen age_bin2 = 1529 * inrange(mother_age, 15, 29) ///
             + 3044 * inrange(mother_age, 30, 44)
label var age_bin2 "2-group mother age split (1529 = 15-29, 3044 = 30-44)"

merge m:1 DHS_code g_year using `bins5'
drop if _merge == 2
drop _merge

merge m:1 DHS_code g_year using `bins2'
drop if _merge == 2
drop _merge

save "`outdir'/region_year_age_birth_weather.dta", replace
di as result "✓ Age-bin birth rate variables added (5-year bins and 2-group split)."

********************************************************************************
* 07. Assemble combined annual precipitation file
*     Source: dm-11b outputs, one file per country.
*     Variables kept: precip, precip_gs, precip_ngs (from annual files),
*       plus gamma-distribution-based variables from gammafit files:
*       abn_precip (dry/wet, gs/ngs variants), precip_spi, precip_gs_spi,
*       precip_ngs_spi.
*     Lag creation: t-1 (_L1) and t-2 (_L2) for all precipitation and
*       gamma-based variables.
*     Method: xtset DHS_code year + Stata time-series L. operator.
*             Gaps in the ERA5 record (if any) are handled correctly —
*             L.precip returns missing across a gap rather than a stale value.
********************************************************************************

local precip_dir "$cf_data/derived/dhs/04-dhs-region-cohort-precipitation/01-annual-data"
local gamma_dir  "$cf_data/derived/dhs/04-dhs-region-cohort-precipitation/02-gammafit-data"

* ---- 07a. Stack annual precipitation files ----

local first = 1
foreach x of local countries_all {
    * Map underscore-separated name → title-case country name used in filenames.
    local cname = proper(subinstr("`x'", "_", " ", .))
    if "`x'" == "cote_divoire" local cname "Ivory Coast"
    if "`x'" == "drc"          local cname "Congo Democratic Republic"
    if "`x'" == "gambia"       local cname "The Gambia"

    local f "`precip_dir'/`cname'_precip_annual.dta"
    capture confirm file "`f'"
    if _rc {
        di as text "  WARNING: Precip file not found for `cname' — skipping."
        continue
    }
    if `first' {
        use "`f'", clear
        keep DHS_code DHSREGEN year precip precip_gs precip_ngs
        local first = 0
    }
    else {
        preserve
            use "`f'", clear
            keep DHS_code DHSREGEN year precip precip_gs precip_ngs
            tempfile tp_p
            save `tp_p'
        restore
        append using `tp_p'
    }
}

label var precip     "Annual total precipitation (mm)"
label var precip_gs  "Annual growing-season precipitation (mm)"
label var precip_ngs "Annual non-growing-season precipitation (mm)"

duplicates drop DHS_code year, force  // safety: ensure one obs per region-year
sort DHS_code year

* ---- 07b. Stack gammafit files and merge into precipitation data ----
* Gammafit files are keyed by 3-letter country codes.  Map country names → codes.

tempfile precip_stacked
save `precip_stacked'

local first_gf = 1
foreach x of local countries_all {
    * Map country name → 3-letter ISO code used by gammafit filenames.
    local ccode ""
    if "`x'" == "benin"                    local ccode "BEN"
    if "`x'" == "burkina_faso"             local ccode "BFA"
    if "`x'" == "burundi"                  local ccode "BDI"
    if "`x'" == "cameroon"                 local ccode "CMR"
    if "`x'" == "central_african_republic" local ccode "CAF"
    if "`x'" == "chad"                     local ccode "TCD"
    if "`x'" == "comoros"                  local ccode "COM"
    if "`x'" == "congo"                    local ccode "COG"
    if "`x'" == "cote_divoire"             local ccode "IVC"
    if "`x'" == "drc"                      local ccode "DRC"
    if "`x'" == "ethiopia"                 local ccode "ETH"
    if "`x'" == "gabon"                    local ccode "GAB"
    if "`x'" == "gambia"                   local ccode "GMB"
    if "`x'" == "ghana"                    local ccode "GHA"
    if "`x'" == "guinea"                   local ccode "GIN"
    if "`x'" == "kenya"                    local ccode "KEN"
    if "`x'" == "lesotho"                  local ccode "LSO"
    if "`x'" == "liberia"                  local ccode "LBR"
    if "`x'" == "madagascar"               local ccode "MDG"
    if "`x'" == "malawi"                   local ccode "MWI"
    if "`x'" == "mali"                     local ccode "MLI"
    if "`x'" == "mozambique"               local ccode "MOZ"
    if "`x'" == "namibia"                  local ccode "NMB"
    if "`x'" == "niger"                    local ccode "NER"
    if "`x'" == "nigeria"                  local ccode "NGA"
    if "`x'" == "rwanda"                   local ccode "RWA"
    if "`x'" == "senegal"                  local ccode "SEN"
    if "`x'" == "sierra_leone"             local ccode "SLE"
    if "`x'" == "south_africa"             local ccode "ZAF"
    if "`x'" == "swaziland"                local ccode "SWZ"
    if "`x'" == "tanzania"                 local ccode "TZA"
    if "`x'" == "togo"                     local ccode "TGO"
    if "`x'" == "uganda"                   local ccode "UGA"
    if "`x'" == "zambia"                   local ccode "ZMB"
    if "`x'" == "zimbabwe"                 local ccode "ZWE"

    if "`ccode'" == "" {
        di as text "  WARNING: No country code mapping for `x' — skipping gammafit."
        continue
    }

    local gf "`gamma_dir'/`ccode'_gammafit.dta"
    capture confirm file "`gf'"
    if _rc {
        di as text "  WARNING: Gammafit file not found for `ccode' — skipping."
        continue
    }
    if `first_gf' {
        use "`gf'", clear
        keep DHS_code year ///
             abn_precip abn_precip_dry abn_precip_wet ///
             abn_precip_gs abn_precip_gs_dry abn_precip_gs_wet ///
             abn_precip_ngs abn_precip_ngs_dry abn_precip_ngs_wet ///
             precip_spi precip_gs_spi precip_ngs_spi
        local first_gf = 0
    }
    else {
        preserve
            use "`gf'", clear
            keep DHS_code year ///
                 abn_precip abn_precip_dry abn_precip_wet ///
                 abn_precip_gs abn_precip_gs_dry abn_precip_gs_wet ///
                 abn_precip_ngs abn_precip_ngs_dry abn_precip_ngs_wet ///
                 precip_spi precip_gs_spi precip_ngs_spi
            tempfile tp_gf
            save `tp_gf'
        restore
        append using `tp_gf'
    }
}

duplicates drop DHS_code year, force
sort DHS_code year

tempfile gamma_stacked
save `gamma_stacked'

* Merge gammafit variables into stacked precipitation data.
use `precip_stacked', clear
merge 1:1 DHS_code year using `gamma_stacked'
drop if _merge == 2   // gammafit obs with no matching precip — should not happen
drop _merge

label var abn_precip         "Abnormal precipitation (binary, gamma-based)"
label var abn_precip_dry     "Abnormally dry year (binary, gamma-based)"
label var abn_precip_wet     "Abnormally wet year (binary, gamma-based)"
label var abn_precip_gs      "Abnormal precipitation, growing season"
label var abn_precip_gs_dry  "Abnormally dry, growing season"
label var abn_precip_gs_wet  "Abnormally wet, growing season"
label var abn_precip_ngs     "Abnormal precipitation, non-growing season"
label var abn_precip_ngs_dry "Abnormally dry, non-growing season"
label var abn_precip_ngs_wet "Abnormally wet, non-growing season"
label var precip_spi         "Standardized Precipitation Index (annual)"
label var precip_gs_spi      "Standardized Precipitation Index (growing season)"
label var precip_ngs_spi     "Standardized Precipitation Index (non-growing season)"

sort DHS_code year

* ---- 07c. Create t-1 and t-2 lag variables ----
* year may be stored as float by dm-11b; convert to int before xtset.
qui replace year = round(year)
qui recast int year
xtset DHS_code year

foreach v of varlist precip precip_gs precip_ngs ///
                     abn_precip abn_precip_dry abn_precip_wet ///
                     abn_precip_gs abn_precip_gs_dry abn_precip_gs_wet ///
                     abn_precip_ngs abn_precip_ngs_dry abn_precip_ngs_wet ///
                     precip_spi precip_gs_spi precip_ngs_spi {
    local lbl : var label `v'
    gen `v'_L1 = L.`v'
    gen `v'_L2 = L2.`v'
    label var `v'_L1 "`lbl' (t-1 lag)"
    label var `v'_L2 "`lbl' (t-2 lag)"
}

xtset, clear   // clear xtset so the file can be freely merged later

save "`outdir'/combined_precip_annual.dta", replace
di as result "✓ Combined precipitation file saved with L1 and L2 lags (incl. gamma-based vars)."

********************************************************************************
* 08. Assemble combined annual temperature file
*     Source: dm-11c outputs, one file per country.
*     Variables kept: temp_mean, temp_max, temperature bins, all degree-day
*       variables (temp_max_dd_* and temp_mean_dd_*, including _gs and _ngs).
*     Lag creation: t-1 (_L1) and t-2 (_L2) for all degree-day variables only.
*       (temp_mean and temp_max levels, and temperature bins, are not lagged —
*        add them to the foreach loop below if needed in the future.)
********************************************************************************

local temp_dir "$cf_data/derived/dhs/05-dhs-region-cohort-temperature/01-annual-data"

local first = 1
foreach x of local countries_all {
    local cname = proper(subinstr("`x'", "_", " ", .))
    if "`x'" == "cote_divoire" local cname "Ivory Coast"
    if "`x'" == "drc"          local cname "Congo Democratic Republic"
    if "`x'" == "gambia"       local cname "The Gambia"

    * Temperature filenames use a dash separator (cf. underscore for precip).
    local f "`temp_dir'/`cname'-temperature_annual.dta"
    capture confirm file "`f'"
    if _rc {
        di as text "  WARNING: Temperature file not found for `cname' — skipping."
        continue
    }
    if `first' {
        use "`f'", clear
        keep DHS_code year temp_mean temp_max ///
             temp_max_less_18 temp_max_18_21 temp_max_21_24 ///
             temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33 ///
             temp_max_dd_* temp_mean_dd_*
        local first = 0
    }
    else {
        preserve
            use "`f'", clear
            keep DHS_code year temp_mean temp_max ///
                 temp_max_less_18 temp_max_18_21 temp_max_21_24 ///
                 temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33 ///
                 temp_max_dd_* temp_mean_dd_*
            tempfile tp_t
            save `tp_t'
        restore
        append using `tp_t'
    }
}

duplicates drop DHS_code year, force
sort DHS_code year

* ---- Create t-1 and t-2 lags for all degree-day variables ----
* This covers temp_max_dd_24 through temp_max_dd_38, temp_mean_dd_24 through
* temp_mean_dd_38, and all growing-season (_gs) and non-growing-season (_ngs)
* variants of each, yielding lags for all degree-day thresholds and season splits.
qui replace year = round(year)
qui recast int year
xtset DHS_code year

foreach v of varlist temp_max_dd_* temp_mean_dd_* {
    local lbl : var label `v'
    gen `v'_L1 = L.`v'
    gen `v'_L2 = L2.`v'
    label var `v'_L1 "`lbl' (t-1 lag)"
    label var `v'_L2 "`lbl' (t-2 lag)"
}

xtset, clear

save "`outdir'/combined_temp_annual.dta", replace
di as result "✓ Combined temperature file saved with L1 and L2 lags for all DD variables."

********************************************************************************
* 09. Merge weather (contemporaneous + lags) into birth panel
*     Merge key: DHS_code × year (= g_year in birth panel).
*     _merge==1: birth panel obs outside ERA coverage window — expected.
*     _merge==2: ERA obs with no birth panel match — ERA spans 1940–2023 while
*                the DHS birth window is a much shorter retrospective period.
*     We drop _merge==2 (weather-only) rows and keep all birth panel obs.
********************************************************************************

use "`outdir'/region_year_age_birth_weather.dta", clear
rename g_year year   // align key name with combined weather files

* --- Merge precipitation (contemporaneous + L1 + L2) ---
merge m:1 DHS_code year using "`outdir'/combined_precip_annual.dta"
tab _merge
drop if _merge == 2
drop _merge

* --- Merge temperature (contemporaneous + L1 + L2) ---
merge m:1 DHS_code year using "`outdir'/combined_temp_annual.dta"
tab _merge
drop if _merge == 2
drop _merge

rename year g_year   // restore original name

********************************************************************************
* 10. Final variable order, labels, and save
********************************************************************************

label var DHS_code   "DHS region code"
label var DHSREGEN   "DHS region name"
label var country    "Country name"
label var g_year     "Growing year"
label var mother_age "Age of mother in growing year"
label var birth      "Births per woman, full sample"
label var n          "Number of women, full sample"

* Bring identifying and birth outcome variables to the front.
order DHS_code DHSREGEN country g_year mother_age ///
      age_bin5 age_bin2 ///
      birth n birth_urban n_urban birth_rural n_rural ///
      birth_15_19 n_15_19 birth_20_24 n_20_24 ///
      birth_25_29 n_25_29 birth_30_34 n_30_34 ///
      birth_35_39 n_35_39 birth_40_44 n_40_44 ///
      birth_15_29 n_15_29 birth_30_44 n_30_44 ///
      precip precip_gs precip_ngs ///
      precip_L1 precip_gs_L1 precip_ngs_L1 ///
      precip_L2 precip_gs_L2 precip_ngs_L2 ///
      abn_precip abn_precip_dry abn_precip_wet ///
      abn_precip_gs abn_precip_gs_dry abn_precip_gs_wet ///
      abn_precip_ngs abn_precip_ngs_dry abn_precip_ngs_wet ///
      precip_spi precip_gs_spi precip_ngs_spi

sort DHS_code g_year mother_age

save "`outdir'/region_year_age_birth_weather.dta", replace

di as result ""
di as result "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
di as result "✓  Analysis panel complete:"
di as result "   `outdir'/region_year_age_birth_weather.dta"
di as result "   Key     : DHS_code × g_year × mother_age"
di as result "   Age bins: age_bin5 (5-year), age_bin2 (2-group)"
di as result "             birth_XX_YY / n_XX_YY for each bin"
di as result "   Weather : t (contemporaneous), t-1 (_L1), t-2 (_L2)"
di as result "   Lag vars: precip / precip_gs / precip_ngs"
di as result "             abn_precip (dry/wet, gs/ngs variants)"
di as result "             precip_spi / precip_gs_spi / precip_ngs_spi"
di as result "             temp_max_dd_[24–38] / temp_mean_dd_[24–38]"
di as result "             (plus _gs and _ngs variants of each DD variable)"
di as result "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
