********************************************************************************
* PROJECT:       Climate Change and Fertility in Sub-Saharan Africa
* AUTHOR:        Prayog Bhattarai
* DATE:          2026
* DESCRIPTION:   Comprehensive diagnostics for the DHS birth panel with
*                merged weather data (output of cf-ssa-dm-13).
*
* INPUT:         data/derived/dhs/09-dhs-birth-panel-weather-merge/
*                    region_year_age_birth_weather.dta
*
* OUTPUTS:       table/  -- LaTeX summary and diagnostic tables
*                figure/ -- PNG distribution and time-trend plots
*
* USAGE:         Safe to re-run at any time. All outputs are replaced.
*                Designed to be defensive: warnings printed to log rather
*                than halting execution, so the full script always completes.
********************************************************************************

********************************************************************************
* 0. Setup
********************************************************************************

clear all
capture file close tex      // close any stale file handle from a prior crashed run
set more off
set linesize 120

* ---- Paths ----
if "`c(username)'" == "prayogbhattarai" {
    global cf "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data"
    global cf_overleaf  "/Users/prayogbhattarai/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
}
else if "`c(username)'" == "prayog" {
    global cf "D:/prayog/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA/data"
    global cf_overleaf  "D:/prayog/NUS Dropbox/Prayog Bhattarai/Apps/Overleaf/climate-fertility-ssa/output"
}
* Add other users here as needed

* ---- Output sub-directories ----
capture mkdir "$cf_overleaf/table"
capture mkdir "$cf_overleaf/figure"

* ---- Log file ----
* Saved in the same derived folder as the panel. Replaced on each re-run.
local logdir "$cf/derived/dhs/09-dhs-birth-panel-weather-merge"
cap log close diagnostics
log using "`logdir'/cf-ssa-dm-14-diagnostics.log", replace name(diagnostics) text
di as result "Log opened: `logdir'/cf-ssa-dm-14-diagnostics.log"
di as result "Run date:   `c(current_date)' `c(current_time)'"

* ---- Country list (35 SSA countries) ----
local countries_all "benin burkina_faso burundi cameroon central_african_republic chad comoros congo cote_divoire drc ethiopia gabon gambia ghana guinea kenya lesotho liberia madagascar malawi mali mozambique namibia niger nigeria rwanda senegal sierra_leone south_africa swaziland tanzania togo uganda zambia zimbabwe"

* ---- Key variable groups ----
local birth_vars   "birth birth_urban birth_rural"
local birth_bin5   "birth_15_19 birth_20_24 birth_25_29 birth_30_34 birth_35_39 birth_40_44"
local birth_bin2   "birth_15_29 birth_30_44"
local n_bin5       "n_15_19 n_20_24 n_25_29 n_30_34 n_35_39 n_40_44"
local n_bin2       "n_15_29 n_30_44"
local precip_vars  "precip precip_gs precip_ngs"
local temp_vars    "temp_mean temp_max"
local dd_vars      "temp_max_dd_24 temp_max_dd_28 temp_max_dd_32 temp_max_dd_36"  // representative subset
local bin_vars     "temp_max_less_18 temp_max_18_21 temp_max_21_24 temp_max_24_27 temp_max_27_30 temp_max_30_33 temp_max_more_33"
local gamma_vars   "abn_precip abn_precip_dry abn_precip_wet abn_precip_gs abn_precip_gs_dry abn_precip_gs_wet abn_precip_ngs abn_precip_ngs_dry abn_precip_ngs_wet"
local spi_vars     "precip_spi precip_gs_spi precip_ngs_spi"

* ---- Load data ----
local panel "$cf/derived/dhs/09-dhs-birth-panel-weather-merge/region_year_age_birth_weather.dta"

capture confirm file "`panel'"
if _rc {
    di as error "FATAL: Panel file not found at: `panel'"
    di as error "Run cf-ssa-dm-13 first."
    exit 601
}

use "`panel'", clear
di as result _n "Loaded panel: `c(N)' observations, `c(k)' variables"


********************************************************************************
* 1. SANITY CHECKS
*    Failures print warnings to the log but do not halt execution.
*    A running tally of warnings is kept and reported at the end.
********************************************************************************

local n_warnings = 0
local warning_log ""

* Helper macro: log a warning
cap program drop log_warning
program define log_warning
    args msg
    di as error "  *** WARNING: `msg'"
end

di _n as result "========================================================"
di    as result " SECTION 1: SANITY CHECKS"
di    as result "========================================================"

* ---- 1.1 Identifier completeness ----
di _n "-- 1.1 Identifier completeness --"
foreach v in DHS_code mother_age g_year {
    qui count if missing(`v')
    if r(N) > 0 {
        log_warning "`v' has `r(N)' missing values — identifiers should never be missing"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' missing identifiers"
    }
    else {
        di "  ✓ `v': no missing values"
    }
}

* ---- 1.2 Panel uniqueness: one row per (DHS_code, mother_age, g_year) ----
di _n "-- 1.2 Panel uniqueness --"
duplicates report DHS_code mother_age g_year
qui duplicates tag DHS_code mother_age g_year, gen(_dup_tag)
qui count if _dup_tag > 0
if r(N) > 0 {
    log_warning "`r(N)' duplicate (DHS_code, mother_age, g_year) observations found"
    local ++n_warnings
    local warning_log "`warning_log' | `r(N)' duplicate panel rows"
}
else {
    di "  ✓ Panel is unique on (DHS_code, mother_age, g_year)"
}
drop _dup_tag

* ---- 1.3 Mother age range: should be 15–44 ----
di _n "-- 1.3 Mother age range --"
qui count if mother_age < 15 | mother_age > 44
if r(N) > 0 {
    log_warning "`r(N)' observations with mother_age outside [15,44]"
    local ++n_warnings
    local warning_log "`warning_log' | `r(N)' obs with mother_age outside [15,44]"
}
else {
    di "  ✓ All mother_age values in [15,44]"
}
qui sum mother_age
di "  mother_age: min=`r(min)', max=`r(max)', mean=`=round(r(mean),0.01)'"

* ---- 1.4 g_year range: plausible range 1940–2023 ----
di _n "-- 1.4 Growing year range --"
qui sum g_year
di "  g_year: min=`r(min)', max=`r(max)'"
if r(min) < 1940 {
    log_warning "g_year min (`r(min)') is before ERA coverage start (1940)"
    local ++n_warnings
    local warning_log "`warning_log' | g_year min `r(min)' < 1940"
}
if r(max) > 2023 {
    log_warning "g_year max (`r(max)') is after ERA coverage end (2023)"
    local ++n_warnings
    local warning_log "`warning_log' | g_year max `r(max)' > 2023"
}
di "  ✓ g_year range checked"

* ---- 1.5 Birth rate bounds ----
*
*  birth, birth_urban, birth_rural are mean births per woman in a cell.
*  Negative values are impossible and indicate a construction error.
*  Values > 1 are plausible in thin cells (e.g. twins in a cell with few
*  women) but are uncommon enough to warrant flagging.  Such cells are
*  saved to birth_rate_flags.dta for review. The panel data is not modified.
*
di _n "-- 1.5 Birth rate bounds --"

* (a) Missing-value count for each birth variable
foreach v of local birth_vars {
    capture confirm variable `v'
    if _rc continue
    qui count if missing(`v')
    if r(N) > 0 di "  `v': `r(N)' missing values (`=round(100*r(N)/_N, 0.1)'%)"
    else         di "  `v': no missing values"
}

* (b) Flag cells outside [0, 1] — negatives are errors; >1 warrants review
capture drop _birth_flag
qui gen byte _birth_flag = 0

foreach v of local birth_vars {
    capture confirm variable `v'
    if _rc continue

    * --- negative values ---
    qui count if `v' < 0 & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' negative values — impossible; construction error in dm-13"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' negative values (error)"
        qui replace _birth_flag = 1 if `v' < 0 & !missing(`v')
    }

    * --- values above 1 ---
    qui count if `v' > 1 & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' values > 1 — likely thin cells (twins/multiple births); review cell sizes"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' values > 1 (review)"
        qui replace _birth_flag = 1 if `v' > 1 & !missing(`v')

        * Drill-down: show the 10 worst offenders (largest value)
        di "  Top offenders for `v' > 1 (up to 10, sorted by magnitude):"
        preserve
            keep if `v' > 1 & !missing(`v')
            gsort -`v'
            local nshow = min(_N, 10)
            forval i = 1/`nshow' {
                local _dc  = DHS_code[`i']
                local _age = mother_age[`i']
                local _yr  = g_year[`i']
                local _n   = n[`i']
                local _bv  = round(`v'[`i'], 0.001)
                di "    DHS_code=`_dc'  age=`_age'  year=`_yr'  n=`_n'  `v'=`_bv'"
            }
        restore
    }

    * Summary line
    qui sum `v'
    di "  `v': min=`=round(r(min),0.001)', max=`=round(r(max),0.001)', mean=`=round(r(mean),0.001)'"
}

* (c) Save flag file if any out-of-range observations found
qui count if _birth_flag == 1
if r(N) > 0 {
    local n_flagged = r(N)
    di _n "  Saving `n_flagged' out-of-range cells to birth_rate_flags.dta ..."
    preserve
        keep if _birth_flag == 1

        * Retain identifying variables + cell size + all birth rates
        local keepvars "DHS_code g_year mother_age"
        capture confirm variable n
        if _rc == 0 local keepvars "`keepvars' n"
        foreach v of local birth_vars {
            capture confirm variable `v'
            if _rc == 0 local keepvars "`keepvars' `v'"
        }
        keep `keepvars'
        sort DHS_code g_year mother_age

        local flagfile "`logdir'/birth_rate_flags.dta"
        save "`flagfile'", replace
        di "  ✓ Flag file saved: `flagfile'"
        di "  Review these cells: most are thin cells with twins/multiples."
    restore
}
else {
    di "  ✓ All birth rate values in [0, 1]: no flags"
}
drop _birth_flag

* ---- 1.6 Weather missingness: flag countries with entirely missing weather ----
di _n "-- 1.6 Weather variable missingness --"
foreach v in precip temp_mean {
    capture confirm variable `v'
    if _rc {
        log_warning "Variable `v' not found in dataset"
        local ++n_warnings
        local warning_log "`warning_log' | `v' missing from dataset entirely"
        continue
    }
    qui count if missing(`v')
    local n_miss = r(N)
    local pct_miss = round(100 * `n_miss' / _N, 0.1)
    if `n_miss' > 0 {
        di "  WARNING: `v' has `n_miss' missing values (`pct_miss'% of panel)"
        * Check if missingness is concentrated in specific DHS_codes
        preserve
            keep if missing(`v')
            qui levelsof DHS_code, local(miss_codes)
            local n_miss_codes = wordcount("`miss_codes'")
        restore
        log_warning "`v': `n_miss' missing obs across `n_miss_codes' DHS regions"
        local ++n_warnings
        local warning_log "`warning_log' | `v' missing in `n_miss_codes' regions"
    }
    else {
        di "  ✓ `v': no missing values"
    }
}

* ---- 1.7 Precipitation internal consistency: precip_gs + precip_ngs ≈ precip ----
di _n "-- 1.7 Precipitation internal consistency (precip_gs + precip_ngs ≈ precip) --"
capture confirm variable precip precip_gs precip_ngs
if _rc == 0 {
    qui gen _precip_check = abs(precip - (precip_gs + precip_ngs))
    qui sum _precip_check
    if r(max) > 1 {  // tolerance: 1mm
        log_warning "precip != precip_gs + precip_ngs for some obs (max discrepancy: `=round(r(max),0.01)'mm)"
        local ++n_warnings
        local warning_log "`warning_log' | precip decomposition inconsistency (max gap `=round(r(max),0.01)'mm)"
    }
    else {
        di "  ✓ precip_gs + precip_ngs ≈ precip (max discrepancy: `=round(r(max),0.01)'mm)"
    }
    drop _precip_check
}
else {
    log_warning "Cannot check precipitation consistency — one or more precip variables missing"
    local ++n_warnings
}

* ---- 1.8 Temperature bins sum to ~365 days ----
di _n "-- 1.8 Temperature bins sum to ~365 days --"
capture confirm variable temp_max_less_18 temp_max_more_33
if _rc == 0 {
    qui egen _bin_sum = rowtotal(`bin_vars')
    qui sum _bin_sum
    di "  Temperature bin sum: min=`r(min)', max=`r(max)', mean=`=round(r(mean),0.1)'"
    qui count if abs(_bin_sum - 365) > 5 & !missing(_bin_sum)  // ±5 days tolerance (leap years)
    if r(N) > 0 {
        log_warning "`r(N)' obs where temperature bins do not sum to ~365 (±5 days)"
        local ++n_warnings
        local warning_log "`warning_log' | `r(N)' obs with bin sum ≠ 365"
    }
    else {
        di "  ✓ Temperature bins sum to ~365 days for all obs"
    }
    drop _bin_sum
}
else {
    log_warning "Cannot check temperature bin sum — bin variables missing"
}

* ---- 1.9 Degree days ordering: DD_24 >= DD_26 >= ... >= DD_38 ----
di _n "-- 1.9 Degree days monotonically decreasing with threshold --"
local prev_dd "temp_max_dd_24"
foreach thr in 26 28 30 32 34 36 38 {
    capture confirm variable temp_max_dd_`thr' `prev_dd'
    if _rc continue
    qui count if temp_max_dd_`thr' > `prev_dd' & !missing(temp_max_dd_`thr') & !missing(`prev_dd')
    if r(N) > 0 {
        log_warning "`r(N)' obs where temp_max_dd_`thr' > temp_max_dd_`=`thr'-2' (should be weakly decreasing)"
        local ++n_warnings
        local warning_log "`warning_log' | DD ordering violated at threshold `thr'C"
    }
    local prev_dd "temp_max_dd_`thr'"
}
di "  ✓ Degree day ordering checked"

* ---- 1.10 N women: sample size cells should be positive ----
di _n "-- 1.10 Women count cells --"
qui count if n <= 0 & !missing(n)
if r(N) > 0 {
    log_warning "`r(N)' obs with n (# women) <= 0"
    local ++n_warnings
    local warning_log "`warning_log' | `r(N)' obs with n <= 0"
}
else {
    di "  ✓ All n (# women) values are positive"
}
qui sum n
di "  n women per cell: min=`r(min)', max=`r(max)', mean=`=round(r(mean),0.1)', median=`=round(r(p50),0.1)'"

* ---- 1.11 DHS_code coverage: check how many of the 205 expected regions have weather ----
di _n "-- 1.11 DHS region coverage --"
qui levelsof DHS_code, local(all_codes)
local n_regions = wordcount("`all_codes'")
di "  Total DHS regions in panel: `n_regions'"

qui count if missing(precip)
local n_miss_precip = r(N)
if `n_miss_precip' > 0 {
    preserve
        keep if missing(precip)
        qui levelsof DHS_code, local(no_precip_codes)
        local n_no_precip = wordcount("`no_precip_codes'")
        di "  Regions with any missing precip: `n_no_precip'"
    restore
}

* ---- 1.12 Implausible weather values ----
di _n "-- 1.12 Implausible weather values --"

* precip == 0 (zero annual rainfall implausible across all SSA regions)
capture confirm variable precip
if _rc == 0 {
    qui count if precip == 0 & !missing(precip)
    if r(N) > 0 {
        log_warning "`r(N)' obs with precip == 0 (zero annual precipitation is implausible for SSA)"
        local ++n_warnings
        local warning_log "`warning_log' | `r(N)' obs with precip == 0"
        preserve
            keep if precip == 0
            qui levelsof DHS_code, local(zero_precip_codes)
            di "  Zero-precip DHS codes: `zero_precip_codes'"
        restore
    }
    else {
        di "  ✓ No observations with precip == 0"
    }
    * Also report how many obs fall below 50mm (suspicious but not impossible)
    qui count if precip < 50 & precip > 0 & !missing(precip)
    di "  Obs with 0 < precip < 50mm (flagged for review): `r(N)'"
}

* temp_max < temp_mean (physically impossible)
capture confirm variable temp_max temp_mean
if _rc == 0 {
    qui count if temp_max < temp_mean & !missing(temp_max) & !missing(temp_mean)
    if r(N) > 0 {
        log_warning "`r(N)' obs where temp_max < temp_mean (physically impossible)"
        local ++n_warnings
        local warning_log "`warning_log' | `r(N)' obs with temp_max < temp_mean"
    }
    else {
        di "  ✓ temp_max >= temp_mean for all observations"
    }
    qui gen _temp_gap = temp_max - temp_mean
    qui sum _temp_gap
    di "  temp_max minus temp_mean: min=`=round(r(min),0.01)', max=`=round(r(max),0.01)', mean=`=round(r(mean),0.01)' (°C)"
    drop _temp_gap
}

* ---- 1.13 Zero-cell check ----
*
*  Two distinct questions:
*
*  (A) Orphan region-YEARS: weather data has (DHS_code, year) pairs that do
*      not appear in the birth panel.  This is EXPECTED: ERA5 covers years
*      before the earliest DHS retrospective birth year and after the latest
*      survey year.  It is informational, NOT a warning.
*
*  (B) Orphan DHS_CODEs: weather data contains a DHS_code that is completely
*      absent from the birth panel (for any year).  This is a TRUE mismatch
*      — it means that region's weather was merged but no birth observations
*      exist for it at all, so those rows contribute no information.
*      This IS a warning requiring upstream investigation.
*
di _n "-- 1.13 Zero-cell check --"
local combined_precip "$cf/derived/dhs/09-dhs-birth-panel-weather-merge/combined_precip_annual.dta"
capture confirm file "`combined_precip'"
if _rc {
    di "  Skipping zero-cell check — combined_precip_annual.dta not found"
}
else {
    * Build unique region × year index from the birth panel
    preserve
        keep DHS_code g_year
        duplicates drop DHS_code g_year, force
        rename g_year year
        tempfile panel_regyears
        save `panel_regyears'
        * Also build a unique DHS_code-only index for the code-level check
        keep DHS_code
        duplicates drop DHS_code, force
        tempfile panel_regcodes
        save `panel_regcodes'
    restore

    * --- (A) Orphan region-years (informational) ---
    preserve
        use "`combined_precip'", clear
        keep DHS_code year
        duplicates drop DHS_code year, force
        qui count
        local n_weather_regyears = r(N)
        merge 1:1 DHS_code year using `panel_regyears', keep(master) nogenerate
        qui count
        local n_orphan_regyears = r(N)
        local pct_orphan = round(100 * `n_orphan_regyears' / `n_weather_regyears', 0.1)
        di "  Weather region-years total:       `n_weather_regyears'"
        di "  Orphan region-years (no birth obs): `n_orphan_regyears' (`pct_orphan'% of weather data)"
        di "  NOTE: Orphan region-years are expected — ERA5 covers years"
        di "        outside the DHS retrospective birth window.  Not a mismatch."
    restore

    * --- (B) Orphan DHS_codes — TRUE mismatch if any found ---
    preserve
        use "`combined_precip'", clear
        keep DHS_code
        duplicates drop DHS_code, force
        qui count
        local n_weather_codes = r(N)
        merge 1:1 DHS_code using `panel_regcodes', keep(master) nogenerate
        qui count
        local n_orphan_codes = r(N)
        if `n_orphan_codes' > 0 {
            qui levelsof DHS_code, local(orphan_dhs_codes)
            log_warning "`n_orphan_codes' DHS_code(s) are in the weather data but NEVER appear in the birth panel — true mismatch: `orphan_dhs_codes'"
            local ++n_warnings
            local warning_log "`warning_log' | `n_orphan_codes' DHS_code(s) in weather but absent from birth panel (true mismatch)"
        }
        else {
            di "  ✓ All `n_weather_codes' weather DHS_codes appear in the birth panel (no true mismatch)"
        }
    restore
}

* ---- 1.14 Gamma-based precipitation variables: missingness and range ----
di _n "-- 1.14 Gamma-based precipitation variables --"

* (a) Missingness
foreach v of local gamma_vars {
    capture confirm variable `v'
    if _rc continue
    qui count if missing(`v')
    if r(N) > 0 {
        di "  `v': `r(N)' missing values (`=round(100*r(N)/_N, 0.1)'%)"
    }
}

* (b) abn_precip variables should be binary (0 or 1)
foreach v of local gamma_vars {
    capture confirm variable `v'
    if _rc continue
    qui count if !inlist(`v', 0, 1) & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' values not in {0, 1} — should be binary"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' non-binary values"
    }
}
di "  ✓ Abnormal precipitation binary checks complete"

* (c) SPI range: should typically be in [-4, 4]; values outside are suspicious
foreach v of local spi_vars {
    capture confirm variable `v'
    if _rc continue
    qui sum `v'
    di "  `v': min=`=round(r(min),0.01)', max=`=round(r(max),0.01)', mean=`=round(r(mean),0.01)'"
    qui count if abs(`v') > 4 & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' values with |SPI| > 4 — extreme, possibly spurious"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' values with |SPI| > 4"
    }
}
di "  ✓ SPI range checks complete"

* (d) Internal consistency: abn_precip should be 1 whenever abn_precip_dry or abn_precip_wet is 1
capture confirm variable abn_precip abn_precip_dry abn_precip_wet
if _rc == 0 {
    qui count if (abn_precip_dry == 1 | abn_precip_wet == 1) & abn_precip == 0 & !missing(abn_precip)
    if r(N) > 0 {
        log_warning "`r(N)' obs where abn_precip_dry or _wet == 1 but abn_precip == 0"
        local ++n_warnings
        local warning_log "`warning_log' | abn_precip inconsistency with dry/wet"
    }
    else {
        di "  ✓ abn_precip consistent with abn_precip_dry and abn_precip_wet"
    }
}

* ---- 1.15 Age-bin birth rate variables ----
di _n "-- 1.15 Age-bin birth rate variables --"

* (a) Check that age_bin5 and age_bin2 identifiers exist and are correct
capture confirm variable age_bin5 age_bin2
if _rc {
    log_warning "age_bin5 and/or age_bin2 not found — run updated dm-13b"
    local ++n_warnings
    local warning_log "`warning_log' | age_bin5/age_bin2 missing from dataset"
}
else {
    * Verify age_bin5 assignment
    qui count if age_bin5 == 0 | missing(age_bin5)
    if r(N) > 0 {
        log_warning "`r(N)' obs with missing or zero age_bin5"
        local ++n_warnings
        local warning_log "`warning_log' | `r(N)' obs with invalid age_bin5"
    }
    else {
        di "  ✓ age_bin5 assigned for all observations"
    }
    di "  ✓ age_bin2 checked"
}

* (b) Check that bin-level birth rates exist; flag negatives as errors,
*     values > 1 as noteworthy (plausible with twins in thin cells)
foreach v of local birth_bin5 {
    capture confirm variable `v'
    if _rc continue
    qui count if `v' < 0 & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' negative values — construction error"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' negative values (error)"
    }
    qui count if `v' > 1 & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' values > 1 — likely thin cells (twins/multiples)"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' values > 1 (review)"
    }
}
foreach v of local birth_bin2 {
    capture confirm variable `v'
    if _rc continue
    qui count if `v' < 0 & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' negative values — construction error"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' negative values (error)"
    }
    qui count if `v' > 1 & !missing(`v')
    if r(N) > 0 {
        log_warning "`v': `r(N)' values > 1 — likely thin cells (twins/multiples)"
        local ++n_warnings
        local warning_log "`warning_log' | `v' has `r(N)' values > 1 (review)"
    }
}
di "  ✓ Age-bin birth rate range checks complete"

* (c) Consistency: bin birth rate should be constant within each bin-region-year
capture confirm variable birth_15_19 age_bin5
if _rc == 0 {
    foreach v of local birth_bin5 {
        capture confirm variable `v'
        if _rc continue
        qui bysort DHS_code g_year age_bin5: egen _sd_`v' = sd(`v')
        qui sum _sd_`v'
        if r(max) > 0.0001 & !missing(r(max)) {
            log_warning "`v' varies within bin-region-year cells (max SD: `=round(r(max),0.0001)')"
            local ++n_warnings
            local warning_log "`warning_log' | `v' not constant within bin"
        }
        drop _sd_`v'
    }
    di "  ✓ Bin-level birth rates are constant within each age bin × region × year"
}

* (d) Summary statistics for bin birth rates
foreach v of local birth_bin5 {
    capture confirm variable `v'
    if _rc continue
    qui sum `v'
    di "  `v': N=`r(N)', mean=`=round(r(mean),0.001)', min=`=round(r(min),0.001)', max=`=round(r(max),0.001)'"
}
foreach v of local birth_bin2 {
    capture confirm variable `v'
    if _rc continue
    qui sum `v'
    di "  `v': N=`r(N)', mean=`=round(r(mean),0.001)', min=`=round(r(min),0.001)', max=`=round(r(max),0.001)'"
}

* ---- Warning summary ----
di _n as result "========================================================"
di    as result " SANITY CHECK SUMMARY: `n_warnings' warnings"
if `n_warnings' > 0 {
    di as error  " Warnings:"
    local i = 1
    foreach w of local warning_log {
        di as error "   `i'. `w'"
        local ++i
    }
}
else {
    di as result " All sanity checks passed."
}
di    as result "========================================================"


********************************************************************************
* 2. OVERALL SUMMARY STATISTICS TABLE (LaTeX)
********************************************************************************

di _n as result "========================================================"
di    as result " SECTION 2: OVERALL SUMMARY STATISTICS"
di    as result "========================================================"

* Compute stats for key variables
local sumvars "birth birth_urban birth_rural n birth_15_19 birth_20_24 birth_25_29 birth_30_34 birth_35_39 birth_40_44 n_15_19 n_20_24 n_25_29 n_30_34 n_35_39 n_40_44 birth_15_29 birth_30_44 n_15_29 n_30_44 precip precip_gs precip_ngs abn_precip abn_precip_dry abn_precip_wet precip_spi precip_gs_spi precip_ngs_spi temp_mean temp_max temp_max_dd_24 temp_max_dd_28 temp_max_dd_32 temp_max_dd_36"

* Open LaTeX file
file open tex using "$cf_overleaf/table/panel_summary_stats.tex", write replace

file write tex "\begin{tabular}{l r r r r r}" _n
file write tex "\toprule" _n
file write tex "Variable & N & Mean & SD & Min & Max \\" _n
file write tex "\midrule" _n

foreach v of local sumvars {
    capture confirm variable `v'
    if _rc {
        file write tex "`v' & \multicolumn{5}{c}{not in dataset} \\" _n
        continue
    }
    qui sum `v'
    local vn  = r(N)
    local vmn = round(r(mean), 0.001)
    local vsd = round(r(sd),   0.001)
    local vmi = round(r(min),  0.001)
    local vmx = round(r(max),  0.001)

    * Clean up variable label for display
    local vlbl: var label `v'
    if "`vlbl'" == "" local vlbl "`v'"

    file write tex "`vlbl' & `vn' & `vmn' & `vsd' & `vmi' & `vmx' \\" _n
}

file write tex "\bottomrule" _n
file write tex "\end{tabular}" _n
file close tex

di "  ✓ Summary stats table saved: $cf_overleaf/table/panel_summary_stats.tex"


********************************************************************************
* 3. BY-COUNTRY DIAGNOSTIC TABLE (LaTeX)
*    For each country: regions, g_year range, mean birth, mean precip,
*    mean temp_mean, weather missing flag.
********************************************************************************

di _n as result "========================================================"
di    as result " SECTION 3: BY-COUNTRY DIAGNOSTIC TABLE"
di    as result "========================================================"

* We need country names — merge in dhs_code.dta to get country labels
* Alternatively, build from DHS_code ranges. Use the source dhs_code crosswalk.
local crosswalk "$cf/source/dhs/dhs_code.dta"
capture confirm file "`crosswalk'"
if _rc {
    di "  WARNING: dhs_code crosswalk not found — country names will not be available"
    gen country_name = string(DHS_code)
}
else {
    merge m:1 DHS_code using "`crosswalk'", keepusing(country) nogenerate keep(master match)
    rename country country_name
    replace country_name = "" if missing(country_name)
}

* Collapse to country level
preserve
    * Tag whether weather is missing
    gen byte miss_precip  = missing(precip)
    gen byte miss_temp    = missing(temp_mean)

    collapse ///
        (min)   gyear_min  = g_year ///
        (max)   gyear_max  = g_year ///
        (mean)  mean_birth = birth ///
        (mean)  mean_precip = precip ///
        (mean)  mean_temp  = temp_mean ///
        (max)   any_miss_precip = miss_precip ///
        (max)   any_miss_temp   = miss_temp ///
        (count) n_obs      = birth ///
        (first) cname      = country_name ///
        , by(DHS_code)

    * Count regions per country
    bysort cname: gen n_regions = _N
    bysort cname: keep if _n == 1

    sort cname

    * Write LaTeX
    file open tex using "$cf_overleaf/table/panel_country_diagnostics.tex", write replace

    file write tex "\begin{tabular}{l r r r r r r r r}" _n
    file write tex "\toprule" _n
    file write tex "Country & Regions & g-year Min & g-year Max & Obs & Mean Birth & Mean Precip & Mean Temp & Weather \\" _n
    file write tex "        &         &            &            &     & (per woman) & (mm/yr)   & (°C)     & Missing? \\" _n
    file write tex "\midrule" _n

    local N = _N
    forval i = 1/`N' {
        local cn   = cname[`i']
        if "`cn'" == "" local cn "DHS\_code " + string(DHS_code[`i'])
        local nr   = n_regions[`i']
        local ymin = gyear_min[`i']
        local ymax = gyear_max[`i']
        local nobs = n_obs[`i']
        local mb   = round(mean_birth[`i'],  0.001)
        local mp   = round(mean_precip[`i'], 0.1)
        local mt   = round(mean_temp[`i'],   0.01)
        local miss_flag = ""
        if any_miss_precip[`i'] == 1 | any_miss_temp[`i'] == 1 {
            local miss_flag "\textbf{YES}"
        }
        else {
            local miss_flag "No"
        }
        file write tex "`cn' & `nr' & `ymin' & `ymax' & `nobs' & `mb' & `mp' & `mt' & `miss_flag' \\" _n
    }

    file write tex "\bottomrule" _n
    file write tex "\end{tabular}" _n
    file close tex

    di "  ✓ Country diagnostics table saved: $cf_overleaf/table/panel_country_diagnostics.tex"
restore


********************************************************************************
* 4. MISSING COUNTRIES TABLE (LaTeX)
*    Countries in countries_all that have no observations in the panel at all.
********************************************************************************

di _n as result "========================================================"
di    as result " SECTION 4: MISSING COUNTRIES CHECK"
di    as result "========================================================"

* Get the list of country names present in the panel
capture confirm variable country_name
if _rc {
    di "  Skipping missing countries check — no country_name variable"
}
else {
    * Build expected country list (proper-cased, with special cases)
    local expected_countries ""
    foreach x of local countries_all {
        local cname = proper(subinstr("`x'", "_", " ", .))
        if "`x'" == "cote_divoire" local cname "Cote d'Ivoire"
        if "`x'" == "drc"          local cname "Congo Democratic Republic"
        if "`x'" == "gambia"       local cname "The Gambia"
        local expected_countries `"`expected_countries' "`cname'""'
    }

    file open tex using "$cf_overleaf/table/panel_missing_countries.tex", write replace
    file write tex "\begin{tabular}{l l}" _n
    file write tex "\toprule" _n
    file write tex "Country & Status \\" _n
    file write tex "\midrule" _n

    local n_missing_countries = 0
    foreach cname of local expected_countries {
        qui count if country_name == "`cname'"
        if r(N) == 0 {
            file write tex "`cname' & \textbf{MISSING — no panel observations} \\" _n
            di "  *** MISSING: `cname' has no observations in the panel"
            local ++n_missing_countries
        }
        else {
            * Check if weather is also missing for this country
            qui count if country_name == "`cname'" & missing(precip)
            if r(N) > 0 {
                file write tex "`cname' & Present but weather missing \\" _n
                di "  WARNING: `cname' present but has missing weather data"
            }
        }
    }

    if `n_missing_countries' == 0 {
        file write tex "\multicolumn{2}{l}{All 35 countries present in panel} \\" _n
        di "  ✓ All expected countries present in panel"
    }

    file write tex "\bottomrule" _n
    file write tex "\end{tabular}" _n
    file close tex
    di "  ✓ Missing countries table saved: $cf_overleaf/table/panel_missing_countries.tex"
    di "  Total missing countries: `n_missing_countries'"
}


********************************************************************************
* 4b. WITHIN VS. BETWEEN VARIATION TABLE (LaTeX)
*     Decomposes total variance into between-region and within-region components
*     for key weather variables. Computed at the region-year level (weather is
*     constant across ages within a region-year).
********************************************************************************

di _n as result "========================================================"
di    as result " SECTION 4b: WITHIN VS. BETWEEN VARIATION"
di    as result "========================================================"

local wbvars "precip precip_spi abn_precip temp_mean temp_max temp_max_dd_28"

capture file close tex      // safety: close stale handle if prior run crashed here
file open tex using "$cf_overleaf/table/panel_within_between.tex", write replace
file write tex "\begin{tabular}{l r r r r r}" _n
file write tex "\toprule" _n
file write tex "Variable & Overall SD & Between-region SD & Within-region SD & Min & Max \\" _n
file write tex "\midrule" _n

preserve
    * Collapse to region-year level: weather is constant across ages,
    * so one observation per (DHS_code, g_year) is the correct unit.
    bysort DHS_code g_year: keep if _n == 1

    foreach v of local wbvars {
        capture confirm variable `v'
        if _rc {
            file write tex "`v' & \multicolumn{5}{c}{not in dataset} \\" _n
            continue
        }

        * Overall SD and range
        qui sum `v'
        local sd_total   = round(r(sd),  0.01)
        local vmin       = round(r(min), 0.01)
        local vmax       = round(r(max), 0.01)
        local grand_mean = r(mean)

        * Between-region SD: SD of region means
        qui bysort DHS_code: egen _rmean_`v' = mean(`v')
        qui sum _rmean_`v'
        local sd_between = round(r(sd), 0.01)

        * Within-region SD: SD after demeaning by region (recentred at grand mean)
        qui gen _within_`v' = `v' - _rmean_`v' + `grand_mean'
        qui sum _within_`v'
        local sd_within = round(r(sd), 0.01)

        drop _rmean_`v' _within_`v'

        local vlbl: var label `v'
        if "`vlbl'" == "" local vlbl "`v'"
        file write tex "`vlbl' & `sd_total' & `sd_between' & `sd_within' & `vmin' & `vmax' \\" _n
        di "  `v': total SD=`sd_total'  between SD=`sd_between'  within SD=`sd_within'"
    }
restore

file write tex "\bottomrule" _n
file write tex "\end{tabular}" _n
file close tex
di "  ✓ Within vs. between variation table saved: $cf_overleaf/table/panel_within_between.tex"


********************************************************************************
* 4c. DECADE-LEVEL SUMMARY TABLE (LaTeX)
*     Mean birth rate, precipitation, temperature, and DD28 by decade,
*     with observation counts and distinct region counts.
********************************************************************************

di _n as result "========================================================"
di    as result " SECTION 4c: DECADE-LEVEL SUMMARY"
di    as result "========================================================"

capture file close tex      // safety: close stale handle if prior run crashed here
file open tex using "$cf_overleaf/table/panel_decade_summary.tex", write replace
file write tex "\begin{tabular}{l r r r r r r r r}" _n
file write tex "\toprule" _n
file write tex "Decade & Obs & Regions & Mean Birth & Mean Precip & Mean SPI & Shr Abn & Mean Temp & Mean DD28 \\" _n
file write tex "       &     &         & (per woman) & (mm/yr)    &          &         & (\$^\circ\$C) & (÷100)  \\" _n
file write tex "\midrule" _n

preserve
    gen decade = floor(g_year / 10) * 10

    * Count distinct DHS regions contributing to each decade
    bysort decade DHS_code: gen _first_in_decade = (_n == 1)
    bysort decade: egen n_regions_decade = total(_first_in_decade)
    drop _first_in_decade

    collapse ///
        (count)  n_obs          = birth         ///
        (mean)   mean_birth     = birth         ///
        (mean)   mean_precip    = precip        ///
        (mean)   mean_spi       = precip_spi    ///
        (mean)   mean_abn       = abn_precip    ///
        (mean)   mean_temp      = temp_mean     ///
        (mean)   mean_dd28      = temp_max_dd_28 ///
        (first)  n_regions      = n_regions_decade ///
        , by(decade)

    sort decade

    local N = _N
    forval i = 1/`N' {
        local dec  = decade[`i']
        local nobs = n_obs[`i']
        local nreg = n_regions[`i']
        local mb   = round(mean_birth[`i'],  0.003)
        local mp   = cond(missing(mean_precip[`i']), ".", string(round(mean_precip[`i'], 0.1)))
        local mspi = cond(missing(mean_spi[`i']),    ".", string(round(mean_spi[`i'],    0.01)))
        local mabn = cond(missing(mean_abn[`i']),    ".", string(round(mean_abn[`i'],    0.01)))
        local mt   = cond(missing(mean_temp[`i']),   ".", string(round(mean_temp[`i'],   0.01)))
        local mdd  = cond(missing(mean_dd28[`i']),   ".", string(round(mean_dd28[`i'],   0.01)))
        file write tex "`dec's & `nobs' & `nreg' & `mb' & `mp' & `mspi' & `mabn' & `mt' & `mdd' \\" _n
    }
restore

file write tex "\bottomrule" _n
file write tex "\end{tabular}" _n
file close tex
di "  ✓ Decade summary table saved: $cf_overleaf/table/panel_decade_summary.tex"


********************************************************************************
* 5. FIGURES
********************************************************************************

di _n as result "========================================================"
di    as result " SECTION 5: FIGURES"
di    as result "========================================================"

* Global graph settings
grstyle clear
capture ssc install grstyle
capture grstyle init
set scheme s2color

* ---- 5.1 Distribution of birth rate (full sample) ----
di _n "-- Figure 5.1: Distribution of birth rate --"
capture confirm variable birth
if _rc == 0 {
    twoway histogram birth, ///
        width(0.01) ///
        color(navy%60) ///
        xtitle("Births per woman (region-age-year cell)") ///
        ytitle("Density") ///
        title("Distribution of Birth Rate", size(medium)) ///
        subtitle("Region × mother age × growing year panel") ///
        note("N = `=_N' observations")
    graph export "$cf_overleaf/figure/diag_birth_distribution.png", replace width(1200)
    di "  ✓ Saved: diag_birth_distribution.png"
}

* ---- 5.2 Distribution of precipitation ----
di _n "-- Figure 5.2: Distribution of annual precipitation --"
capture confirm variable precip precip_gs precip_ngs
if _rc == 0 {
    twoway ///
        (kdensity precip,     lcolor(navy)   lwidth(medthick) lpattern(solid)) ///
        (kdensity precip_gs,  lcolor(forest_green) lwidth(medthick) lpattern(dash)) ///
        (kdensity precip_ngs, lcolor(cranberry) lwidth(medthick) lpattern(shortdash)), ///
        xtitle("Annual precipitation (mm)") ///
        ytitle("Density") ///
        title("Distribution of Annual Precipitation", size(medium)) ///
        legend(order(1 "Total" 2 "Growing season" 3 "Non-growing season") ///
               pos(1) ring(0) cols(1) size(small)) ///
        note("Region × mother age × growing year panel")
    graph export "$cf_overleaf/figure/diag_precip_distribution.png", replace width(1200)
    di "  ✓ Saved: diag_precip_distribution.png"
}

* ---- 5.3 Distribution of mean temperature ----
di _n "-- Figure 5.3: Distribution of mean temperature --"
capture confirm variable temp_mean temp_max
if _rc == 0 {
    twoway ///
        (kdensity temp_mean, lcolor(navy)    lwidth(medthick) lpattern(solid)) ///
        (kdensity temp_max,  lcolor(cranberry) lwidth(medthick) lpattern(dash)), ///
        xtitle("Temperature (°C)") ///
        ytitle("Density") ///
        title("Distribution of Annual Temperature", size(medium)) ///
        legend(order(1 "Mean temperature" 2 "Max temperature") ///
               pos(11) ring(0) cols(1) size(small)) ///
        note("Region × mother age × growing year panel")
    graph export "$cf_overleaf/figure/diag_temp_distribution.png", replace width(1200)
    di "  ✓ Saved: diag_temp_distribution.png"
}

* ---- 5.4 Temperature bin composition (average days per year in each bin) ----
di _n "-- Figure 5.4: Temperature bin composition --"
capture confirm variable temp_max_less_18 temp_max_more_33
if _rc == 0 {
    preserve
        collapse (mean) `bin_vars'
        * Reshape to long for bar chart
        gen id = 1
        reshape long temp_max_, i(id) j(bin) string

        * Assign numeric labels
        gen bin_num = .
        replace bin_num = 1 if bin == "less_18"
        replace bin_num = 2 if bin == "18_21"
        replace bin_num = 3 if bin == "21_24"
        replace bin_num = 4 if bin == "24_27"
        replace bin_num = 5 if bin == "27_30"
        replace bin_num = 6 if bin == "30_33"
        replace bin_num = 7 if bin == "more_33"

        label define binlbl 1 "<18°C" 2 "18–21°C" 3 "21–24°C" 4 "24–27°C" ///
                             5 "27–30°C" 6 "30–33°C" 7 ">33°C"
        label values bin_num binlbl

        graph bar temp_max_, over(bin_num) ///
            bar(1, color(navy%70)) ///
            ytitle("Mean days per year") ///
            title("Mean Days per Year in Each Temperature Bin", size(medium)) ///
            subtitle("Averaged across all region-age-year cells") ///
            note("Based on daily max temperature")
        graph export "$cf_overleaf/figure/diag_temp_bins.png", replace width(1200)
        di "  ✓ Saved: diag_temp_bins.png"
    restore
}

* ---- 5.5 Time trend: mean birth rate by growing year ----
di _n "-- Figure 5.5: Birth rate trend over time --"
capture confirm variable birth g_year
if _rc == 0 {
    preserve
        collapse (mean) birth birth_urban birth_rural (count) n_cells=birth, by(g_year)
        * Only plot years with reasonable coverage (at least 10 cells)
        keep if n_cells >= 10
        twoway ///
            (connected birth       g_year, lcolor(navy)    mcolor(navy)    msymbol(circle)    lwidth(medthick)) ///
            (connected birth_urban g_year, lcolor(forest_green) mcolor(forest_green) msymbol(triangle) lwidth(thin) lpattern(dash)) ///
            (connected birth_rural g_year, lcolor(cranberry) mcolor(cranberry) msymbol(square) lwidth(thin) lpattern(shortdash)), ///
            xtitle("Growing year") ///
            ytitle("Mean births per woman") ///
            title("Birth Rate Over Time", size(medium)) ///
            subtitle("Mean across all region-age cells (ages 15–44)") ///
            legend(order(1 "Full sample" 2 "Urban" 3 "Rural") ///
                   pos(1) ring(0) cols(1) size(small)) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_birth_trend.png", replace width(1200)
        di "  ✓ Saved: diag_birth_trend.png"
    restore
}

* ---- 5.6 Time trend: mean precipitation by growing year ----
di _n "-- Figure 5.6: Precipitation trend over time --"
capture confirm variable precip g_year
if _rc == 0 {
    preserve
        collapse (mean) precip precip_gs precip_ngs (count) n_cells=precip, by(g_year)
        keep if n_cells >= 10
        twoway ///
            (connected precip     g_year, lcolor(navy)    mcolor(navy)    msymbol(circle) lwidth(medthick)) ///
            (connected precip_gs  g_year, lcolor(forest_green) mcolor(forest_green) msymbol(triangle) lwidth(thin) lpattern(dash)) ///
            (connected precip_ngs g_year, lcolor(cranberry) mcolor(cranberry) msymbol(square) lwidth(thin) lpattern(shortdash)), ///
            xtitle("Growing year") ///
            ytitle("Mean annual precipitation (mm)") ///
            title("Precipitation Over Time", size(medium)) ///
            subtitle("Mean across all region-age cells") ///
            legend(order(1 "Total" 2 "Growing season" 3 "Non-growing season") ///
                   pos(2) ring(0) cols(1) size(small)) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_precip_trend.png", replace width(1200)
        di "  ✓ Saved: diag_precip_trend.png"
    restore
}

* ---- 5.6b Distribution of SPI (Standardized Precipitation Index) ----
di _n "-- Figure 5.6b: Distribution of SPI --"
capture confirm variable precip_spi precip_gs_spi precip_ngs_spi
if _rc == 0 {
    twoway ///
        (kdensity precip_spi,     lcolor(navy)         lwidth(medthick) lpattern(solid)) ///
        (kdensity precip_gs_spi,  lcolor(forest_green)  lwidth(medthick) lpattern(dash)) ///
        (kdensity precip_ngs_spi, lcolor(cranberry)     lwidth(medthick) lpattern(shortdash)), ///
        xline(-2 2, lcolor(gs10) lpattern(dash) lwidth(thin)) ///
        xtitle("Standardized Precipitation Index (SPI)") ///
        ytitle("Density") ///
        title("Distribution of SPI", size(medium)) ///
        subtitle("Gamma-distribution-based standardised anomaly") ///
        legend(order(1 "Annual" 2 "Growing season" 3 "Non-growing season") ///
               pos(1) ring(0) cols(1) size(small)) ///
        note("Dashed vertical lines mark SPI = ±2 (abnormal thresholds)")
    graph export "$cf_overleaf/figure/diag_spi_distribution.png", replace width(1200)
    di "  ✓ Saved: diag_spi_distribution.png"
}

* ---- 5.6c Time trend: share of abnormal precipitation years ----
di _n "-- Figure 5.6c: Abnormal precipitation trend over time --"
capture confirm variable abn_precip abn_precip_dry abn_precip_wet g_year
if _rc == 0 {
    preserve
        collapse (mean) abn_precip abn_precip_dry abn_precip_wet ///
                 (count) n_cells=abn_precip, by(g_year)
        keep if n_cells >= 10
        * Convert to percentages for readability
        foreach v in abn_precip abn_precip_dry abn_precip_wet {
            replace `v' = `v' * 100
        }
        twoway ///
            (connected abn_precip     g_year, lcolor(navy)         mcolor(navy)         msymbol(circle)   lwidth(medthick)) ///
            (connected abn_precip_dry g_year, lcolor(orange_red)   mcolor(orange_red)   msymbol(triangle) lwidth(thin) lpattern(dash)) ///
            (connected abn_precip_wet g_year, lcolor(blue)         mcolor(blue)         msymbol(square)   lwidth(thin) lpattern(shortdash)), ///
            xtitle("Growing year") ///
            ytitle("Share of region-age cells (%)") ///
            title("Share of Abnormal Precipitation Years", size(medium)) ///
            subtitle("Mean across all region-age cells") ///
            legend(order(1 "Any abnormal" 2 "Abnormally dry" 3 "Abnormally wet") ///
                   pos(1) ring(0) cols(1) size(small)) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_abn_precip_trend.png", replace width(1200)
        di "  ✓ Saved: diag_abn_precip_trend.png"
    restore
}

* ---- 5.6d Time trend: mean SPI over time ----
di _n "-- Figure 5.6d: SPI trend over time --"
capture confirm variable precip_spi g_year
if _rc == 0 {
    preserve
        collapse (mean) precip_spi precip_gs_spi precip_ngs_spi ///
                 (count) n_cells=precip_spi, by(g_year)
        keep if n_cells >= 10
        twoway ///
            (connected precip_spi     g_year, lcolor(navy)         msymbol(none) lwidth(medthick)) ///
            (connected precip_gs_spi  g_year, lcolor(forest_green) msymbol(none) lwidth(medthick) lpattern(dash)) ///
            (connected precip_ngs_spi g_year, lcolor(cranberry)    msymbol(none) lwidth(medthick) lpattern(shortdash)), ///
            yline(0, lcolor(gs8) lwidth(thin) lpattern(solid)) ///
            xtitle("Growing year") ///
            ytitle("Mean SPI") ///
            title("Standardized Precipitation Index Over Time", size(medium)) ///
            subtitle("Mean across all region-age cells") ///
            legend(order(1 "Annual" 2 "Growing season" 3 "Non-growing season") ///
                   pos(2) ring(0) cols(1) size(small)) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_spi_trend.png", replace width(1200)
        di "  ✓ Saved: diag_spi_trend.png"
    restore
}

* ---- 5.7 Time trend: mean temperature by growing year ----
di _n "-- Figure 5.7: Temperature trend over time --"
capture confirm variable temp_mean g_year
if _rc == 0 {
    preserve
        collapse (mean) temp_mean temp_max (count) n_cells=temp_mean, by(g_year)
        keep if n_cells >= 10
        twoway ///
            (connected temp_mean g_year, lcolor(navy)    mcolor(navy)    msymbol(circle) lwidth(medthick)) ///
            (connected temp_max  g_year, lcolor(cranberry) mcolor(cranberry) msymbol(square) lwidth(thin) lpattern(dash)), ///
            xtitle("Growing year") ///
            ytitle("Temperature (°C)") ///
            title("Temperature Over Time", size(medium)) ///
            subtitle("Mean across all region-age cells") ///
            legend(order(1 "Mean temperature" 2 "Max temperature") ///
                   pos(11) ring(0) cols(1) size(small)) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_temp_trend.png", replace width(1200)
        di "  ✓ Saved: diag_temp_trend.png"
    restore
}

* ---- 5.8 Birth rate by mother age (averaged over all years and regions) ----
di _n "-- Figure 5.8: Birth rate by mother age --"
capture confirm variable birth mother_age
if _rc == 0 {
    preserve
        collapse (mean) birth birth_urban birth_rural, by(mother_age)
        twoway ///
            (connected birth       mother_age, lcolor(navy)    mcolor(navy)    msymbol(circle) lwidth(medthick)) ///
            (connected birth_urban mother_age, lcolor(forest_green) mcolor(forest_green) msymbol(triangle) lwidth(thin) lpattern(dash)) ///
            (connected birth_rural mother_age, lcolor(cranberry) mcolor(cranberry) msymbol(square) lwidth(thin) lpattern(shortdash)), ///
            xtitle("Mother's age") ///
            ytitle("Mean births per woman") ///
            title("Birth Rate by Age", size(medium)) ///
            subtitle("Averaged over all regions and growing years") ///
            legend(order(1 "Full sample" 2 "Urban" 3 "Rural") ///
                   pos(1) ring(0) cols(1) size(small)) ///
            xlabel(15(5)44)
        graph export "$cf_overleaf/figure/diag_birth_by_age.png", replace width(1200)
        di "  ✓ Saved: diag_birth_by_age.png"
    restore
}

* ---- 5.8b Birth rate by age bin over time ----
di _n "-- Figure 5.8b: Birth rate by 5-year age bin over time --"
capture confirm variable birth_15_19 birth_40_44 g_year
if _rc == 0 {
    preserve
        * Collapse to region-year level (bin rates are constant across ages within bin)
        bysort DHS_code g_year: keep if _n == 1
        collapse (mean) birth_15_19 birth_20_24 birth_25_29 ///
                        birth_30_34 birth_35_39 birth_40_44 ///
                 (count) n_cells=birth_15_19, by(g_year)
        keep if n_cells >= 10
        twoway ///
            (connected birth_15_19 g_year, msymbol(none) lwidth(medthick) lcolor(navy)) ///
            (connected birth_20_24 g_year, msymbol(none) lwidth(medthick) lcolor(forest_green)) ///
            (connected birth_25_29 g_year, msymbol(none) lwidth(medthick) lcolor(orange)) ///
            (connected birth_30_34 g_year, msymbol(none) lwidth(medthick) lcolor(cranberry)) ///
            (connected birth_35_39 g_year, msymbol(none) lwidth(medthick) lcolor(purple) lpattern(dash)) ///
            (connected birth_40_44 g_year, msymbol(none) lwidth(medthick) lcolor(gs8) lpattern(dash)), ///
            xtitle("Growing year") ///
            ytitle("Mean births per woman (weighted)") ///
            title("Birth Rate by 5-Year Age Bin Over Time", size(medium)) ///
            subtitle("Mean across all regions (region-year level)") ///
            legend(order(1 "15-19" 2 "20-24" 3 "25-29" 4 "30-34" 5 "35-39" 6 "40-44") ///
                   pos(1) ring(0) cols(2) size(small)) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_birth_by_age_bin.png", replace width(1200)
        di "  ✓ Saved: diag_birth_by_age_bin.png"
    restore
}

* ---- 5.9 Degree days trend over time (representative thresholds) ----
di _n "-- Figure 5.9: Degree days trend over time --"
capture confirm variable temp_max_dd_28 temp_max_dd_32 g_year
if _rc == 0 {
    preserve
        collapse (mean) temp_max_dd_24 temp_max_dd_28 temp_max_dd_32 temp_max_dd_36 ///
                 (count) n_cells=temp_max_dd_28, by(g_year)
        keep if n_cells >= 10
        twoway ///
            (connected temp_max_dd_24 g_year, lcolor(navy%80)     msymbol(none) lwidth(medthick)) ///
            (connected temp_max_dd_28 g_year, lcolor(forest_green) msymbol(none) lwidth(medthick)) ///
            (connected temp_max_dd_32 g_year, lcolor(orange)       msymbol(none) lwidth(medthick)) ///
            (connected temp_max_dd_36 g_year, lcolor(cranberry)    msymbol(none) lwidth(medthick)), ///
            xtitle("Growing year") ///
            ytitle("Mean degree days (÷100)") ///
            title("Degree Days (Max Temp) Over Time", size(medium)) ///
            subtitle("Mean across all region-age cells") ///
            legend(order(1 "DD above 24°C" 2 "DD above 28°C" 3 "DD above 32°C" 4 "DD above 36°C") ///
                   pos(11) ring(0) cols(1) size(small)) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_dd_trend.png", replace width(1200)
        di "  ✓ Saved: diag_dd_trend.png"
    restore
}


* ---- 5.10 Cell size distribution: histogram of n (women per cell) ----
di _n "-- Figure 5.10: Cell size distribution --"
capture confirm variable n
if _rc == 0 {
    * Trim display at 99th percentile so a handful of large cells don't squash the histogram
    qui sum n, detail
    local p99 = r(p99)
    twoway histogram n if n <= `p99', ///
        width(1) ///
        color(navy%60) ///
        xline(5,  lcolor(orange)    lpattern(dash) lwidth(medthick)) ///
        xline(10, lcolor(cranberry) lpattern(dash) lwidth(medthick)) ///
        xtitle("Women per region-age-year cell") ///
        ytitle("Frequency") ///
        title("Distribution of Cell Size", size(medium)) ///
        subtitle("Trimmed at 99th percentile (`=round(`p99',1)' women) for display") ///
        note("Dashed lines: n = 5 (orange) and n = 10 (red). Full sample N = `=_N' cells.")
    graph export "$cf_overleaf/figure/diag_cell_size.png", replace width(1200)
    di "  ✓ Saved: diag_cell_size.png"

    * Log key threshold counts
    foreach thr in 5 10 20 {
        qui count if n < `thr'
        di "  Cells with n < `thr':  `r(N)' (`=round(100*r(N)/_N,0.1)'%)"
    }
}

* ---- 5.11 Urban-rural birth rate gap over time ----
di _n "-- Figure 5.11: Urban-rural birth rate gap over time --"
capture confirm variable birth_rural birth_urban g_year
if _rc == 0 {
    preserve
        collapse (mean) birth_rural birth_urban (count) n_cells=birth, by(g_year)
        keep if n_cells >= 10
        gen gap = birth_rural - birth_urban
        label var gap "Rural minus urban births per woman"
        twoway ///
            (connected gap g_year, ///
                lcolor(navy) mcolor(navy) msymbol(circle) lwidth(medthick)), ///
            yline(0, lcolor(gs8) lwidth(thin) lpattern(solid)) ///
            xtitle("Growing year") ///
            ytitle("Rural minus urban births per woman") ///
            title("Urban–Rural Birth Rate Gap Over Time", size(medium)) ///
            subtitle("Mean across all region-age cells") ///
            legend(off) ///
            xlabel(1950(10)2020)
        graph export "$cf_overleaf/figure/diag_urban_rural_gap.png", replace width(1200)
        di "  ✓ Saved: diag_urban_rural_gap.png"
    restore
}


********************************************************************************
* 6. FINAL SUMMARY
********************************************************************************

di _n as result "========================================================"
di    as result " SECTION 6: FINAL SUMMARY"
di    as result "========================================================"

qui count
di as result "  Panel observations:    `r(N)'"

qui levelsof DHS_code
di as result "  DHS regions:           `r(r)'"

qui sum g_year
di as result "  Growing year range:    `r(min)' – `r(max)'"

qui sum mother_age
di as result "  Mother age range:      `r(min)' – `r(max)'"

foreach v in precip abn_precip precip_spi temp_mean {
    capture confirm variable `v'
    if _rc == 0 {
        qui count if missing(`v')
        di as result "  Missing `v':  `r(N)' obs (`=round(100*r(N)/_N, 0.1)'%)"
    }
    else {
        di as error "  `v' not found in dataset"
    }
}

di _n as result "  SANITY CHECKS: `n_warnings' warning(s) flagged"
di    as result ""
di    as result "  OUTPUTS:"
di    as result "    Tables:"
di    as result "      $cf_overleaf/table/panel_summary_stats.tex"
di    as result "      $cf_overleaf/table/panel_country_diagnostics.tex"
di    as result "      $cf_overleaf/table/panel_missing_countries.tex"
di    as result "      $cf_overleaf/table/panel_within_between.tex"
di    as result "      $cf_overleaf/table/panel_decade_summary.tex"
di    as result "    Figures:"
di    as result "      $cf_overleaf/figure/diag_birth_distribution.png"
di    as result "      $cf_overleaf/figure/diag_precip_distribution.png"
di    as result "      $cf_overleaf/figure/diag_temp_distribution.png"
di    as result "      $cf_overleaf/figure/diag_temp_bins.png"
di    as result "      $cf_overleaf/figure/diag_birth_trend.png"
di    as result "      $cf_overleaf/figure/diag_precip_trend.png"
di    as result "      $cf_overleaf/figure/diag_spi_distribution.png"
di    as result "      $cf_overleaf/figure/diag_abn_precip_trend.png"
di    as result "      $cf_overleaf/figure/diag_spi_trend.png"
di    as result "      $cf_overleaf/figure/diag_temp_trend.png"
di    as result "      $cf_overleaf/figure/diag_birth_by_age.png"
di    as result "      $cf_overleaf/figure/diag_birth_by_age_bin.png"
di    as result "      $cf_overleaf/figure/diag_dd_trend.png"
di    as result "      $cf_overleaf/figure/diag_cell_size.png"
di    as result "      $cf_overleaf/figure/diag_urban_rural_gap.png"
di    as result "========================================================"

log close diagnostics
