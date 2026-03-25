
	* Generate analysis variables for DD regressions
	* Date: March 24, 2026

	clear all
	set more off

	local username "`c(username)'"

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

	cd "$cf"
	global basepath "data/derived/census-climate-merged"

	*** Load and prepare analysis dataset ***

	use "$basepath/merged_all_countries_temperature.dta", replace

	keep if inrange(COHORT,1941,1970)

	* assign Cameroon to West Africa, otherwise Central Africa is just one country

	replace west = 1 if country=="Cameroon"

	* classify SMALLEST units into above/below median education areas "at baseline"

	gen highedu = primary_edu

	* generate average education by birthplace
	* mean within birthplace using only cohorts 1941-1945
	bysort BIRTH_SMALLEST: egen highedu_41_45 = mean(highedu) if inrange(COHORT,1941,1945)

	* propagate to everyone in the birthplace (xfill replacement)
	bysort BIRTH_SMALLEST: egen avg_highedu_birthplace1 = min(highedu_41_45)

	* generate median of birthplace averages within each country (pooling together all census years in a country)
	bysort country: egen median_avg_highedu_country1 = median(avg_highedu_birthplace1)
	gen quartile_avg_highedu_cntry = .
	levelsof country, local(countries)
	foreach c of local countries {
		cap drop temp_q
		cap xtile temp_q = avg_highedu_birthplace1 if country == "`c'", nq(4)
		cap replace quartile_avg_highedu_cntry = temp_q if country == "`c'"
		cap drop temp_q
	}

	* generate indicator for birthplaces above country median
	gen above_median_highedu1 = (avg_highedu_birthplace1 > median_avg_highedu_country1) if !missing(avg_highedu_birthplace1)
	lab var above_median_highedu1 "Birthplace initial share of primary school graduates $>$ country median"

	gen below_p25_avg_highedu_cntry = quartile_avg_highedu_cntry == 1
	replace below_p25_avg_highedu_cntry = . if quartile_avg_highedu_cntry == .

	gen above_p75_avg_highedu_cntry = quartile_avg_highedu_cntry == 4
	replace above_p75_avg_highedu_cntry = . if quartile_avg_highedu_cntry ==.

	* sample restrictions:

	drop if country == "Zimbabwe" | country == "Togo" | country == "Lesotho" | country == "Ethiopia" | country == "SouthAfrica"
	drop if no_valid_pixels == 1
	keep if inrange(AGE,45,54) & inrange(COHORT,1946,1970)

	foreach XX in 28 30 32 34 {
		lab var temp_max_dd_`XX'_age_15_29 "Degree days $>$ `XX', 15-29"
		lab var temp_max_dd_`XX'_age_30_44 "Degree days $>$ `XX', 30-44"
		lab var temp_max_dd_`XX'_gs "Degree days $>$ `XX', growing season"
		lab var temp_max_dd_`XX'_gs_age_15_29 "Degree days $>$ `XX', growing season, 15-29"
		lab var temp_max_dd_`XX'_gs_age_30_44 "Degree days $>$ `XX', growing season, 30-44"
		lab var temp_max_dd_`XX'_ngs "Degree days $>$ `XX', non-growing season"
		lab var temp_max_dd_`XX'_ngs_age_15_29 "Degree days $>$ `XX', non-growing season, 15-29"
		lab var temp_max_dd_`XX'_ngs_age_30_44 "Degree days $>$ `XX', non-growing season, 30-44"
	}
	lab var temp_max_dd_q2_15_29 "Degree days $>$ country-specific $\tau$, 15-29"
	lab var temp_max_dd_q2_30_44 "Degree days $>$ country-specific $\tau$, 30-44"
	lab var avg_precip_spi_15_29 "average rainfall index, 15-29"
	lab var avg_precip_spi_30_44 "average rainfall index, 30-44"
	lab var avg_precip_spi "average rainfall index"
	lab var avg_precip_gs_spi "average rainfall index (GS)"
	lab var avg_precip_ngs_spi "average rainfall index (NGS)"
	lab var avg_precip_gs_spi_15_29 "average rainfall index, growing season, 15-29"
	lab var avg_precip_gs_spi_30_44 "average rainfall index, growing season, 30-44"
	lab var avg_precip_ngs_spi_15_29 "average rainfall index, non-growing season, 15-29"
	lab var avg_precip_ngs_spi_30_44 "average rainfall index, non-growing season, 30-44"
	lab var share_abn_precip_gs_dry_15_29 "Share abnormal dry, GS, 15-29"
	lab var share_abn_precip_ngs_dry_15_29 "Share abnormal dry, NGS, 15-29"
	lab var share_abn_precip_gs_dry_30_44 "Share abnormal dry, GS, 30-44"
	lab var share_abn_precip_ngs_dry_30_44 "Share abnormal dry, NGS, 30-44"
	lab var share_abn_precip_gs_wet_15_29 "Share abnormal wet, GS, 15-29"
	lab var share_abn_precip_ngs_wet_15_29 "Share abnormal wet, NGS, 15-29"
	lab var share_abn_precip_gs_wet_30_44 "Share abnormal wet, GS, 30-44"
	lab var share_abn_precip_ngs_wet_30_44 "Share abnormal wet, NGS, 30-44"
	lab var temp_max_less_18 "Number of days $<$ 18C"
	lab var temp_max_18_21 "Number of days 18-21C"
	lab var temp_max_21_24 "Number of days 21-24C"
	lab var temp_max_24_27 "Number of days 24-27C"
	lab var temp_max_27_30 "Number of days 27-30C"
	lab var temp_max_30_33 "Number of days 30-33C"
	lab var temp_max_more_33 "Number of days $>$ 33C"

	gen temp_max_less_24 = temp_max_less_18 + temp_max_18_21 + temp_max_21_24
	lab var temp_max_less_24 "Number of days $<$ 24C"

	gen temp_max_bins_totdays = temp_max_less_18 + temp_max_18_21 + temp_max_21_24 + temp_max_24_27 + temp_max_27_30 + temp_max_30_33 + temp_max_more_33

	gen cohort5yr = .
	replace cohort5yr = 1 if inrange(COHORT, 1946, 1950)
	replace cohort5yr = 2 if inrange(COHORT, 1951, 1955)
	replace cohort5yr = 3 if inrange(COHORT, 1956, 1960)
	replace cohort5yr = 4 if inrange(COHORT, 1961, 1965)
	replace cohort5yr = 5 if inrange(COHORT, 1966, 1970)
	label define COHORT5YR 1 "1946–50" 2 "1951–55" 3 "1956–60" 4 "1961–65" 5 "1966–70"
	label values cohort5yr COHORT5YR

	encode census_name, gen(cntryXyear_id)
	lab var cntryXyear_id "census country-year ID"
	order cntryXyear_id, after(census_name)

	*** Save analysis dataset ***

	save "data/derived/analysis/analysis_dd_regressions.dta", replace
