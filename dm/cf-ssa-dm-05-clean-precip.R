#' Project:     Climate and Fertility in Sub-Saharan Africa
#' Purpose:     Clean temperature data
#' Author:      Prayog Bhattarai
#' Date:        2025-03-26
#' Description: This script creates precipitation variables from ERA5 data for Rwanda and generates standardized precipitation variables as done in Kenya project. 
#'              It also generates growing season specific versions of these variables and finally collapses the data to the year level. 
#'              Finally, it generates cohort-year combinations and processes the census year data.

# -------------------------
# 01. set up environment
# -------------------------
# required packages
rm(list = ls())
library(ncdf4)
library(sf)
library(sp)
library(ggplot2)
library(dplyr)
library(raster) 
library(collapse)
library(haven)
library(data.table)
library(tidyverse)
library(foreign)
library(anytime)
library(stringr)
library(data.table)
library(lubridate)
library(parallel)

# main path
user_dir <- "D:/NUS Dropbox/Prayog Bhattarai"   # modify this
cf_ssa_path <- paste0(user_dir, "/Climate_Change_and_Fertility_in_SSA")

#' Combine all ERA5 CSV files for a country
#'
#' @param country_name Character name of the country (must match directory name)
#' @param cf_ssa_path Character path to the main project directory
#' @return A data.table combining all ERA5 CSV files
#' @note Failure points: 
#' - Invalid country_name/path will fail silently
#' - Non-CSV files in directory may cause errors
#' - Column name mismatches between files may cause fill=TRUE issues

combine_csvs <- function(country_name, cf_ssa_path) {
  # [Flagpost 1/3] Setting up paths
  derived_path <- file.path(cf_ssa_path, "data/derived/era", country_name)
  message(sprintf("Attempting to read CSVs from: %s", derived_path))
  
  # [Flagpost 2/3] Reading files
  csv_files <- list.files(derived_path, 
              pattern = "\\precip.csv$", full.names = TRUE)
  message(sprintf("Found %d CSV files", length(csv_files)))
  
  # [Flagpost 3/3] Combining data
  country_data <- rbindlist(
    lapply(csv_files, fread),
    use.names = TRUE,
    fill = TRUE
  )
  message(sprintf("Successfully combined %d rows", nrow(country_data)))
  return(country_data)
}


#' Prepare IPUMS shapefile for spatial operations
#'
#' @param country_name Character name of the country
#' @param cf_ssa_path Character path to the main project directory
#' @return An sf object with cleaned administrative boundaries
#' @note Failure points:
#' - Shapefile path construction may fail if naming conventions change
#' - Missing BPL_GEO values will be filtered out
prepare_shapefile <- function(country_name, cf_ssa_path) {
    shapefile_path <- file.path(cf_ssa_path, 
                    "data/source/shapefiles/ipums", 
                    country_name, 
                    tolower(gsub(" ", "-", paste0(country_name, ".shp")))) 
    
    shapefile <- st_read(shapefile_path) %>%
      filter(!is.na(BPL_GEO)) %>%
      mutate(BPL_NAM = str_to_title(str_trim(BPL_NAM)),
             GEO_CONCAT = paste0(toupper(country_name), "__", toupper(BPL_NAM))) %>%
      dplyr::select(GEO_CONCAT, BPL_GEO, geometry)
    
    # message(paste0("Shapefile loaded with ", nrow(shapefile), " features") 
    return(shapefile)
}

# Function: Make a raster file of polygons that is the set of grid squares
create_raster_grid <- function(country_data) {
  # Extract unique lat-lons
  unique_lon <- sort(unique(country_data$longitude))
  unique_lat <- sort(unique(country_data$latitude))
  message("Unique latitude and longitude extracted.")

  # Calculate step size
  lon_step <- unique_lon[2] - unique_lon[1] # difference between two consecutive longitudes
  lat_step <- unique_lat[2] - unique_lat[1] # difference between two consecutive latitudes
  message("Step size calculated.")

  # Create country raster file
  country_raster <- raster(
    xmn = min(unique_lon) - lon_step/2,
    xmx = max(unique_lon) + lon_step/2,
    ymn = min(unique_lat) - lat_step/2,
    ymx = max(unique_lat) + lat_step/2,
    nrows = length(unique_lat),
    ncols = length(unique_lon),
    crs = "+proj=longlat +ellps=WGS84 +datum=WGS84") %>%
    rasterToPolygons() %>%
    st_as_sf(CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +units=km")) %>%
    mutate(group = row_number()) %>%
    st_cast("MULTIPOLYGON")
  message("Raster grid created")
  return(country_raster)
}

# Function: overlap grid squares and sub-units
overlap_grid_subunits <- function(country_raster, shapefile) {
  # 4. Intersect grid with admin boundaries
  intersected_areas <- st_make_valid(st_intersection(country_raster, shapefile))
  message("Intersected grid with admin boundaries")

  area_values <- intersected_areas |> 
    mutate(loc_area = st_area(intersected_areas))
  area1 <- area_values |> 
    group_by(GEO_CONCAT) |> 
    summarize(total_area = sum(loc_area)) 
  area2 <- st_drop_geometry(area1)
  area3 <- right_join(area2, area_values)
  area4 <- mutate(area3, shr_of_subunit = as.vector(loc_area/total_area))
  area5 <- dplyr::select(area4, GEO_CONCAT, geometry, shr_of_subunit, total_area, loc_area, group)
  country_area <- rename(area5)
  grid_final <- left_join(country_area, country_raster, by = "group") |> 
  rename(coordinates = geometry.y, geometry = geometry.x, grid_id = group) |> 
  dplyr::select(-layer)

  grid_final_sf <- st_as_sf(grid_final)

  rm(area_values, grid_final, intersected_areas, area1, area2, area3, area4, area5)
  return(grid_final_sf)
}
 
create_grid_ids <- function(country_data) {
  # Merge in precipitation data values and collapse it to the subcounty level by finding area weighted means
  nlat <- length(unique(country_data$latitude))
  nlon <- length(unique(country_data$longitude))
  unique_lon <- sort(unique(country_data$longitude))
  unique_lat <- sort(unique(country_data$latitude))

  # create grid
  lonlat <- as.matrix(expand.grid(unique_lon, unique_lat))
  gridsize <- nlat * nlon
  grid_id <- c(1:gridsize)

  # Create the data frame with grid IDs
  era_gridpoints <- cbind(lonlat, grid_id)
  colnames(era_gridpoints) <- c("longitude", "latitude", "grid_id")
  era_gridpoints_df <- as.data.frame(era_gridpoints)
  return(era_gridpoints_df)
}

# function to join precipitation data with grids
merge_precip_data <- function(country_data, era_gridpoints_df, grid_final_sf) {
  country_data <- country_data |> 
  left_join(era_gridpoints_df, by = c("latitude", "longitude"))
  
  # Only now convert era_gridpoints_df to spatial after the join
  coordinates(era_gridpoints_df) <- ~longitude + latitude
  proj4string(era_gridpoints_df) <- CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +units=km")
  erasf <- st_as_sf(era_gridpoints_df)

  country_joined <- inner_join(country_data, grid_final_sf, by = "grid_id")
  return(country_joined)
}

# Function to collapse country data to sub-unit level by finding area-weighted means
collapse_area_weighted <- function(country_joined, growing_season_range) {
  subunit_collapsed <- country_joined |> 
    subset(select = -c(total_area, loc_area, geometry, coordinates, grid_id)) |> 
    group_by(GEO_CONCAT, valid_time) |> 
    summarise(
      across(
        where(is.numeric),
        ~fmean(.x, w = shr_of_subunit, use.g.names = TRUE),
        .names = "{.col}"
      ),
      .groups = "drop"
    )
    subunit_collapsed <- subunit_collapsed %>%
    mutate(date = anydate(valid_time),
           day = day(date),
           month = month(date),
           year = year(date),
           precip = tp * 1000,  # Convert to mm
           decade = floor((year - 1940)/10) * 10 + 1940,
           growing_season = month %in% growing_season_range,
           precip_gs = precip * growing_season,
           precip_ngs = precip * (!growing_season)) %>%
    dplyr::select(-valid_time)
  message("Created time variables and growing-non-growing season variables.")  
  return(subunit_collapsed)
}

# Function to collapse to annual level 
collapse_to_annual <- function(subunit_collapsed) {
  final_output <- subunit_collapsed %>%
  group_by(GEO_CONCAT, year) %>%
  summarise(across(c(precip, precip_gs, precip_ngs), 
            mean, na.rm = TRUE),
            .groups = "drop")
  message("Precipitation collapsed to the annual level")
  message("The function for processing country precipitation to annual level is complete!")
  return(final_output)
}

# Function to conduct everything up to annual precipitation calculation in one shot
process_country_precip <- function(country_name, cf_ssa_path, growing_season_range) {
  country_data <- combine_csvs(country_name, cf_ssa_path)
  shapefile <- prepare_shapefile(country_name, cf_ssa_path)
  country_raster <- create_raster_grid(country_data)
  grid_final_sf <- overlap_grid_subunits(country_raster, shapefile)
  era_gridpoints_df <- create_grid_ids(country_data)
  country_joined <- merge_precip_data(country_data, era_gridpoints_df, grid_final_sf)
  subunit_collapsed <- collapse_area_weighted(country_joined, growing_season_range)
  annual_precipitation <- collapse_to_annual(subunit_collapsed)
  message("Country precipitation data created at sub-unit year level.")
  return(annual_precipitation)
}

calculate_spi_indices <- function(annual_precipitation) {
  require(dplyr)
  require(MASS)
  message("Beginning calculation of SPI indices")
  # Input validation
  if (!all(c("GEO_CONCAT", "year") %in% names(annual_precipitation))) {
    stop("Input data must contain GEO_CONCAT and year columns")
  }

  message("Creating a new dataframe to store result of the gammafit")
  # Create a new data frame to store result
  gamma_results <- annual_precipitation |> 
    mutate(alpha = NA_real_, beta = NA_real_, P = NA_real_, spi = NA_real_) |> 
    dplyr::select(-any_of(c("tp", "latitude", "longitude", 
                          "shr_of_subunit", "day", "month", 
                          "date", "growing_season", "non_growing")))
  
  # Define precipitation types dynamically based on what exists in the data
  precip_types <- intersect(c("precip", "precip_gs", "precip_ngs"), names(annual_precipitation))

  # Create columns for wet/dry indicators and SPI for each precipitation type
  for (var in precip_types) {
    gamma_results <- gamma_results |>
      mutate(
        !!paste0("abn_", var, "_wet") := NA_real_,
        !!paste0("abn_", var, "_dry") := NA_real_,
        !!paste0("abn_", var) := NA_real_,
        !!paste0(var, "_spi") := NA_real_
      )
  }
  
  # Get unique geographic locations
  unique_locations <- unique(gamma_results$GEO_CONCAT)

  message("Processing gammafit results for each geographic subunit")
  # Process each geographic location
  for (sc in unique_locations) {
    message("Processing location: ", sc)
    
    # Process each precipitation type
    for (var in precip_types) {
      # Subset data for current location and variable
      loc_data <- annual_precipitation |> 
        filter(GEO_CONCAT == sc) |> 
        pull(!!sym(var))
      
      # Skip if no data or all zeros (FIXED: added missing parenthesis)
      if (length(loc_data) == 0 || all(is.na(loc_data))) {
        message("No data for ", var, " in location: ", sc)
        next
      }
      
      # Remove zeros for gamma fitting
      positive_data <- loc_data[loc_data > 0 & !is.na(loc_data)]
      
      if (length(positive_data) < 5) {
        message("Insufficient positive values (n < 5) for ", var, " in location: ", sc)
        next
      }
      
      # Fit gamma distribution with error handling
      gamma_fit <- tryCatch({
          fitdistr(positive_data, "gamma", lower = c(0, 0))
        }, error = function(e) {
          message("Gamma fit failed for ", var, " in location: ", sc, " with error: ", e$message)
          return(NULL)
        })
      
      if (!is.null(gamma_fit)) {
        # Extract shape and scale parameters
        alpha_val <- gamma_fit$estimate["shape"]
        beta_val <- 1 / gamma_fit$estimate["rate"]  # Convert rate to scale
        
        # Calculate probabilities and SPI
        gamma_results <- gamma_results |>
          mutate(
            alpha = ifelse(GEO_CONCAT == sc, alpha_val, alpha),
            beta = ifelse(GEO_CONCAT == sc, beta_val, beta),
            P = ifelse(GEO_CONCAT == sc, 
                       tryCatch({
                         pgamma(get(var), shape = alpha_val, scale = beta_val)
                       }, error = function(e) {
                         message("pgamma failed for ", var, " in location: ", sc, " with error: ", e$message)
                         NA_real_
                       }), 
                       P),
            spi = ifelse(GEO_CONCAT == sc, 
                         tryCatch({
                           qnorm(P)
                         }, error = function(e) {
                           message("qnorm failed for ", var, " in location: ", sc, " with error: ", e$message)
                           NA_real_
                         }), 
                         spi),
            !!paste0("abn_", var, "_wet") := ifelse(GEO_CONCAT == sc, as.numeric(P >= 0.85), get(paste0("abn_", var, "_wet"))),
            !!paste0("abn_", var, "_dry") := ifelse(GEO_CONCAT == sc, as.numeric(P <= 0.15), get(paste0("abn_", var, "_dry"))),
            !!paste0("abn_", var) := ifelse(GEO_CONCAT == sc, as.numeric(P >= 0.85 | P <= 0.15), get(paste0("abn_", var))),
            !!paste0(var, "_spi") := ifelse(GEO_CONCAT == sc, spi, get(paste0(var, "_spi")))
          )
      }
    }
  }
  message("Gammafit operation successful. Check the terminal logs to see if we could not complete calculations for any of the subunits.")
  # Select final columns to keep
  final_cols <- c("GEO_CONCAT", "year", 
                  paste0("abn_", precip_types), 
                  paste0("abn_", precip_types, "_wet"),
                  paste0("abn_", precip_types, "_dry"),
                  paste0(precip_types, "_spi"),
                  "alpha", "beta", "P", "spi")
  
  # Return results with only the selected columns
  gamma_results |>
    dplyr::select(any_of(final_cols))
  return(gamma_results)
  message("Function to process gamma distribution based SPI measures completed.")
}

#' Calculate Precipitation Metrics for Demographic Analysis in SSA 
#'
#' This function calculates various precipitation metrics for demographic analysis in Kenya.
#' It processes precipitation data by age groups (14-29 and 30-44) and generates:
#' 
#' 1. Total precipitation metrics:
#'     - Overall, growing season (gs), and non-growing season (ngs) precipitation
#'     - Abnormal precipitation measures (dry/wet periods)
#'     
#' 2. Age-specific precipitation metrics:
#'     - Separate calculations for age groups 14-29 and 30-44
#'     - Includes growing and non-growing season measures
#'     
#' 3. Standardized Precipitation Index (SPI) averages:
#'     - Overall, growing season, and non-growing season
#'     - Age-group specific calculations
#'
#' 4. Share calculations:
#'     - Proportion of abnormal precipitation years over reproductive span
#'     - Separated by growing/non-growing seasons and age groups
#'
#' @param data Input data frame containing precipitation and demographic data
#' @param reproductive_span Length of reproductive period in years
#' @param save_file Name of output file to save results (without extension)
#' @param census_year Optional: Census year (default 2019)
#' @param era_intermediate Path to ERA intermediate directory for the country chosen
#' @return Saves calculated metrics to specified file in ERA intermediate country directory
#' @export
calculate_precip_metrics <- function(data, gamma_fit, reproductive_span, save_file, census_year = 2019, era_intermediate) {
  
  # Create age group indicators
  data$age_14_29 <- ifelse(data$age >= 14 & data$age <= 29, 1, 0)
  data$age_30_44 <- ifelse(data$age >= 30 & data$age <= 44, 1, 0)
  
  
  # Identify which variables you want from gamma_fit
  gamma_vars <- setdiff(names(gamma_fit), names(data))
  gamma_vars <- c("GEO_CONCAT", "year", gamma_vars)  # Keep merge keys
  
  # Merge with main data
  data <- merge(data, gamma_fit[, gamma_vars], by = c("GEO_CONCAT", "year"), all.x = TRUE)
  message(names(data))
  
  # List of variables to create age-group interactions for
  varlist <- c("precip", "precip_gs", "precip_ngs", "abn_precip_dry", "abn_precip_wet",
               "abn_precip", "abn_precip_gs_dry", "abn_precip_gs_wet", "abn_precip_gs",
               "abn_precip_ngs_dry", "abn_precip_ngs_wet", "abn_precip_ngs", 
               "precip_spi", "precip_gs_spi", "precip_ngs_spi")
  
  # Create age-group interactions
  for (var in varlist) {
    if (var %in% names(data)) {
      data[[paste0(var, "_14_29")]] <- data[[var]] * data$age_14_29
      data[[paste0(var, "_30_44")]] <- data[[var]] * data$age_30_44
    }
  }
  names(data)

  # Function to safely summarize if variable exists
  safe_sum <- function(var, data) {
    if (var %in% names(data)) {
      return(sum(data[[var]], na.rm = TRUE))
    } else {
      return(NA_real_)
    }
  }
  
  safe_mean <- function(var, data) {
    if (var %in% names(data)) {
      return(mean(data[[var]], na.rm = TRUE))
    } else {
      return(NA_real_)
    }
  }

  # Collapse/aggregate the data
  result <- data %>%
    group_by(GEO_CONCAT, cohort) %>%
    summarize(
      # Total precipitation sums
      precip_total = sum(precip, na.rm = TRUE),
      precip_total_14_29 = sum(precip_14_29, na.rm = TRUE),
      precip_total_30_44 = sum(precip_30_44, na.rm = TRUE),
      precip_gs_total = sum(precip_gs, na.rm = TRUE),
      precip_ngs_total = sum(precip_ngs, na.rm = TRUE),
      
      # Abnormal precipitation sums
      tot_abn_precip = sum(abn_precip, na.rm = TRUE),
      tot_abn_precip_gs = sum(abn_precip_gs, na.rm = TRUE),
      tot_abn_precip_ngs = sum(abn_precip_ngs, na.rm = TRUE),
      tot_abn_dry = sum(abn_precip_dry, na.rm = TRUE),
      tot_abn_wet = sum(abn_precip_wet, na.rm = TRUE),
      tot_abn_dry_gs = sum(abn_precip_gs_dry, na.rm = TRUE),
      tot_abn_wet_gs = sum(abn_precip_gs_wet, na.rm = TRUE),
      tot_abn_dry_ngs = sum(abn_precip_ngs_dry, na.rm = TRUE),
      tot_abn_wet_ngs = sum(abn_precip_ngs_wet, na.rm = TRUE),
      
      # Age-specific abnormal precipitation
      tot_abn_precip_gs_14_29 = sum(abn_precip_gs_14_29, na.rm = TRUE),
      tot_abn_precip_gs_30_44 = sum(abn_precip_gs_30_44, na.rm = TRUE),
      tot_abn_precip_ngs_14_29 = sum(abn_precip_ngs_14_29, na.rm = TRUE),
      tot_abn_precip_ngs_30_44 = sum(abn_precip_ngs_30_44, na.rm = TRUE),
      tot_abn_dry_gs_14_29 = sum(abn_precip_gs_dry_14_29, na.rm = TRUE),
      tot_abn_dry_gs_30_44 = sum(abn_precip_gs_dry_30_44, na.rm = TRUE),
      tot_abn_wet_gs_14_29 = sum(abn_precip_gs_wet_14_29, na.rm = TRUE),
      tot_abn_wet_gs_30_44 = sum(abn_precip_gs_wet_30_44, na.rm = TRUE),
      tot_abn_dry_ngs_14_29 = sum(abn_precip_ngs_dry_14_29, na.rm = TRUE),
      tot_abn_dry_ngs_30_44 = sum(abn_precip_ngs_dry_30_44, na.rm = TRUE),
      tot_abn_wet_ngs_14_29 = sum(abn_precip_ngs_wet_14_29, na.rm = TRUE),
      tot_abn_wet_ngs_30_44 = sum(abn_precip_ngs_wet_30_44, na.rm = TRUE),
      
      # SPI averages
      avg_precip_spi = mean(precip_spi, na.rm = TRUE),
      avg_precip_spi_14_29 = mean(precip_spi_14_29, na.rm = TRUE),
      avg_precip_spi_30_44 = mean(precip_spi_30_44, na.rm = TRUE),
      avg_precip_gs_spi = mean(precip_gs_spi, na.rm = TRUE),
      avg_precip_gs_spi_14_29 = mean(precip_gs_spi_14_29, na.rm = TRUE),
      avg_precip_gs_spi_30_44 = mean(precip_gs_spi_30_44, na.rm = TRUE),
      avg_precip_ngs_spi = mean(precip_ngs_spi, na.rm = TRUE),
      avg_precip_ngs_spi_14_29 = mean(precip_ngs_spi_14_29, na.rm = TRUE),
      avg_precip_ngs_spi_30_44 = mean(precip_ngs_spi_30_44, na.rm = TRUE)
    ) %>%
    ungroup()
  
  # Add census year
  result$census <- census_year
  
  # Calculate shares
  for (var in c("precip", "dry", "wet")) {
    result[[paste0("share_abnormal_", var)]] <- result[[paste0("tot_abn_", var)]] / reproductive_span
    
    for (cond in c("gs", "ngs")) {
      result[[paste0("share_abnormal_", var, "_", cond)]] <- 
        result[[paste0("tot_abn_", var, "_", cond)]] / reproductive_span
      
      for (age in c("14_29", "30_44")) {
        result[[paste0("share_abnormal_", var, "_", cond, "_", age)]] <- 
          result[[paste0("tot_abn_", var, "_", cond, "_", age)]] / (reproductive_span / 2)
      }
    }
  }
  
  # # Save result
  # save_path <- file.path(era_intermediate, "Burkina", paste0(save_file, ".rds"))
  # saveRDS(result, file = save_path)
  # message(paste("result saved to", save_path))
  
  return(result)
}

# Generate cohort-year combinations
generate_cohort_years <- function(start_cohort, end_cohort, 
  start_year, end_year, 
  age_min = 14, age_max = 44) {
  # Generate cohort-year combinations
  cohort_years <- expand.grid(
      cohort = start_cohort:end_cohort,
      year = start_year:end_year
      ) %>%
      as.data.frame() %>%
      mutate(age = year - cohort) %>%
      filter(age >= age_min & age <= age_max)
      return(cohort_years)
}

# Function to process precipitation data for a specific census year
process_census_year <- function(annual_precipitation, gamma_fit, cohort_years, census_yr, max_cohort, max_year) {
  # Load cohort and year filters 
  cohort_years <- cohort_years %>%
    filter(cohort <= max_cohort, year <= max_year)
  
  # Join the datasets
  stopifnot(!any(duplicated(cohort_years)))
  stopifnot(!any(duplicated(annual_precipitation)))

  setDT(cohort_years)
  setDT(annual_precipitation)
  combined_data <- cohort_years[annual_precipitation, on = "year", nomatch = 0, allow.cartesian=TRUE]

  # Calculate precipitation metrics
  result <- calculate_precip_metrics(
    data = combined_data,
    gamma_fit = gamma_fit,
    reproductive_span = 30,
    census_year = census_yr,
    era_intermediate = era_intermediate
  )

  result <- result |> 
    mutate(census = census_yr)
  
  return(result)
}

# Main workflow function
climate_fertility_workflow <- function(country_name, cf_ssa_path, 
  census_years = c(1989, 1999, 2009, 2019),
  growing_season_range = c(6:9),
  cohort_filters = list(
    c(1975, 1989),  # For 1989
    c(1985, 1999),  # For 1999
    c(1995, 2009),  # For 2009
    c(Inf, Inf)     # For 2019
  )) {
    # 1. Process country precipitation data
    message("Processing precipitation data for ", country_name)
    annual_precipitation <- process_country_precip(country_name, cf_ssa_path, growing_season_range)

    # 2. Calculate SPI indices
    message("Calculating SPI indices")
    gamma_fit <- calculate_spi_indices(annual_precipitation)
  
    # 3. Generate base cohort-year combinations
    cohort_years <- generate_cohort_years(
      start_cohort = 1926, 
      end_cohort = 2005,
      start_year = 1940, 
      end_year = 2019,
      age_min = 14, 
      age_max = 44
    )

    # 4. Process each census year with appropriate filters
    era_intermediate <- file.path(cf_ssa_path, "data/derived/era", country_name)
  
    results <- list()
    for(i in seq_along(census_years)) {
      census_yr <- census_years[i]
      max_cohort <- cohort_filters[[i]][1]
      max_year <- cohort_filters[[i]][2]

      message("Processing census year: ", census_yr, 
            " (cohort filter: ", max_cohort, 
            ", year filter: ", max_year, ")")
    
      results[[i]] <- process_census_year(
        annual_precipitation,
        gamma_fit,
        cohort_years,
        census_yr = census_yr,
        max_cohort = max_cohort,
        max_year = max_year
      )
    }
  
    # 5. combine all results
    combined_precip <- bind_rows(results) |> 
      filter(!is.na(cohort)) |>     # drop missing cohorts
      arrange(GEO_CONCAT, cohort, census) |> 
      group_by(GEO_CONCAT, cohort, census) |> 
      filter(row_number() == 1) |>    # remove duplicates
      ungroup()
  
    # 6. Save results
    country_slug <- tolower(gsub(" ", "-", country_name))
    output_path <- file.path(era_intermediate, paste0(country_slug, "-precip-combined.dta"))

  # Identify the size of the dataset
    message("There are 50 cohorts from the 1989 census")
    message("There are 60 cohorts from the 1999 census")
    message("There are 70 cohorts from the 2009 census")
    message("There are 80 cohorts from the 2019 census")
    message("In total there are 260 cohort observations being made.")

    message(paste0("There are ", length(unique(combined_precip$GEO_CONCAT)), " sub-units in this data."))
    expected_N <- length(unique(combined_precip$GEO_CONCAT)) * 260
    message(paste0("So, ultimately, the resulting dataset should have ", expected_N, " observations."))
    message("Check that this is true.")
    
    write_dta(combined_precip, output_path, version = 14)
    message("Results saved to:", output_path)
    return(combined_precip)
}

# Set up
cf_path <- "D:/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"

# Burkina Faso results
burkina_results <- climate_fertility_workflow(
  country_name = "Burkina Faso",
  cf_ssa_path = cf_path,
  growing_season_range = c(6:9)
)

# Zambia results
zambia_results <- climate_fertility_workflow(
  country_name = "Zambia",
  cf_ssa_path = cf_path,
  growing_season_range = c(3:7)
)