#' Project:     Climate and Fertility in Sub-Saharan Africa
#' Purpose:     Clean temperature data
#' Author:      Prayog Bhattarai
#' Date:        2025-03-26
#' Description: This script creates precipitation variables from ERA5 and generates standardized precipitation variables as done in Kenya project. 
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
library(lubridate)
library(parallel)

cf_ssa_path <- "D:/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"


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
              pattern = "\\_temp.csv$", full.names = TRUE)
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

# function to join temperatuere data with grids
merge_temp_data <- function(country_data, era_gridpoints_df, grid_final_sf) {
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
collapse_area_weighted <- function(country_name, 
                                  country_joined, growing_season_range) {
  subunit_collapsed <- country_joined |> 
  subset(select = -c(total_area, loc_area, 
    geometry, coordinates, grid_id)) |> 
  group_by(GEO_CONCAT, valid_time) |> 
  summarise(
      temp_mean = fmean(t2m_mean, w = shr_of_subunit, na.rm = TRUE),
      temp_max = max(t2m_max, na.rm = TRUE),
      .groups = "drop"
    )
  df <- subunit_collapsed %>%
    mutate(country = country_name,
           date = anydate(valid_time),
           day = day(date),
           month = month(date),
           year = year(date),
           temp_avg = temp_mean - 273,    # conversion from Kelvin to Celsius
           temp_max = temp_max - 273,
           decade = floor((year - 1940)/10) * 10 + 1940,
           growing_season = month %in% growing_season_range) |> 
    dplyr::select(-valid_time)
  message("Created time variables and growing-non-growing season variables.")  
  return(df)
}

generate_degree_days <- function(df, thresholds) {
  # Vectorized calculation for max and avg temperatures
  print(thresholds)
  for (i in thresholds) {
    print(paste0("Threshold: ", i))
    df <- df %>%
      mutate(
      !!paste0("temp_max_dd_", i) := pmax(temp_max, i) - i,
      !!paste0("temp_avg_dd_", i) := pmax(temp_avg, i) - i
      )
  }
  message("- Generated degree day variables")
  return(df)
}

collapse_to_annual <- function(df) {
  final_output <- df %>%
    dplyr::select(-temp_max, -temp_mean, -temp_avg) |>
    group_by(GEO_CONCAT, year) %>%
    summarise(across(starts_with("temp_"), 
      sum, na.rm = TRUE),
      .groups = "drop")
  message("Temperature collapsed to the annual level")
  message("The function for processing country temperature to annual level is complete!")
  return(final_output)
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

process_census_year <- function(cohort_year, temperature_data, censusyear, maxcohort = NULL, maxyear = NULL, era_intermediate) {
  # Function to process temperature data for a given year
  # 
  # Required parameters:
  # - censusyear: year of the census (1989, 1999, 2009, 2019)
  #
  # Optional parameters:
  # - maxcohort: latest cohort year to include in the analysis
  #
  # Actions:
  # - Creates age-specific temperature exposure variables
  # - Collapses data by subcounty and cohort
  
  require(dplyr)
  require(haven)
  
  message("Census year: ", censusyear)
  
  # Step 1: Filter data based on maxcohort and censusyear
  if (!is.null(maxcohort)) {
    message("Maximum cohort: ", maxcohort)
    cohort_data <- cohort_data %>%
      filter(cohort <= maxcohort, year <= maxyear)
  }
  message("- Step 1. Data filtered to cohort and census.")
  
  # Step 2: Join with temperature data
  stopifnot(!any(duplicated(cohort_data)))
  stopifnot(!any(duplicated(temperature_data)))

  setDT(cohort_data)
  setDT(temperature_data)
  combined_data <- cohort_data[temperature_data, on = "year", nomatch = 0, allow.cartesian=TRUE]

  message("- Step 2. Joined with temperature data")
  
  # Step 3: Generate age variables
  age_ranges <- list(
    "14_18" = c(14, 18),
    "19_23" = c(19, 23),
    "24_28" = c(24, 28),
    "29_33" = c(29, 33),
    "34_38" = c(34, 38),
    "39_44" = c(39, 44),
    "14_29" = c(14, 29),
    "30_44" = c(30, 44)
  )
  
  for (range in names(age_ranges)) {
    start <- age_ranges[[range]][1]
    end <- age_ranges[[range]][2]
    joined_data <- joined_data %>%
      mutate(!!paste0("age_", range) := ifelse(age >= start & age <= end, 1, 0))
  }
  message("- Step 3. Generated age variables.")
  
  # Step 4: Generate degree day variables
  temp_vars <- c()
  for (temp in c(22, 24, 26, 28)) {
    temp_vars <- c(temp_vars, 
                   paste0("temp_max_dd_", temp), 
                   paste0("temp_max_dd_", temp, "_gs"), 
                   paste0("temp_max_dd_", temp, "_ngs"))
  }
  
  for (var in temp_vars) {
    for (range in names(age_ranges)) {
      joined_data <- joined_data %>%
        mutate(!!paste0(var, "_age_", range) := !!sym(var) * !!sym(paste0("age_", range)))
    }
  }
  message("- Step 4. Generated degree day variables.")
  
  # Step 5: Collapse data
  collapsed_data <- joined_data %>%
    group_by(GEO_CONCAT, cohort) %>%
    summarise(across(starts_with("temp_"), 
      sum, na.rm = TRUE), 
      .groups = "drop") %>%
    mutate(census = censusyear)
  message("- Step 5. Collapsed at the subcounty cohort level.")
  
  return(collapsed_data)
}


# -------------------------
# 01. Workflow Execution
# -------------------------

#' Workflow for processing climate and fertility data
#'
#' @param country_name Character name of the country (e.g., "Zambia")
#' @param cf_ssa_path Character path to the main project directory
#' @param growing_season_range Numeric vector specifying the months of the growing season (e.g., c(3:7))
#' @param census_years Numeric vector of census years to process (default: c(1989, 1999, 2009, 2019))
#' @param cohort_filters List of cohort-year filters for each census year. Each element is a vector of two numbers:
#'                       the maximum cohort year and the maximum year to include in the analysis.
#'                       Default: list(c(1975, 1989), c(1985, 1999), c(1995, 2009), c(Inf, Inf)).
#' @return A data.table containing the processed dataset with cohort-level observations.
#' @note This function orchestrates the entire workflow, including data preparation, processing, and saving results.
workflow <- function(country_name, cf_ssa_path, 
  growing_season_range, 
  census_years = c(1989, 1999, 2009, 2019), 
  cohort_filters = list(
    c(1975, 1989),  # For 1989
    c(1985, 1999),  # For 1999
    c(1995, 2009),  # For 2009
    c(Inf, Inf)     # For 2019
  )) {
  # Step 1: Combine ERA5 CSV files for the specified country
  country_data <- combine_csvs(country_name, cf_ssa_path)

  # Step 2: Prepare the shapefile for spatial operations
  shapefile <- prepare_shapefile(country_name, cf_ssa_path)

  # Step 3: Create a raster grid for the country
  country_raster <- create_raster_grid(country_data)

  # Step 4: Overlap grid squares with administrative sub-units
  grid_final_sf <- overlap_grid_subunits(country_raster, shapefile)

  # Step 5: Create grid IDs for the ERA5 data
  era_gridpoints_df <- create_grid_ids(country_data)

  # Step 6: Merge temperature data with the grid
  country_joined <- merge_temp_data(country_data, era_gridpoints_df, grid_final_sf)

  # Step 7: Collapse data to sub-unit level using area-weighted means
  subunit_data <- collapse_area_weighted(country_name, country_joined, growing_season_range)

  # Step 8: Generate degree day variables for temperature thresholds
  df <- generate_degree_days(subunit_data, c(22, 24, 26, 28))

  # Step 9: Collapse data to the annual level
  df <- collapse_to_annual(df)

  # Step 10: Generate base cohort-year combinations
  cohort_years <- generate_cohort_years(
  start_cohort = 1926, 
  end_cohort = 2005,
  start_year = 1940, 
  end_year = 2019,
  age_min = 14, 
  age_max = 44
  )

  # Step 11: Process census years and generate cohort-level data
  era_intermediate <- file.path(cf_ssa_path, "data/derived/era", country_name)
  results <- list()

  for (i in seq_along(census_years)) {
    census_yr <- census_years[i]
    max_cohort <- cohort_filters[[i]][1]
    max_year <- cohort_filters[[i]][2]

    message("Processing census year: ", census_yr, 
    " (cohort filter: ", max_cohort, 
    ", year filter: ", max_year, ")")

    results[[i]] <- process_census_year(
    cohort_years,
    df,
    censusyear = census_yr,
    maxcohort = max_cohort,
    maxyear = max_year,
    era_intermediate = era_intermediate
    )
  }

  # Step 12: Combine all results into a single dataset
  combined_precip <- bind_rows(results) |> 
    filter(!is.na(cohort)) |>     # Drop missing cohorts
    arrange(GEO_CONCAT, cohort, census) |> 
    group_by(GEO_CONCAT, cohort, census) |> 
    filter(row_number() == 1) |>  # Remove duplicates
    ungroup()

  # Step 13: Display summary messages
  message(paste0("There are ", length(unique(combined_precip$GEO_CONCAT)), " sub-units in this data."))
  expected_N <- length(unique(combined_precip$GEO_CONCAT)) * 260
  message(paste0("So, ultimately, the resulting dataset should have ", expected_N, " observations."))
  message("Check that this is true.")

  # Step 14: Save the results to a .dta file
  country_slug <- tolower(gsub(" ", "-", country_name))
  output_path <- file.path(era_intermediate, paste0(country_slug, "-precip-combined.dta"))
  write_dta(combined_precip, output_path, version = 14)
  message("Results saved to:", output_path)
}

# -------------------------
# 02. Execute Workflow
# -------------------------

# Define the main project path
cf_path <- "D:/NUS Dropbox/Prayog Bhattarai/Climate_Change_and_Fertility_in_SSA"

# Execute the workflow for Zambia with the specified growing season range
df <- workflow("Benin", cf_path, c(3:7))

# Print the number of unique sub-units in the resulting dataset
print(length(unique(df$GEO_CONCAT)))