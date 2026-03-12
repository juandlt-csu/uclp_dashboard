# Data Collection Workflow ====

# Description: Here we collect the data for all of the APIs, clean them up,
# autoqaqc them, collate them, and save them into a single parquet file that
# will be sourced by the shiny app for plotting the data and pulling the data.

# This workflow is set up to be run in this repository's GitHub Actions.

# Set up environment ----
message(paste("Collation Step:", "set up libraries"))

package_loader <- function(x) {
  if (x %in% installed.packages()) {
    suppressMessages({
      library(x, character.only = TRUE)
    })
  } else {
    suppressMessages({
      install.packages(x)
      library(x, character.only = TRUE)
    })
  }
}

invisible(
  lapply(c(
    # Date/time handling
    "zoo",
    "padr",
    # Data cleaning and utilities
    "janitor",
    "here",
    # Stats/modeling
    "RcppRoll",
    # Web scraping/data retrieval
    "rvest",
    "httr",
    "httr2",
    "yaml",
    # Core data manipulation
    "tidyverse",
    "data.table",
    "arrow",
    "ross.wq.tools"
  ),
  package_loader)
)

# Helper function
`%nin%` <- Negate(`%in%`)

# suppress scientific notation to ensure consistent formatting
options(scipen = 999)

# Load in saved data ----
message(paste("Collation Step:", "loading in cached data"))
cached_data <- arrow::read_parquet(here("data", "data_backup.parquet"),
                                   as_data_frame = TRUE)

# Get the min max DT from all sites ----
message(paste("Collation Step:", "getting start dates"))
# The cached data will have all of the data from all of the
# sources, so this should not have to change.
mm_DT <- cached_data %>%
  bind_rows() %>%
  filter(parameter != "ORP") %>%
  group_by(site, parameter) %>%
  summarize(max_dt = max(DT_round, na.rm = T),
            .groups = "drop") %>%
  filter(max_dt == min(max_dt, na.rm = T)) %>%
  slice(1) %>%
  pull(max_dt) %>%
  # This prevents data pulling errors from HV
  {if (format(., "%H:%M:%S") == "00:00:00") . + lubridate::seconds(1) else .}

# Set up dates ----
# Check mm_DT timezone
mm_DT_tz <- lubridate::tz(mm_DT)
if (mm_DT_tz == "UTC") {

  # Grab Sys.time()
  t <- Sys.time()

  # Make the start/end time in UTC
  utc_start_DT <- lubridate::with_tz(mm_DT, "UTC")
  utc_end_DT <- lubridate::with_tz(t, "UTC")

  # Make the start/end time in America/Denver
  denver_start_DT <- lubridate::with_tz(mm_DT, "America/Denver")
  denver_end_DT <- lubridate::with_tz(t, "America/Denver")

} else if (mm_DT_tz != "UTC") {
  # Wrong timezone specified
  stop("wrong timezone available in cached data.")
} else if (is.null(mm_DT_tz) || mm_DT_tz == "") {
  # No timezone specified
  stop("no timezone available in cached data.")
} else {
  # Unknown error
  stop("unknown tz error.")
}


# Pull in data ----

## Radio Telemetry Data ----
# This comes through the WET API service.
# Pulled in by the function `pull_wet_api()`.
invalid_wet_values <- c(-9999, 638.30, -99.99)
source(file = here("R", "pull_wet_api.R"))

wet_sites <- c("sfm", "chd", "pfal")

wet_data <- map(wet_sites,
                ~pull_wet_api(
                  target_site = .x,
                  start_datetime = denver_start_DT,
                  end_datetime = denver_end_DT
                ))%>%
  rbindlist()%>%
  filter(value %nin% invalid_wet_values, !is.na(value))%>%
  split(f = list(.$site, .$parameter), sep = "-")

## HydroVu Livestream Data ----
message(paste("Collation Step:", "getting HydroVu livestream data"))
staging_directory = tempdir()

# Read in credentials from environment variables (GitHub Secrets)
client_id <- Sys.getenv("HYDROVU_CLIENT_ID")
client_secret <- Sys.getenv("HYDROVU_CLIENT_SECRET")
# Check if credentials are available
if(client_id == "" || client_secret == "") {
  stop("HydroVu credentials not found. Please check GitHub Secrets.")
} else {
  message(paste("....Collation Step Update:", "HydroVu secrets retrieved"))
}

hv_token <- tryCatch({
  hv_auth(client_id = client_id, client_secret = client_secret)
}, error = function(e) {
  message("HydroVu authentication failed: ", e$message)
  return(NULL)
})

if(is.null(hv_token)) {
  stop("Could not authenticate with HydroVu API. Check credentials and API status.")
}

# Pulling in the data from hydrovu
hv_sites <- hv_locations_all(hv_token) %>%
  filter(!grepl("vulink", name, ignore.case = TRUE))%>%
  filter(!grepl("2024", name, ignore.case = TRUE))

message(paste("....Collation Step Update:", "successfully pulled in hv_sites object from HydroVu"))

sites <- c("pbd")

message(paste("....Collation Step Update:", "Attempting to pull data from HydroVu API"))
walk(sites,
     function(site) {
       message("Requesting HV data for: ", site)
       ross.wq.tools::api_puller(
         site = site,
         start_dt = utc_start_DT,
         end_dt = utc_end_DT,
         api_token = hv_token,
         hv_sites_arg = hv_sites,
         dump_dir = staging_directory
       )
     }
)

hv_data <- list.files(staging_directory, full.names = TRUE, pattern = ".parquet") %>%
  map_dfr(function(file_path){
    site_df <- read_parquet(file_path, as_data_frame = TRUE)
    return(site_df)
  }) %>%
  select(-id) %>%
  mutate(units = as.character(units)) %>%
  filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
  mutate(
    DT = timestamp,
    DT_round = round_date(DT, "15 minutes"),
    DT_join = as.character(DT_round),
    site = tolower(site)) %>%
  select(-name) %>%
  distinct(.keep_all = TRUE)%>%
  split(f = list(.$site, .$parameter), sep = "-") %>%
  keep(~nrow(.) > 0)

message(paste("....Collation Step Update:", "successfully pulled and munged HydroVu API data"))

## Contrail Data ----
source(file = here("R", "pull_contrail_api.R"))

contrail_un <- Sys.getenv("CONTRAIL_CLIENT_ID")
contrail_pw <- Sys.getenv("CONTRAIL_CLIENT_SECRET")
contrail_url <- Sys.getenv("CONTRAIL_CLIENT_URL")

contrail_data <- pull_contrail_api(
  start_DT = denver_start_DT,
  end_DT = denver_end_DT,
  username = contrail_un,
  password = contrail_pw,
  login_url = contrail_url
  )

# Collating Datasets ----
# Add previous 26 hours of data to flag properly
cached_context <- cached_data %>%
  filter(DT_round >= (utc_start_DT - hours(26))) %>%
  split(f = list(.$site, .$parameter), sep = "-") %>%
  keep(~nrow(.) > 0)

# combine all data and remove duplicate site/sensor combos (from log data + livestream)
all_data_with_context <- c(hv_data, wet_data, contrail_data, cached_context) %>%
  bind_rows() %>%
  mutate(value = ifelse(parameter == "Depth" & units == "ft", value * 0.3048, value),
         units = case_when(parameter == "Depth" & units == "ft" ~ "m",
                           parameter == "Temperature" & units == "C" ~ "°C",
                           TRUE ~ units),
         timestamp = DT) %>%
  split(f = list(.$site, .$parameter), sep = "-") %>%
  keep(~nrow(.) > 0)

# remove stage data
list_names <- names(all_data_with_context)
keep_indices <- !grepl("stage", list_names, ignore.case = TRUE)
all_data_with_context <- all_data_with_context[keep_indices]

# Tidy all the raw files
tidy_data <- all_data_with_context %>%
  map(~tidy_api_data(api_data = .)) %>%
  keep(~!is.null(.)) %>%
  keep_at(imap_lgl(., ~!grepl("ORP", .y)))

# Read in threshold and sensor notes ----
sensor_thresholds_file <- "data/qaqc/sensor_spec_thresholds.yml"
seasonal_thresholds_file <- "data/qaqc/updated_seasonal_thresholds_2025_sjs.csv"
fc_seasonal_thresholds_file <- "data/qaqc/fc_seasonal_thresholds_2025_sjs.csv"
fc_field_notes_file <- "data/qaqc/fc_field_notes_formatted.rds"

sensor_thresholds <- read_yaml(sensor_thresholds_file)
fc_seasonal_thresholds <- read_csv(fc_seasonal_thresholds_file, show_col_types = FALSE)
season_thresholds <- read_csv(seasonal_thresholds_file, show_col_types = FALSE)%>%
  bind_rows(fc_seasonal_thresholds)

# Pulling in the data from mWater (where we record our field notes)
message(paste("Collation Step:", "getting mWater creds"))
mWater_creds <- Sys.getenv("MWATER_SECRET")
mWater_data <- ross.wq.tools::load_mWater(creds = mWater_creds)

fc_field_notes <- read_rds(fc_field_notes_file)
all_field_notes <- grab_mWater_sensor_notes(mWater_api_data = mWater_data) %>%
  bind_rows(fc_field_notes)

sensor_malfunction_notes <- grab_mWater_malfunction_notes(mWater_api_data = mWater_data)%>%
  mutate(start_DT = with_tz(start_DT, tzone = "UTC"),
         end_DT = with_tz(end_DT, tzone = "UTC"))

# Add field notes and summary stats ----
combined_data <- tidy_data %>%
  map(~add_field_notes(df = ., notes = all_field_notes), .progress = TRUE)

summarized_data <- combined_data %>%
  map(~generate_summary_statistics(.))

# Applying AutoQAQC ----

# Single Sensor Flags
summarized_data_chunks <- split(1:length(summarized_data),
                                ceiling(seq_along(1:length(summarized_data))/10))

single_sensor_flags <- list()

for (chunk_idx in seq_along(summarized_data_chunks)) {
  message("\n=== Processing chunk ", chunk_idx, " of ", length(summarized_data_chunks), " ===")

  indices <- summarized_data_chunks[[chunk_idx]]
  chunk_data <- summarized_data[indices]

  chunk_results <- chunk_data %>%
    map(
      function(data) {
        flagged_data <- data %>%
          data.table(.) %>%
          add_field_flag(df = .) %>%
          add_na_flag(df = .) %>%
          find_do_noise(df = .) %>%
          add_drift_flag(df = .)

        if (unique(data$parameter) %in% names(sensor_thresholds)) {
          flagged_data <- flagged_data %>%
            data.table(.) %>%
            add_spec_flag(df = ., spec_table = sensor_thresholds)
        }

        if (unique(data$parameter) %in% unique(season_thresholds$parameter)) {
          flagged_data <- flagged_data %>%
            data.table(.) %>%
            add_seasonal_flag(df = ., threshold_table = season_thresholds)
        }

        return(flagged_data)
      },
      .progress = TRUE
    )

  single_sensor_flags <- c(single_sensor_flags, chunk_results)

  if (chunk_idx < length(summarized_data_chunks)) {
    gc()
    Sys.sleep(0.1)
  }
}

# Intrasensor Flags
intrasensor_flags <- single_sensor_flags %>%
  rbindlist(fill = TRUE) %>%
  split(by = "site")

intrasensor_data_chunks <- split(1:length(intrasensor_flags),
                                 ceiling(seq_along(1:length(intrasensor_flags))/2))

intrasensor_flags_list <- list()
for (chunk_idx in seq_along(intrasensor_data_chunks)) {
  message("\n=== Processing chunk ", chunk_idx, " of ", length(intrasensor_data_chunks), " ===")

  indices <- intrasensor_data_chunks[[chunk_idx]]
  chunk_data <- intrasensor_flags[indices]

  chunk_results <- chunk_data %>%
    map(
      function(data) {
        flagged_data <- data %>%
          data.table() %>%
          add_frozen_flag(.) %>%
          intersensor_check(.) %>%
          add_burial_flag(.) %>%
          add_unsubmerged_flag(.)

        return(flagged_data)
      }
    ) %>%
    rbindlist(fill = TRUE) %>%
    mutate(flag = ifelse(flag == "", NA, flag)) %>%
    split(f = list(.$site, .$parameter), sep = "-") %>%
    purrr::discard(~ nrow(.) == 0)%>%
    map(~add_malfunction_flag(df = ., malfunction_records = sensor_malfunction_notes))

  intrasensor_flags_list <- c(intrasensor_flags_list, chunk_results)

  if (chunk_idx < length(intrasensor_data_chunks)) {
    gc()
    Sys.sleep(0.1)
  }
}

# Network Check and Final Flags
site_order_list <- list(
  clp = c("joei", "cbri", "chd", "pfal", "pbr_fc", "pman", "pbd",
          "bellvue", "salyer", "udall", "riverbend", "cottonwood", "elc",
          "archery", "riverbluffs"),
  sfm = c("sfm")
)

network_flags <- intrasensor_flags_list %>%
  purrr::map(~ross.wq.tools::network_check(df = .,
                                            intrasensor_flags_arg = intrasensor_flags_list,
                                            site_order_arg = site_order_list)) %>%
  rbindlist(fill = TRUE) %>%
  tidy_flag_column() %>%
  split(f = list(.$site, .$parameter), sep = "-") %>%
  purrr::map(~add_suspect_flag(.)) %>%
  rbindlist(fill = TRUE)


# final data cleaning and preparation
v_final_flags <- network_flags %>%
  dplyr::mutate(auto_flag = ifelse(
    is.na(auto_flag), NA,
    ifelse(auto_flag == "suspect data" &
             is.na(lag(auto_flag, 1)) &
             is.na(lead(auto_flag, 1)), NA, auto_flag)
  )) %>%
  dplyr::select(c("DT_round", "DT_join", "site", "parameter", "mean", "units",
                  "n_obs", "spread", "auto_flag", "mal_flag", "sonde_moved",
                  "sonde_employed", "season", "last_site_visit")) %>%
  dplyr::mutate(auto_flag = ifelse(is.na(auto_flag), NA,
                                   ifelse(auto_flag == "", NA, auto_flag))) %>%
  split(f = list(.$site, .$parameter), sep = "-") %>%
  keep(~nrow(.) > 0) %>%
  bind_rows()

# Remove overlapping time periods from cached data, then add all new data ----
final_data <- cached_data %>%
  anti_join(v_final_flags, by = c("site", "parameter", "DT_round")) %>%
  bind_rows(v_final_flags) %>%
  arrange(site, parameter, DT_round)

# Write to new file ----
arrow::write_parquet(final_data, here("data", "data_backup.parquet"))
