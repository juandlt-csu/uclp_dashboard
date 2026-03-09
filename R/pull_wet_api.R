
#' @title Download water quality data from WET website
#'
#' @description
#' Downloads raw water quality monitoring data from the Water and Earth Technology platform for
#' three water quality sites maintained by ROSS, South Fork at Pingree Bridge (sfm), Poudre below Poudre Falls (pfal),
#' and Joe Wright Creek below Chambers Lake dam (chd) for the specified time period.
#'
#' This script filters data for the specified time period and sets any values less than -9000 to NA, as these are error values from the WET sensors. It also rounds the datetime to the nearest 15 minute interval and converts it to both UTC and Mountain Time for easier joining with other datasets.
#' Note the timezones for the returned dataset and the input timestamps.
#'
#' @param target_site Character string for specific site of interest (see list below)
#'                    Available sites: "sfm", "pfal", "chd"
#'
#' @param start_datetime POSIXct timestamp or string (format: "%Y-%M-%D %H:%M) indicating the starting point for data retrieval.
#' POSIXct timesteps will be converted to `America/Denver` timezones. ** String DTs are assumed to be in `America/Denver` timezone.**
#' Default is the current system time (Sys.time()- days(7)).
#'
#' @param end_datetime POSIXct timestamp or string (format: "%Y-%M-%D %H:%M) indicating the end point for data retrieval.
#' POSIXct timesteps will be converted to `America/Denver` timezones. ** String DTs are assumed to be in `America/Denver` timezone.**
#' Default is the current system time (Sys.time()).
#'
#' @return Dataframe containing the water quality data for a specific site.
#' The dataframe contain the following columns:
#' - `site`: The site where the measurement was taken (e.g., "sfm", "chd", pfal)
#' - `DT_round`: Datetime of the measurement in UTC, rounded to the nearest 15 minute interval.
#' - `DT_round_MT`: Datetime of the measurement in America/Denver timezone, rounded to the nearest 15 minute interval.
#' - `DT_join`: Character form of DT_round
#' - `parameter`: The water quality parameter being measured (e.g., "DO", "Turbidity", etc.), supplied by the `sensor_numbers` dataframe that is created within this function.
#' - `value`: The raw value of the measurement for the specified parameter. NAs included
#' - `units`: The units of the measurement (e.g., "mg/L", "NTU", etc.), supplied by the sensor_numbers dataframe
#'
#'
#' @examples
#'
#' Call the function for a single site and time period
#' data <- pull_wet_api(target_site = "sfm",
#'                      start_datetime = "2025-10-20 10:00",
#'                      end_datetime = "2025-10-23 10:00")
#'
#' Call for multiple sites for a specific time period (last 7 seven days)
#' sites <- c("sfm", "chd", "pfal")
#' new_WET_data <- map_dfr(sites,
#'                        ~pull_wet_api(
#'                          target_site = .x,
#'                          start_datetime = Sys.time()-days(7),
#'                          end_datetime = Sys.time()
#'                        ))
#'
#'
pull_wet_api <- function(target_site, start_datetime, end_datetime = Sys.time()) {

  # Pre-define constants using explicit data.table calls
  sensor_numbers <- data.table::data.table(
    data_type = c("Stage", "Depth", "pH", "Turbidity", "Specific Conductivity",
                  "FDOM Fluorescence", "DO", "Chl-a Fluorescence", "Temperature"),
    parameter_units = c("ft", "ft", "pH", "NTU", "uS/cm", "RFU", "mg/L", "RFU", "C"),
    sensor_number = c("07", "11", "12", "13", "15", "16", "17", "19", "20")
  )

  site_numbers <- data.table::data.table(
    site_code = c("sfm", "chd", "pfal"),
    site_num = c(115170, 115290, 115310)
  )

  deployment_dates <- data.table::data.table(
    site_code = c("sfm", "chd", "pfal"),
    deploy_date = as.POSIXct(c("2024-04-25 10:00:00", "2025-03-28 10:00:00", "2024-09-25 10:00:00"), tz = "America/Denver")
  )

  # Check to see if the target site is one of our sites
  if (!(target_site %in% site_numbers$site_code)) {
    stop("Invalid target site. Please choose from: ", paste(site_numbers$site_code, collapse = ", "))
  }

  # Validation and Setup
  site_info <- site_numbers[site_code == target_site]
  deploy_date <- deployment_dates[site_code == target_site, deploy_date]

  parse_dt <- function(dt) {
    if (is.character(dt)){
      attempted_parse <- lubridate::ymd_hm(dt, tz = "America/Denver")
      if(is.na(attempted_parse)) {
        stop("datetime could not be parsed. Please check formatting")
      }
      return(attempted_parse)
    }
    return(lubridate::with_tz(dt, tzone = "America/Denver"))
  }

  start_dt_parsed <- parse_dt(start_datetime) - lubridate::minutes(5)
  end_dt_parsed <- parse_dt(end_datetime) + lubridate::minutes(5)

  # Calculate increments using namespaced lubridate/base functions
  increments <- round(as.numeric(difftime(lubridate::with_tz(Sys.time() + lubridate::hours(1), "America/Denver"),
                                          start_dt_parsed, units = "hours")) * 12, 0)

  # Filter sensors (CHD stage check)
  urls_dt <- sensor_numbers[!(target_site == "chd" & data_type == "Stage")]

  # Generate a list of httr2 request objects
  req_list <- purrr::map(urls_dt$sensor_number, ~{
    httr2::request("https://wetmapgc.wetec.us/cgi-bin/datadisp_q") |>
      httr2::req_url_query(ID = paste0(site_info$site_num, .x), NM = increments) |>
      httr2::req_retry(max_tries = 3)
  })

  # --- Perform all requests in parallel ---
  resps <- httr2::req_perform_parallel(req_list, on_error = "continue", progress = FALSE)

  # Process responses
  WQ_data_list <- purrr::map2(resps, seq_along(resps), function(resp, idx) {
    if (inherits(resp, "httr2_response")) {
      tryCatch({
        content <- httr2::resp_body_string(resp, encoding = "UTF-8")
        lines <- strsplit(content, "\n", fixed = TRUE)[[1]]

        # Fast filter for lines with dates
        data_lines <- lines[grep("\\d{2}/\\d{2}/\\d{4}", lines)]
        if (length(data_lines) == 0) return(NULL)

        # Vectorized split
        cols <- data.table::tstrsplit(data_lines, "\\s+", fill = "right")

        # Build DT
        dt <- data.table::data.table(
          site = target_site,
          DT_round_MT = as.POSIXct(paste(cols[[1]], cols[[2]]), format="%m/%d/%Y %H:%M:%S", tz="America/Denver"),
          parameter = urls_dt$data_type[idx],
          value = as.numeric(cols[[3]]),
          units = urls_dt$parameter_units[idx]
        )
        return(dt)
      }, error = function(e) NULL)
    }
    return(NULL)
  })

  # Combine Results
  WQ_data <- data.table::rbindlist(WQ_data_list)

  if (nrow(WQ_data) > 0) {
    # Filter using data.table i syntax
    WQ_data <- WQ_data[DT_round_MT >= start_dt_parsed & DT_round_MT <= end_dt_parsed & DT_round_MT > deploy_date]

    # Batch processing with lubridate and data.table specialized functions
    WQ_data[, `:=`(
      DT_round = lubridate::with_tz(lubridate::round_date(DT_round_MT, unit = "15 minutes"), tz = "UTC"),
      value = data.table::fifelse(value < -9000 | is.nan(value), NA_real_, value)
    )]

    WQ_data[, DT_join := as.character(DT_round)]

    data.table::setcolorder(WQ_data, c("site", "DT_round", "DT_round_MT", "DT_join", "parameter", "value", "units"))
  }

  return(WQ_data)
}
