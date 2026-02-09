#' @title Download water quality data from WET website
#'
#' @description
#' Downloads raw water quality monitoring data from the Water and Earth Technology platform for
#' three water quality sites maintained by ROSS, South Fork at Pingree Bridge (sfm), Poudre below Poudre Falls (pfal),
#' and Joe Wright Creek below Chambers Lake dam (chd) for the specified time period.
#'
#' This script does not filter the data obtained and returns all data available from API call.
#' Note the timezones for the returned dataset and the input timestamps.
#'
#' @param target_site Character string for specific site of interest (see list below)
#'                    Available sites: "sfm", "pfal", "chd"
#'
#' @param start_datetime POSIXct timestamp or string (format: "%Y-%M-%D %H:%M) indicating the starting point for data retrieval.
#' Timestamp should be in America/Denver timezone, if this is not the case, it will be updated to America/Denver.
#' Default is the current system time (Sys.time()).
#'
#' @param end_datetime POSIXct timestamp or string (format: "%Y-%M-%D %H:%M) indicating the endpoint for data retrieval.
#' Timestamp should be in America/Denver timezone, if this is not the case, it will be updated to America/Denver.
#' Default is the current system time (Sys.time()).
#'
#'
#' @param data_type Character string for specific parameter of interest (see list below) or "all" for all available parameters from these sites
#'                  Available data types: "Stage", "Depth", "pH", "Turbidity", "Specific Conductivity", "FDOM Fluorescence", "DO", "Chl-a Fluorescence", "Temperature"
#'                  Note: Stage not available at CHD
#'
#' @param time_window Character string indicating if this will be a two hour call (for scheduling)
#'or the period specified by start_DT and end_DT.
#'
#'
#' @return A list of dataframes, each containing the water quality data for a specific site.
#' The dataframes contain the following columns:
#' - `site`: The site where the measurement was taken (e.g., "sfm", "chd", pfal)
#' - `DT_round`: Datetime of the measurement in UTC, rounded to the nearest 15 minute interval.
#' - `DT_round_MT`: Datetime of the measurement in America/Denver timezone, rounded to the nearest 15 minute interval.
#' - `DT_join`: Character form of DT_round
#' - `parameter`: The water quality parameter being measured (e.g., "DO", "Turbidity", etc.), supplied by the api_urls dataframe
#' - `value`: The raw value of the measurement for the specified parameter. NAs included
#' - `units`: The units of the measurement (e.g., "mg/L", "NTU", etc.), supplied by Contrail
#'
#'
#' @examples
#'
#' Call the downloader function for a single site
#' data <- pull_wet_api(target_site = "sfm",
#'                      start_datetime = "2025-06-20 10:00:00",
#'                      end_datetime = "2025-06-23 10:00:00",
#'                      data_type = "all",
#'                      time_window = "")
#'
#' Call for multiple sites for a specific time period
#' sites <- c("sfm", "chd", "pfal")
#' new_WET_data <- map_dfr(sites,
#'                        ~pull_wet_api(
#'                          target_site = .x,
#'                          start_datetime = Sys.time()-days(7),
#'                          end_datetime = Sys.time(),
#'                          data_type = "all",
#'                        time_window = "all"
#'                        ))
#' Call for multiple sites for a 2 hour window
#' new_WET_data <- map_dfr(sites,
#'                       ~pull_wet_api(
#'                       target_site = .x,
#'                       start_datetime = Sys.time()-hours(2),
#'                       end_datetime = Sys.time(),
#'                       data_type = "all",
#'                       time_window = "2 hours"
#'                       ))
#'
#'

pull_wet_api <- function(target_site, start_datetime, end_datetime = Sys.time(), data_type = "all", time_window = "all") {


  if (!exists("%nin%")) {
    `%nin%` = Negate(`%in%`)
  }

  # Pre-define constants to avoid repeated object creation
  sensor_numbers <- data.table(
    data_type = c("Stage", "Depth", "pH", "Turbidity", "Specific Conductivity",
                  "FDOM Fluorescence", "DO", "Chl-a Fluorescence", "Temperature"),
    parameter_units = c("ft", "ft", "pH", "NTU", "uS/cm",
                        "RFU", "mg/L", "RFU", "C"),
    sensor_number = c("07", "11", "12", "13", "15",
                      "16", "17", "19", "20"),
    stringsAsFactors = FALSE
  )

  # Select parameters, if all, keep everything in sensor_numbers
  if (data_type == "all") {
    selected_parameters <- sensor_numbers
  } else {
    selected_parameters <- sensor_numbers[data_type %in% data_type]
  }

  # Site numbers in WET system
  site_numbers <- data.table(
    site_code = c("sfm", "chd", "pfal"),
    site_num = c(115170, 115290, 115310),
    stringsAsFactors = FALSE
  )

  # Get site info (early validation)
  site_info <- site_numbers[site_code == target_site]
  if (nrow(site_info) == 0) {
    stop("Invalid site_code: ", target_site)
  }

  # Sensors were used for testing in office before deployed
  deployment_dates <- data.table(
    site_code = c("sfm", "chd", "pfal"),
    deploy_date = as.POSIXct(c("2024-04-25 10:00:00", "2025-03-28 10:00:00", "2024-09-25 10:00:00"),
                             tz = "America/Denver"),
    stringsAsFactors = FALSE
  )

  # Get deployment date for early filtering
  deploy_date <- deployment_dates[site_code == target_site, deploy_date]

  # Optimized datetime parsing function
  parse_datetime_optimized <- function(dt, target_tz = "America/Denver") {
    if (is.character(dt)) {
      # Try required format first
      parsed <- as.POSIXct(dt, format = "%Y-%m-%d %H:%M", tz = target_tz)
      if (!is.na(parsed)) return(parsed)

      # Fallback to lubridate parsing
      parsed <- ymd_hm(dt, tz = target_tz)
      if (!is.na(parsed)) return(parsed)

      # Final fallback
      parsed <- parse_date_time2(dt, orders = c("Ymd HMS", "Ymd HM", "Ymd HMS z", "Y md HM z"), tz = target_tz)
      if (is.na(parsed)) {
        stop("datetime could not be parsed. Please provide a valid datetime in 'YYYY-MM-DD HH:MM' or POSIXct format.")
      }
      return(parsed)
    } else {
      # If dt is already a POSIXct object, ensure it is in the correct timezone
      return(with_tz(dt, tzone = target_tz))
    }
  }

  # Parse datetimes
  start_datetime <- parse_datetime_optimized(start_datetime)
  end_datetime <- parse_datetime_optimized(end_datetime)
  if (!is.character(end_datetime)) {
    end_datetime <- end_datetime + hours(2)
  }

  # Calculate increments
  if (time_window == "2 hours") {
    increments_since_now <- 26 # 2 hours = 24 increments of 5 minutes + 2 additional in case of repeats or errors
  } else {
    # Otherwise get the difference between now and the start time to calculate the # of intervals we need to query
    # For safer performance do 12 intervals per hour (5 min data) in the event of errors with radio system
    increments_since_now <- round(as.numeric(difftime(with_tz(Sys.time() + hours(1), tzone = "America/Denver"),
                                                      start_datetime, units = "hours")) * 12, digits = 0)
  }

  # Create URLs to look up for site
  urls_dt <- data.table(
    data_type = selected_parameters$data_type,
    sensor_number = selected_parameters$sensor_number,
    parameter_units = selected_parameters$parameter_units,
    site_code = site_info$site_code,
    site_num = site_info$site_num,
    stringsAsFactors = FALSE
  )

  # CHD does not have stage data
  urls_dt <- urls_dt[!(site_code == "chd" & data_type == "Stage")]

  # Create URL for each sensor and increments
  urls_dt[, indv_url := paste0("https://wetmapgc.wetec.us/cgi-bin/datadisp_q?ID=", site_num, sensor_number, "&NM=", increments_since_now)]

  # Data html processing function
  process_urls_optimized <-

    # # Not sure if this works yet...
    # is_shiny <- tryCatch({
    #   shiny::isRunning()
    # }, error = function(e) FALSE)
    #
    # # TODO: Test this in a shiny app, perhaps we add a T/F param to decide which method to use
    # if (is_shiny) {
    #   # Running in Shiny - use progress bar and sequential processing
    #   progress <- shiny::Progress$new(session, min = 0, max = nrow(urls_dt))
    #   progress$set(message = "Retrieving data...", value = 0)
    #   on.exit(progress$close())
    #
    #   WQ_data_list <- vector("list", nrow(urls_dt))
    #   for (i in seq_len(nrow(urls_dt))) {
    #     progress$set(detail = paste("Processing", urls_dt$data_type[i]), value = i)
    #     WQ_data_list[[i]] <- process_urls_optimized(
    #       urls_dt$indv_url[i],
    #       urls_dt$data_type[i],
    #       urls_dt$site_code[i],
    #       urls_dt$parameter_units[i]
    #     )
    #   }
    # } else if (requireNamespace("future", quietly = TRUE) && requireNamespace("furrr", quietly = TRUE)) {
    #   # Not in Shiny - use parallel processing if available
    #   future::plan(future::multisession, workers = min(4, nrow(urls_dt)))
    #   WQ_data_list <- furrr::future_pmap(
    #     list(urls_dt$indv_url, urls_dt$data_type,
    #          urls_dt$site_code, urls_dt$parameter_units),
    #     process_urls_optimized,
    #     .options = furrr::furrr_options(seed = TRUE)
    #   )
    #   future::plan(future::sequential)
    # } else {
    # Sequential processing
    WQ_data_list <- pmap(list(urls_dt$indv_url, urls_dt$data_type,urls_dt$site_code, urls_dt$parameter_units),
                         #function to actually pull data based on parameters/urls
                         function(indv_url, data_type, site_code, parameter_units) {
                           tryCatch({

                             # Read and parse HTML
                             response <- GET(indv_url)
                             html_content <- content(response, "text", encoding = "UTF-8")

                             # Use base R instead of stringr (faster)
                             lines <- strsplit(html_content, "\n", fixed = TRUE)[[1]]
                             data_lines <- lines[grepl("\\d{2}/\\d{2}/\\d{4}", lines)]

                             if (data_type == "Stage") {
                               # Parse stage data
                               parsed_dt <- data.table(raw_line = data_lines)
                               parsed_dt[, c("Date", "Time", "value", "col4", "col5") :=
                                           tstrsplit(raw_line, "\\s+", fill = "right", type.convert = FALSE)]
                             } else {
                               # Parse other data types
                               parsed_dt <- data.table(raw_line = data_lines)
                               parsed_dt[, c("Date", "Time", "value", "col4") :=
                                           tstrsplit(raw_line, "\\s+", fill = "right", type.convert = FALSE)]

                             }

                             # Parse data to desired formats
                             parsed_dt[, `:=`(
                               Date = mdy(Date),
                               Time = hms::as_hms(Time),
                               value = suppressWarnings(as.numeric(value)) # This is for -9999.00 values which do not get read in correctly yet
                             )]

                             # Create datetime column using America/Denver time zone
                             parsed_dt[, datetime := as_datetime(paste0(Date, " ", Time), tz = "America/Denver")]

                             # Create final columns
                             parsed_dt[, `:=`(
                               DT_round = with_tz(round_date(datetime, unit = "15 minutes"), tz = "UTC"),
                               DT_round_MT = datetime,
                               site = site_code,
                               parameter = data_type,
                               units = parameter_units,
                               value = fifelse(is.nan(value), NA_real_, value)
                             )]

                             # Create DT_join after DT_round is created
                             parsed_dt[, DT_join := as.character(DT_round)]

                             # Select final columns
                             result_dt <- parsed_dt[, .(DT_round, DT_round_MT, DT_join, site, parameter, value, units)]

                             return(result_dt)

                           }, error = function(e) {
                             warning("Error processing URL for ", data_type, ": ", e$message)
                             return(NULL)
                           })
                         })

  # Remove NULL results
  WQ_data_list <- WQ_data_list[!sapply(WQ_data_list, is.null)]

  if (length(WQ_data_list) == 0) {
    return(data.table())
  }

  # Combine results using rbindlist (data.table equivalent of bind_rows)
  WQ_data <- rbindlist(WQ_data_list)

  # Apply final filters
  WQ_data <- WQ_data[between(DT_round_MT, start_datetime, end_datetime) &
                       DT_round_MT > deploy_date]

  # Reorder columns
  setcolorder(WQ_data, c("site", "DT_round", "DT_round_MT", "DT_join", "parameter", "value", "units"))

  return(WQ_data)
}
