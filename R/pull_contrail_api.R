#' @title Download water quality data from Contrail website
#'
#' @description
#' Downloads raw water quality monitoring data from the Contrail platform for
#' specified two water quality sites maintained by the City of Fort Collins,
#' Manner's Bridge (PMAN) and Indian Meadows (Poudre Below Rustic, PBR) for the specified time period.
#'
#' This script does not filter the data obtained from Contrail and returns all data available from API call.
#' Note the timezones for the returned dataset and the input timestamps.
#'
#' @param start_DT POSIXct timestamp indicating the starting point for data
#' retrieval. Timestamp should be in America/Denver timezone, if this is not the case, it will be updated to America/Denver.
#'
#' @param end_DT POSIXct timestamp indicating the endpoint for data retrieval.
#' Timestamp should be in America/Denver timezone, if this is not the case, it will be updated to America/Denver.
#' Default is the current system time (Sys.time()).
#'
#' @param username Character string for the Contrail account username used to access the site.
#'                  Obtained from City of Fort Collins staff
#'
#' @param password Character string for the Contrail account password used to access the site
#'                 Obtained from City of Fort Collins staff
#'
#' @param login_url Character string for the Contrail login page used to access the site
#'                 Obtained from City of Fort Collins staff
#'
#' @return A list of dataframes, each containing the water quality data for a specific site.
#' The dataframes contain the following columns:
#' - `DT_round`: Datetime of the measurement in UTC, rounded to the nearest 15 minute interval.
#' - `DT_round_MT`: Datetime of the measurement in America/Denver timezone, rounded to the nearest 15 minute interval.
#' - `DT_join`: Character form of DT_round
#' - `parameter`: The water quality parameter being measured (e.g., "DO", "Turbidity", etc.), supplied by the api_urls dataframe
#' - `site`: The site where the measurement was taken (e.g., "PMAN", "PBR") with "_fc" added to the end of the site name
#' - `value`: The value of the measurement for the specified parameter
#' - `units`: The units of the measurement (e.g., "mg/L", "NTU", etc.), supplied by Contrail
#'
#'
#' @examples
#'
#'# Get start and end dates for the data pull
#'start_DT <- Sys.time() - days(7) # in America/Denver timezone
#'end_DT <- Sys.time() # in America/Denver timezone
#'
#' #Get credentials for Contrail API
#'creds <- yaml::read_yaml("creds/ContrailCreds.yml") %>%
#' unlist()
#'
#' username <- as.character(creds["username"])
#' password <- as.character(creds["password"])
#' login_url <- as.character(creds["login_url"])
#'
#'
# Call the downloader function
#' data <- pull_contrail_api(start_DT = start_DT,
#'                           end_DT = end_DT,
#'                           username = username,
#'                           password = password,
#'                           login_url = login_url)

pull_contrail_api <- function(start_DT, end_DT = Sys.time(), username, password, login_url) {

  # Check if both objects are datetime objects (POSIXct or POSIXlt)
  if (!inherits(start_DT, c("POSIXct", "POSIXlt"))) {
    stop("start_DT is not a datetime object")
  }
  if (!inherits(end_DT, c("POSIXct", "POSIXlt"))) {
    stop("end_DT is not a datetime object")
  }
  # Check timezone
  start_tz <- attr(start_DT, "tzone")
  end_tz <- attr(end_DT, "tzone")

  # Handle NULL timezone (system default) and set to Sys.timezone to check later
  if (is.null(start_tz)|start_tz == "") start_tz <- Sys.timezone()
  if (is.null(end_tz)||end_tz == "") end_tz <- Sys.timezone()
  #check to see if start_tz is in America/Denver from user submission or if Sys.timezone is in America/Denver
  #correct start_DT to America/Denver if not
  if (start_tz != "America/Denver") {
    message("start_DT or Sys.timezone() is not in America/Denver timezone.\n Changing start_DT to America/Denver timezone.")
    start_DT <- with_tz(start_DT, tzone = "America/Denver")
  }
  # Same as above for end_DT
  if (end_tz != "America/Denver") {
    message("end_DT or Sys.timezone() is not in America/Denver timezone.\nChanging end_DT to America/Denver timezone.")
    end_DT <- with_tz(end_DT, tzone = "America/Denver")
  }


  # Create session file for persistent cookies
  session_file <- tempfile()

  contrail_login_url <- login_url
  continue_key <- gsub(x = str_extract(contrail_login_url, "continue=([^&]+)"), pattern = "continue=", replacement = "")

  # Get the login page and extract CSRF token
  login_page_req <- request(contrail_login_url) |>
    req_cookie_preserve(session_file)

  login_page_resp <- req_perform(login_page_req)

  # Extract CSRF token
  csrf_token <- login_page_resp %>%
    resp_body_html() |>
    html_node("input[name='csrf_token']") |>
    html_attr("value")

  #  Login with credentials
  login_req <- request(contrail_login_url) |>
    req_cookie_preserve(session_file) |>
    req_method("POST") |>
    req_body_form(
      username = username,
      password = password,
      login = "login",
      continue = continue_key,  # from end of url
      csrf_token = csrf_token
    )

  login_resp <- req_perform(login_req)

  # Check if login was successful
  if(resp_status(login_resp) == 200 && !grepl("login", login_resp$url)) {
    message("Login successful! Testing Download")
  } else {
    stop("Login failed - check credentials")
  }

  start_DT_encoded <- format(start_DT, "%Y-%m-%d 20%H:%M:%S")%>%
    gsub(" ", "%", .)

  end_DT_encoded <- format(end_DT, "%Y-%m-%d 20%H:%M:%S")%>%
    gsub(" ", "%", .)


  # Define sites to download
  sites <- c("Manners Bridge", "Indian Meadows")
  # Define sensors to download
  sensors <- c("Chlorophyll", "Conductivity", "Dissolved Oxygen",
               "Stage", "pH", "Temperature", "Turbidity", "Phycocyanin")


  # Iterate over sites and parameters to download data
  results <- map_dfr(sites, function(site) {

    # Get site page (contains all parameter links)
    resp <- request("https://contrail.fcgov.com/list/") |>
      req_cookie_preserve(session_file) |>
      req_perform()

    page <- resp |> resp_body_html()
    #Navigate to the site page
    href <- page |>
      html_element(xpath = paste0("//a[contains(text(), '", site,"')]")) |>
      html_attr("href")
    full_url <- paste0("https://contrail.fcgov.com", href)
    # move into site page
    site_resp <- request(full_url) |>
      req_cookie_preserve(session_file) |>
      req_perform()

    site_page <- site_resp |> resp_body_html()

    # Create site code
    site_code <- ifelse(site == "Manners Bridge", "PMAN", "PBR")

    # Iterate over parameters for each site
    map(sensors, function(.param) {

        #Correct param for downstream uses
        parameter <- case_when(
          .param == "Chlorophyll" ~ "Chl-a Fluorescence",
          .param == "Conductivity" ~ "Specific Conductivity",
          .param == "Dissolved Oxygen" ~ "DO",
          .param == "Stage" ~ "Depth",
          .param == "pH" ~ "pH",
          .param == "Temperature" ~ "Temperature",
          .param == "Turbidity" ~ "Turbidity",
          .param == "Phycocyanin" ~ "TAL PC Fluorescence",
          TRUE ~ NA_character_
        )

        page <- site_page
        # Find parameter
        href <- page |>
          html_element(xpath = paste0("//a[contains(text(),'",.param ,"')]")) |>
          html_attr("href")

        # Check if found
        if (is.na(href)) {
          message("No link found for ", site, " - ", .param)
          return(NULL)
        }

        full_param_url <- paste0("https://contrail.fcgov.com", href)
        # Test param link
        site_param_page <- request(full_param_url) |>
          req_cookie_preserve(session_file) |>
          req_perform()
        # Check if accessible
        if (resp_status(site_param_page) != 200) {
          message("Param: ", .param, " for site: ", site, " not accessible")
          return(NULL)
        }

        #Parse out site_id from url
        site_id_info <- str_extract(full_param_url, "(?<=site_id=).*")
        #encode url for download
        encoded_url <- paste0(
          "https://contrail.fcgov.com/export/file/?site_id=", site_id_info,
          "&mode=&hours=&data_start=", start_DT_encoded,
          "&data_end=", end_DT_encoded,
          "&tz=US%2FMountain&format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S&mime=txt&delimiter=comma"
        )

        tryCatch({
          #request download from encoded url
          download_req <- request(encoded_url) |> req_cookie_preserve(session_file)
          download_resp <- req_perform(download_req)

          if (resp_status(download_resp) == 200) {
            #message("Downloading for: ", .param, " for site: ", site)
            # Read CSV content directly from response
            csv_content <- resp_body_string(download_resp)

            data <- read_csv(csv_content, show_col_types = FALSE) |>
              mutate(
                DT_MT = force_tz(Reading, tzone = "America/Denver"),
                DT = with_tz(DT_MT, tzone = "UTC"),
                timestamp = DT,
                DT_round = round_date(DT, "15 minutes"),
                DT_round_MT = with_tz(DT_round, tz = "America/Denver"),
                DT_join = as.character(DT_round),
                parameter = parameter,
                site_cd = paste0(tolower(site_code), "_fc"),
                units = case_when(
                  parameter == "Specific Conductivity" ~ "µS/cm",
                  parameter == "Turbidity" ~ "NTU",
                  parameter == "DO" ~ "mg/L",
                  parameter == "pH" ~ "pH",
                  parameter == "Temperature" ~ "°C",
                  parameter == "Chl-a Fluorescence" ~ "RFU",
                  parameter == "Blue-Green Algae Fluorescence" ~ "RFU",
                  parameter == "Depth" ~ "ft",
                  TRUE ~ NA_character_
                )
              ) |>
              select(timestamp, DT, DT_round, DT_round_MT, DT_join, parameter, site_cd,
                     value = Value, units)

            return(data)
          } else {
            message("Download failed for: ", site_code, " ", parameter)
            return(NULL)
          }

        }, error = function(e) {
          message("Error downloading from: ", site_code, " ", parameter)
          return(NULL)
        })
      })
  })%>%
    rename(site = site_cd)%>%
    split(f = list(.$site, .$parameter), sep = "-") %>%
    keep(~nrow(.) > 0)

  # Message of how many site/parameters where successfully accessed from Contrail
  message("Download Successfull for ", length(results), " of ", (length(sensors)*length(sites)), " datasets")
  return(results)
}



