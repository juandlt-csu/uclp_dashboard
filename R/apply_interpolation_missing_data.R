#' Gap fill short periods of missing df with linear interpolation
#'
#' This function uses [zoo::na.approx()] to linearly interpolate short gaps of
#' missing data in a time series. Interpolation is performed separately for each
#' combination of `site_col` and `parameter_col`. It is designed to fill only
#' short gaps and should be used cautiously to avoid introducing artifacts into
#' long missing periods.
#'
#' @param df A data frame containing time series data with missing values
#'   represented as `NA`. Data should be padded to regular time intervals (e.g.,
#'   using [padr::pad()]) before applying this function. The function will warn users
#'   if data is not equally spaced within site/parameter groups.
#' @param site_col A string giving the name of the column containing site
#'   identifiers.
#' @param parameter_col A string giving the name of the column containing
#'   parameter identifiers.
#' @param dt_col A string giving the name of the column containing datetime
#'   information. Must be a `POSIXct` or `df` will be rejected The function checks that the datetime
#'   column is equally spaced within each site/parameter group.
#'
#' @param value_col A string giving the name of the column containing the values
#'   to be gap filled.
#' @param max_gap An integer giving the maximum number of consecutive `NA`
#'   values to interpolate. The interpretation depends on the data’s time
#'   interval (e.g., `max_gap = 4` corresponds to 1 hour of gaps in 15-minute
#'   data).
#' @param new_value_col A string giving the name of the new column to store the
#'   gap-filled values. Default is `"mean_filled"`. If set to the same name as
#'   `value_col`, the original column will be replaced with the gap-filled values (not recommended)
#' @param method A string specifying the interpolation method. Options are "linear"
#' for linear interpolation (default) and "spline" for spline interpolation.
#' See documentation for [zoo::na.approx()] and [zoo::na.spline()] for details.
#' Method "spline" may perform better for data with diurnal cycles (Temperature, pH, DO) if missing data is at daily peaks or minimums
#'
#' @return A data frame with the original columns plus an additional column
#'   containing the linearly interpolated values (or with the original column
#'   replaced if `new_value_col` = `value_col`).
#'
#' @details
#' This function uses grouped operations (`dplyr::group_by()`) by `site_col` and
#' `parameter_col`. Within each group, linear interpolation is applied to the
#' `value_col` based on the numeric representation of `dt_col` for the number
#' of time intervals specified by `max_gap`
#'
#' @importFrom zoo na.approx
#'
#' @examples
#' \dontrun{
#' result <- linear_interp_missing_data(
#'   df = your_sensor_data,
#'   site_col = "site",
#'   parameter_col = "parameter",
#'   dt_col = "DT_round",
#'   value_col = "mean",
#'   max_gap = 4, # e.g., fill up to 1 hour of gaps in 15-minute data
#'   new_value_col = "mean_filled",
#'   method = "spline",
#' )
#' }
#'
#'
apply_interpolation_missing_data <- function(df, site_col = "site", parameter_col = "parameter",
                                       dt_col = "DT_round", value_col = "mean",
                                       max_gap = 4, new_value_col = "mean_filled",
                                       method = "linear") {

  # Check that dt is POSIXct
  dt_vals <- df[[dt_col]]
  if (!lubridate::is.POSIXt(dt_vals)) {
    stop("Error: The datetime column must be POSIXct/POSIXt.")
  }

  #check that dt is equally spaced (padded) within each site/parameter group
  pad_check <- df %>%
    dplyr::group_by(dplyr::across(all_of(c(site_col, parameter_col)))) %>%
    dplyr::group_walk(~ {
      dt_vals <- sort(unique(.x[[dt_col]]))  # grab unique times
      dt_diffs <- diff(dt_vals)              # differences
      unique_diffs <- unique(dt_diffs)
      if (length(unique_diffs) > 1) {
        warn(
          paste0("Error: Datetime column is not equally spaced for site = ",
                 unique(.y[[site_col]]), ", parameter = ", unique(.y[[parameter_col]]),
                 ". Please pad with padr::pad() first.")
        )
      }
    })

  # Ensure users can use `%nin%` operator
  `%nin%` = Negate(`%in%`)

  if(method %nin% c("spline", "linear")){
    stop("method must be either 'spline' or 'linear'")
  }
  if(method == "linear") {
    return(
    df %>%
      arrange(!!sym(site_col), !!sym(parameter_col), !!sym(dt_col)) %>%
      group_by(!!sym(site_col), !!sym(parameter_col)) %>%
      mutate(
        !!sym(new_value_col) := zoo::na.approx(
          x = as.numeric(!!sym(dt_col)),
          object = !!sym(value_col),
          maxgap = max_gap,
          na.rm = FALSE
        )
      ) %>%
      ungroup()
    )

  }
  if(method == "spline"){
    return(
    df %>%
      arrange(!!sym(site_col), !!sym(parameter_col), !!sym(dt_col)) %>%
      group_by(!!sym(site_col), !!sym(parameter_col)) %>%
      mutate(
        !!sym(new_value_col) := zoo::na.spline(
          x = as.numeric(!!sym(dt_col)),
          object = !!sym(value_col),
          maxgap = max_gap,
          na.rm = FALSE
        )
      ) %>%
      ungroup()
    )

  }

}

