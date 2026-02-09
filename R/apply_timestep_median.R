#' @title Apply timestep median aggregation
#' @export
#'
#' @description
#' Aggregates water quality data to a user-defined timestep (e.g., hourly, daily) by
#' computing the median of measurements within each timestep interval. This approach
#' reduces the influence of short-term variability and outliers, while retaining the
#' overall temporal patterns in the data.
#' Users can submit either a single long-format dataframe containing multiple sites and parameters,
#' or a map over a list of dataframes where each dataframe contains data for a single site-parameter combination.
#'
#' @param df Input data containing water quality measurements. Must include columns for
#' site, datetime, parameter, and value.
#' @param site_col The name of the column containing site identifiers. Default is `"site"`.
#' @param parameter_col The name of the column containing parameter identifiers. Default is `"parameter"`.
#' @param dt_col The name of the column containing datetime information. Must be a `POSIXct`
#' or otherwise compatible with `lubridate::floor_date()`. Default is `"DT_round"`.
#' @param value_col The name of the column containing the values to be aggregated. Default is `"mean"`.
#' @param new_value_col The name of the new column to store median-aggregated values.
#' Default is `"hourly_median"`.
#' @param timestep The time resolution for aggregation, passed to `lubridate::floor_date()`.
#' Common values include `"1 hour"`, `"1 day"`, `"15 minutes"`, etc. Default is `"1 hour"`.
#'
#' @return
#' If `data_format = "dataframe"`, returns a dataframe with one row per `(site, parameter, timestep)`
#' combination and a column containing the median values.
#' If `data_format = "list"`, returns a list of dataframes aggregated in the same way.
#'
#' @examples
#' \dontrun{
#'
#' # Aggregate to hourly median
#' result <- apply_timestep_median(
#'   df = your_sensor_data,
#'   site_col = "site",
#'   parameter_col = "parameter",
#'   dt_col = "DT_round",
#'   value_col = "mean",
#'   new_value_col = "hourly_median",
#'   timestep = "1 hour"
#' )
#'
#' # Aggregate to daily median with list input
#' result_list <- apply_timestep_median(
#'   df = your_sensor_data_list,
#'   data_format = "list",
#'   timestep = "1 day"
#' )
#' }


apply_timestep_median <- function(df,
                                  site_col = "site",
                                  parameter_col = "parameter",
                                  dt_col = "DT_round",
                                  value_col = "mean",
                                  new_value_col = "hourly_median",
                                  timestep = "1 hour") {
  # Check that dt_col is POSIXct
  time_vals <- df[[dt_col]]
  if (!lubridate::is.POSIXt(time_vals)) {
    stop("Error: The datetime column must be POSIXct/POSIXt.")
  }
  # Aggregate to specified timestep using median
  df %>%
    mutate(
      DT_group = lubridate::floor_date(!!sym(dt_col), unit = timestep)
    ) %>%
    group_by(!!sym(site_col), !!sym(parameter_col), DT_group) %>%
    mutate(!!sym(new_value_col) := median(!!sym(value_col), na.rm = TRUE)) %>%
    ungroup()


}
