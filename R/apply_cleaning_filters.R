
#' Apply cleaning filters to a dataframe based on flags
#'
#' @param df A dataframe containing columns 'mal_flag', 'auto_flag', 'parameter', 'mean', and 'site'.
#' @param value_col The name of the column containing the original values to be cleaned. Default is "mean".
#' @param new_value_col The name of the new column to store cleaned values. Default is "mean".
#' @return A dataframe with a new column containing cleaned values and a 'clean_flag' column indicating the reason for any NA values.
#'
#' @example
#'
#' df <- data.frame(
#'  mal_flag = c(NA, "malfunction", NA, NA),
#'  auto_flag = c(NA, NA, "drift", NA),
#'  parameter = c("FDOM Fluorescence", "Turbidity", "FDOM Fluorescence", "Chl-a Fluorescence"),
#'  mean = c(5, 15, 20, 1),
#'  site = c("sfm", "chd", "sfm", "sfm")
#'  )
#'
#'  cleaned_df <- apply_cleaning_filters(df)
#'
#'
apply_cleaning_filters <- function(df, value_col = "mean", new_value_col = "mean", new_flag_col = "clean_flag") {

  # Apply QA/QC
   df %>%
    mutate(
      !!sym(new_value_col) := case_when(
        !is.na(mal_flag) ~ NA,
        !is.na(auto_flag) & parameter == "FDOM Fluorescence" & auto_flag == "drift" ~ !!sym(value_col), # FDOM sensor over flagged for downwards drift
        !is.na(auto_flag) & parameter == "FDOM Fluorescence" & auto_flag == "outside of seasonal range" ~ !!sym(value_col), # FDOM sensor over flagged for seasonal range due to lack of data
        !is.na(auto_flag) & parameter == "Turbidity" & auto_flag == "outside of seasonal range" & !!sym(value_col) <= 10 & !!sym(value_col) != 0 ~ !!sym(value_col), # Turbidity sensor over flagged for seasonal range due to lack of data at lower values
        !is.na(auto_flag) & parameter == "Chl-a Fluorescence" & str_detect(auto_flag, regex("outside of seasonal range|slope violation", ignore_case = TRUE)) & !!sym(value_col) <= 1 ~ !!sym(value_col), # Chl-a sensor over flagged at lower ranged for seasonal thresholds or slope violations
        !is.na(auto_flag) & parameter == "Chl-a Fluorescence" & auto_flag == "outside of seasonal range" & site == "sfm" & !!sym(value_col) <= 2 ~ !!sym(value_col), # Chl-a sensor over flagged at lower ranged for seasonal thresholds or slope violations
        !is.na(auto_flag) & auto_flag == "outside of seasonal range" & site == "chd" & parameter != "pH" ~ !!sym(value_col), #chambers is being overflagged across all parameters except pH
        !is.na(auto_flag) & auto_flag == "drift"  & parameter == "Turbidity" & !!sym(value_col) <= 15 ~ !!sym(value_col), # Drift below 15 NTU can be neglected (not cause for alarms)
        !is.na(auto_flag) & auto_flag == "outside of seasonal range"  & parameter == "Temperature" & !!sym(value_col) <= 15 ~ !!sym(value_col), # overflagged temp at lower range
        is.na(auto_flag) & is.na(mal_flag) ~ !!sym(value_col),  # Otherwise keep original value
        TRUE ~ NA_real_ # if it is not one of these cases, set to NA
      ),
      clean_flag = case_when(
        !is.na(mal_flag) ~ mal_flag,
        !is.na(auto_flag) ~ auto_flag,
        TRUE ~ NA_character_
      )
    )

}
