#' Predict TOC using a pre-trained model
#'
#' This function applies a pre-trained TOC model to new sensor data. Data (df) should be pre-processed using
#' @param sensor_data A data frame containing the new sensor data to predict TOC. This should be in long format with the following data:
#'  - `site_col`: Site identifier
#'  - `time_col`: Timestamp of the data
#'  - `parameter_col`: Parameter name (e.g., "FDOM Fluorescence", "Temperature", "Specific Conductivity", "Turbidity", "Chl-a Fluorescence")
#'  - `value_col`: Measured value for the parameter
#'  The following features are required for the model:
#'  - `FDOM Fluorescence (RFU)` of In Situ Sonde
#'  - `Temperature (Celsius)` of In Situ Sonde
#'  - `Specific Conductivity (uS/cm)` of In Situ Sonde
#'  - `Turbidity (NTU)`` of In Situ Sonde
#'  - `Chl-a Fluorescence (RFU)` of In Situ Sonde
#'  The model uses the following features computed from the variables above
#'  - `log_sensor_turb`: log transformed Turbidity (NTU) of In Situ Sonde
#'  - `log_chl_a`: log transformed Chl-a Fluorescence (RFU) of In Situ Sonde
#'  - `f_c_turb`: interaction term between FDOM, Turbidity & Chl-a (f_c_turb = FDOM/(Chl_a + Sensor_Turb + FDOM)))
#'  The data should be pre-processed to ensure that the columns match the model's expected input.
#'  Input data will be normalized to the training scale (see scaling parameters)
#'
#' @param toc_model_file_path String character. The file path to the saved TOC model (RDS format).
#' @param scaling_params_file_path String character. The file path to the saved scaling parameters (RDS format).
#' @param summarize_interval String character. The time interval to which the data should be summarized before prediction.
#' Default is "1 hour". Other common values include "15 minutes", "30 minutes", "1 day", etc.
#' @param time_col String character. The name of the column in `sensor_data` containing the datetime information.
#' Default is "DT_round".
#' @param site_col String character. The name of the column in `sensor_data` containing the site identifiers.
#' Default is "site".
#' @param parameter_col String character. The name of the column in `sensor_data` containing the parameter identifiers.
#' Default is "parameter".
#' @param value_col String character. The name of the column in `sensor_data` containing the measured values.
#' Default is "mean".
#' @details
#' The function first processes the input `sensor_data` to ensure it contains the necessary features for the model. The median based on the summarize interval of `time_col`
#' is then taken to reduce noise and short-term variability.
#' It then normalizes the data using the provided scaling parameters, applies each fold of the pre-trained TOC model to generate predictions,
#' and computes the ensemble mean of the predictions. The output includes the original data along with the predicted TOC values from each model fold
#' and the ensemble mean.
#'
#' @return A data frame with the original data with additional columns `toc_guess_#` containing the predicted TOC values from each model fold
#'and `toc_ensemble_mean` containing the ensemble mean of the predicted TOC values.
#'
#' @examples
#'
#' result <- predict_toc(
#'  new_data = your_sensor_data,
#'  toc_model_file_path = "data/upper_clp_dss/modeling/toc_xgboost_20250801.rds",
#'   scaling_params_file_path = "data/upper_clp_dss/modeling/scaling_parameters_20250801.rds"
#' )
#'


apply_toc_model <- function(sensor_data, toc_model_file_path, scaling_params_file_path, summarize_interval = "1 hour",
                            time_col = "DT_round", site_col = "site", parameter_col = "parameter", value_col = "mean"){

  # Define features for prediction (same order as training)
  features <- c('FDOM', 'Sensor_Turb',"log_sensor_turb" ,  'Sensor_Cond', 'Chl_a','Temp','f_c_turb', "log_chl_a")
  # Define target
  target <- 'TOC'
  # Load saved scaling parameters and model
  scaling_params <- readRDS(scaling_params_file_path)

  # Helper function to apply training scale
  apply_training_scale <- function(data,mod_features,  scaling_params, direction) {

    scaled_data <- data

    for (col in mod_features) {
      min_val <- scaling_params[[paste0(col, "_min")]]
      max_val <- scaling_params[[paste0(col, "_max")]]

      if(direction == "normalize"){
        # Normalize: (x - min) / (max - min)
        scaled_data[[col]] <- (data[[col]] - min_val) / (max_val - min_val)
      } else {
        # Denormalize: x * (max - min) + min
        scaled_data[[col]] <- data[[col]] * (max_val - min_val) + min_val
      }

    }

    return(scaled_data)
  }

  # Process the new data to match model requirements
  processed_sensor_data <- sensor_data %>%
    select(!!sym(time_col), !!sym(site_col), !!sym(parameter_col), !!sym(value_col)) %>%
    distinct()%>%
    #filter(!is.na(!!sym(value_col)))%>%
    pivot_wider(names_from = !!sym(parameter_col), values_from = !!sym(value_col))%>%
    #fix site names to match model
    select(!!sym(time_col), !!sym(site_col),
           #hrs_since_last_cleaning, # no longer used in the current model
           FDOM = `FDOM Fluorescence`,
           Temp = Temperature,
           Sensor_Cond = `Specific Conductivity`,
           Sensor_Turb = Turbidity,
           Chl_a = `Chl-a Fluorescence`)%>%
    mutate(#capping sensor turb at 1000 for outliers
      Sensor_Turb = ifelse(Sensor_Turb > 1000, 1000,
                           ifelse(Sensor_Turb == 0, 0.01, Sensor_Turb)), # if turb is zero, replace with 0.01 to avoid division by zero
      log_sensor_turb = log(Sensor_Turb),
      Chl_a = ifelse(Chl_a == 0,0.001, Chl_a),# if Chl_A is zero, replace with 0.001 to avoid division by zero
      log_chl_a = log(Chl_a),
      #compute optical bands
      f_c_turb = FDOM/(Chl_a + Sensor_Turb + FDOM),

      #round DT to summarize interval
      !!sym(time_col) := round_date(!!sym(time_col), unit = summarize_interval))%>%
    #summarize to the specified interval
    summarize(across(all_of(c(features)), median, na.rm = TRUE),.by = c(!!sym(site_col), !!sym(time_col)))

  model_input_data <-  processed_sensor_data %>%
    na.omit() # remove any rows missing needed values

  #Apply scaling normalization and convert to matrix
  model_input_matrix <- model_input_data %>%
    apply_training_scale(features, scaling_params = scaling_params, direction = "normalize") %>%
    select(all_of(features)) %>%
    mutate(across(everything(), as.numeric)) %>%
    as.matrix()

  #Load Models
  #TODO Move to global file
  toc_models <- readRDS(file = toc_model_file_path)

  # Add predictions from each fold model as new columns
  for (i in seq_along(toc_models)) {
    fold_model <- toc_models[[i]]
    col_name <- glue("{target}_guess_fold{i}")
    model_input_data[[col_name]] <- predict(fold_model, model_input_matrix, iteration_range = c(1,fold_model$best_iteration))

  }

  # Create ensemble mean
  fold_cols <- glue("{target}_guess_fold{seq_along(toc_models)}")
  ensemble_col <- glue("{target}_guess_ensemble")
  model_input_data[[ensemble_col]] <- rowMeans(model_input_data[fold_cols], na.rm = TRUE)

  #Find all sensor data that is not in model input data (e.g. missing values)
  missing_vals <- anti_join(processed_sensor_data, model_input_data)


  # make min and max toc guess col based on fold guesses
  final_dataset <- model_input_data %>%
    bind_rows(missing_vals) %>%
    mutate(!!glue("{target}_guess_min") := pmin(!!!syms(fold_cols)),
           !!glue("{target}_guess_max") := pmax(!!!syms(fold_cols)))%>%
    mutate(!!glue("{target}_guess_ensemble") := pmax(0, !!sym(glue("{target}_guess_ensemble"))))%>%
    arrange(!!sym(site_col), !!sym(time_col))

# Return the data with TOC predictions
return(final_dataset)

}

