#' Plot Predicted TOC using a pre-trained xgboost model
#'
#' This function applies a pre-trained TOC model to new sensor data. Data (df) should be pre-processed using `prep_toc_model_data()`
#' @param df A data frame containing the new sensor data to predict TOC. This should be in wide format with the following columns:
#'  - `site`: Site identifier
#'  - `DT_round`: Timestamp of the data rounded to the nearest 15 min interval
#'  - `FDOM`: FDOM Fluorescence (RFU) of In Situ Sonde
#'  - `Temp`: Temperature (Celsius) of In Situ Sonde
#'  - `Sensor_Cond`: Specific Conductivity (uS/cm) of In Situ Sonde
#'  - `Sensor_Turb`: Turbidity (NTU) of In Situ Sonde
#'  - `log_sensor_turb`: log transformed Turbidity (NTU) of In Situ Sonde
#'  - `Chl_a`: Chl-a Fluorescence (RFU) of In Situ Sonde
#'  - `log_chl_a`: log transformed Chl-a Fluorescence (RFU) of In Situ Sonde
#'  - `f_c_turb`: interaction term between FDOM, Turbidity & Chl-a (f_c_turb = FDOM/(Chl_a + Sensor_Turb + FDOM)))
#'  The data should be pre-processed to ensure that the columns match the model's expected input and that data is normalized to the training scale (see `prep_toc_model_data()`)
#'
#' #' @param toc_model_file_path String character. The file path to the saved TOC model (RDS format). The final models are saved in the `$final_model` of each ensemble member
#' #' @param summarize_interval String character. The time interval to summarize the data. Default is 1 hour. Cannot be less than 15 minutes.
#'
#' #' @return A data frame with the original data summarized to 1 hour and an additional column `toc_guess` containing the predicted TOC values
#'
#' @examples
#'
#' toc_model <- create_toc_model_plot(
#'  df = your_sensor_data,
#'  summary_interval = "6 hours",
#'  toc_model_file_path = "data/upper_clp_dss/modeling/toc_xgboost_20250801.rds",
#' )

create_toc_model_plot <- function(df, site_sel, summary_interval,start_DT,end_DT){

  #convert datetimes
  start_DT <- ymd_hm(start_DT,tz = "MST")
  end_DT <- ymd_hm(end_DT,tz = "MST")
  # Get testing data RMSE for the model
  # find half way point between start and end DT
  halfway_DT <- start_DT + (end_DT - start_DT) / 2

  # Summarize data to desired interval and trim to individual site
  sum_data_norm <- prepped_data_norm%>%
    filter(site == site_sel)%>%
    select(site, DT_round, all_of(features)) %>%
    mutate(DT_round = round_date(DT_round, unit = summary_interval )) %>% # ensure DT_round is rounded to the nearest
    group_by(site, DT_round) %>%
    summarise(across(everything(), mean, na.rm = TRUE), .groups = 'drop')
  #convert to matrix
  model_input_matrix_sum <- sum_data_norm[, features]%>%
    mutate(across(everything(), as.numeric)) %>%
    as.matrix()

  # Add predictions from each fold model as new columns
  for (i in seq_along(toc_models)) {
    fold_model <- toc_models[[i]]$finalModel
    col_name <- glue("{target}_guess_fold{i}")
    sum_data_norm[[col_name]] <- predict(fold_model, model_input_matrix_sum)
  }

  fold_cols <- glue("{target}_guess_fold{seq_along(toc_models)}")
  ensemble_col <- glue("{target}_guess_ensemble")
  sum_data_norm[[ensemble_col]] <- rowMeans(sum_data_norm[, fold_cols])

  # make min and max toc guess col based on fold guesses
  plot_sensor_data <- sum_data_norm %>%
    mutate(!!glue("{target}_guess_min") := pmin(!!!syms(fold_cols)),
           !!glue("{target}_guess_max") := pmax(!!!syms(fold_cols)))%>%
    mutate(!!glue("{target}_guess_ensemble") := pmax(0, !!sym(glue("{target}_guess_ensemble"))))%>% # remove TOC guesses < 0
    #filter to desired time window
    filter(between(DT_round, start_DT , end_DT))

  # #Water chem
  # plot_water_chem <- water_chem %>%
  #   filter(site == site_sel)%>%
  #   filter(between(DT_round, start_DT, end_DT))%>%
  #   mutate(DT_round = round_date(DT_round, unit = sum_int )) #round date to match sensor data

  #get correct dates for plot title
  start_DT <- min(plot_sensor_data$DT_round, na.rm = TRUE)
  end_DT <- max(plot_sensor_data$DT_round, na.rm = TRUE)

  toc_plot <- ggplot() +
    geom_ribbon(data = plot_sensor_data,
                aes(x = DT_round,
                    ymin = TOC_guess_min,
                    ymax = TOC_guess_max,
                    fill = "Models Estimate Range"),
                alpha = 0.5) +
    # one line per fold_col, each with its own color
    # map(fold_cols, ~
    #   geom_path(data = plot_sensor_data,
    #             aes(x = DT_round,
    #                 y = .data[[.x]],
    #                 color = .x),    # <- put fold name in legend
    #             linewidth = 0.3)
    # ) +
    geom_path(data = plot_sensor_data,
              aes(x = DT_round,
                  y = .data[[ensemble_col]],
                  color = "Mean Model Estimate"),
              linewidth = 1) +
    geom_point(data = plot_water_chem,
               aes(x = DT_round,
                   y = TOC,
                   shape = collector,
                   color = "Sample Values")) +
    labs(title = paste0("Estimated TOC at ", str_to_title(site_title)," from ", as.Date(start_DT), " - ", as.Date(end_DT)),
         x = "Date",
         y = "Model Estimated TOC (mg/L)",
         shape = "Sample Collector",
         fill = "",
         color = "") +
    scale_fill_manual(values = c("Models Estimate Range" = "grey")) +
    scale_color_manual(
      values = c(
        #setNames(scales::hue_pal()(length(fold_cols)), fold_cols),  # distinct colors for folds
        "Mean Model Estimate" = "#E70870",
        "Sample Values" = "#002EA3"
      )
    ) +
    theme(legend.position = "bottom") +
    guides(fill = guide_legend(override.aes = list(alpha = 0.5))) +
    annotate("text",
             x = max(plot_sensor_data$DT_round, na.rm = TRUE),
             y = Inf,
             label = "PRELIMINARY RESULTS",
             hjust = 1.1,
             vjust = 1.5,
             size = 7,
             fontface = "bold",
             color = "red") +
    ROSS_theme

  # combine grabs and sensor data for plotting to determine axes
  plot_toc <- c(plot_sensor_data$TOC_guess_min, plot_sensor_data$TOC_guess_max, plot_water_chem$TOC)
  max_plot_toc <- max(plot_toc, na.rm = TRUE)
  min_plot_toc <- min(plot_toc, na.rm = TRUE)

  if(max_plot_toc > max(water_chem$TOC, na.rm = T)){
    toc_plot <- toc_plot +
      geom_hline(yintercept = toc_max, linetype = "dashed", color = "red") +
      annotate("text", x = halfway_DT, y = toc_max - 0.2, label = "Max TOC Measured")
  }

  return(toc_plot)

}
