#### Server ####


server <- function(input, output, session) {
  # Call secure_server to check credentials
  # res_auth <- secure_server(
  #   check_credentials = check_credentials(
  #     db = "setup/credentials.sqlite",
  #     passphrase = Sys.getenv("DB_PASSWORD")
  #   )
  # )

  # Reactive values for storing data
  values <- reactiveValues(
    all_data = NULL,
    site_locations = NULL,
    flow_sites = NULL,
    last_refresh = Sys.time()
  )

  output$dynamic_load_button <- renderUI({
    div(
      actionBttn("load_data",
                 "Load Data",
                 color = "default",
                 style = "fill",
                 size = "lg"),
      switchInput("apply_qaqc_filter",
                  "Apply Data QAQC Filters",
                  value = TRUE,
                  inline = TRUE)
    )
  })

  #setup loaded data
  loaded_data <- reactiveVal(NULL)

#TODO: This is done at load up, should it be moved to global?

  #### Pre loading API/Cached Data ####
  observe({
    withProgress(message = "Retrieving CLP WQ Data...", {

      # read inputs once at startup
      date_range <- isolate(input$date_range) # This is where the user picks the start DT
      sites_select <- isolate(input$sites_select)
      parameters_select <- isolate(input$parameters_select)

      req(date_range, sites_select, parameters_select)

      sites_sel <- filter(site_table, site_name %in% sites_select) %>%
        pull(site_code)

      #Read in cached data which has been passed through autoQAQC
      #github_link <- "https://github.com/rossyndicate/uclp_dashboard/raw/main/data/data_backup.parquet"
      github_link <- "data/data_backup.parquet" #remove on live version and use ^ github link instead ^
      cached_data <- arrow::read_parquet(github_link, as_data_frame = TRUE)

  #### ---- Data pull between QAQC and Live ---- ####
      # #Calculate max datetimes in cached dataset by site
      # max_dts <- cached_data%>%
      #   summarise(max_DT = max(DT_round, na.rm = TRUE), .by = "site")
      #
      # end_DT   <- as.POSIXct(paste0(date_range[2], " 23:55"), tz = "America/Denver")
      #
      # #### WET API Pull ####
      # #check to see if we need to pull WET data
      # if(any(c("sfm", "chd", "pfal") %in% sites_sel)){
      #   # Define invalid values to filter out (these are used by WET team for testing or if data is down)
      #   invalid_values <- c(-9999, 638.30, -99.99)
      #   # Define sites to pull data for
      #   wet_sites <- c("sfm", "chd", "pfal")
      #   #grab the sites from sites_sel
      #   sites <- sites_sel[sites_sel %in% wet_sites]
      #   #report out progress pre pull
      #   #incProgress(0.2, detail = "Connecting to WET API...")
      #   incProgress(0.2, detail = "Importing ROSS radio telemetry data...")
      #   # Determine start date for each site based on cached data
      #   site_start_DT <- filter(max_dts, site %in% sites)%>%
      #     mutate(max_cached_DT = with_tz(max_DT, "America/Denver"))
      #
      #   # Pull in data from WET API for sfm, chd and pfal sites
      #   wet_data <- map2(site_start_DT$site, site_start_DT$max_cached_DT,
      #                    ~pull_wet_api(
      #                      target_site = .x,
      #                      start_datetime = .y,
      #                      end_datetime = end_DT, #always  today at midnight
      #                      data_type = "all",
      #                      time_window = "all"
      #                    )) %>%
      #     rbindlist()%>%
      #     filter(value %nin% invalid_values, !is.na(value)) %>%
      #     split(f = list(.$site, .$parameter), sep = "-")
      #
      #   #Saving to parquet file for faster loading later on
      #   #arrow::write_parquet(wet_data, paste0("data/wet_testing_subset_",as.Date(start_DT),"_",as.Date(end_DT),".parquet"))
      #   #Pre loading API pulled data for faster displaying
      #   #wet_data <- read_parquet(file = "data/wet_testing_subset_2025-06-22_2025-08-08.parquet")%>%   #Update dates as needed
      #   #remove invalid values or NAs
      #   # filter(value %nin% invalid_values, !is.na(value)) %>%
      #   # split(f = list(.$site, .$parameter), sep = "-")
      #
      #
      # }else{
      #   #return blank list
      #   wet_data <- list()
      # }
      #
      # #### HydroVu API Pull ####
      #
      # # check to see if we need to pull from PBD
      # if("pbd" %in% sites_sel){
      #   sites <- c("pbd")
      #
      #   #incProgress(0.4, detail = "Connecting to Hydro Vu API...")
      #   incProgress(0.4, detail = "Importing to HydroVu Data...")
      #
      #   # Code to actually pull in from API
      #
      #   # # suppress scientific notation to ensure consistent formatting
      #   options(scipen = 999)
      #
      #   # Establishing staging directory (temp_dir()) for storing API pulled data to merge with cached data
      #   staging_directory = tempdir()
      #
      #   # Read in credentials
      #   hv_creds <- read_yaml("creds/HydroVuCreds.yml")
      #   hv_token <- hv_auth(client_id = as.character(hv_creds["client"]),
      #                       client_secret = as.character(hv_creds["secret"]))
      #
      #   # Pulling in the data from hydrovu
      #   # Making the list of sites that we need
      #   hv_sites <- hv_locations_all(hv_token) %>%
      #     filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
      #     #sondes with 2024 in the name can be avoided to speed up the live data pull
      #     #these should be included in the historical data pull
      #     filter(!grepl("2024", name, ignore.case = TRUE))
      #
      #   site_start_DT <- filter(max_dts, site %in% sites)
      #
      #   walk(sites,
      #        function(site) {
      #          message("Requesting HV data for: ", site)
      #          api_puller(
      #            site = site_start_DT$site,
      #            network = "all",
      #            start_dt = site_start_DT$max_DT, # api puller needs UTC dates
      #            end_dt = with_tz(end_DT, tzone = "UTC"),
      #            api_token = hv_token,
      #            hv_sites_arg = hv_sites,
      #            dump_dir = staging_directory
      #          )
      #        }
      #   )
      #   #
      #   # # read in data from staging directory
      #   hv_data <- list.files(staging_directory, full.names = TRUE, pattern = ".parquet") %>%
      #     map_dfr(function(file_path){
      #       site_df <- read_parquet(file_path, as_data_frame = TRUE)
      #       return(site_df)
      #     }) %>%
      #     #doing some clean up
      #     select(-id) %>%
      #     mutate(units = as.character(units)) %>%
      #     #double check that Vulink data has been removed
      #     filter(!grepl("vulink", name, ignore.case = TRUE)) %>%
      #     mutate(
      #       DT = timestamp, #timestamp comes in UTC
      #       DT_round = round_date(DT, "15 minutes"), #rounding
      #       #DT_round_MT = with_tz(DT_round, tzone = "America/Denver"),
      #       DT_join = as.character(DT_round), #keeping in UTC but character form
      #       site = tolower(site)) %>%
      #     select(-name) %>%
      #     distinct(.keep_all = TRUE)%>%
      #     split(f = list(.$site, .$parameter), sep = "-") %>%
      #     keep(~nrow(.) > 0)
      #
      #
      #   #Saving to parquet file for faster loading later on
      #   #arrow::write_parquet(hv_data, paste0("data/hv_testing_subset_",as.Date(start_DT),"_",as.Date(end_DT),".parquet"))
      #   #Pre loading API pulled data for faster displaying
      #   # hv_data <- read_parquet(file = "data/hv_testing_subset_2025-06-22_2025-08-08.parquet")%>%   #Update dates as needed
      #   #   split(f = list(.$site, .$parameter), sep = "-") %>%
      #   #   keep(~nrow(.) > 0)
      #
      # }else{
      #   hv_data <- list()
      # }
      #
      # #### Contrail API Pull ####
      #
      # #check to see if we need to access contrail
      # if(any(c("pbr_fc", "pman_fc") %in% sites_sel)) {
      #   incProgress(0.7, detail = "Importing Contrail Data...")
      #
      #   # Define sites to pull data for
      #   contrail_sites <- c("pbr_fc", "pman_fc")
      #   #grab the sites from sites_sel
      #   sites <- sites_sel[sites_sel %in% contrail_sites]
      #
      #   trim_sites <- toupper(gsub("_fc", "", sites))
      #
      #   # Read/set up credentials
      #   creds <- yaml::read_yaml( "creds/ContrailCreds.yml") %>%
      #     unlist()
      #   username <- as.character(creds["username"])
      #   password <- as.character(creds["password"])
      #   login_url <- as.character(creds["login_url"])
      #   # Determine start date for contrail sites based on cached data
      #   contrail_start_DTs <- filter(max_dts, site %in% sites)%>%
      #     mutate(max_cached_DT = with_tz(max_DT, "America/Denver"))
      #
      #   #get the earliest max date to ensure we get all data)
      #   contrail_start_DT <- min(contrail_start_DTs$max_cached_DT)
      #
      #   # Call the downloader function
      #   contrail_data <- pull_contrail_api(contrail_start_DT, end_DT, username,password, login_url)
      #
      #   #Saving to parquet file for faster loading later on
      #   #arrow::write_parquet(contrail_data, paste0("data/contrail_testing_subset_",as.Date(start_DT),"_",as.Date(end_DT),".parquet"))
      #   #Pre loading API pulled data for faster displaying
      #   # contrail_data <- read_parquet(file = "data/contrail_testing_subset_2025-06-22_2025-08-08.parquet")%>%   #Update dates as needed
      #   #   split(f = list(.$site, .$parameter), sep = "-") %>%
      #   #   keep(~nrow(.) > 0)
      #
      # }else{
      #   contrail_data <- list()
      # }
      #
      # #### Data Aggregation  ####
      #
      # incProgress(0.9, detail = "Processing data...")
      # # combine all data
      # all_data_raw <- c(hv_data, wet_data, contrail_data)
      #
      # # remove stage data
      # list_names <- names(all_data_raw)
      # keep_indices <- !grepl("stage", list_names, ignore.case = TRUE)
      # all_data_raw <- all_data_raw[keep_indices]
      # # Failsafe if there is no data
      # if(length(all_data_raw) == 0){
      #   stop("No data found for the selected sites and date range.")
      # }
      #
      # # Tidy all the raw files
      # tidy_data <- all_data_raw %>%
      #   map(~tidy_api_data(api_data = .)) %>%  # the summarize interval default is 15 minutes
      #   keep(~!is.null(.))
      #
      # #add field notes
      # # Pulling in the data from mWater (where we record our field notes)
      # mWater_creds <- read_yaml("creds/mWaterCreds.yml")
      # mWater_data <- load_mWater(creds = mWater_creds)
      # all_field_notes <- grab_mWater_sensor_notes(mWater_api_data = mWater_data) %>%
      #   #notes come in as MST, converting to UTC
      #   mutate(DT_round = with_tz(DT_round, tzone = "UTC"),
      #          last_site_visit = with_tz(last_site_visit, tzone = "UTC"),
      #          DT_join = as.character(DT_round))
      #
      # sensor_malfunction_notes <- grab_mWater_malfunction_notes(mWater_api_data = mWater_data) %>%
      #   #notes come in as MST, converting to UTC
      #   mutate(start_DT = with_tz(start_DT, tzone = "UTC"),
      #          end_DT = with_tz(end_DT, tzone = "UTC"))
      #
      # # Add the field note data to all of the data
      # # This is the most recent uncleaned data that we got from the API
      # combined_data <- tidy_data %>%
      #   map(~add_field_notes(df = ., notes = all_field_notes), .progress = TRUE)%>%
      #   bind_rows()%>%
      #   mutate(auto_flag = NA,
      #          mal_flag = NA)
      #### ---- End of data pull between QAQC and Live ---- ####


      dashboard_data <- cached_data %>%
        #bind_rows(combined_data) %>% #unhash for live version (this is live data)
        arrange(site, parameter, DT_round) %>%
        distinct(site, parameter, DT_round, .keep_all = TRUE) %>%
        ungroup()


      # Set the loaded data
      incProgress(1, detail = "Data Loaded")

      loaded_data(dashboard_data)
    })
  })

  #### Base filtered data reactive ####
  # Only updates when load_data is pressed
  base_filtered_data <- eventReactive(input$load_data, {
    req(loaded_data(), input$date_range, input$sites_select, input$parameters_select)

    start_DT <- as.POSIXct(paste0(input$date_range[1], " 00:01"), tz = "America/Denver")
    end_DT   <- as.POSIXct(paste0(input$date_range[2], " 23:55"), tz = "America/Denver")

    sites_sel <- filter(site_table, site_name %in% input$sites_select) %>% pull(site_code)

    loaded_data() %>%
      mutate(DT_round_MT = with_tz(DT_round, tzone = "America/Denver")) %>%
      filter(
        between(DT_round_MT, start_DT, end_DT),
        site %in% sites_sel,
        parameter %in% input$parameters_select
      ) %>%
      mutate(
        mean = ifelse(!is.na(mal_flag), NA, mean),
        mean = if_else(units == "m", mean * 3.28084, mean), #converting depth readings to feet for better interpretability
        units = if_else(units == "m", "ft", units) # fix units for depth readings to reflect conversion to feet
      )
  })
#
 ####  Reactive value to store filtered data ####
  filtered_data <- reactive({
    req(base_filtered_data())

    if (isTRUE(input$apply_qaqc_filter)) {
      #remove overflagged data (autogenerated flags)
      apply_cleaning_filters(df = base_filtered_data(), new_value_col = "mean_cleaned")%>%
        # interpolate missing data (<1 hour, 4 points)
        apply_interpolation_missing_data(df = .,  value_col = "mean_cleaned", dt_col = "DT_round_MT", method = "linear", max_gap = 4 )%>%
        # apply binomial filter to turbidity and Chl-a
        apply_low_pass_binomial_filter(df = .,  value_col = "mean_filled", new_value_col = "mean_smoothed", dt_col = "DT_round_MT")%>%
        # apply timestep median to reduce noise
        apply_timestep_median(df = ., value_col = "mean_smoothed", new_value_col = "timestep_median", timestep = input$data_timestep, dt_col = "DT_round_MT")%>%
        #trim down dataset and rename columns
        select(DT_round_MT = DT_group, site, parameter, mean = timestep_median)%>%
        #remove duplicates
        distinct(site, parameter, mean, DT_round_MT, .keep_all = TRUE)

    } else {
      # If QA/QC filter is not applied, just return base filtered data with timestep median applied
      apply_interpolation_missing_data(df = base_filtered_data(),  value_col = "mean", dt_col = "DT_round_MT", method = "spline", max_gap = 4)%>%
        #take timestep median
        apply_timestep_median(df = ., value_col = "mean_filled", new_value_col = "timestep_median", timestep = input$data_timestep, dt_col = "DT_round_MT")%>%
        #trim down dataset and rename columns
        select(DT_round_MT = DT_group, site, parameter, mean = timestep_median)%>%
        #remove duplicates
        distinct(site, parameter, mean, DT_round_MT, .keep_all = TRUE)
    }
  })

  #### Time Series Plots ####
  #### Log Controls Dynamic UI ####
  output$log_controls <- renderUI({
    req(filtered_data())
    data <- filtered_data()
    req(nrow(data) > 0)

    parameters <- unique(data$parameter)

    # Create checkbox inputs for each parameter
    checkbox_list <- lapply(parameters, function(param) {
      # Create a clean input ID from parameter name
      input_id <- paste0("log_", gsub("[^A-Za-z0-9]", "_", tolower(param)))
      checkboxInput(input_id, paste("Log10 Scale -", param), FALSE)
    })

    do.call(tagList, checkbox_list)
  })

  #### Setup Dynamic Plots ####
  output$dynamic_plots <- renderUI({
    req(input$parameters_select, filtered_data())
    # Calculate number of rows needed (1 plot per row)
    n_params <- length(input$parameters_select)
    n_rows <- n_params  # One row per parameter

    # Create rows with plots
    plot_rows <- lapply(1:n_rows, function(row) {
      parameter <- input$parameters_select[row]
      plot_id <- paste0("time_series_plot_", row)

      units <- plot_param_table%>%
        filter(parameter == !!parameter)%>%
        pull(units)

      fluidRow(
        column(
          width = 12,  # Full width for single plot
          h4(paste0("Time Series for ", parameter, " (", units, ")")),
          plotlyOutput(plot_id, height = "400px")
        )
      )
    })

    do.call(tagList, plot_rows)
  })

  #### Generate plots ####
  observe({
    req(input$parameters_select, filtered_data())

    # Create a plot for each selected parameter
    lapply(seq_along(input$parameters_select), function(i) {
      parameter <- input$parameters_select[i]
      plot_id <- paste0("time_series_plot_", i)
      output[[plot_id]] <- renderPlotly({
        # Filter data for this specific parameter
        plot_data <-  filtered_data() %>%
          filter(parameter == !!parameter)%>%
          left_join(site_table, by = c("site" = "site_code" ))

        # Check if log scale is enabled for this parameter
        log_input_id <- paste0("log_", gsub("[^A-Za-z0-9]", "_", tolower(parameter)))
        use_log <- if (!is.null(input[[log_input_id]])) input[[log_input_id]] else FALSE

        # Get parameter bounds from lookup table
        param_bounds <- plot_param_table %>%
          filter(parameter == !!parameter)

        units <- plot_param_table%>%
          filter(parameter == !!parameter)%>%
          pull(units)

        # Determine y-axis limits
        if(nrow(param_bounds) > 0 && nrow(plot_data) > 0) {
          data_min <- min(plot_data$mean, na.rm = TRUE)
          data_max <- max(plot_data$mean, na.rm = TRUE)

          # Use parameter bounds as default, but extend if data goes outside
          y_min <- min(param_bounds$lower, data_min) - 0.2
          y_max <- max(param_bounds$upper, data_max) + 0.2

          # Add small buffer if data exactly matches bounds
          if(data_min >= param_bounds$lower && data_max <= param_bounds$upper) {
            y_min <- param_bounds$lower
            y_max <- param_bounds$upper
          }
        } else if(nrow(plot_data) > 0) {
          # Fallback to data range if no bounds available
          y_min <- min(plot_data$mean, na.rm = TRUE)
          y_max <- max(plot_data$mean, na.rm = TRUE)
        } else {
          # Default range if no data
          y_min <- 0
          y_max <- 1
        }

        # Set consistent colors for plotting by site
        color_mapping <- setNames(site_table$color, site_table$site_code)

        # Create the plotly plot
        p <- plot_ly(plot_data,
                     x = ~DT_round_MT,
                     y = ~mean,
                     type = "scatter",
                     color = ~site,
                     colors = color_mapping,
                     mode = "lines",
                     name = ~site_name) %>%
          layout(
            xaxis = list(title = "Date"),
            yaxis = list(
              title = if (use_log) paste0("Log10(", parameter,"(" ,units, ")", ")") else paste0(parameter," (" ,units, ")"),
              type = if (use_log) "log" else "linear",
              range = if (use_log) c(log10(max(y_min, 0.01)), log10(y_max)) else c(y_min, y_max)
            ),
            hovermode = "closest"
          )
        return(p)
      })
    })
  })

  #### TOC model plots ####
#TODO: move most of this code to a separate function to clean up the server and make it more modular
  observe({
    req(input$parameters_select, input$sites_select, input$data_timestep, filtered_data())

    # get site codes for filtering
    sites_sel <- filter(site_table, site_name %in% input$sites_select )%>%
      pull(site_code)

    # remove FC sonde data
    input_data <- filtered_data() %>%
      filter(!str_detect( site, "_fc")) #FC sondes will not have correct parameters so we can omit them entirely

    # Define required parameters
    required_params <- c("FDOM Fluorescence", "Temperature", "Specific Conductivity",
                         "Turbidity", "Chl-a Fluorescence")
    # Check if all required parameters are present
    available_params <- unique(input_data$parameter)
    missing_params <- setdiff(required_params, available_params)


    #if missing parameters or sites do not have model parameters (FC)
    if(length(missing_params) > 0 & nrow(input_data) > 0) {
      # Create warning message plot
      warning_text <- paste("Cannot generate TOC model plots.",
                            "Missing required parameters:",
                            paste(missing_params, collapse = ", "))

      # Create a plotly text plot with warning message
      p <- plot_ly() %>%
        add_text(x = 0.5, y = 0.5, text = warning_text,
                 textfont = list(size = 16, color = "red"),
                 showlegend = FALSE) %>%
        layout(
          xaxis = list(showgrid = FALSE, showticklabels = FALSE, zeroline = FALSE, range = c(0, 1)),
          yaxis = list(showgrid = FALSE, showticklabels = FALSE, zeroline = FALSE, range = c(0, 1)),
          plot_bgcolor = 'rgba(0,0,0,0)',
          paper_bgcolor = 'rgba(0,0,0,0)',
          margin = list(t = 50, b = 50, l = 50, r = 50)
        )

      return(p)
    }
    # check to make sure not all rows are NA
    na_check <- input_data %>%
      filter(parameter %in% required_params)%>%
      select(mean)%>%
      na.omit()
    # If no data available, show Error message
    if(nrow(na_check) == 0) {
      no_data_text <- "No data available to estimate TOC for the selected time period and sites."

      p <- plot_ly() %>%
        add_text(x = 0.5, y = 0.5, text = no_data_text,
                 textfont = list(size = 16, color = "red"),
                 showlegend = FALSE) %>%
        layout(
          xaxis = list(showgrid = FALSE, showticklabels = FALSE, zeroline = FALSE, range = c(0, 1)),
          yaxis = list(showgrid = FALSE, showticklabels = FALSE, zeroline = FALSE, range = c(0, 1)),
          plot_bgcolor = 'rgba(0,0,0,0)',
          paper_bgcolor = 'rgba(0,0,0,0)',
          margin = list(t = 50, b = 50, l = 50, r = 50)
        )

      return(p)
    }
    # Apply TOC model on relevant data
    toc_plot_data <- apply_toc_model(sensor_data = input_data,
                                     toc_model_file_path = "data/models/toc_xgboost_models_light_2025-09-09.rds",
                                     scaling_params_file_path = "data/models/scaling_parameters_20250820.rds",
                                     #summarizing model input results to user selected timestep (15 min -> 1 day)
                                     summarize_interval = input$data_timestep,
                                     time_col = "DT_round_MT",
                                     value_col = "mean") %>%
      left_join(site_table, by = c("site" = "site_code"))%>%
      mutate(across(contains("TOC_guess"), ~ round(.x, 2)))
    #parameter to plot
    plot_param <- "TOC_guess_ensemble"
    # get the list of sites
    site_list <- unique(toc_plot_data$site_name)

    # dynamically create plot containers
    output$toc_plots_panel <- renderUI({
      tagList(
        lapply(site_list, function(site_cd) {
          plotlyOutput(outputId = paste0("toc_plot_", site_cd), height = "400px")
        })
      )
    })

    # render one plot per site
    lapply(site_list, function(site_cd) {
      output[[paste0("toc_plot_", site_cd)]] <- renderPlotly({
        site_toc_data <- toc_plot_data %>%
          filter(site_name == site_cd)%>%
          dplyr::arrange(DT_round_MT) %>%
          dplyr::mutate(
            gap = is.na(TOC_guess_min) | is.na(TOC_guess_max) | is.na(.data[[plot_param]]),
            gid = cumsum(dplyr::lag(gap, default = TRUE) != gap)
          ) %>%
          dplyr::filter(!gap)


        #get sample data
        site_samples <- water_chem%>%
          left_join(site_table, by = c("site_code"))%>%
          filter(site_name == site_cd & !is.na(TOC))%>%
          mutate(DT_round = with_tz(round_date(DT_sample, unit = "15 minutes"), tzone = "America/Denver"))%>%
          filter(between(DT_round, min(site_toc_data$DT_round_MT) - days(1), max(site_toc_data$DT_round_MT) + days(1)))


        p <- plot_ly() %>%
          ##### add shapes for general categories of TOC values ####
        layout(
          shapes = list(
            # Green band 0–2
            list(type = "rect", xref = "paper", x0 = 0, x1 = 1,
                 yref = "y", y0 = 0, y1 = 2,
                 fillcolor = "rgba(0,255,0,0.2)", line = list(width = 0)),
            # Yellow band 2–4
            list(type = "rect", xref = "paper", x0 = 0, x1 = 1,
                 yref = "y", y0 = 2, y1 = 4,
                 fillcolor = "rgba(255,255,0,0.2)", line = list(width = 0)),
            # Red band 4–8
            list(type = "rect", xref = "paper", x0 = 0, x1 = 1,
                 yref = "y", y0 = 4, y1 = 8,
                 fillcolor = "rgba(255,0,0,0.2)", line = list(width = 0))
          )
        )

        if (nrow(site_toc_data) > 0) {
          gids <- unique(site_toc_data$gid)
          ##### add RIBBONS complete model set of TOC values ####
          for (i in seq_along(gids)) {

            d <- site_toc_data[site_toc_data$gid == gids[i], ]

            # make sure customdata has the same number of rows
            d$custom_range <- paste0(d$TOC_guess_min, " - ", d$TOC_guess_max)

            p <- p %>%
              add_ribbons(
                data = d,
                x = ~DT_round_MT,
                ymin = ~TOC_guess_min,
                ymax = ~TOC_guess_max,
                fillcolor = "grey",
                line = list(color = "transparent"),
                opacity = 0.5,
                name = "Models Range of Estimates",
                customdata = ~custom_range,
                hovertemplate = paste(
                  "%{customdata}<br>"),
                showlegend = (i == 1)   # Only one legend entry
              )
          }

          ##### add LINES ensemble mean model set of TOC values ####
          for (i in seq_along(gids)) {
            d <- site_toc_data[site_toc_data$gid == gids[i], ]
            p <- p %>%
              add_lines(
                data = d,
                x = ~DT_round_MT,
                y = ~.data[[plot_param]],
                line = list(color = "#E70870", width = 2),
                name = "Mean Model Estimate",
                hovertemplate = paste(
                  "Ensemble Estimate: %{y:.2f}<extra></extra>"
                ),
                showlegend = (i == 1)   # Only show one legend
              )
          }
        }

        # add a placeholder so the plot still renders if all data is NA
        if (nrow(site_toc_data) == 0) {
          p <- p %>%
            add_text(x = 0.5, y = 0.5, text = "No complete data segments", showlegend = FALSE) %>%
            layout(
              xaxis = list(showgrid = FALSE, showticklabels = FALSE, zeroline = FALSE, range = c(0, 1)),
              yaxis = list(showgrid = FALSE, showticklabels = FALSE, zeroline = FALSE, range = c(0, 1))
            )
        }

        if (nrow(site_samples) > 0) {

          p <- p %>%
            add_markers(
              data = site_samples,
              x = ~DT_sample,
              y = ~TOC,
              #symbol = ~collector,
              marker = list(color = "blue", size = 8, shape = "circle"),
              name = "Lab TOC",
              hovertemplate = paste(
                "Measured TOC: %{y:.2f} mg/L<br>"
              )
            )
        }

        # Get parameter y axis bounds from lookup in Global.R table
        param_bounds <- plot_param_table %>%
          filter(parameter == "TOC")

        # Determine y-axis limits
        if(nrow(param_bounds) > 0 && nrow(toc_plot_data) > 0) {
          data_min <- min(site_toc_data$TOC_guess_min, site_samples$TOC,  na.rm = TRUE)
          data_max <- max(site_toc_data$TOC_guess_max,site_samples$TOC,  na.rm = TRUE)

          # Use parameter bounds as default, but extend if data goes outside
          y_min <- min(param_bounds$lower, data_min)
          y_max <- max(param_bounds$upper, data_max)

          # Add small buffer if data exactly matches bounds
          if(data_min >= param_bounds$lower && data_max <= param_bounds$upper) {
            y_min <- param_bounds$lower
            y_max <- param_bounds$upper
          }
        } else if(nrow(site_toc_data) > 0) {
          # Fallback to data range if no bounds available
          y_min <- min(site_toc_data$TOC_guess_min, na.rm = TRUE) + 0.1 # add padding to avoid cutting off lowest data point
          y_max <- max(site_toc_data$TOC_guess_max, na.rm = TRUE) + 0.1# add padding to avoid cutting off highes data point
        } else {
          # Default range if no data
          y_min <- 0
          y_max <- 1
        }

        #create empty shapes and annotations lists
        shapes_list <- list()
        annotations_list <- list()
        # add preliminary text to annotations
        # annotations_list <- append(annotations_list,
        #                            list(
        #                              x = max(site_toc_data$DT_round_MT, na.rm = TRUE),
        #                              y = y_max * 0.85,
        #                              text = "PRELIMINARY RESULTS",
        #                              showarrow = FALSE,
        #                              xanchor = "right",
        #                              yanchor = "top",
        #                              font = list(size = 16, color = "black", family = "Arial Black")
        #                            )
        # )

        # Add line/annotation if data is less than lower model bound
        if(y_min <= toc_model_bounds$TOC_lower){
          shapes_list <- append(shapes_list, list(
            list(type = "line",
                 x0 = 0, x1 = 1, xref = "paper",
                 y0 = toc_model_bounds$TOC_lower,
                 y1 = toc_model_bounds$TOC_lower,
                 line = list(dash = "dash", width = 2, color = "black"))
          ))

          annotations_list <- append(annotations_list, list(
            list(x = 0.02, xref = "paper",
                 y = toc_model_bounds$TOC_lower,
                 text = "Model Lower Limit",
                 showarrow = FALSE,
                 xanchor = "left",
                 yanchor = "top")
          ))
        }

        # Add line/annotation if data exceeds upper model bound
        if(y_max >= toc_model_bounds$TOC_upper) {
          shapes_list <- append(shapes_list, list(
            list(type = "line",
                 x0 = 0, x1 = 1, xref = "paper",
                 y0 = toc_model_bounds$TOC_upper,
                 y1 = toc_model_bounds$TOC_upper,
                 line = list(dash = "dash", width = 2, color = "black"))
          ))

          annotations_list <- append(annotations_list, list(
            list(x = 0.02, xref = "paper",
                 y = toc_model_bounds$TOC_upper,
                 text = "Model Upper Limit",
                 showarrow = FALSE,
                 xanchor = "left",
                 yanchor = "bottom")
          ))
        }



        #### Final layout tweaks ####
        p %>%
          layout(
            title = paste0("Estimated TOC (mg/L) at: ", site_cd),
            xaxis = list(title = "Date"),
            yaxis = list(title = "Model Estimated TOC (mg/L)", range = c(y_min - 0.25, y_max+0.25)),
            legend = list(orientation = "h", x = 0.5, xanchor = "center", y = -0.2),
            hovermode = "x unified",
            annotations = annotations_list
          )
      })
    })
  })


  #### Site Map Output ####
  #### Get site locations from metadata ####
  observeEvent(input$sites_select, {
    sites_sel <- filter(site_table, site_name %in% input$sites_select )%>%
      pull(site_code)
    # Sample site locations
    values$site_locations <- read_csv("data/sonde_location_metadata.csv", show_col_types = F) %>%
      separate(col = "lat_long", into = c("lat", "lon"), sep = ",", convert = TRUE) %>%
      st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
      mutate(site = tolower(Site),
             site = ifelse(site %in% c("pman", "pbr"), paste0(site, "_fc"), site))%>%
      filter(site %in% sites_sel)%>%
      left_join(site_table, by = c("site" = "site_code" ))
  })
  #### Generate site map ####
  output$site_map <- renderLeaflet({
    req(values$site_locations)

    pal <- colorFactor(
      palette = c("red", "blue", "green", "purple", "orange"),
      domain = values$site_locations$watershed
    )

    leaflet(values$site_locations) %>%
      addTiles() %>%
      addCircleMarkers(
        radius = 8,
        color = ~pal(watershed),
        fillOpacity = 0.7,
        popup = ~paste("Site:", site_name, "<br>",
                       "Watershed:", watershed)
      ) %>%
      addLegend(
        pal = pal,
        values = ~watershed,
        title = "Watershed",
        position = "bottomright"
      )
  })

  #### Flow data Page ####

#TODO: pull out into separate function to clean up server and make more modular
  # get data for site conditions
  flow_sites_data <- reactive({
    withProgress(message = "Retrieving Poudre flow sites data...", {

      #Code used to generate the list of sites stored in `data/cdwr_sites_oi.csv` to pull data for (only need to run this once and can save the output as a CSV to read in later)
      # clp_sites <- get_telemetry_stations(water_district = 3 )%>% # Specify Poudre basin
      #   filter(station_por_end > Sys.Date() - days(30))%>%# only grab active sites
      #   filter(grepl("DIS", parameter))#filter for flow sites only
      #
      # laramie_sites <- get_telemetry_stations(water_district = 48 )%>% # Specify Laramie basin
      #   filter(station_por_end > Sys.Date() - days(30))%>%# only grab active sites
      #   filter(grepl("DIS", parameter))#filter for flow sites only
      #
      # sites <- bind_rows(clp_sites, laramie_sites)%>%
      #   filter(abbrev %in% cdwr_upper_clp_sites)

      sites <- read_csv(file = "data/cdwr_sites_oi.csv", show_col_types = F)

      # If no sites found, return empty tibble
      if (nrow(sites) == 0) {
        return(tibble())
      }

      # Define date range for the last 8 days
      end_date <- as.character(Sys.Date()+ days(1))
      start_date <- as.character(Sys.Date() - days(7))

      # Map over sites to get the last 7 days of flow
      active_sites <- sites %>%
        split(1:nrow(.)) %>%
        map_dfr(function(site_row) {
          site_id <- site_row$abbrev
          param_code <- site_row$parameter

          # Try to get recent flow data
          result <- tryCatch({
            flow_data <- get_telemetry_ts(
              abbrev = site_id,
              parameter = param_code,
              start_date = start_date,
              end_date = end_date,
              api_key = cdwr_api_key,
              timescale = "raw")%>%
              mutate(DT_round = round_date(datetime, "15 min"))%>%
              group_by(DT_round)%>%
              summarise(flow = mean(meas_value, na.rm = TRUE))%>%
              mutate(abbrev = site_id)

            # Check if we have data
            if (nrow(flow_data) > 1) {
              # Get the most recent 24 hours of data
              end_time <- Sys.time()
              start_time <- end_time - hours(24)

              iv_data <- flow_data %>%
                filter(DT_round >= start_time & DT_round <= end_time)

              if (nrow(iv_data) > 1) {
#TODO: create better metric for flow increasing/ decreasing
                # Perform linear regression to calculate slope
                flow_model <- lm(flow ~ DT_round, data = iv_data)
                slope <- coef(flow_model)[2]

                current_flow <- iv_data %>%
                  arrange(DT_round) %>%
                  slice(n()) %>%
                  pull(flow)

                site_row %>%
                  as_tibble() %>%
                  mutate(
                    current_flow_cfs = current_flow,
                    flow_slope = slope * 60 * 60, # Convert slope to cfs/hr
                    trend = if_else(current_flow_cfs == 0, "NoFlow",
                                    if_else(flow_slope > 0, "increasing", "decreasing")),
                    nested_data = list(flow_data)
                  )%>%
                  select(abbrev, station_name, data_source, water_source, gnis_id, latitude, longitude,
                         current_flow_cfs, flow_slope, trend, structure_type, site_type = station_type, nested_data)
              } else {
                tibble()
              }
            } else {
              tibble()
            }
          }, error = function(e) {
            #Create Empty dataframe with the same structure as the successful API call to avoid losing sites in the map  when API calls fail
            start_date <- as.POSIXct(Sys.Date() - days(7))
            end_date   <- as.POSIXct(Sys.Date() + days(1))

            flow_data <- tibble(
              DT_round = seq(from = start_date, to = end_date, by = "1 hour"),
              flow = NA, abbrev = site_id)

              site_row %>%
                as_tibble() %>%
                mutate(
                  current_flow_cfs = NA,
                  flow_slope = NA,
                  trend = "NoFlow",
                  nested_data = list(flow_data)
                ) %>%
                select(abbrev, station_name, data_source, water_source, gnis_id, latitude, longitude,
                       current_flow_cfs, flow_slope, trend, structure_type, site_type = station_type, nested_data)

          })

          return(result)
        })

      return(active_sites)
    })
  })
  # Create the map of the flow sites
  output$map <- renderLeaflet({


    sites <- flow_sites_data()%>%
      mutate(group_status = case_when(trend == "increasing" & site_type == "Stream Gage" ~ "river_up",
                                      trend == "decreasing" & site_type == "Stream Gage" ~ "river_down",
                                      trend == "increasing" & site_type != "Stream Gage" ~ "ditch_up",
                                      trend == "decreasing" & site_type != "Stream Gage" ~ "ditch_down",
                                      TRUE ~ "no_flow"))
    # Check if sites data is empty
    if (nrow(sites) == 0) {
      return(leaflet() %>%
               addTiles() %>%
               setView(lng = -105.5, lat = 40.6, zoom = 10) %>%
               addControl(html = "No active flow sites found in Cache la Poudre watershed",
                          position = "topright"))
    }

    # Create popup content
    popup_content <- sites %>%
      mutate(
        plot_image = map(nested_data, ~{
          plot <- ggplot(.x, aes(x = DT_round, y = flow)) +
            geom_line(color = "blue") +
            labs(title = "Flow Data", x = "Time", y = "Flow (cfs)") +
            theme_minimal()
          plot_path <- tempfile(fileext = ".png")
          ggsave(plot_path, plot, width = 5, height = 3)
          b64_plot <- base64enc::dataURI(file = plot_path, mime = "image/png")
          file.remove(plot_path)
          paste0('<img src="', b64_plot, '" width="300"/>')
        }),
        popup = str_c(
          "<b>Site Name:</b> ", station_name, "<br>",
          "<b>Site ID:</b> ", abbrev, "<br>",
          "<b>Current Flow:</b> ", round(current_flow_cfs, 1), " cfs<br>",
          "<b>24-hour Trend:</b> ",
          if_else(trend == "increasing",
                  str_c("Increasing (+", round(flow_slope, 1), " cfs/hr)"),
                  str_c("Decreasing (", round(flow_slope, 1), " cfs/hr)")),
          "<br>", plot_image
        )
      ) %>%
      pull(popup)

    # Create labels with current flow
    flow_labels <- sites %>%
      #fix station name to make more sense
      mutate(
        label = if_else(group_status == "no_flow", "", str_c(abbrev, ": ", round(current_flow_cfs, 1), " cfs") %>%
                          htmlEscape())
      ) %>%
      pull(label)

    # Determine icon colors/shape based on trend
    icons <- awesomeIconList(
      river_up = makeAwesomeIcon(icon = "arrow-up", markerColor = "green", library = "fa", squareMarker = F),
      river_down = makeAwesomeIcon(icon = "arrow-down", markerColor = "red", library = "fa", squareMarker = F),
      ditch_up = makeAwesomeIcon(icon = "arrow-up", markerColor = "green", library = "fa", squareMarker = T),
      ditch_down = makeAwesomeIcon(icon = "arrow-down", markerColor = "red", library = "fa", squareMarker = T),
      no_flow = makeAwesomeIcon(icon = "circle", markerColor = "gray", library = "fa")
    )


    leaflet(sites) %>%
      addTiles() %>%
      addAwesomeMarkers(
        lng = ~longitude,
        lat = ~latitude,
        icon = ~icons[group_status],
        group = ~group_status,
        label = lapply(flow_labels, HTML),
        labelOptions = labelOptions(
          noHide = TRUE,
          direction = "auto",
          textSize = "12px",
          style = list(
            "color" = "black",
            "font-weight" = "bold",
            "background-color" = "rgba(255, 255, 255, 0.7)",
            "border" = "1px solid black",
            "padding" = "3px",
            "border-radius" = "3px"
          )
        ),
        popup = popup_content
      ) %>%
      addLegend(
        position = "bottomright",
        colors = c("green", "red", "grey"),
        labels = c("Increasing Flow", "Decreasing Flow", "No Current Flow"),
        title = "24-hour Flow Trend"
      ) %>%
      addProviderTiles("Esri.WorldTopoMap", group = "Topo") %>%
      addProviderTiles("Esri.WorldImagery", group = "Satellite") %>%
      addLayersControl(
        baseGroups = c("Default", "Topo", "Satellite"),
        position = "topright",
        options = layersControlOptions(collapsed = FALSE)
      ) %>%
      setView(lng = -105.5, lat = 40.6, zoom = 10)
  })
  #plot for three days of data
  # Generate 7-day Q plot
  output$seven_day_q_plot <- renderPlotly({


    data <- flow_sites_data()%>%
      filter(abbrev %in% c("CLASRKCO", "CLAFTCCO", "JWCCHACO","CLANSECO","MUNCANCO"))
    #extract the data from nested data
    flow_data <- bind_rows(data$nested_data)%>%
      mutate(site_name = case_when(
        abbrev == "CLASRKCO" ~ "South Fork CLP",
        abbrev == "CLAFTCCO" ~ "CLP @ Canyon Mouth",
        abbrev == "JWCCHACO" ~ "Chambers Lake Outflow",
        abbrev == "CLANSECO" ~ "North Fork below Seaman Res",
        abbrev == "MUNCANCO" ~ "Munroe Canal"
      ))%>%
      filter(DT_round >= Sys.Date() - days(7))

    p <- ggplot(flow_data, aes(x = DT_round, y = flow, color = site_name)) +
      geom_line()+
      scale_color_manual(values = c("South Fork CLP" = "#256BF5",
                                    "CLP @ Canyon Mouth" = "#002EA3",
                                    "Chambers Lake Outflow" = "#1E4D2B",
                                    "North Fork below Seaman Res" = "#E70870",
                                    "Munroe Canal" = "#56104E")) +
      labs(x = "Date",
           y = "Discharge (cfs)",
           color = "Site") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))

    ggplotly(p)
  })

  # Conditionally show SNOTEL plot between October and July
  output$snotel_card <- renderUI({

    current_month <- month(Sys.Date())

    # Show if month is between October (10) and July (7)
    if(current_month > 10 || current_month < 6) {
      card(
        card_header("Current SNOTEL data for CLP Basin"),
        card_body(
          plotlyOutput("snotel_plot")
        )
      )
    } else {
      # Return empty UI if outside of October-July


      card(
        card_header("Flows Historical Stats"),
        #TODO: change this to Dropdown from available sites
        pickerInput("site_abbrev_selected",
                    label = "Select Site to View Historical Conditions:",
                    choices = cdwr_lookup_table$site_title,
                    selected = c("Canyon Mouth")
        ),
        card_body(
          plotlyOutput("clp_rainbow_plot")
        )
      )

    }
  })

  # Generate SNOTEL plot (will only be displayed if the card is visible)
#TODO: Functionalize similar to clp rainbow plot to clean up server and make it more modular
  output$snotel_plot <- renderPlotly({

    clp_snotel_url <- "https://nwcc-apps.sc.egov.usda.gov/awdb/basin-plots/POR/WTEQ/assocHUC8/10190007_Cache_La_Poudre.csv"
    #download the csv
    #clp_snotel_data <- read.csv(clp_snotel_url, skip = 1)
    clp_snotel_data <- read.csv(clp_snotel_url)
    #drop the X from names
    names(clp_snotel_data) <- gsub("^X", "", names(clp_snotel_data))

    #grab last year
    cur_year = year(Sys.Date())

    # Reshape the data from wide to long format
    clp_snotel_non_stats <- clp_snotel_data %>%
      select(-c("10.", "30.", "70.", "90.", "Min", "Median...91..20.", "Median..POR.", "Max", "Median.Peak.SWE" ))%>%
      pivot_longer(
        cols = -date,
        names_to = "year",
        values_to = "swe"
      )%>%
      mutate(month_day = date,
             date = ymd(paste0("2000-", month_day)),
             full_date = if_else(
               month(date) >= 10,
               as.Date(paste0(as.numeric(cur_year)-1, "-", month_day), format = "%Y-%m-%d"),
               as.Date(paste0(as.numeric(cur_year), "-", month_day), format = "%Y-%m-%d"))
      )

    curr_year <- clp_snotel_non_stats%>%
      filter(year == as.character(cur_year))%>%
      select(date, swe, full_date)

    cur_swe <- curr_year%>%
      filter(full_date == Sys.Date())%>%
      pull(swe)

    recent_years <- clp_snotel_non_stats%>%
      filter(year <= as.character(cur_year-1) & year >= as.character(cur_year-6))%>%
      select(date, swe, full_date, year)



    # Extract statistical columns
    stats_data <- clp_snotel_data %>%
      select(date, "10.", "30.", "70.", "90.", "Min", "Median...91..20.", "Median..POR.", "Max", "Median.Peak.SWE" )%>%
      pivot_longer(
        cols = -date,
        names_to = "stat",
        values_to = "swe"
      ) %>%
      mutate(stat = case_when(
        stat == "Min" ~ "Min",
        stat == "Median...91..20." ~ "Median ('91-'20)",
        stat == "Median..POR." ~ "Median (POR)",
        stat == "Max" ~ "Max",
        stat == "Median.Peak.SWE" ~ "Median Peak SWE",
        stat == "10." ~ "10th Percentile",
        stat == "30." ~ "30th Percentile",
        stat == "70." ~ "70th Percentile",
        stat == "90." ~ "90th Percentile"))%>%
      filter(!is.na(stat)&!is.na(swe))%>%
      filter(stat != "Median (POR)")%>%
      mutate(month_day = date,
             date = ymd(paste0("2000-", date)),
             full_date = if_else(
               month(date) >= 10,
               as.Date(paste0(as.numeric(cur_year) - 1, "-", month_day), format = "%Y-%m-%d"),
               as.Date(paste0(as.numeric(cur_year), "-", month_day), format = "%Y-%m-%d")
             ))

    percentile_ribbons <- stats_data %>%
      pivot_wider(names_from = stat, values_from = swe)

    # grab the median peak swe stat
    median_peak = clp_snotel_data %>%
      filter(!is.na(`Median.Peak.SWE`))%>%
      select(date, med_peak_swe = `Median.Peak.SWE`)%>%
      mutate(month_day = date,
             date = ymd(paste0("2000-", date)),
             full_date = if_else(
               month(date) >= 10,
               as.Date(paste0(as.numeric(cur_year)-1, "-", month_day), format = "%Y-%m-%d"),
               as.Date(paste0(as.numeric(cur_year) , "-", month_day), format = "%Y-%m-%d")
             ),
             days_until = as.numeric(full_date - Sys.Date()),
             perc_peak = round((cur_swe/med_peak_swe)*100, 0)
      )
    days_until = median_peak%>%
      pull(days_until)

    #grab the median peak swe percent
    perc_peak = median_peak%>%
      filter(full_date == Sys.Date())%>%
      pull(perc_peak)

    #grab the median percent


    perc_med = stats_data%>%
      filter(stat == "Median ('91-'20)" & full_date == Sys.Date())%>%
      mutate(perc_med = round((cur_swe/swe)*100, 0))%>%
      pull(perc_med)


    #create a plotly line plot of each stat over a year
    stats_plot <- plot_ly()%>%
      add_ribbons(
        data = percentile_ribbons,
        x = ~full_date,
        ymin = ~`Min`,
        ymax = ~`10th Percentile`,
        fillcolor = 'rgba(255,0, 0, 0.2)',
        line = list(color = 'transparent'),
        showlegend = FALSE
      )%>%
      add_ribbons(
        data = percentile_ribbons,
        x = ~full_date,
        ymin = ~`10th Percentile`,
        ymax = ~`30th Percentile`,
        fillcolor = 'rgba(255,162, 0, 0.2)',
        line = list(color = 'transparent'),
        showlegend = FALSE
      )%>%
      add_ribbons(
        data = percentile_ribbons,
        x = ~full_date,
        ymin = ~`30th Percentile`,
        ymax = ~`70th Percentile`,
        fillcolor = 'rgba(0,227, 116, 0.2)',
        line = list(color = 'transparent'),
        showlegend = FALSE
      )%>%
      add_ribbons(
        data = percentile_ribbons,
        x = ~full_date,
        ymin = ~`70th Percentile`,
        ymax = ~`90th Percentile`,
        fillcolor = 'rgba(0,255, 252, 0.2)',
        line = list(color = 'transparent'),
        showlegend = FALSE
      )%>%
      add_ribbons(
        data = percentile_ribbons,
        x = ~full_date,
        ymin = ~`90th Percentile`,
        ymax = ~`Max`,
        fillcolor = 'rgba(0,109,255, 0.2)',
        line = list(color = 'transparent'),
        showlegend = FALSE
      )%>%
      add_trace(
        data = stats_data%>%filter(stat %nin% c("10th Percentile", "30th Percentile", "70th Percentile", "90th Percentile")),
        x = ~full_date,
        y = ~swe,
        color =  ~stat,
        type = "scatter",
        mode = "lines",
        line = list(width = 2),
        colors = c("Min" = "rgba(255,0, 0, 0.8)", "Median ('91-'20)" = "rgba(0,227, 116, 0.8)", "Max" = "rgba(0,109,255, 0.8)")) %>%
      add_trace(
        data = median_peak,
        x = ~full_date, y = ~med_peak_swe, type = "scatter", mode = "markers",
        marker = list(color = "green", size = 10, symbol = "x"),
        name = "Median Peak SWE" ) %>%
      add_trace(
        data = curr_year,
        x = ~full_date, y = ~swe, type = "scatter", mode = "lines",
        line = list(color = "black", width = 2),
        name = paste0("Current Year (", cur_year, ")"),
        showlegend = TRUE
      ) %>%
      add_trace(
        data = recent_years,
        x = ~full_date, y = ~swe, type = "scatter", mode = "lines",
        split = ~factor(year, levels = rev(unique(recent_years$year))),
        line = list(width = 0.5),
        showlegend = TRUE,
        name = ~factor(year, levels = rev(unique(recent_years$year))), visible="legendonly"
      ) %>%
      layout(
        title = "SNOTEL SWE Statistics",
        xaxis = list(title = "Date"),
        yaxis = list(title = "Snow Water Equivalent (inches)"),
        showlegend = TRUE) %>%
      layout(
        title = "",
        xaxis = list(title = "",tickformat = "%b %d",range = c(min(stats_data$full_date, na.rm = T),max(stats_data$full_date, na.rm = T))
        ),
        yaxis = list(title = "Snow Water Equivalent (in)", range = c(0, 40)),
        legend = list(x = 1, y = 0.95,title = list(text = "")),
        annotations = list(
          list(
            x = min(stats_data$full_date, na.rm = T) + 15, y = 32.5,
            text = paste0(
              "Current as of ", Sys.Date(), ":<br>",
              "Current Basin SWE:", cur_swe, "<br>",
              "% of Median:", perc_med , " <br>",
              "% Median Peak:", pull(median_peak, name = perc_peak), " <br>",
              "Days Until Median Peak:",days_until," <br>"
            ),
            showarrow = FALSE,bordercolor = "black",borderwidth = 1,bgcolor = "white",xanchor = "left"
          )
        ),
        margin = list(t = 40, r = 100, b = 40, l = 60)
      )

    stats_plot

  })


  output$clp_rainbow_plot <- renderPlotly({


    site_abbrev <- cdwr_lookup_table%>%
      filter(site_title == input$site_title_selected)%>%
      pull(site_abbrev)

    create_rainbow_flow_plot(station_abbrev = site_abbrev,
                             station_plot_name = input$site_title_selected,
                             years = 30,
                             incl_winter = F,
                             api_key = cdwr_api_key)

  })



}
