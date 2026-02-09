#' @title CDWR Flow Data Plot including statistics
#'
#' @description
#' Downloads flow data from the Colorado Division of Water Resources (CDWR) API, using the CDSSR package (https://github.com/anguswg-ucsb/cdssr)
#' and creates a plotly plot with the current year's flow data, and statistics (min, median, max, and quantiles) for the specified number of years.
#' Attempt was to mirror the plots from SNOTEL stations
#' (ie: https://nwcc-apps.sc.egov.usda.gov/awdb/basin-plots/POR/WTEQ/assocHUC2/10_Missouri_Region.html?hideAnno=true&hideControls=true&activeOnly=true&showYears=2025)
#'
#' Sites with very few years of data (<5) or no data will not work properly. Additionally, only streamflow sites will work (ie no reservoir levels)
#'
#' @param station_abbrev Character string for station abbreviation to be used to query CDWR API. Station names can be gathered
#' from https://dwr.state.co.us/Tools/Stations.
#'
#' @param station_plot_name Character string for the name of the station to be used in the plot title.
#'
#' @param years Numeric for the number of years of data to include in the plot and stats. Defaults to 30.
#'
#' @param incl_winter Logical for whether winter data (Dec-Feb) should be included.
#'                    High elevation gages typically have ice issues and can not be as reliable
#'
#' @param min_val Numeric adjusting the minimum value of the y-axis. Defaults to 0.1.
#'                This is useful for gages that have very low flows or few years of data
#'
#' @return A plotly plot with:
#' - Log y axes
#' - Black line: Current year of data
#' - Orange line: Median over time period
#' - Blue line: Min over time period
#' - Green line: Max over time period
#' - Ribbons:
#'           Red: Min to 10 %
#'           Orange: 10 % to 30 %
#'           Green: 30 % to 70 %
#'           Light Blue: 70 % to 90 %
#'           Dark Blue: 90 % to Max
#' - Toggle lines: Last 6 years of data
#' @examples
#'
#' CLP Canyon Mouth for 30 years, no winter
#'create_rainbow_flow_plot(station_abbrev = "CLAFTCCO",
#'                         station_plot_name = "CLP Canyon Mouth",
#'                         years = 30,
#'                         incl_winter = F)
#'
#' USGS gage on CLP at Lincoln Ave for 10 years
#'create_rainbow_flow_plot(station_abbrev = "CLAFORCO",
#'                          station_plot_name = "CLP @ Lincoln",
#'                          years = 10 ,
#'                          incl_winter = F)
#'
#' #Chambers Lake Outflow for 30 years, incl winter
#'create_rainbow_flow_plot(station_abbrev = "JWCCHACO",
#'                         station_plot_name = "Chambers Outflow",
#'                          years = 30,
#'                          incl_winter = T)
#'
#' Setting min value to 10 cfs to improve plot
#'create_rainbow_flow_plot(station_abbrev = "CLASRKCO",
#'                          station_plot_name = "South Fork CLP",
#'                          years = 5,
#'                          incl_winter = F,
#'                          min_val = 10)

create_rainbow_flow_plot <- function(station_abbrev, station_plot_name, years = 30, incl_winter = T, min_val = 0.1){

  if(!is.character(station_abbrev) | !is.character(station_plot_name)){
    stop("station_abbrev and station_plot_name must be character strings.")
  }


  #loading packages
  if (!require(cdssr, quietly = TRUE)) {
    library(cdssr)
  }
  if (!require(tidyverse, quietly = TRUE)) {
    library(tidyverse)
  }
  if (!exists("%nin%")) {
    `%nin%` = Negate(`%in%`)
  }


  #find the min date of the X years ago
  start_date = paste0((year(Sys.Date()) - as.numeric(years)), "-01-01")
  #find the max date of this year
  end_date = paste0((year(Sys.Date())), "-12-31")

  #Get flow data for site over the last X years
  flow_data <- get_sw_ts(
    abbrev = station_abbrev,
    start_date = start_date,
    end_date = end_date,
    timescale = "day")%>%
    select(datetime, value, flag_a, flag_b, flag_c, flag_d)%>%
    #clean up dates and filter values to be greater than min val
    mutate(year = year(datetime),
           date = format(datetime, "%m-%d"),
           value = ifelse(value <= min_val, min_val, value))%>%
    #remove any flags which include the tag ice
    filter(!grepl("ice", flag_a, ignore.case = TRUE) &
             !grepl("ice", flag_b, ignore.case = TRUE) &
             !grepl("ice", flag_c, ignore.case = TRUE) &
             !grepl("ice", flag_d, ignore.case = TRUE))

  if(incl_winter == F){
    # Filter out winter months (December, January, February)
    flow_data <- flow_data %>%
      filter(!month(datetime) %in% c(12, 1, 2))
  }

  #calculate min, median, max, Q10, Q30, Q70 and Q90 for each day of year

  wide_flow_stats <- flow_data %>%
    summarise(
      Min = min(value, na.rm = TRUE),
      Median = median(value, na.rm = TRUE),
      Max = max(value, na.rm = TRUE),
      `10th Percentile` = as.numeric(quantile(value, probs = 0.1, na.rm = TRUE)),
      `30th Percentile` = as.numeric(quantile(value, probs = 0.3, na.rm = TRUE)),
      `70th Percentile` = as.numeric(quantile(value, probs = 0.7, na.rm = TRUE)),
      `90th Percentile` = as.numeric(quantile(value, probs = 0.9, na.rm = TRUE))
      , .by = date)

  #grab current year
  cur_year = year(Sys.Date())
  recent_date = Sys.Date() - days(1)

  # grab all data and reshape date for same axes plotting
  non_stats <- flow_data%>%
    select(date, flow = value, datetime)%>%
    mutate(month_day = date,
           year = year(datetime),
           #setting date to 2000-month-day formatting for plotting on same axes
           date = ymd(paste0("2000-", month_day)),
           full_date = as.Date(paste0(as.numeric(cur_year), "-", month_day), format = "%Y-%m-%d"))

  #set aside current year's data
  curr_year <- non_stats%>%
    filter(year == as.character(cur_year))%>%
    select(date, flow, full_date)

  #grab yesterdays flow
  cur_flow <- curr_year %>%
    filter(full_date == recent_date) %>%
    pull(flow)

  # If yesterday is not available, try to find the most recent flow data (within a week)
  if(is_empty(cur_flow)){
    # Try up to 7 days back to find data
    for(i in 2:8) {
      cur_flow <- curr_year %>%
        filter(full_date == Sys.Date() - days(i)) %>%
        pull(flow)

      if(!is_empty(cur_flow)) {
        break  # Exit loop when we find data
        recent_date = Sys.Date() - days(i)
      }
    }
  }

  # grab the 5 most recent years
  recent_years <- non_stats%>%
    filter(year <= as.character(cur_year-1) & year >= as.character(cur_year-5))%>%
    dplyr::select(date, flow, full_date, year)

  # Extract statistical columns
  stats_data <- wide_flow_stats %>%
    pivot_longer(
      cols = -date,
      names_to = "stat",
      values_to = "flow"
    ) %>%
    filter(!is.na(stat)&!is.na(flow))%>%
    mutate(month_day = date,
           date = ymd(paste0("2000-", date)),
           full_date = as.Date(paste0(as.numeric(cur_year), "-", month_day), format = "%Y-%m-%d"),
           flow = ifelse(flow <= min_val, min_val, flow)) %>% # Ensure low/negative flows are set to min val for plotting
    arrange(full_date, stat)

  percentile_ribbons <- stats_data %>%
    pivot_wider(names_from = stat, values_from = flow)

  #grab the median percent

  perc_med <- stats_data%>%
    filter(stat == "Median" & full_date == recent_date)%>%
    mutate(perc_med = round((cur_flow/flow)*100, 0))%>%
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
      y = ~flow,
      color =  ~stat,
      split = ~stat,
      type = "scatter",
      mode = "lines",
      line = list(width = 2),
      colors = c("Min" = "rgba(255,0, 0, 0.8)", "Median" = "rgba(0,227, 116, 0.8)", "Max" = "rgba(0,109,255, 0.8)")) %>%
    add_trace(
      data = curr_year,
      x = ~full_date, y = ~flow, type = "scatter", mode = "lines",
      line = list(color = "black", width = 2),
      name = paste0("Current Year (", cur_year, ")"),
      showlegend = TRUE
    ) %>%
    add_trace(
      data = recent_years,
      x = ~full_date, y = ~flow, type = "scatter", mode = "lines",
      split = ~factor(year, levels = rev(unique(recent_years$year))),
      line = list(width = 0.5),
      showlegend = TRUE,
      name = ~factor(year, levels = rev(unique(recent_years$year))), visible="legendonly"
    ) %>%
    layout(
      title = "",
      xaxis = list(title = "",tickformat = "%b %d",range = c(min(stats_data$full_date, na.rm = T),max(stats_data$full_date, na.rm = T))
      ),
      yaxis = list(title = "Flow (cfs)",type = "log"),
      legend = list(x = 1, y = 0.95,title = list(text = "")),
      annotations = list(
        list(
          x = .70, y = .95,
          xref = "paper", yref = "paper",
          #x = min(stats_data$full_date, na.rm = T) + 15, y = log(max(stats_data$flow, na.rm = T)*.005),
          text = paste0(
            "Flow data for ",station_plot_name, "<br>",
            "Years of data: ", years, "<br>",
            "Current as of ", recent_date, ":<br>",
            "Current Basin Flow: ", cur_flow, "(cfs)", "<br>"
          ),
          showarrow = FALSE,bordercolor = "black",borderwidth = 1,bgcolor = "white",xanchor = "left"
        )
      ),
      margin = list(t = 40, r = 100, b = 40, l = 60)
    )

  stats_plot
}


