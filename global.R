#### Global Variables ####

#Prototype dashboard
# Fixed package loading for shinyapps.io deployment
# Remove the custom package_loader function and install.packages() calls

# Simply load required packages - shinyapps.io will automatically detect and install them
suppressMessages({
  #library(furrr)
  library(ross.wq.tools)
  # Date/time handling
  library(zoo)
  library(padr)
  # Data cleaning and utilities
  library(janitor)
  library(broom)
  library(here)
  library(glue)
  # Stats/modeling
  library(stats)
  library(RcppRoll)
  library(trend)
  library(scales)
  library(xgboost)
  # Spatial packages
  library(sf)
  library(leaflet)
  # Vis
  library(ggpubr)
  library(ggthemes)
  library(scales)
  library(plotly)
  library(ggpmisc)
  # Web scraping/data retrieval
  library(rvest)
  library(httr)
  library(httr2)
  library(cdssr)
  library(yaml)
  # Development tools
  library(devtools)
  # Shiny
  library(shiny)
  library(shinymanager)
  library(shinycssloaders)
  library(shinyTime)
  library(bslib)
  library(shinyWidgets)
  library(shinydashboard)
  library(htmltools)
  library(readr)
  # Core data manipulation
  library(tidyverse)
  library(DT)
  library(purrr)
  library(data.table)
  library(arrow)
})

#### Set up ####

options(shiny.maxRequestSize = 10000 * 1024^2)

#negate %in% call for easier filtering
`%nin%` = Negate(`%in%`)

#set consistent site colors and names
site_table <- tibble(site_code = c("sfm", "chd", "pfal", "pbd", "pbr_fc", "pman_fc"),
                     site_name = c("South Fork CLP", "Chambers Lake Outflow", "CLP at Poudre Falls", "Canyon Mouth", "CLP at Indian Meadows", "CLP at Manners Bridge"),
                     color = c("#002EA3", "#E70870", "#256BF5", "#1E4D2B", "#56104E", "#FFCA3A"))

#CDWR sites we are interested in
cdwr_upper_clp_sites <- c(   "LAPLODCO", "JOEBELCO", "JWCCHACO", "CLANSECO", "CLANLICO", "NPRCANCO", #Upper CLP Basin sites
                             "MUNCANCO","CLANHACO", "NOCALACO", "CLASRKCO", "CLAFTCCO", #Upper CLP Basin sites
                             "HOROUTCO", "HSCCLPCO", #Lower CLP Diversions (Horsetooth)
                             "LAPTUNCO", "CAPDCPCO" #laramie river basin
)

water_chem <- read_parquet("data/chem/ROSS_FC_water_chemistry.parquet")

#Parameter plot bounds
plot_param_table <- tibble(
  parameter = c("Temperature", "Turbidity", "pH", "DO",
                 "Specific Conductivity", "Chl-a Fluorescence", "FDOM Fluorescence", "Depth",
                "TOC"),
  lower = c(10, 0.1, 6.5, 6, 20, 0.1, 0.1, 0.1, 2),
  upper = c(20, 40, 9, 10, 60, 1, 1, 2, 5),
  units = c("°C", "NTU", "", "mg/L", "µS/cm", "RFU", "RFU", "ft", "mg/L")
)
toc_model_bounds <- read_parquet("data/chem/ROSS_FC_water_chemistry.parquet")%>%
  summarise(TOC_lower  = min(TOC, na.rm = T),
            TOC_upper = max(TOC, na.rm = T))


