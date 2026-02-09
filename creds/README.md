# API Creds

This folder contains API templates to pull data from APIs used in this project. Please Contact Sam Struthers or Juan De La Torre for creds. Each service contains a template file. 

# Services

`HydroVu`: This accesses the HydroVu API which is where our sonde data is stored, and is the main source of data for the dashboard. Used by the `ross.wq.tools` package

`Contrail`: This accesses WQ/stage data from the Fort Collins' sondes in the CLP. Used by the function `pull_contrail_data`

`mWater`: This accesses our field notes, which is mainly for auto QAQC but may be useful in the dashboard. Used by the `ross.wq.tools` package

`CDWR`: This accesses CDWR & USGS flow data which is public but since we may pull large amounts of data we need a API key. Used by the `cdssr` package
