
library(tidyverse)
library(lubridate)

# --- Generate Synthetic Data ---
set.seed(42)
sites <- c("Site A", "Site B", "Site C", "Site D", "Site E")
distances <- c(50, 40, 25, 10, 0)
velocity_km_hr <- 2.0

# Create empty dataframe
df <- data.frame()

# Time range
dates <- seq(from = as.POSIXct("2023-01-01 00:00"),
             to = as.POSIXct("2023-01-03 00:00"), by = "hour")

for (i in 1:length(sites)) {
  site <- sites[i]
  dist <- distances[i]

  # Physics simulation (Pulse moving downstream)
  dist_traveled <- 50 - dist
  peak_time_delay <- dist_traveled / velocity_km_hr
  peak_time_index <- 10 + peak_time_delay # starts at hour 10

  # Create pulse curve
  hours <- 0:(length(dates)-1)
  width <- 3 + (dist_traveled * 0.1)
  amplitude <- 1000 * exp(-0.02 * dist_traveled)

  values <- 10 + amplitude * exp(-0.5 * ((hours - peak_time_index) / width)^2)

  # Add noise
  values <- values + rnorm(length(values), 0, 5)
  values[values < 0] <- 0

  # Bind to main dataframe
  temp_df <- data.frame(
    site = site,
    distance_upstream_km = dist,
    DT_round = dates,
    mean = values
  )
  df <- rbind(df, temp_df)
}

# Ensure site is a factor ordered by distance (Upstream -> Downstream)
df$site <- factor(df$site, levels = rev(sites)) # A is top (upstream)


### ggplot ##

library(ggplot2)
library(ggridges)

# Define scaling factor (how much the peaks overlap)
scale_factor <- 0.01

p_static <- ggplot(df%>%filter(DT_round <= "2023-01-02 12:00"), aes(x = DT_round, y = site, height = mean, fill = site)) +
  # geom_ridgeline draws the shape based on 'height' relative to 'y'
  geom_ridgeline(
    scale = 0.002,     # Adjusts vertical height of pulses
    alpha = 0.7,       # Transparency
    color = "white",   # Line color
    linewidth = 0.7
  ) +
  scale_fill_viridis_d(direction = -1) +
  labs(
    title = "Turbidity Pulse Moving Downstream",
    subtitle = "Ridgeline Plot (Distance based ordering)",
    x = "Date Time",
    y = "Site Location"
  ) +
  theme_minimal() +
  theme(
    legend.position = "none",
    axis.text.y = element_text(size = 11, face = "bold"),
    panel.grid.minor = element_blank()
  )

print(p_static)

ggplotly(p_static)

# --- 2. Calculate Peaks for Labels ---
# We need a separate dataframe that contains only the peak information
peaks <- df %>%
  group_by(site) %>%
  summarize(
    max_val = max(mean),
    max_dt = DT_round[which.max(mean)], # The timestamp of the max value
    site_num = as.numeric(site)         # The numeric baseline (1, 2, 3...)
  )

# --- 3. Define Plotting Parameters ---
# This factor determines how tall the ridges are.
# It must be the SAME in both geom_ridgeline and the label calculation.
ridge_scale <- 0.002
label_buffer <- 0.15 # Extra spacing to put text slightly above the line

# --- 4. Plot ---
p_static <- ggplot() +
  # Draw the Ridges
  # Note: We use as.numeric(site) for Y to treat it as continuous numbers (1, 2, 3...)
  geom_ridgeline(
    data = df,
    aes(x = DT_round, y = as.numeric(site), height = mean, fill = site),
    scale = ridge_scale,
    alpha = 0.7,
    color = "white"
  ) +

  # Add the Labels
  # y = Baseline (site_num) + Height (max_val * scale) + Buffer
  geom_text(
    data = peaks,
    aes(
      x = max_dt,
      y = site_num + (max_val * ridge_scale) + label_buffer,
      label = round(max_val, 0) # Round to whole number
    ),
    size = 3.5,
    fontface = "bold",
    vjust = 0 # Sit on top of the coordinate
  ) +

  # Styling
  scale_fill_viridis_d(direction = -1) +
  # Since we used numeric Y, we must manually label the Y-axis back to Site Names
  scale_y_continuous(
    breaks = 1:length(sites),
    labels = levels(df$site),
    expand = c(0, 0.5) # Add space at top for the highest label
  ) +
  labs(
    title = "Turbidity Pulse Tracking (with Peak Values)",
    x = "Date Time",
    y = "Monitoring Site"
  ) +
  theme_minimal() +
  theme(legend.position = "none")

ggplotly(p_static)
