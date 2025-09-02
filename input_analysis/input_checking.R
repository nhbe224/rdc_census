# Input Checking
# Set Working Directory ---------------------------------------------------
setwd("D:/neeco/rdc_census/input_analysis/")

# Install Packages --------------------------------------------------------
library(tidyverse)
library(data.table)
library(sf)
library(tidycensus)
library(tigris)
options(scipen = 999999)


# Read in Data ------------------------------------------------------------
file_paths <- list.files("../acs_change_file/outputs/", full.names = TRUE, pattern = "\\.csv$")
acs_files <- map_dfr(file_paths, read_csv)
head(acs_files)


# Summary of Data ---------------------------------------------------------
summary(acs_files$hh_den_acre)
summary(acs_files$pop_den_acre)
summary(acs_files$median_income_2022dollars)
summary(acs_files$median_income)
summary(acs_files$pct_minority)


# Get random sample of block groups ---------------------------------------
sample_bgs <- sample(unique(acs_files$bg2010), 15)
sample_bgs

acs_sample_bgs <- acs_files %>%
  filter(bg2010 %in% sample_bgs)

# Check Income ------------------------------------------------------------
## Plot Income Over Time
acs_sample_bgs %>%
  ggplot(aes(x=year, y=median_income_2022dollars, group=bg2010, color=bg2010)) +
  geom_line() + ggtitle("Median Income (2022 Dollars) over Time") + 
  geom_vline(xintercept = 2010, linetype = "dashed") +
  geom_text(aes(label = "Interpolated\nValues", x=2005, y = max(acs_sample_bgs$median_income_2022dollars, na.rm =  T) - 20000),
            color = "black", size = 3.5) +
  geom_text(aes(label = "From\nACS", x=2015, y = max(acs_sample_bgs$median_income_2022dollars, na.rm =  T) - 20000),
            color = "black", size = 3.5) +
  geom_vline(xintercept = 2020, linetype = "dashed") +
  geom_text(aes(label = "Crosswalked\nValues", x=2021.5, y = max(acs_sample_bgs$median_income_2022dollars, na.rm =  T) - 20000),
            color = "black", size = 3.5)

acs_sample_bgs %>%
  ggplot(aes(x=year, y=median_income, group=bg2010, color=bg2010)) +
  geom_line() + ggtitle("Median Income (Unadjusted) over Time") + 
  geom_vline(xintercept = 2010, linetype = "dashed") +
  geom_text(aes(label = "Interpolated\nValues", x=2005, y = max(acs_sample_bgs$median_income, na.rm =  T) - 10000),
            color = "black", size = 3.5) +
  geom_text(aes(label = "From\nACS", x=2015, y = max(acs_sample_bgs$median_income, na.rm =  T) - 10000),
            color = "black", size = 3.5) +
  geom_vline(xintercept = 2020, linetype = "dashed") +
  geom_text(aes(label = "Crosswalked\nValues", x=2021.5, y = max(acs_sample_bgs$median_income, na.rm =  T) - 10000),
            color = "black", size = 3.5)

acs_files %>%
  filter(bg2010 %in% c("040131094002")) %>% # North Park San Diego, Woodland Park Lexington, Mass Ave Indianapolis
  ggplot(aes(x=year, y=median_income, group=bg2010, color=bg2010)) +
  geom_line() + ggtitle("Median Income in Select Neighborhoods over Time")


# Check Pct Minority ------------------------------------------------------
acs_sample_bgs %>%
  ggplot(aes(x=year, y=pct_minority, group=bg2010, color=bg2010)) +
  geom_line() + ggtitle("Percent Minority over Time")

acs_sample <- acs_files %>%
  filter(bg2010 == "040131094002")
