## Traffic Counts
setwd("D:/neeco/rdc_census/traffic_volumes/")


# Libraries ---------------------------------------------------------------
library(foreign)
library(tidyverse)

# Load in Data ------------------------------------------------------------
traffic_counts_load <- read.dta("./ICPSR_38584/DS0001/38584-0001-Data.dta")


# Clean Data --------------------------------------------------------------
traffic_counts <- traffic_counts_load %>%
  select(TRACT_FIPS10, YEAR, MEAN_TRAFFIC, MEAN_HW_TRAFFIC, MEAN_NONHW_TRAFFIC) %>%
  rename_with(tolower) %>%
  rename(tract2010 = tract_fips10)


# Write Data --------------------------------------------------------------
write.dta(traffic_counts, "./traffic_volumes.dta")

