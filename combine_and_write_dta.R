### Combine Files and Export as DTA
# Set Working Directory ---------------------------------------------------
setwd("D:/neeco/rdc_census/")

# Install Packages --------------------------------------------------------
library(tidyverse)
library(data.table)
library(sf)
library(tidycensus)
library(tigris)
library(foreign)
options(scipen = 9999)

# ACS Data -------------------------------------------------------------
acs_file_paths <- list.files("./acs_change_file/outputs/", full.names = TRUE, pattern = "\\.csv$")
acs_files <- map_dfr(acs_file_paths, read_csv)
describe(acs_files)
write.dta(acs_files, "./to_rdc/acs_ALL_to2023.dta")
rm(acs_files)

# LEHD Data -------------------------------------------------------------
lehd_file_paths <- list.files("./lehd_change_file/outputs/", full.names = TRUE, pattern = "\\.csv$")
lehd_files <- map_dfr(lehd_file_paths, read_csv)
write.dta(lehd_files, "./to_rdc/lehd_ALL.dta")
rm(lehd_files)

# SLD Data -------------------------------------------------------------
sld_file_paths <- list.files("./sld_change_file/outputs/", full.names = TRUE, pattern = "\\.csv$")
sld_files <- map_dfr(sld_file_paths, read_csv)
write.dta(sld_files, "./to_rdc/sld_ALL.dta")
rm(sld_files)


# AAA Data ----------------------------------------------------------------
## Transit ----------------------------------------------------------------
transit_file_paths <- list.files("./aaa_change_file/outputs/", full.names = TRUE, pattern = "^transit")
transit_files <- map_dfr(transit_file_paths, read_csv)
write.dta(transit_files, "./to_rdc/transit_ALL.dta")
rm(transit_files)

## Walk ------------------------------------------------------------------
walk_file_paths <- list.files("./aaa_change_file/outputs/", full.names = TRUE, pattern = "^walk")
walk_files <- map_dfr(walk_file_paths, read_csv)
write.dta(walk_files, "./to_rdc/walk_ALL.dta")
rm(walk_files)

## Bike ------------------------------------------------------------------
bike_file_paths <- list.files("./aaa_change_file/outputs/", full.names = TRUE, pattern = "^bike")
bike_files <- map_dfr(bike_file_paths, read_csv)
write.dta(bike_files, "./to_rdc/bike_ALL.dta")
rm(bike_files)

## Auto ------------------------------------------------------------------
auto_file_paths <- list.files("./aaa_change_file/outputs/", full.names = TRUE, pattern = "^auto")
auto_files <- map_dfr(auto_file_paths, read_csv)
write.dta(auto_files, "./to_rdc/auto_ALL.dta")
rm(auto_files)

## 2000 Block to 2010 BG -------------------------------------------------
xwalk_2000blk_2010bg <- read.csv("./to_rdc/nhgis_blk2000_bg2010.csv")
nrow(xwalk_2000blk_2010bg)

## 2000 Block to 2010 BG -------------------------------------------------
xwalk_2020blk_2010bg <- read.csv("./to_rdc/nhgis_blk2020_bg2010.csv")
head(xwalk_2020blk_2010bg)
