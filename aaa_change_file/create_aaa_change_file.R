### Access Across America Change File

# Set Working Directory ---------------------------------------------------
setwd("D:/neeco/rdc_census/aaa_change_file/")

### AAA: aggregate blocks using average
## Document via readme
## Make a data dictionary
## Make a powerpoint presentation, include list of unanswered questions
## Some maps?? One set for absolute, one set for change. 
#### US, Lexington (or other places we have local knowledge about)

# Install Packages --------------------------------------------------------
library(tidyverse)
library(data.table)
library(sf)
options(scipen = 99999)


# Read in Crosswalk -------------------------------------------------------
nhgis_2020bg_2010bg <- read.csv("./nhgis_bg2020_bg2010.csv")
nhgis_2020bg_2010bg$bg2020ge <- as.character(nhgis_2020bg_2010bg$bg2020ge)
nhgis_2020bg_2010bg$bg2020ge <- ifelse(nchar(nhgis_2020bg_2010bg$bg2020ge) == 11, paste0("0", nhgis_2020bg_2010bg$bg2020ge),
                                       nhgis_2020bg_2010bg$bg2020ge)
nhgis_2020bg_2010bg$bg2010ge <- as.character(nhgis_2020bg_2010bg$bg2010ge)
nhgis_2020bg_2010bg$bg2010ge <- ifelse(nchar(nhgis_2020bg_2010bg$bg2010ge) == 11, paste0("0", nhgis_2020bg_2010bg$bg2010ge),
                                       nhgis_2020bg_2010bg$bg2010ge)
nhgis_2020bg_2010bg <- nhgis_2020bg_2010bg %>%
  select(-bg2020gj, -bg2010gj)

# Transit ----------------------------------------------------------------
## 2014-2017 -------------------------------------------------------------
transit_years_csv <- c(2014, 2015, 2016, 2017)
transit_dfs_csv <- list()

for(i in 1:length(transit_years_csv)){
  transit_dfs_csv[[i]] <- do.call(rbind, lapply(paste0(getwd(), "/inputs/transit", 
                                                        transit_years_csv[i], "/", 
                                                        list.files(path = paste0("./inputs/transit", transit_years_csv[i]))), 
                                                 fread))
  transit_dfs_csv[[i]]$year <- transit_years_csv[i]
  if(transit_years_csv[i] == 2014){
    transit_dfs_csv[[i]] <- transit_dfs_csv[[i]] %>%
      rename(jobs = tot_jobs)}
  transit_dfs_csv[[i]]$geoid <- as.character(transit_dfs_csv[[i]]$geoid)
  # Make sure character length is right
  transit_dfs_csv[[i]]$bg2010 <- ifelse(nchar(transit_dfs_csv[[i]]$geoid) == 14, paste0("0", transit_dfs_csv[[i]]$geoid), transit_dfs_csv[[i]]$geoid)
  # Make into block groups
  transit_dfs_csv[[i]]$bg2010 <- substr(as.character(transit_dfs_csv[[i]]$bg2010), 1, 12)
  transit_dfs_csv[[i]] <- transit_dfs_csv[[i]] %>%
    select(bg2010, threshold, jobs, year)
  transit_dfs_csv[[i]] <- transit_dfs_csv[[i]] %>%
    select(bg2010, threshold, jobs, year) %>%
    group_by(bg2010, threshold, year) %>%
    summarize(jobs = mean(jobs))
  write.csv(transit_dfs_csv[[i]], paste0("./outputs/transit",  transit_years_csv[i], ".csv"), row.names = F)
}

#rm(transit_dfs_csv)

## Unzip These Years
transit_years_unzip <- c(2018, 2019, 2021, 2022)
for(i in 1:length(transit_years_unzip)){
  files <- list.files(path = paste0("./inputs/transit", transit_years_unzip[i]), pattern=".zip$")
  outDir <- paste0("./inputs/transit", transit_years_unzip[i])
  for(j in files) {
    print(paste0("./inputs/transit", transit_years_unzip[i] ,"/", j))
    unzip(paste0("./inputs/transit", transit_years_unzip[i] ,"/", j), exdir=outDir)
  }
}

## Account for zipfiles with folders in them (instead of files)
transit_years_unzip <- c(2018, 2019, 2021, 2022)
for(i in 1:length(transit_years_unzip)){
  folders <- list.dirs(path = paste0("./inputs/transit", transit_years_unzip[i]), recursive = FALSE)
  folders_use <- folders[!grepl("__MACOSX", folders)]
  print(folders_use)
  for(j in folders_use){
    # List files in the subdirectory
    files <- list.files(folders_use, full.names = TRUE)
    # Construct destination paths
    dest_files <- file.path(paste0("./inputs/transit", transit_years_unzip[i]), basename(files))
    # Move files up one level
    file.rename(files, dest_files)
    # Optionally, remove empty subdirectory
    unlink(folders_use, recursive = TRUE)
  }
}

## 2018-2019 -------------------------------------------------------------
### Delete anything that's not .gpkg. if 2018 and 2019
transit_years_gpkg <- c(2018, 2019)
for(i in 1:length(transit_years_gpkg)){
  delete_files <- grep(list.files(path=paste0("./inputs/transit", transit_years_gpkg[i])), pattern='*.gpkg', invert=TRUE, value=TRUE)
  file.remove(paste0(getwd(), "/inputs/transit", transit_years_gpkg[i], "/", delete_files))  
  file.remove(paste0(getwd(), "/inputs/transit", transit_years_gpkg[i], "/", list.files(pattern = "*.txt")))
}

st_read_select <- function(file){
  data <- st_read(file, layer = "tr_30_minutes")  
  state_df <- data %>% select(blockid, w_c000_16)
  return(state_df)
}


transit_dfs_gpkg <- list()
for(i in 1:length(transit_years_gpkg)){
  file_paths <- paste0(getwd(), "/inputs/transit", 
                       transit_years_gpkg[i], "/", 
                       list.files(path = paste0("./inputs/transit", transit_years_gpkg[i])))
  transit_dfs_gpkg[[i]] <- do.call(rbind, lapply(file_paths, st_read_select))
  transit_dfs_gpkg[[i]]$year <- transit_years_gpkg[i]
  transit_dfs_gpkg[[i]]$threshold <- 30
  transit_dfs_gpkg[[i]] <- transit_dfs_gpkg[[i]] %>%
    select(blockid, w_c000_16, year, threshold) %>%
    rename(block2010 = blockid, jobs = w_c000_16) %>%
    select(block2010, threshold, jobs, year) %>%
    mutate(bg2010 = substr(as.character(block2010), 1, 12)) %>%
    group_by(bg2010, threshold, year) %>%
    summarize(jobs = mean(jobs, na.rm = T))
}

for(i in 1:length(transit_years_gpkg)){
  write.csv(transit_dfs_gpkg[[i]], paste0("./outputs/transit",  transit_years_gpkg[i], ".csv"), row.names = F)
}

## 2021 ----------------------------------------------------------------------
keep_files <- list.files(path=paste0("./inputs/transit2021"), pattern=paste0('*block_group_2021.csv'))
print(keep_files)
transit_dfs_2021 <- do.call(rbind,lapply(paste0(getwd(), "/inputs/transit2021", '/', keep_files), fread))
transit_dfs_2021$year <- 2021
transit_dfs_2021 <- transit_dfs_2021 %>%
  select(geoid, threshold, weighted_average, year) %>%
  filter(threshold == 1800) %>%
  rename(jobs = weighted_average, bg2010 = geoid) %>%
  mutate(threshold = threshold / 60)
transit_dfs_2021$bg2010 <- as.character(transit_dfs_2021$bg2010)
transit_dfs_2021$bg2010 <- ifelse(nchar(transit_dfs_2021$bg2010) == 11, paste0("0", transit_dfs_2021$bg2010),
                                       transit_dfs_2021$bg2010)
write.csv(transit_dfs_2021, paste0("./outputs/transit2021.csv"), row.names = F)


## 2022 ----------------------------------------------------------------------
transit_dfs_2022_load <- list()
keep_files <- list.files(path=paste0("./inputs/transit2022"), pattern=paste0('*block_group_2022.csv'))
print(keep_files)
transit_dfs_2022_load <- do.call(rbind,lapply(paste0(getwd(), "/inputs/transit2022", '/', keep_files), fread))
transit_dfs_2022_load <- transit_dfs_2022_load %>%
  select(`Census ID`, Threshold, Weighted_average_total_jobs, Year) %>%
  filter(Threshold == 30) %>%
  rename(jobs = Weighted_average_total_jobs,
         bg2020 = `Census ID`,
         year = Year,
         threshold = Threshold) %>%
  select(bg2020, threshold, jobs, year)
transit_dfs_2022_load$bg2020 <- as.character(transit_dfs_2022_load$bg2020)
transit_dfs_2022_load$bg2020 <- ifelse(nchar(transit_dfs_2022_load$bg2020) == 11, paste0("0", transit_dfs_2022_load$bg2020),
                                  transit_dfs_2022_load$bg2020)

## Crosswalk to 2010 BGs
transit_dfs_2022 <- transit_dfs_2022_load %>%
  left_join(nhgis_2020bg_2010bg, by = c("bg2020"="bg2020ge")) %>%
  mutate(jobs = jobs * wt_pop) %>%
  group_by(bg2010ge, threshold, year) %>%
  summarize(jobs = sum(jobs, na.rm = FALSE)) %>%
  rename(bg2010 = bg2010ge)

write.csv(transit_dfs_2022, paste0("./outputs/transit2022.csv"), row.names = F)


# Walk --------------------------------------------------------------------
## 2014 -------------------------------------------------------------------
walk2014_files <- paste0(getwd(), "/inputs/walk2014/", list.files(path = "./inputs/walk2014", pattern = "*.csv"))
walk2014 <- do.call(rbind, lapply(walk2014_files, fread))
walk2014$threshold <- 30
walk2014$year <- 2014
walk2014 <- walk2014 %>%
  rename(block2010 = geoid10, jobs = jobs_tot) %>%
  select(block2010, threshold, jobs, year) %>%
  mutate(block2010 = as.character(block2010)) %>%
  mutate(block2010 = ifelse(nchar(block2010) == 14, paste0("0", block2010), block2010),
         bg2010 = substr(as.character(block2010), 1, 12)) %>%
  group_by(bg2010, threshold, year) %>%
  summarize(jobs = mean(jobs, na.rm = TRUE))
write.csv(walk2014, "./outputs/walk2014.csv", row.names = F)

## 2022 -------------------------------------------------------------------
walk_unzip <- list.files(path ="./inputs/walk2022/", pattern=".zip$")
outDir <- "./inputs/walk2022/"
for(j in walk_unzip) {
  print(paste0("Now unzipping ", "./inputs/walk2022/", j))
  unzip(paste0("./inputs/walk2022/", j), exdir=outDir)
}

walk2022_files <- paste0(getwd(), "/inputs/walk2022/", list.files(path = "./inputs/walk2022", pattern = "*block_group_2022.csv"))
walk2022_load <- do.call(rbind, lapply(walk2022_files, fread))
walk2022_load <- walk2022_load %>%
  rename(bg2020 = `Census ID`, jobs = Weighted_average_total_jobs, year = Year, threshold = Threshold) %>%
  select(bg2020, threshold, jobs, year) %>%
  filter(threshold == 30) %>%
  mutate(bg2020 = as.character(bg2020),
         bg2020 = ifelse(nchar(bg2020) == 11, paste0("0", bg2020), bg2020))

walk2022 <- walk2022_load %>%
  left_join(nhgis_2020bg_2010bg, by = c("bg2020" = "bg2020ge")) %>%
  mutate(jobs = jobs * wt_pop) %>%
  group_by(bg2010ge, threshold, year) %>%
  summarize(jobs = sum(jobs, na.rm = FALSE)) %>%
  rename(bg2010 = bg2010ge)

  
write.csv(walk2022, "./outputs/walk2022.csv", row.names = F)

# Bike --------------------------------------------------------------------
## 2019 -------------------------------------------------------------------
bike_unzip <- list.files(path ="./inputs/bike2019/", pattern=".zip$")
outDir <- "./inputs/bike2019/"
for(j in bike_unzip) {
  print(paste0("Now unzipping ", "./inputs/bike2019/", j))
  unzip(paste0("./inputs/bike2019/", j), exdir=outDir)
}

lts4_unzip <- list.files(path ="./inputs/bike2019/", pattern="*lts4.zip")
outDir <- "./inputs/bike2019/"
for(j in lts4_unzip) {
  print(paste0("Now unzipping ", "./inputs/bike2019/", j))
  unzip(paste0("./inputs/bike2019/", j), exdir=outDir)
}

st_read_select_bike <- function(file){
  data <- st_read(file, layer = "bi_30_minutes")  
  state_df <- data %>% select(blockid, w_c000_16)
  return(state_df)
}

file_paths <- paste0(getwd(), "/inputs/bike2019/", 
                     list.files(path = "./inputs/bike2019", pattern = "*lts4.gpkg"))
bike2019 <- do.call(rbind, lapply(file_paths, st_read_select_bike))
bike2019$year <- 2019
bike2019$threshold <- 30
bike2019 <- bike2019 %>%
  rename(jobs = w_c000_16) %>%
  select(blockid, threshold, jobs, year) %>%
  mutate(bg2010 = substr(blockid, 1, 12)) %>%
  group_by(bg2010, threshold, year) %>%
  summarize(jobs = mean(jobs, na.rm = T))
  
write.csv(bike2019, "./outputs/bike2019.csv", row.names = F)

## 2021 -------------------------------------------------------------------
bike_unzip <- list.files(path ="./inputs/bike2021/", pattern=".zip$")
outDir <- "./inputs/bike2021/"
for(j in bike_unzip) {
  print(paste0("Now unzipping ", "./inputs/bike2021/", j))
  unzip(paste0("./inputs/bike2021/", j), exdir=outDir)
}

folders <- list.dirs(path = "./inputs/bike2021", recursive = FALSE)
folders_use <- folders[!grepl("__MACOSX", folders)]
print(folders_use)
# List files in the subdirectory
files <- list.files(folders_use, full.names = TRUE)
# Construct destination paths
dest_files <- file.path("./inputs/bike2021", basename(files))
# Move files up one level
file.rename(files, dest_files)
# Optionally, remove empty subdirectory
unlink(folders_use, recursive = TRUE)

keep_files <- list.files(path="./inputs/bike2021", pattern = '*block_group_2021.csv')
print(keep_files) 
bike2021 <- do.call(rbind, lapply(paste0(getwd(), "/inputs/bike2021/", keep_files), fread))
bike2021 <- bike2021 %>%
  select(geoid, threshold, weighted_average, year) %>%
  filter(threshold == 1800) %>%
  rename(jobs = weighted_average,
         bg2010 = geoid) %>%
  mutate(threshold = threshold / 60) %>%
  mutate(bg2010 = as.character(bg2010),
         bg2010 = ifelse(nchar(bg2010) == 11, paste0("0", bg2010), bg2010))

write.csv(bike2021, "./outputs/bike2021.csv", row.names = F)


## 2022 -------------------------------------------------------------------
bike_unzip <- list.files(path ="./inputs/bike2022/", pattern=".zip$")
outDir <- "./inputs/bike2022/"
for(j in bike_unzip) {
  print(paste0("Now unzipping ", "./inputs/bike2022/", j))
  unzip(paste0("./inputs/bike2022/", j), exdir=outDir)
}

keep_files <- list.files(path="./inputs/bike2022", pattern = '*block_group_2022.csv')
print(keep_files) 
bike2022_load <- do.call(rbind, lapply(paste0(getwd(), "/inputs/bike2022/", keep_files), fread))
bike2022_load <- bike2022_load %>%
  select(`Census ID`, Threshold, Weighted_average_total_jobs, Year) %>%
  rename(bg2020 = `Census ID`, threshold = Threshold, 
         jobs = Weighted_average_total_jobs, year = Year) %>%
  filter(threshold == 30) %>%
  mutate(bg2020 = as.character(bg2020),
         bg2020 = ifelse(nchar(bg2020) == 11, paste0("0", bg2020), bg2020))

bike2022 <- bike2022_load %>% 
  left_join(nhgis_2020bg_2010bg, by = c("bg2020" = "bg2020ge")) %>%
  mutate(jobs = jobs * wt_pop) %>%
  group_by(bg2010ge, threshold, year) %>%
  summarize(jobs = sum(jobs, na.rm = FALSE)) %>%
  rename(bg2010 = bg2010ge)
  

write.csv(bike2022, "./outputs/bike2022.csv", row.names = F)

# Auto --------------------------------------------------------------------
## 2018 -------------------------------------------------------------------
auto_unzip <- list.files(path ="./inputs/auto2018/", pattern=".zip$")
outDir <- "./inputs/auto2018/"
for(j in auto_unzip) {
  print(paste0("Now unzipping ", "./inputs/auto2018/", j))
  unzip(paste0("./inputs/auto2018/", j), exdir=outDir)
}

st_read_select_auto <- function(file){
  data <- st_read(file, layer = "au_30_minutes")  
  df <- data %>% select(blockid, w_c000_16)
  return(df)
}

file_paths <- paste0(getwd(), "/inputs/auto2018/", 
                     list.files(path = "./inputs/auto2018", pattern = "*_0800.gpkg"))
file_paths
auto2018 <- do.call(rbind, lapply(file_paths, st_read_select_auto))
auto2018$year <- 2018
auto2018$threshold <- 30
auto2018 <- auto2018 %>%
  rename(jobs = w_c000_16) %>%
  select(blockid, threshold, jobs, year) %>%
  mutate(bg2010 = substr(blockid, 1, 12)) %>%
  group_by(bg2010, threshold, year) %>%
  summarize(jobs = mean(jobs, na.rm = T))

write.csv(auto2018, "./outputs/auto2018.csv", row.names = F)

## 2021 -------------------------------------------------------------------
auto_unzip <- list.files(path ="./inputs/auto2021/", pattern=".zip$")
outDir <- "./inputs/auto2021/"
for(j in auto_unzip) {
  print(paste0("Now unzipping ", "./inputs/auto2021/", j))
  unzip(paste0("./inputs/auto2021/", j), exdir=outDir)
}

folders <- list.files(path = "./inputs/auto2021")
unzipped_folders = folders[!grepl(".zip", folders)]
unzipped_folders

auto2021 <- list()
for(i in 1:length(unzipped_folders)){
  keep_files <- list.files(path = paste0("./inputs/auto2021/", unzipped_folders[i]),
                           pattern = "*block_group_2021.csv")
  auto2021[[i]] <- fread(paste0(getwd(), "/inputs/auto2021/", unzipped_folders[i], "/", keep_files))
  auto2021[[i]]$geoid <- as.character(auto2021[[i]]$geoid)
}

auto2021 <- bind_rows(auto2021, .id = "column_label")
auto2021 <- auto2021 %>%
  rename(jobs = weighted_average) %>%
  mutate(threshold = threshold / 60,
         bg2010 = ifelse(nchar(geoid) == 11, paste0("0", geoid), geoid)) %>%
  filter(threshold == 30) %>%
  select(bg2010, threshold, jobs, year) 

## Examine BGs ending in 0000000
auto2021_examine <- auto2021 %>%
  filter(str_sub(bg2010, start = -7) == "0000000")

## 2022 -------------------------------------------------------------------
auto_unzip <- list.files(path ="./inputs/auto2022/", pattern=".zip$")
outDir <- "./inputs/auto2022/"
for(j in auto_unzip) {
  print(paste0("Now unzipping ", "./inputs/auto2022/", j))
  unzip(paste0("./inputs/auto2022/", j), exdir=outDir)
}

keep_files <- list.files(path="./inputs/auto2022", pattern = '*block_group_2022.csv')
print(keep_files) 
auto2022_load <- do.call(rbind, lapply(paste0(getwd(), "/inputs/auto2022/", keep_files), fread))
auto2022_load$geoid = as.character(auto2022_load$`Census ID`)
auto2022_load <- auto2022_load %>%
  rename(jobs = Weighted_average_total_jobs,
         year = Year,
         threshold = Threshold) %>%
  mutate(bg2020 = ifelse(nchar(geoid) == 11, paste0("0", geoid), geoid)) %>%
  filter(threshold == 30) %>%
  select(bg2020, threshold, jobs, year)

auto2022 <- auto2022_load %>% 
  left_join(nhgis_2020bg_2010bg, by = c("bg2020" = "bg2020ge")) %>%
  mutate(jobs = jobs * wt_pop) %>%
  group_by(bg2010ge, threshold, year) %>%
  summarize(jobs = sum(jobs, na.rm = FALSE)) %>%
  rename(bg2010 = bg2010ge)

write.csv(auto2022, "./outputs/auto2022.csv", row.names = F)

# Make Sure Files are Consistent ------------------------------------------
output_files <- paste0("./outputs/", list.files(path = "./outputs", pattern = "*.csv"))
output_files
for(i in output_files){
  print(i)
  print(head(fread(i)))
}




