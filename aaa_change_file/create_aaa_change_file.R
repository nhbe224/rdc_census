### Access Across America Change File

# Set Working Directory ---------------------------------------------------
setwd("D:/neeco/rdc_census/aaa_change_file/")

# Install Packages --------------------------------------------------------
library(tidyverse)
library(data.table)
library(sf)
library(foreach)
library(doParallel)

completed_years <- c()
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
      rename(jobs = tot_jobs)
  }
  write.csv(transit_dfs_csv[[i]], paste0("./outputs/transit",  transit_years_csv[i], ".csv"), row.names = F)
}
rm(transit_dfs_csv)

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
    rename(geoid = blockid, jobs = w_c000_16) %>%
    select(geoid, threshold, jobs, year)
  write.csv(transit_dfs_gpkg[[i]], paste0("./outputs/transit",  transit_years_gpkg[i], ".csv"), row.names = F)
}

## 2021 ----------------------------------------------------------------------
transit_years_2021 <- c(2021)
transit_dfs_2021 <- list()
for(i in 1:length(transit_years_2021)){
  keep_files <- list.files(path=paste0("./inputs/transit", transit_years_2021[i]), pattern=paste0('*block_', transit_years_2021[i], '.csv'))
  print(keep_files)
  transit_dfs_2021[[i]] <- do.call(rbind,lapply(paste0(getwd(), "/inputs/transit", transit_years_2021[i], '/', keep_files), fread))
  transit_dfs_2021[[i]]$year <- transit_years_2021[i]
  transit_dfs_2021[[i]] <- transit_dfs_2021[[i]] %>%
    select(geoid, threshold, weighted_average, year) %>%
    filter(threshold == 1800) %>%
    rename(jobs = weighted_average) %>%
    mutate(threshold = threshold / 60)
  write.csv(transit_dfs_2021[[i]], paste0("./outputs/transit",  transit_years_2021[i], ".csv"), row.names = F)
}

## 2022 ----------------------------------------------------------------------
transit_years_2022 <- c(2022)
transit_dfs_2022 <- list()
for(i in 1:length(transit_years_2022)){
  keep_files <- list.files(path=paste0("./inputs/transit", transit_years_2022[i]), pattern=paste0('*block_', transit_years_2022[i], '.csv'))
  print(keep_files)
  transit_dfs_2022[[i]] <- do.call(rbind,lapply(paste0(getwd(), "/inputs/transit", transit_years_2022[i], '/', keep_files), fread))
  transit_dfs_2022[[i]] <- transit_dfs_2022[[i]] %>%
    select(`Census ID`, Threshold, total_jobs, Year) %>%
    filter(Threshold == 30) %>%
    rename(jobs = total_jobs,
           geoid = `Census ID`,
           year = Year,
           threshold = Threshold) %>%
    select(geoid, threshold, jobs, year)
  write.csv(transit_dfs_2022[[i]], paste0("./outputs/transit",  transit_years_2022[i], ".csv"), row.names = F)
}


# Walk --------------------------------------------------------------------
## 2014 -------------------------------------------------------------------
walk2014_files <- paste0(getwd(), "/inputs/walk2014/", list.files(path = "./inputs/walk2014", pattern = "*.csv"))
walk2014 <- do.call(rbind, lapply(walk2014_files, fread))
walk2014$threshold <- 30
walk2014$year <- 2014
walk2014 <- walk2014 %>%
  rename(geoid = geoid10, jobs = jobs_tot) %>%
  select(geoid, threshold, jobs, year)
write.csv(walk2014, "./outputs/walk2014.csv", row.names = F)

## 2022 -------------------------------------------------------------------
walk_unzip <- list.files(path ="./inputs/walk2022/", pattern=".zip$")
outDir <- "./inputs/walk2022/"
for(j in walk_unzip) {
  print(paste0("Now unzipping ", "./inputs/walk2022/", j))
  unzip(paste0("./inputs/walk2022/", j), exdir=outDir)
}

walk2022_files <- paste0(getwd(), "/inputs/walk2022/", list.files(path = "./inputs/walk2022", pattern = "*block_2022.csv"))
walk2022 <- do.call(rbind, lapply(walk2022_files, fread))
walk2022 <- walk2022 %>%
  rename(geoid = `Census ID`, jobs = total_jobs, year = Year, threshold = Threshold) %>%
  select(geoid, threshold, jobs, year) %>%
  filter(threshold == 30)
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
  rename(geoid = blockid,
         jobs = w_c000_16) %>%
  select(geoid, threshold, jobs, year)
bike2019$geoid <- as.numeric(bike2019$geoid)
write.csv(bike2019, "./outputs/bike2019.csv", row.names = F)

## 2021 -------------------------------------------------------------------
bike_unzip <- list.files(path ="./inputs/bike2021/", pattern=".zip$")
outDir <- "./inputs/bike2021/"
for(j in bike_unzip) {
  print(paste0("Now unzipping ", "./inputs/bike2021/", j))
  unzip(paste0("./inputs/bike2021/", j), exdir=outDir)
}


keep_files <- list.files(path="./inputs/bike2021", pattern = '*block_2021.csv')
print(keep_files) 
bike2021 <- do.call(rbind, lapply(paste0(getwd(), "/inputs/bike2021/", keep_files), fread))
bike2021 <- bike2021 %>%
  select(geoid, threshold, weighted_average, year) %>%
  filter(threshold == 1800) %>%
  rename(jobs = weighted_average) %>%
  mutate(threshold = threshold / 60)
write.csv(bike2021, "./outputs/bike2021.csv", row.names = F)


## 2022 -------------------------------------------------------------------
bike_unzip <- list.files(path ="./inputs/bike2022/", pattern=".zip$")
outDir <- "./inputs/bike2022/"
for(j in bike_unzip) {
  print(paste0("Now unzipping ", "./inputs/bike2022/", j))
  unzip(paste0("./inputs/bike2022/", j), exdir=outDir)
}


keep_files <- list.files(path="./inputs/bike2022", pattern = '*block_2022.csv')
print(keep_files) 
bike2022 <- do.call(rbind, lapply(paste0(getwd(), "/inputs/bike2022/", keep_files), fread))
bike2022 <- bike2022 %>%
  select(`Census ID`, Threshold, total_jobs, Year) %>%
  rename(geoid = `Census ID`, threshold = Threshold, 
         jobs = total_jobs, year = Year) %>%
  filter(threshold == 30)
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
  rename(geoid = blockid, jobs = w_c000_16) %>%
  select(geoid, threshold, jobs, year) %>%
  distinct()

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
                           pattern = "*block_2021.csv")
  auto2021[[i]] <- fread(paste0(getwd(), "/inputs/auto2021/", unzipped_folders[i], "/", keep_files))
}

auto2021 <- bind_rows(auto2021, .id = "column_label")
auto2021 <- auto2021 %>%
  rename(jobs = weighted_average) %>%
  mutate(threshold = threshold / 60) %>%
  filter(threshold == 30) %>%
  select(geoid, threshold, jobs, year) %>%
  distinct()

write.csv(auto2021, "./outputs/auto2021.csv", row.names = F)

## 2022 -------------------------------------------------------------------
auto_unzip <- list.files(path ="./inputs/auto2022/", pattern=".zip$")
outDir <- "./inputs/auto2022/"
for(j in auto_unzip) {
  print(paste0("Now unzipping ", "./inputs/auto2022/", j))
  unzip(paste0("./inputs/auto2022/", j), exdir=outDir)
}

keep_files <- list.files(path="./inputs/auto2022", pattern = '*block_2022.csv')
print(keep_files) 
auto2022 <- do.call(rbind, lapply(paste0(getwd(), "/inputs/auto2022/", keep_files), fread))
auto2022 <- auto2022 %>%
  select(`Census ID`, Threshold, total_jobs, Year) %>%
  rename(geoid = `Census ID`, threshold = Threshold, 
         jobs = total_jobs, year = Year) %>%
  filter(threshold == 30)
write.csv(auto2022, "./outputs/auto2022.csv", row.names = F)

# Make Sure Files are Consistent ------------------------------------------
output_files <- paste0("./outputs/", list.files(path = "./outputs", pattern = "*.csv"))
output_files
for(i in output_files){
  print(head(fread(i)))
}




