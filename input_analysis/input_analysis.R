# Input Analysis
# Set Working Directory ---------------------------------------------------
setwd("D:/neeco/rdc_census/input_analysis/")

# Install Packages --------------------------------------------------------
library(tidyverse)
library(data.table)
library(sf)
library(tidycensus)
library(tigris)
options(scipen = 999999)


# Get Colors --------------------------------------------------------------
color_vec <- c("cornsilk", "gold", "darkgoldenrod1", "darkorange", "firebrick3", "darkred", 
               "darkorchid1", "darkorchid4", "darkslateblue", "royalblue", "royalblue4", "midnightblue")
color_vec2 <- c( "lightcoral", 
                 "gray", 
                "skyblue", 
                 "royalblue", 
                 "midnightblue")

# Get block group references ----------------------------------------------
bg2010 <- list()
states_list <- c(state.abb, "DC")
states_fips_df <- fips_codes %>%
  select(state, state_code) %>%
  distinct()

for(i in c(1:length(states_list))){
  bg2010[[i]] <- block_groups(state = states_list[i], year = 2010)
  bg2010[[i]] <- bg2010[[i]] %>%
    select(GEOID10, STATEFP) %>%
    rename(bg2010 = GEOID10,
           state_code = STATEFP) %>%
    left_join(states_fips_df, by = "state_code") %>%
    select(bg2010, state)
}

bg2010 <- bind_rows(bg2010)

counties <- fips_codes %>% mutate(state_county_fips = paste0(state_code, county_code)) %>%
  select(state, county, state_county_fips)


# Access Across America ---------------------------------------------------
transit2014 <- read.csv("../aaa_change_file/outputs/transit2014.csv", colClasses=c("bg2010"="character"))
transit2017 <- read.csv("../aaa_change_file/outputs/transit2017.csv", colClasses=c("bg2010"="character"))
transit2022 <- read.csv("../aaa_change_file/outputs/transit2022.csv", colClasses=c("bg2010"="character"))

transitData <- rbind(transit2014, transit2017, transit2022)
transitData <- transitData %>%
  mutate(jobs = case_when(jobs > - 1 & jobs < 25000 ~ "<=25,000",
                          jobs >= 25000 & jobs < 75000 ~ "25,000 to 75,000",
                          jobs >= 75000 & jobs < 150000 ~ "75,000 to 150,000",
                          jobs >= 150000 & jobs < 400000 ~ "150,000 to 400,000", 
                          jobs >= 400000 & jobs < 1000000 ~ "400,000 to 1,000,000",
                          jobs > 1000000 ~ ">1,000,000"))

transitData$jobs <- factor(transitData$jobs, levels = c("<=25,000", "25,000 to 75,000", "75,000 to 150,000", "150,000 to 400,000", "400,000 to 1,000,000", ">1,000,000"))

transit2014 <- transitData %>% filter(year == 2014)
transit2017 <- transitData %>% filter(year == 2017)
transit2022 <- transitData %>% filter(year == 2022)


# LEHD Data ---------------------------------------------------------------
lehd2005 <- read.csv("../lehd_change_file/outputs/lehd2005.csv", colClasses=c("bg2010"="character"))
lehd2010 <- read.csv("../lehd_change_file/outputs/lehd2010.csv", colClasses=c("bg2010"="character"))
lehd2015 <- read.csv("../lehd_change_file/outputs/lehd2015.csv", colClasses=c("bg2010"="character"))
lehd2020 <- read.csv("../lehd_change_file/outputs/lehd2020.csv", colClasses=c("bg2010"="character"))

lehdData <- rbind(lehd2005, lehd2010, lehd2015, lehd2020)

# ACS Data -------------------------------------lehd2020# ACS Data ----------------------------------------------------------------
acs2005 <- read.csv("../acs_change_file/outputs/acs2005.csv", colClasses=c("bg2010"="character"))
acs2010 <- read.csv("../acs_change_file/outputs/acs2010.csv", colClasses=c("bg2010"="character"))
acs2015 <- read.csv("../acs_change_file/outputs/acs2015.csv", colClasses=c("bg2010"="character"))
acs2020 <- read.csv("../acs_change_file/outputs/acs2020.csv", colClasses=c("bg2010"="character"))

acsData <- rbind(acs2005, acs2010, acs2015, acs2020)

acsData <- acsData %>%
  mutate(income_group_2022dollars = case_when(median_income_2022dollars > 0 & median_income_2022dollars < 50000 ~ "<$50,000",
                                  median_income_2022dollars >= 50000 & median_income_2022dollars < 100000 ~ "$50,000 to <$100,000",
                                  median_income_2022dollars >= 100000 & median_income_2022dollars < 150000 ~ "$100,000 to <$150,000",
                                  median_income_2022dollars >= 150000 & median_income_2022dollars < 200000 ~ "$150,000 to <$200,000",
                                  median_income_2022dollars >= 200000 ~ "$200,000+"))

acsData$income_group_2022dollars <- factor(acsData$income_group_2022dollars, levels = c("<$50,000", "$50,000 to <$100,000", "$100,000 to <$150,000", "$150,000 to <$200,000", "$200,000+"))

acsData <- acsData %>%
  mutate(pct_minority_group = case_when(pct_minority > 0 & pct_minority < 0.15 ~ "<15%",
                                  pct_minority >= 0.15 & pct_minority < 0.30 ~ "15% to <30%",
                                  pct_minority >= 0.30 & pct_minority < 0.50 ~ "30% to <50%",
                                  pct_minority >= 0.50 & pct_minority < 0.70 ~ "50% to <70%",
                                  pct_minority >= 0.70 ~ "70%+"))

acsData$pct_minority_group  <- factor(acsData$pct_minority_group, levels = c("<15%", "15% to <30%", "30% to <50%", "50% to <70%", "70%+"))


acs2005 <- acsData %>% filter(year == 2005)  %>%
  arrange(bg2010)
acs2010 <- acsData %>% filter(year == 2010)  %>%
  arrange(bg2010)
acs2015 <- acsData %>% filter(year == 2015)  %>%
  arrange(bg2010)
acs2020 <- acsData %>% filter(year == 2020)  %>%
  arrange(bg2010)


acs_columns_use <- c("area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre", 
       "median_income", "median_income_2022dollars", "pct_minority")                   

# ACS Differences
## Pct Difference
acs2010_pct_diff2005 <- cbind(acs2010[1], ((acs2010[acs_columns_use] - acs2005[match(acs2010$bg2010, acs2005$bg2010), acs_columns_use]) / acs2005[match(acs2010$bg2010, acs2005$bg2010), acs_columns_use]))
acs2015_pct_diff2010 <- cbind(acs2015[1], ((acs2015[acs_columns_use] - acs2010[match(acs2015$bg2010, acs2010$bg2010), acs_columns_use]) / acs2010[match(acs2015$bg2010, acs2010$bg2010), acs_columns_use]))
acs2020_pct_diff2015 <- cbind(acs2020[1], ((acs2020[acs_columns_use] - acs2015[match(acs2020$bg2010, acs2015$bg2010), acs_columns_use]) / acs2015[match(acs2020$bg2010, acs2015$bg2010), acs_columns_use]))

## Absolute Difference
acs2010_diff2005 <- cbind(acs2010[1], ((acs2010[acs_columns_use] - acs2005[match(acs2010$bg2010, acs2005$bg2010), acs_columns_use])))
acs2015_diff2010 <- cbind(acs2015[1], ((acs2015[acs_columns_use] - acs2010[match(acs2015$bg2010, acs2010$bg2010), acs_columns_use])))
acs2020_diff2015 <- cbind(acs2020[1], ((acs2020[acs_columns_use] - acs2015[match(acs2020$bg2010, acs2015$bg2010), acs_columns_use])))

## Clean Pct Difference
acs2010_pct_diff2005 <- acs2010_pct_diff2005 %>%
  mutate(hh_den_acre_change = case_when(hh_den_acre < -0.05 ~ "<-5%",
                                        hh_den_acre >= -0.05 & hh_den_acre < 0.1 ~ "-5% to <10%",
                                        hh_den_acre >= 0.1 & hh_den_acre < 0.25 ~ "10% to <25%",
                                        hh_den_acre >= 0.25 & hh_den_acre < 0.50 ~ "25% to <50%",
                                        hh_den_acre >= 0.5 ~ ">=50%"))
acs2010_pct_diff2005$hh_den_acre_change <- factor(acs2010_pct_diff2005$hh_den_acre_change, levels = c("<-5%", "-5% to <10%", "10% to <25%", "25% to <50%", ">=50%"))

acs2015_pct_diff2010 <- acs2015_pct_diff2010 %>%
  mutate(hh_den_acre_change = case_when(hh_den_acre < -0.05 ~ "<-5%",
                                        hh_den_acre >= -0.05 & hh_den_acre < 0.1 ~ "-5% to <10%",
                                        hh_den_acre >= 0.1 & hh_den_acre < 0.25 ~ "10% to <25%",
                                        hh_den_acre >= 0.25 & hh_den_acre < 0.50 ~ "25% to <50%",
                                        hh_den_acre >= 0.5 ~ ">=50%"))
acs2015_pct_diff2010$hh_den_acre_change <- factor(acs2015_pct_diff2010$hh_den_acre_change, levels = c("<-5%", "-5% to <10%", "10% to <25%", "25% to <50%", ">=50%"))


acs2020_pct_diff2015 <- acs2020_pct_diff2015 %>%
  mutate(hh_den_acre_change = case_when(hh_den_acre < -0.05 ~ "<-5%",
                                        hh_den_acre >= -0.05 & hh_den_acre < 0.1 ~ "-5% to <10%",
                                        hh_den_acre >= 0.1 & hh_den_acre < 0.25 ~ "10% to <25%",
                                        hh_den_acre >= 0.25 & hh_den_acre < 0.50 ~ "25% to <50%",
                                        hh_den_acre >= 0.5 ~ ">=50%"))
acs2020_pct_diff2015$hh_den_acre_change <- factor(acs2020_pct_diff2015$hh_den_acre_change, levels = c("<-5%", "-5% to <10%", "10% to <25%", "25% to <50%", ">=50%"))

## Clean Absolute Difference
acs2010_diff2005 <- acs2010_diff2005 %>%
  mutate(hh_den_acre_change = case_when(hh_den_acre < -0.10 ~ "<-0.10",
                                        hh_den_acre >= -0.10 & hh_den_acre < 0 ~ "-0.10 to 0",
                                        hh_den_acre >= 0 & hh_den_acre < 0.1 ~ "0 to 0.1",
                                        hh_den_acre >= 0.1 & hh_den_acre < 0.25 ~ "0 to 0.25", 
                                        hh_den_acre > 0.25 ~  ">0.25"))
acs2010_diff2005$hh_den_acre_change <- factor(acs2010_diff2005$hh_den_acre_change, levels = c("<-0.10", "-0.10 to 0", "0 to 0.1", "0 to 0.25", ">0.25"))


acs2015_diff2010 <- acs2015_diff2010 %>%
  mutate(hh_den_acre_change = case_when(hh_den_acre < -0.10 ~ "<-0.10",
                                        hh_den_acre >= -0.10 & hh_den_acre < 0 ~ "-0.10 to 0",
                                        hh_den_acre >= 0 & hh_den_acre < 0.1 ~ "0 to 0.1",
                                        hh_den_acre >= 0.1 & hh_den_acre < 0.25 ~ "0 to 0.25", 
                                        hh_den_acre > 0.25 ~  ">0.25"))
acs2015_diff2010$hh_den_acre_change <- factor(acs2015_diff2010$hh_den_acre_change, levels = c("<-0.10", "-0.10 to 0", "0 to 0.1", "0 to 0.25", ">0.25"))


acs2020_diff2015 <- acs2020_diff2015 %>%
  mutate(hh_den_acre_change = case_when(hh_den_acre < -0.10 ~ "<-0.10",
                                        hh_den_acre >= -0.10 & hh_den_acre < 0 ~ "-0.10 to 0",
                                        hh_den_acre >= 0 & hh_den_acre < 0.1 ~ "0 to 0.1",
                                        hh_den_acre >= 0.1 & hh_den_acre < 0.25 ~ "0 to 0.25", 
                                        hh_den_acre > 0.25 ~  ">0.25"))
acs2020_diff2015$hh_den_acre_change <- factor(acs2020_diff2015$hh_den_acre_change, levels = c("<-0.10", "-0.10 to 0", "0 to 0.1", "0 to 0.25", ">0.25"))


# LEHD Differences
lehd_columns_use <- c("emp_den_acre", "emp_entropy",  "emp_hh_entropy",   "emp_hh_den_acre",  "emp_pop_den_acre")  

lehd2010_pct_diff2005 <- cbind(lehd2010[1], ((lehd2010[lehd_columns_use] - lehd2005[match(lehd2010$bg2010, lehd2005$bg2010), lehd_columns_use]) / lehd2005[match(lehd2010$bg2010, lehd2005$bg2010), lehd_columns_use]))
lehd2015_pct_diff2010 <- cbind(lehd2015[1], ((lehd2015[lehd_columns_use] - lehd2010[match(lehd2015$bg2010, lehd2010$bg2010), lehd_columns_use]) / lehd2010[match(lehd2015$bg2010, lehd2010$bg2010), lehd_columns_use]))
lehd2020_pct_diff2015 <- cbind(lehd2020[1], ((lehd2020[lehd_columns_use] - lehd2015[match(lehd2020$bg2010, lehd2015$bg2010), lehd_columns_use]) / lehd2015[match(lehd2020$bg2010, lehd2015$bg2010), lehd_columns_use]))

lehd2010_diff2005 <- cbind(lehd2010[1], ((lehd2010[lehd_columns_use] - lehd2005[match(lehd2010$bg2010, lehd2005$bg2010), lehd_columns_use])))
lehd2015_diff2010 <- cbind(lehd2015[1], ((lehd2015[lehd_columns_use] - lehd2010[match(lehd2015$bg2010, lehd2010$bg2010), lehd_columns_use])))
lehd2020_diff2015 <- cbind(lehd2020[1], ((lehd2020[lehd_columns_use] - lehd2015[match(lehd2020$bg2010, lehd2015$bg2010), lehd_columns_use])))

lehd2010_pct_diff2005 <- lehd2010_pct_diff2005 %>%
  mutate(emp_den_acre_change = case_when(emp_den_acre < -0.05 ~ "<-5%",
                                         emp_den_acre >= -0.05 & emp_den_acre < 0.1 ~ "-5% to <10%",
                                         emp_den_acre >= 0.1 & emp_den_acre < 0.25 ~ "10% to <25%",
                                         emp_den_acre >= 0.25 & emp_den_acre < 0.50 ~ "25% to <50%",
                                         emp_den_acre >= 0.5 ~ ">=50%"))

lehd2015_pct_diff2010 <- lehd2015_pct_diff2010 %>%
  mutate(emp_den_acre_change = case_when(emp_den_acre < -0.05 ~ "<-5%",
                                         emp_den_acre >= -0.05 & emp_den_acre < 0.1 ~ "-5% to <10%",
                                         emp_den_acre >= 0.1 & emp_den_acre < 0.25 ~ "10% to <25%",
                                         emp_den_acre >= 0.25 & emp_den_acre < 0.50 ~ "25% to <50%",
                                         emp_den_acre >= 0.5 ~ ">=50%"))

lehd2020_pct_diff2015 <- lehd2020_pct_diff2015 %>%
  mutate(emp_den_acre_change = case_when(emp_den_acre < -0.05 ~ "<-5%",
                                         emp_den_acre >= -0.05 & emp_den_acre < 0.1 ~ "-5% to <10%",
                                         emp_den_acre >= 0.1 & emp_den_acre < 0.25 ~ "10% to <25%",
                                         emp_den_acre >= 0.25 & emp_den_acre < 0.50 ~ "25% to <50%",
                                         emp_den_acre >= 0.5 ~ ">=50%"))


lehd2010_pct_diff2005$emp_den_acre_change <- factor(lehd2010_pct_diff2005$emp_den_acre_change, levels = c("<-5%", "-5% to <10%", "10% to <25%", "25% to <50%", ">=50%"))
lehd2015_pct_diff2010$emp_den_acre_change <- factor(lehd2015_pct_diff2010$emp_den_acre_change, levels = c("<-5%", "-5% to <10%", "10% to <25%", "25% to <50%", ">=50%"))
lehd2020_pct_diff2015$emp_den_acre_change <- factor(lehd2020_pct_diff2015$emp_den_acre_change, levels = c("<-5%", "-5% to <10%", "10% to <25%", "25% to <50%", ">=50%"))

lehd2010_diff2005 <- lehd2010_diff2005 %>%
  mutate(emp_den_acre_change = case_when(emp_den_acre < -0.10 ~ "<-0.10",
                                         emp_den_acre >= -0.10 & emp_den_acre < 0 ~ "-0.10 to 0",
                                         emp_den_acre >= 0 & emp_den_acre < 0.1 ~ "0 to 0.1",
                                         emp_den_acre >= 0.1 & emp_den_acre < 0.25 ~ "0 to 0.25", 
                                         emp_den_acre > 0.25 ~  ">0.25"))
lehd2010_diff2005$emp_den_acre_change <- factor(lehd2010_diff2005$emp_den_acre_change, levels = c("<-0.10", "-0.10 to 0", "0 to 0.1", "0 to 0.25", ">0.25"))


lehd2015_diff2010 <- lehd2015_diff2010 %>%
  mutate(emp_den_acre_change = case_when(emp_den_acre < -0.10 ~ "<-0.10",
                                         emp_den_acre >= -0.10 & emp_den_acre < 0 ~ "-0.10 to 0",
                                         emp_den_acre >= 0 & emp_den_acre < 0.1 ~ "0 to 0.1",
                                         emp_den_acre >= 0.1 & emp_den_acre < 0.25 ~ "0 to 0.25", 
                                         emp_den_acre > 0.25 ~  ">0.25"))
lehd2015_diff2010$emp_den_acre_change <- factor(lehd2015_diff2010$emp_den_acre_change, levels = c("<-0.10", "-0.10 to 0", "0 to 0.1", "0 to 0.25", ">0.25"))


lehd2020_diff2015 <- lehd2020_diff2015 %>%
  mutate(emp_den_acre_change = case_when(emp_den_acre < -0.10 ~ "<-0.10",
                                         emp_den_acre >= -0.10 & emp_den_acre < 0 ~ "-0.10 to 0",
                                         emp_den_acre >= 0 & emp_den_acre < 0.1 ~ "0 to 0.1",
                                         emp_den_acre >= 0.1 & emp_den_acre < 0.25 ~ "0 to 0.25", 
                                         emp_den_acre > 0.25 ~  ">0.25"))
lehd2020_diff2015$emp_den_acre_change <- factor(lehd2020_diff2015$emp_den_acre_change, levels = c("<-0.10", "-0.10 to 0", "0 to 0.1", "0 to 0.25", ">0.25"))


# Discrete Plot Function --------------------------------------------------
ext_data_plot <- function(ext_df, metric, bg_df, state_abb, county_name){
  if(missing(county_name)){
    ext_df <- bg_df %>%
      left_join(ext_df) %>%
      filter(state %in% state_abb)
    metric <- as.name(metric)
    ggplot(ext_df) + 
      geom_sf(mapping = aes(fill = get(metric)), color = "black", linetype = 1, lwd = 0.1) +
      theme_void() + 
      ggtitle(paste0(state_abb, " ", unique(ext_df$year), ":", " ", metric)) +
      guides(fill=guide_legend(metric)) +
      scale_color_manual(values = color_vec)
    ggsave(paste0("C:/Users/nhbe224/OneDrive - University of Kentucky/RA Tasks/Census Work/map_checks/", state_abb, unique(ext_df$year), metric, ".jpg"), width = 8, height = 6, units = "in")
    ggsave(paste0("./outputs/", state_abb, unique(ext_df$year), metric, ".jpg"), width = 8, height = 6, units = "in")
  }else {
    ext_df <- bg_df %>%
      left_join(ext_df) %>%
      filter(state %in% state_abb) %>%
      mutate(state_county_fips = substr(bg2010, 1, 5)) %>%
      left_join(counties, by = c("state_county_fips", "state")) %>%
      filter(county %in% county_name)
    metric <- as.name(metric)
    ggplot(ext_df) + 
      geom_sf(mapping = aes(fill = get(metric)), color = "black", linetype = 1, lwd = 0.1) +
      theme_void() + 
      ggtitle(paste0(county_name, ", ", state_abb, " ", unique(ext_df$year), ":", " ", metric)) +
      guides(fill=guide_legend(metric)) +
      scale_fill_manual(values = color_vec)
    ggsave(paste0("C:/Users/nhbe224/OneDrive - University of Kentucky/RA Tasks/Census Work/map_checks/", county_name, state_abb, unique(ext_df$year), metric, ".jpg"), width = 8, height = 6, units = "in")
    ggsave(paste0("./outputs/", county_name, state_abb, unique(ext_df$year), metric, ".jpg"), width = 8, height = 6, units = "in")
  }
}

# Absolute Change Plot Function -----------------------------------------
abs_change_plot <- function(ext_df, metric, bg_df, state_abb, county_name, year_old, year_new){
  if(missing(county_name)){
    ext_df <- bg_df %>%
      left_join(ext_df) %>%
      filter(state %in% state_abb)
    metric <- as.name(metric)
    ggplot(ext_df) + 
      geom_sf(mapping = aes(fill = get(metric)), color = "black", linetype = 1, lwd = 0.1) +
      theme_void() + 
      ggtitle(paste0(state_abb, " Absolute Change From ", year_old, " to ", year_new, ":", " ", metric)) +
      scale_fill_manual(values = color_vec2, na.value = "white")
    ggsave(paste0("C:/Users/nhbe224/OneDrive - University of Kentucky/RA Tasks/Census Work/map_checks/", state_abb, unique(ext_df$year), metric, "_abs_",  year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
    ggsave(paste0("./outputs/", state_abb, unique(ext_df$year), metric, year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
  }else {
    ext_df <- bg_df %>%
      left_join(ext_df) %>%
      filter(state %in% state_abb) %>%
      mutate(state_county_fips = substr(bg2010, 1, 5)) %>%
      left_join(counties, by = c("state_county_fips", "state")) %>%
      filter(county %in% county_name)
    metric <- as.name(metric)
    ggplot(ext_df) + 
      geom_sf(mapping = aes(fill = get(metric)), color = "black", linetype = 1, lwd = 0.1) +
      theme_void() + 
      ggtitle(paste0(county_name, ", ", state_abb, " Absolute Change From ", year_old, " to ", year_new, ":", " ", metric)) +
      scale_fill_manual(values = color_vec2, na.value = "white")
    ggsave(paste0("C:/Users/nhbe224/OneDrive - University of Kentucky/RA Tasks/Census Work/map_checks/", county_name, state_abb, unique(ext_df$year), metric, "_abs_", year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
    ggsave(paste0("./outputs/", county_name, state_abb, unique(ext_df$year), metric, year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
  }
}

# Pct. Change Plot Function -----------------------------------------
pct_change_plot <- function(ext_df, metric, bg_df, state_abb, county_name, year_old, year_new){
  if(missing(county_name)){
    ext_df <- bg_df %>%
      left_join(ext_df) %>%
      filter(state %in% state_abb)
    metric <- as.name(metric)
    ggplot(ext_df) + 
      geom_sf(mapping = aes(fill = get(metric)), color = "black", linetype = 1, lwd = 0.1) +
      theme_void() + 
      ggtitle(paste0(state_abb, " Percent Change From ", year_old, " to ", year_new, ":", " ", metric)) +
      scale_fill_manual(values = color_vec2, na.value = "white")
    ggsave(paste0("C:/Users/nhbe224/OneDrive - University of Kentucky/RA Tasks/Census Work/map_checks/", state_abb, unique(ext_df$year), metric, "_pct_", year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
    ggsave(paste0("./outputs/", state_abb, unique(ext_df$year), metric, "_pct_", year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
  }else {
    ext_df <- bg_df %>%
      left_join(ext_df) %>%
      filter(state %in% state_abb) %>%
      mutate(state_county_fips = substr(bg2010, 1, 5)) %>%
      left_join(counties, by = c("state_county_fips", "state")) %>%
      filter(county %in% county_name)
    metric <- as.name(metric)
    ggplot(ext_df) + 
      geom_sf(mapping = aes(fill = get(metric)), color = "black", linetype = 1, lwd = 0.1) +
      theme_void() + 
      ggtitle(paste0(county_name, ", ", state_abb, " Percent Change From ", year_old, " to ", year_new, ":", " ", metric)) +
      scale_fill_manual(values = color_vec2, na.value = "white")
    ggsave(paste0("C:/Users/nhbe224/OneDrive - University of Kentucky/RA Tasks/Census Work/map_checks/", county_name, state_abb, unique(ext_df$year), metric, "_pct_", year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
    ggsave(paste0("./outputs/", county_name, state_abb, unique(ext_df$year), metric, "_pct_", year_old, "vs", year_new, ".jpg"), width = 8, height = 6, units = "in")
  }
}


# Write out plots ---------------------------------------------------------
## Median income ----------------------------------------------------------
for(i in 1:(length(states_list))){
  print(states_list[i])
  ext_data_plot(ext_df = acsData[acsData$year == 2005, ], metric = "income_group_2022dollars", bg_df = bg2010, state = states_list[i])
}

for(i in 1:(length(states_list))){
  print(states_list[i])
  ext_data_plot(ext_df = acsData[acsData$year == 2010, ], metric = "income_group_2022dollars", bg_df = bg2010, state = states_list[i])
}

for(i in 1:(length(states_list))){
  print(states_list[i])
  ext_data_plot(ext_df = acsData[acsData$year == 2015, ], metric = "income_group_2022dollars", bg_df = bg2010, state = states_list[i])
}

for(i in 1:(length(states_list))){
  print(states_list[i])
  ext_data_plot(ext_df = acsData[acsData$year == 2020, ], metric = "income_group_2022dollars", bg_df = bg2010, state = states_list[i])
}

## Lexington Median Income ------------------------------------------------
ext_data_plot(ext_df = acs2005, metric = "income_group_2022dollars", bg_df = bg2010, "KY", county_name = "Fayette County")
ext_data_plot(ext_df = acs2010, metric = "income_group_2022dollars", bg_df = bg2010, "KY", county_name = "Fayette County")
ext_data_plot(ext_df = acs2015, metric = "income_group_2022dollars", bg_df = bg2010, "KY", county_name = "Fayette County")
ext_data_plot(ext_df = acs2020, metric = "income_group_2022dollars", bg_df = bg2010, "KY", county_name = "Fayette County")

## Indianapolis Median Income ------------------------------------------------
ext_data_plot(ext_df = acs2005, metric = "income_group_2022dollars", bg_df = bg2010, "IN", county_name = "Marion County")
ext_data_plot(ext_df = acs2010, metric = "income_group_2022dollars", bg_df = bg2010, "IN", county_name = "Marion County")
ext_data_plot(ext_df = acs2015, metric = "income_group_2022dollars", bg_df = bg2010, "IN", county_name = "Marion County")
ext_data_plot(ext_df = acs2020, metric = "income_group_2022dollars", bg_df = bg2010, "IN", county_name = "Marion County")


## San Francisco Median Income ------------------------------------------------
ext_data_plot(ext_df = acs2005, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Francisco County")
ext_data_plot(ext_df = acs2010, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Francisco County")
ext_data_plot(ext_df = acs2015, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Francisco County")
ext_data_plot(ext_df = acs2020, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Francisco County")

## San Diego Median Income ------------------------------------------------
ext_data_plot(ext_df = acs2005, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Diego County")
ext_data_plot(ext_df = acs2010, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Diego County")
ext_data_plot(ext_df = acs2015, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Diego County")
ext_data_plot(ext_df = acs2020, metric = "income_group_2022dollars", bg_df = bg2010, "CA", county_name = "San Diego County")

## DC Median Income ------------------------------------------------
ext_data_plot(ext_df = acs2005, metric = "income_group_2022dollars", bg_df = bg2010, "DC")
ext_data_plot(ext_df = acs2010, metric = "income_group_2022dollars", bg_df = bg2010, "DC")
ext_data_plot(ext_df = acs2015, metric = "income_group_2022dollars", bg_df = bg2010, "DC")
ext_data_plot(ext_df = acs2020, metric = "income_group_2022dollars", bg_df = bg2010, "DC")


## Pct Minority ------------------------------------------------------------
for(i in 1:length(states_list)){
  print(states_list[i])
  ext_data_plot(ext_df = acs2010, metric = "pct_minority_group", bg_df = bg2010, state = states_list[i])
}

for(i in 1:length(states_list)){
  print(states_list[i])
  ext_data_plot(ext_df = acs2010, metric = "pct_minority_group", bg_df = bg2010, state = states_list[i])
}

for(i in 1:length(states_list)){
  print(states_list[i])
  ext_data_plot(ext_df = acs2015, metric = "pct_minority_group", bg_df = bg2010, state = states_list[i])
}

for(i in 1:length(states_list)){
  print(states_list[i])
  ext_data_plot(ext_df = acs2020, metric = "pct_minority_group", bg_df = bg2010, state = states_list[i])
}


ext_data_plot(ext_df = acs2005, metric = "pct_minority_group", bg_df = bg2010, "KY", county_name = "Fayette County")
ext_data_plot(ext_df = acs2010, metric = "pct_minority_group", bg_df = bg2010, "KY", county_name = "Fayette County")
ext_data_plot(ext_df = acs2015, metric = "pct_minority_group", bg_df = bg2010, "KY", county_name = "Fayette County")
ext_data_plot(ext_df = acs2020, metric = "pct_minority_group", bg_df = bg2010, "KY", county_name = "Fayette County")

ext_data_plot(ext_df = acs2005, metric = "pct_minority_group", bg_df = bg2010, "DC")
ext_data_plot(ext_df = acs2010, metric = "pct_minority_group", bg_df = bg2010, "DC")
ext_data_plot(ext_df = acs2015, metric = "pct_minority_group", bg_df = bg2010, "DC")
ext_data_plot(ext_df = acs2020, metric = "pct_minority_group", bg_df = bg2010, "DC")


# Transit Jobs Access -----------------------------------------------------
ext_data_plot(ext_df = transit2014, metric = "jobs", bg_df = bg2010, "VA", county_name = "Arlington County")
ext_data_plot(ext_df = transit2017, metric = "jobs", bg_df = bg2010, "VA", county_name = "Arlington County")
ext_data_plot(ext_df = transit2022, metric = "jobs", bg_df = bg2010, "VA", county_name = "Arlington County")

ext_data_plot(ext_df = transit2014, metric = "jobs", bg_df = bg2010, "VA", county_name = "Fairfax County")
ext_data_plot(ext_df = transit2017, metric = "jobs", bg_df = bg2010, "VA", county_name = "Fairfax County")
ext_data_plot(ext_df = transit2022, metric = "jobs", bg_df = bg2010, "VA", county_name = "Fairfax County")

ext_data_plot(ext_df = transit2014, metric = "jobs", bg_df = bg2010, "MN", county_name = "Hennepin County")
ext_data_plot(ext_df = transit2017, metric = "jobs", bg_df = bg2010, "MN", county_name = "Hennepin County")
ext_data_plot(ext_df = transit2022, metric = "jobs", bg_df = bg2010, "MN", county_name = "Hennepin County")

ext_data_plot(ext_df = transit2014, metric = "jobs", bg_df = bg2010, "CO", county_name = "Denver County")
ext_data_plot(ext_df = transit2017, metric = "jobs", bg_df = bg2010, "CO", county_name = "Denver County")
ext_data_plot(ext_df = transit2022, metric = "jobs", bg_df = bg2010, "CO", county_name = "Denver County")

# Absolute Change Plots ----------------------------------------------------
abs_change_plot(ext_df = lehd2010_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = lehd2015_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = lehd2020_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = acs2010_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
            state_abb ="KY", county_name = "Fayette County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = acs2015_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
            state_abb ="KY", county_name = "Fayette County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = acs2020_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
            state_abb ="KY", county_name = "Fayette County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = lehd2010_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = lehd2015_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = lehd2020_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = acs2010_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = acs2015_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = acs2020_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = lehd2010_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = lehd2015_abs_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = lehd2020_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = acs2010_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = acs2015_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = acs2020_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = lehd2010_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = lehd2015_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = lehd2020_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = acs2010_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = acs2015_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = acs2020_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2015", year_new = "2020")


abs_change_plot(ext_df = lehd2010_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = lehd2015_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = lehd2020_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2015", year_new = "2020")

abs_change_plot(ext_df = acs2010_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old = "2005", year_new = "2010")
abs_change_plot(ext_df = acs2015_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2010", year_new = "2015")
abs_change_plot(ext_df = acs2020_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2015", year_new = "2020")

# Percent Change Plot -----------------------------------------------------
pct_change_plot(ext_df = lehd2010_pct_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = lehd2015_pct_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = lehd2020_pct_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = acs2010_pct_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = acs2015_pct_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = acs2020_pct_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="KY", county_name = "Fayette County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = lehd2010_pct_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = lehd2015_pct_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = lehd2020_pct_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = acs2010_pct_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = acs2015_pct_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = acs2020_pct_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="IL", county_name = "Cook County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = lehd2010_pct_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = lehd2015_pct_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = lehd2020_pct_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = acs2010_pct_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = acs2015_pct_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = acs2020_pct_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "San Diego County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = lehd2010_pct_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = lehd2015_pct_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = lehd2020_pct_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = acs2010_pct_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = acs2015_pct_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = acs2020_pct_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Riverside County", year_old =  "2015", year_new = "2020")


pct_change_plot(ext_df = lehd2010_pct_diff2005, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = lehd2015_pct_diff2010, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = lehd2020_pct_diff2015, metric = "emp_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2015", year_new = "2020")

pct_change_plot(ext_df = acs2010_pct_diff2005, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old = "2005", year_new = "2010")
pct_change_plot(ext_df = acs2015_pct_diff2010, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2010", year_new = "2015")
pct_change_plot(ext_df = acs2020_pct_diff2015, metric = "hh_den_acre_change", bg_df = bg2010, 
                state_abb ="CA", county_name = "Alameda County", year_old =  "2015", year_new = "2020")

# Summit at Fritz Farm ----------------------------------------------------
summit_fritz_acs <- acsData %>% filter(bg2010 == "210670041031")
summit_fritz_lehd <- lehdData %>% filter(bg2010 == "210670041031")
summit_fritz_lehd_change <- lehd2020_pct_diff2015 %>% filter(bg2010 == "210670041031")
summit_fritz_acs_change <- acs2020_pct_diff2015 %>% filter(bg2010 == "210670041031")

