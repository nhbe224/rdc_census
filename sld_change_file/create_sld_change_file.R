### Smart Location Database Change File

# Set Working Directory ---------------------------------------------------
setwd("D:/neeco/rdc_census/sld_change_file/")

# Install Packages --------------------------------------------------------
library(tidyverse)
library(data.table)
library(sf)
library(foreach)
library(tigris)
library(foreign)

# Read in Data ------------------------------------------------------------
sld_v2_load <- read.dbf("./inputs/sld_v2/SmartLocationDb.dbf", as.is = F)
sld_v3_load <- st_read(dsn = "./inputs/sld_v3/SmartLocationDatabase.gdb")


# Clean Data --------------------------------------------------------------
## Select relevant variables from each
sld_v2 <- sld_v2_load %>%
  select(GEOID10, TOTPOP10, EMPTOT, D1A, D1B, D1C, ### Density
         D2A_JPHH, D2B_E8MIX, D2A_EPHHM, ### Employment
         D3b, D3aao, ### Urban Design
         D4c, D4d, D4a, ### Transit
         D5br, D5be, D5ar, D5ae, ## Employment Accessibility
         PCT_AO0, PCT_AO1, PCT_AO2P, ## Pct cars in household
         WORKERS, R_LOWWAGEW, R_MEDWAGEW, R_HIWAGEWK, E_LOWWAGEW, E_MEDWAGEW, E_HIWAGEWK, ## Income
         E5_RET10, E5_OFF10, E5_IND10, E5_SVC10, E5_ENT10, ## Tier 5 Employment
         E8_RET10, E8_OFF10, E8_IND10, E8_SVC10, E8_ENT10, E8_ED10, E8_HLTH10, E8_PUB10) %>% ## Tier 8 Employment
  mutate(year = 2010)

sld_v3 <- sld_v3_load %>%
  select(GEOID10, TotPop, TotEmp, D1A, D1B, D1C, ### Density
         D2A_JPHH, D2B_E8MIX, D2A_EPHHM, ### Employment
         D3B, D3AAO, ### Urban Design
         D4C, D4D, D4A, ### Transit
         D5BR, D5BE, D5AR, D5AE, ## Employment Accessibility
         Pct_AO0, Pct_AO1, Pct_AO2p, ## Pct cars in household
         Workers, R_LowWageWk, R_MedWageWk, R_HiWageWk, E_LowWageWk, E_MedWageWk, E_HiWageWk, ## Income
         E5_Ret, E5_Off, E5_Ind, E5_Svc, E5_Ent, ## Tier 5 Employment
         E8_Ret, E8_off, E8_Ind, E8_Svc, E8_Ent, E8_Ed, E8_Hlth, E8_Pub) %>% ## Tier 8 Employment
  mutate(year = 2018)

## Drop geometry column
sld_v3 <- st_drop_geometry(sld_v3)

## Rename columns 
sld_colnames <- c("geoid", "population", "employment", "res_den", "pop_den", "emp_den",
                  "jobs_per_hh", "emp8_ent", "emp_hh_ent",
                  "intersection_den", "auto_only_den",
                  "transit_freq", "transit_den", "transit_dist", 
                  "transit_jobs45", "transit_jobs45_working_age", "auto_jobs45", "auto_jobs45_working_age",
                  "pct_hh_0car", "pct_hh_1car", "pct_hh_2pcar",
                  "workers", "r_lowwagewk", "r_medwagewk", "r_hiwagewk", "e_lowwagewk", "e_medwagewk", "e_hiwagewk",
                  "e5ret", "e5off", "e5ind", "e5svc", "e5ent",
                  "e8ret", "e8off", "e8ind", "e8svc", "e8ent", "e8ed", "e8hlth", "e8pub", "year")

colnames(sld_v2) <- sld_colnames
colnames(sld_v3) <- sld_colnames


## Join together
sld_change_file <- rbind(sld_v2, sld_v3)

## Get percentages for r_wages and e_wages
sld_change_file <- sld_change_file %>%
  mutate(r_lowwagewk_pct = r_lowwagewk / population,
         r_medwagewk_pct = r_medwagewk / population,
         r_hiwagewk_pct = r_hiwagewk / population,
         e_lowwagewk_pct = e_lowwagewk / employment,
         e_medwagewk_pct = e_medwagewk / employment,
         e_hiwagewk_pct = e_hiwagewk / employment) %>%
  select(-r_lowwagewk, -r_medwagewk, -r_hiwagewk, -e_lowwagewk, -e_medwagewk, -e_hiwagewk)

## Get SLD V2 and SLD V3 separately
sld_v2 <- sld_change_file %>%
  filter(year == 2010)

sld_v3 <- sld_change_file %>%
  filter(year == 2018)


# Examine San Diego -------------------------------------------------------
san_diego_fips <- "06073"

san_diego_2010 <- sld_v2 %>%
  filter(substr(geoid, 1, 5) == san_diego_fips & year == 2010)

san_diego_2018 <- sld_v3 %>%
  filter(substr(geoid, 1, 5) == san_diego_fips & year == 2018)


san_diego_bgs <- block_groups("California", "San Diego", year = 2010)

san_diego_2010 <- san_diego_bgs %>% 
  left_join(san_diego_2010, by = c("GEOID10" = "geoid")) %>%
  filter(r_lowwagewk_pct <= 1)

san_diego_2018 <- san_diego_bgs %>% 
  left_join(san_diego_2018, by = c("GEOID10" = "geoid")) %>%
  filter(r_lowwagewk_pct <= 1)

san_diego_2010_transit <- san_diego_2010[san_diego_2010$transit_jobs45>=0, ]
san_diego_2018$transit_jobs45[san_diego_2018$transit_jobs45 == -99999] <- 0 #### -99999 is missing
  
## Choropleth Maps
plot(san_diego_2010[, "r_lowwagewk_pct"],
     breaks = "jenks",
     main = "San Diego County Low Wage Worker Percentage, Home Location (2010)")

plot(san_diego_2018[, "r_lowwagewk_pct"],
     breaks = "jenks",
     main = "San Diego County Low Wage Worker Percentage, Home Location (2018)")

plot(san_diego_2010_transit[, "transit_jobs45"],
     breaks = "jenks",
     main = "San Diego County Jobs within 45 minutes of Transit (2010)")

plot(san_diego_2018[, "transit_jobs45"],
     breaks = "jenks",
     main = "San Diego County Jobs within 45 minutes of Transit (2018)")

### Map of Differences between 2018 and 2010.
## DC silver line NOVA
## Look at variables highlighted in yellow in Public Data Inventory

# Write Out to CSV --------------------------------------------------------
write.csv(sld_v2, "./outputs/sld_change_file2010.csv", row.names = F)
write.csv(sld_v3, "./outputs/sld_change_file2018.csv", row.names = F)
write.csv(sld_change_file, "./outputs/sld_change_fileALL.csv", row.names = F)


