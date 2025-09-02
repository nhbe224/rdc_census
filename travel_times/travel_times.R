#### Travel Times for RDC Project
setwd("D:/neeco/rdc_census/travel_times/")


# Load Packages -----------------------------------------------------------
library(foreign)
library(tidyverse)
library(data.table)

travel_times_load <- fread("./inputs/travel_times_load.csv", nrows = 10)


# Write to DTA ------------------------------------------------------------
head(travel_times_load)

travel_times_load$from_tract <- as.character(travel_times_load$from_tract)
travel_times_load$from_tract <- ifelse(nchar(travel_times_load$from_tract) == 10, paste0("0", travel_times_load$from_tract), travel_times_load$from_tract)

travel_times_load$to_tract <- as.character(travel_times_load$to_tract)
travel_times_load$to_tract <- ifelse(nchar(travel_times_load$to_tract) == 10, paste0("0", travel_times_load$to_tract), travel_times_load$to_tract)

head(travel_times_load)


travel_times <- travel_times_load %>%
  rename(home_tr2010 = from_tract,
         work_tr2010 = to_tract)

rm(travel_times_load)


# Check Duplicates --------------------------------------------------------
## Pairs
travel_times_tracts <- travel_times %>%
  select(home_tr2010, work_tr2010)
print(sum(duplicated(travel_times_tracts)))

write.dta(travel_times, "./outputs/travel_times.dta")
write.dta(travel_times, "../to_rdc/travel_times.dta")

head(travel_times)
