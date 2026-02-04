#### Travel Times for RDC Project
rm(list = ls())
setwd("D:/neeco/rdc_census/travel_times/")

# Load Packages -----------------------------------------------------------
library(foreign)
library(tidyverse)
library(data.table)
library(haven)

travel_times_load <- fread("./inputs/travel_times_load.csv")

travel_times_load <- travel_times_load %>% filter(miles >= 0 | minutes >=  0)
# Get other direction as well ---------------------------------------------
### These only have distances from A to B but not B to A. This fixes that.
travel_times_BA <- travel_times_load %>%
  rename(to_tract = from_tract, from_tract = to_tract) %>%
  select(from_tract, to_tract, miles, minutes)

travel_times_load <- rbind(travel_times_BA, travel_times_load)
rm(travel_times_BA)

travel_times_load <- data.table(travel_times_load)
gc()
# Account for Intrazonals -------------------------------------------------
### Drop duplicates
travel_times_load <- unique(travel_times_load)
gc()

# Group by origin and calculate the minimum non-zero distance for each group
travel_times_load[, min_dist_group := {
  # Get all non-zero distances within the group
  distances <- miles[miles > 0]
  # Calculate the minimum, use Inf if no non-zero distances exist
  ifelse(length(distances) > 0, min(distances), Inf)
}, by = from_tract]

travel_times_load[, min_minutes_group := {
  # Get all non-zero distances within the group
  minutes <- minutes[minutes > 0]
  # Calculate the minimum, use Inf if no non-zero distances exist
  ifelse(length(minutes) > 0, min(minutes), Inf)
}, by = from_tract]

# Update the travel_distance column in place using :=
# The condition is: if origin zone equals destination zone OR the current distance is 0
# replace it with half of the pre-calculated minimum distance for that origin group
travel_times_load[from_tract == to_tract | miles == 0, 
   miles := min_dist_group / 2]

travel_times_load[from_tract == to_tract | minutes == 0, 
   minutes := min_minutes_group / 2]

# Remove the temporary min_dist_group column
travel_times_load[, min_dist_group := NULL]
travel_times_load[, min_minutes_group := NULL]
### Check to make sure there are non-zero distances
travel_times_load %>%
  filter(miles == 0)

# # Create a sample data.table (replace with your actual data)
# dt <- data.table(
#   origin_zone = c("A", "A", "A", "B", "B", "B", "C", "C"),
#   destination_zone = c("A", "B", "C", "A", "B", "C", "B", "C"),
#   travel_distance = c(0, 0, 20, 10, 0, 50, 30, 0),
#   travel_minutes = c(0, 0, 15, 20, 0, 40, 40, 0)
# )
# 
# # Group by origin and calculate the minimum non-zero distance for each group
# dt[, min_dist_group := {
#   # Get all non-zero distances within the group
#   distances <- travel_distance[travel_distance > 0]
#   # Calculate the minimum, use Inf if no non-zero distances exist
#   ifelse(length(distances) > 0, min(distances), Inf)
# }, by = origin_zone]
# 
# dt[, min_minutes_group := {
#   # Get all non-zero distances within the group
#   minutes <- travel_minutes[travel_minutes > 0]
#   # Calculate the minimum, use Inf if no non-zero distances exist
#   ifelse(length(minutes) > 0, min(minutes), Inf)
# }, by = origin_zone]
# 
# # Update the travel_distance column in place using :=
# # The condition is: if origin zone equals destination zone OR the current distance is 0
# # replace it with half of the pre-calculated minimum distance for that origin group
# dt[origin_zone == destination_zone | travel_distance == 0, 
#    travel_distance := min_dist_group / 2]
# 
# dt[origin_zone == destination_zone | travel_minutes == 0, 
#    travel_minutes := min_minutes_group / 2]
# 
# # Remove the temporary min_dist_group column
# dt[, min_dist_group := NULL]
# dt[, min_minutes_group := NULL]

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

#write.dta(travel_times, "./outputs/travel_times.dta")
write.dta(travel_times, "../to_rdc/travel_times.dta")
rm(list = ls())

travel_times_dta <- setDT(haven::read_dta("../to_rdc/travel_times.dta"))
head(travel_times_dta)

travel_times_tracts <- travel_times_dta %>%
  select(home_tr2010, work_tr2010)
setDT(travel_times_tracts)
str(travel_times_tracts)
print(uniqueN(travel_times_tracts, by = c("home_tr2010", "work_tr2010")))
