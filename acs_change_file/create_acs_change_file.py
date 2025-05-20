# Create ACS Change File
# This gives us Land Area, HH Density, Population Density, Percent of 0-Veh HHs, Median Income, and Percent Minority
# at the block group level by year, from 2005 to 2022.

# Load Libraries
import os
import glob
import numpy as np
import pandas as pd
import census
import us
import io
from census import Census
import geopandas as gpd
from us import states
from pathlib import Path
import pygris
import requests
import ipumspy
import zipfile
from ipumspy import IpumsApiClient, AggregateDataExtract, MicrodataExtract, Dataset, DatasetMetadata, readers, ddi
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Set working directory
os.chdir('D:/neeco/rdc_census/acs_change_file/')
intermediate_dir = 'D:/neeco/rdc_census/acs_change_file/intermediate/'

# Paramters and Other Functions
# ALAND takes 20 minutes for each year to read. What I do is run it once and export as a CSV.
# If you're running for the first time, set parameter below to true. Otherwise, set it to False.
aland_need_to_pull = False

def add_leading_zero(value):
	if len(value) == 11:
		return '0' + value
	else:
		return value

# Read in Data
api_key = "4049ee84e96e784c0042da45d81f95514d53b7fd"
c = Census(api_key)

## NHGIS API Key
os.environ["IPUMS_API_KEY"] = "59cba10d8a5da536fc06b59d23a1e21a315f492fa8cf28e6b302798b"
IPUMS_API_KEY = os.environ.get("IPUMS_API_KEY")
ipums = IpumsApiClient(IPUMS_API_KEY)
DOWNLOAD_DIR = Path("D:/neeco/rdc_census/acs_change_file/intermediate/")


# Get number of households and population by block group
# Define variables to retrieve (Total Population - B01003_001E)
# variables = ['B01003_001E', 'B03002_003E', 'B11001_001E'] # Total population, total white population, total households, median income

# Create Lookup for States
# Get dataframe of FIPS codes and states (50 states plus DC)
print("Getting FIPS codes...")
fips_lookup = pd.DataFrame([us.states.mapping('fips', 'abbr')]).T
fips_lookup['fips'] = fips_lookup.index
fips_lookup = fips_lookup.rename(columns={fips_lookup.columns[0]: 'state'})
fips_lookup.reset_index(drop=True, inplace=True)
fips_lookup

# Drop AS, GU, MP, PR, VI
fips_drop = ['AS', 'GU', 'MP', 'PR', 'VI']
fips_lookup = fips_lookup[~fips_lookup['state'].isin(fips_drop)]
fips_lookup

# Add DC
add_dc = pd.DataFrame({"state": ["DC"], "fips": ["11"]})
fips_lookup = pd.concat([fips_lookup, add_dc], ignore_index=True)
fips_lookup

# FIPS List
fips_list = list(fips_lookup["fips"])
fips_list

# States List
states_list = list(fips_lookup["state"])

# Start with total population
# 2012 uses IPUMS NHGIS API Python library
# 2013-2022 uses Census API Python library
print("Getting total population by Census block...")

# 2010
extract = AggregateDataExtract(
   collection="nhgis",
   datasets=[
      Dataset(name="2006_2010_ACS5a", data_tables=["B01003"], geog_levels=["blck_grp"])
   ]
)

ipums.submit_extract(extract)
ipums.wait_for_extract(extract)
ipums.download_extract(extract, download_dir=DOWNLOAD_DIR)

list_of_files = glob.glob(intermediate_dir + "*")
latest_file = max(list_of_files, key=os.path.getctime)
latest_file_no_zip = latest_file[:-4]

with zipfile.ZipFile(latest_file, 'r') as zip_ref:
	zip_ref.extractall(intermediate_dir)

os.rename(latest_file_no_zip, intermediate_dir + "pop2010")
csv_file = glob.glob(intermediate_dir + "pop2010/" "*.csv")
os.rename(csv_file[0], intermediate_dir + "pop2010/pop2010_load.csv")

pop2010_load = pd.read_csv(intermediate_dir + "pop2010/pop2010_load.csv")
pop2010_load = pop2010_load[["GEOID", "JMAE001", "STUSAB"]]
pop2010_load.columns = ["geoid", "population", "state"]
pop2010_load["geoid"] = pop2010_load["geoid"].str[-12:]
pop2010 = pop2010_load[pop2010_load['state'].isin(states_list)]
pop2010 = pop2010[["geoid", "population"]]
pop2010["year"] = 2010
print(pop2010.head())
print(pop2010.shape)

# 2011
extract = AggregateDataExtract(
   collection="nhgis",
   datasets=[
      Dataset(name="2007_2011_ACS5a", data_tables=["B01003"], geog_levels=["blck_grp"])
   ]
)

ipums.submit_extract(extract)
ipums.wait_for_extract(extract)
ipums.download_extract(extract, download_dir=DOWNLOAD_DIR)

list_of_files = glob.glob(intermediate_dir + "*")
latest_file = max(list_of_files, key=os.path.getctime)
latest_file_no_zip = latest_file[:-4]

with zipfile.ZipFile(latest_file, 'r') as zip_ref:
	zip_ref.extractall(intermediate_dir)

os.rename(latest_file_no_zip, intermediate_dir + "pop2011")
csv_file = glob.glob(intermediate_dir + "pop2011/" "*.csv")
os.rename(csv_file[0], intermediate_dir + "pop2011/pop2011_load.csv")

pop2011_load = pd.read_csv(intermediate_dir + "pop2011/pop2011_load.csv")
pop2011_load = pop2011_load[["GEOID", "MNTE001", "STUSAB"]]
pop2011_load.columns = ["geoid", "population", "state"]
pop2011_load["geoid"] = pop2011_load["geoid"].str[-12:]
pop2011 = pop2011_load[pop2011_load['state'].isin(states_list)]
pop2011 = pop2011[["geoid", "population"]]
pop2011["year"] = 2011
print(pop2011.head())
print(pop2011.shape)

# 2012
extract = AggregateDataExtract(
   collection="nhgis",
   datasets=[
      Dataset(name="2008_2012_ACS5a", data_tables=["B01003"], geog_levels=["blck_grp"])
   ]
)

ipums.submit_extract(extract)
ipums.wait_for_extract(extract)
ipums.download_extract(extract, download_dir=DOWNLOAD_DIR)

list_of_files = glob.glob(intermediate_dir + "*")
latest_file = max(list_of_files, key=os.path.getctime)
latest_file_no_zip = latest_file[:-4]

with zipfile.ZipFile(latest_file, 'r') as zip_ref:
	zip_ref.extractall(intermediate_dir)

os.rename(latest_file_no_zip, intermediate_dir + "pop2012")
csv_file = glob.glob(intermediate_dir + "pop2012/" "*.csv")
os.rename(csv_file[0], intermediate_dir + "pop2012/pop2012_load.csv")

pop2012_load = pd.read_csv(intermediate_dir + "pop2012/pop2012_load.csv")
pop2012_load = pop2012_load[["GEOID", "QSPE001", "STUSAB"]]
pop2012_load.columns = ["geoid", "population", "state"]
pop2012_load["geoid"] = pop2012_load["geoid"].str[-12:]
pop2012 = pop2012_load[pop2012_load['state'].isin(states_list)]
pop2012 = pop2012[["geoid", "population"]]
pop2012["year"] = 2012
print(pop2012.head())
print(pop2012.shape)

# 2013
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

pop2013 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2013["geoid"] = pop2013["state"] + pop2013["county"] + \
	pop2013["tract"] + pop2013["block group"]
# Rename population column
pop2013 = pop2013.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2013["year"] = 2013
# Keep relevant columns
pop2013 = pop2013.loc[:, ["geoid", "population", "year"]]
pop2013.head()
print(pop2013.shape)

# 2014
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

pop2014 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2014["geoid"] = pop2014["state"] + pop2014["county"] + \
	pop2014["tract"] + pop2014["block group"]
# Rename population column
pop2014 = pop2014.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2014["year"] = 2014
# Keep relevant columns
pop2014 = pop2014.loc[:, ["geoid", "population", "year"]]
pop2014.head()
print(pop2014.shape)

# 2015
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

pop2015 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2015["geoid"] = pop2015["state"] + pop2015["county"] + \
	pop2015["tract"] + pop2015["block group"]
# Rename population column
pop2015 = pop2015.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2015["year"] = 2015
# Keep relevant columns
pop2015 = pop2015.loc[:, ["geoid", "population", "year"]]
pop2015.head()
print(pop2015.shape)

# 2016
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

pop2016 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2016["geoid"] = pop2016["state"] + pop2016["county"] + \
	pop2016["tract"] + pop2016["block group"]
# Rename population column
pop2016 = pop2016.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2016["year"] = 2016
# Keep relevant columns
pop2016 = pop2016.loc[:, ["geoid", "population", "year"]]
pop2016.head()
print(pop2016.shape)

# 2017
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

pop2017 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2017["geoid"] = pop2017["state"] + pop2017["county"] + \
	pop2017["tract"] + pop2017["block group"]
# Rename population column
pop2017 = pop2017.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2017["year"] = 2017
# Keep relevant columns
pop2017 = pop2017.loc[:, ["geoid", "population", "year"]]
pop2017.head()
print(pop2017.shape)

# 2018
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

pop2018 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2018["geoid"] = pop2018["state"] + pop2018["county"] + \
	pop2018["tract"] + pop2018["block group"]
# Rename population column
pop2018 = pop2018.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2018["year"] = 2018
# Keep relevant columns
pop2018 = pop2018.loc[:, ["geoid", "population", "year"]]
pop2018.head()
print(pop2018.shape)

# 2019
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

pop2019 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2019["geoid"] = pop2019["state"] + pop2019["county"] + \
	pop2019["tract"] + pop2019["block group"]
# Rename population column
pop2019 = pop2019.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2019["year"] = 2019
# Keep relevant columns
pop2019 = pop2019.loc[:, ["geoid", "population", "year"]]
pop2019.head()
print(pop2019.shape)

# 2020
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2020)))

pop2020 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2020["geoid"] = pop2020["state"] + pop2020["county"] + \
	pop2020["tract"] + pop2020["block group"]
# Rename population column
pop2020 = pop2020.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2020["year"] = 2020
# Keep relevant columns
pop2020 = pop2020.loc[:, ["geoid", "population", "year"]]
pop2020.head()
print(pop2020.shape)

# 2021
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2021)))

pop2021 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2021["geoid"] = pop2021["state"] + pop2021["county"] + \
	pop2021["tract"] + pop2021["block group"]
# Rename population column
pop2021 = pop2021.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2021["year"] = 2021
# Keep relevant columns
pop2021 = pop2021.loc[:, ["geoid", "population", "year"]]
pop2021.head()
print(pop2021.shape)

# 2022
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2022)))

pop2022 = pd.concat(pop_by_state, ignore_index=True)

# Create geoid column
pop2022["geoid"] = pop2022["state"] + pop2022["county"] + \
	pop2022["tract"] + pop2022["block group"]
# Rename population column
pop2022 = pop2022.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2022["year"] = 2022
# Keep relevant columns
pop2022 = pop2022.loc[:, ["geoid", "population", "year"]]
pop2022.head()
print(pop2022.shape)

print("Done getting total population by Census block...")
print("Now getting non-Hispanic white population by Census block...")

# Total White Population (Non-Hispanic)
# 2013
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

pop_white2013 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2013["geoid"] = pop_white2013["state"] + pop_white2013["county"] + \
	pop_white2013["tract"] + pop_white2013["block group"]
# Rename pop_white column
pop_white2013 = pop_white2013.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2013["year"] = 2013
# Keep relevant columns
pop_white2013 = pop_white2013.loc[:, ["geoid", "pop_white", "year"]]
pop_white2013.head()
print(pop_white2013.shape)

# 2014
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

pop_white2014 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2014["geoid"] = pop_white2014["state"] + pop_white2014["county"] + \
	pop_white2014["tract"] + pop_white2014["block group"]
# Rename pop_white column
pop_white2014 = pop_white2014.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2014["year"] = 2014
# Keep relevant columns
pop_white2014 = pop_white2014.loc[:, ["geoid", "pop_white", "year"]]
pop_white2014.head()
print(pop_white2014.shape)

# 2015
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

pop_white2015 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2015["geoid"] = pop_white2015["state"] + pop_white2015["county"] + \
	pop_white2015["tract"] + pop_white2015["block group"]
# Rename pop_white column
pop_white2015 = pop_white2015.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2015["year"] = 2015
# Keep relevant columns
pop_white2015 = pop_white2015.loc[:, ["geoid", "pop_white", "year"]]
pop_white2015.head()
print(pop_white2015.shape)

# 2016
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

pop_white2016 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2016["geoid"] = pop_white2016["state"] + pop_white2016["county"] + \
	pop_white2016["tract"] + pop_white2016["block group"]
# Rename pop_white column
pop_white2016 = pop_white2016.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2016["year"] = 2016
# Keep relevant columns
pop_white2016 = pop_white2016.loc[:, ["geoid", "pop_white", "year"]]
pop_white2016.head()
print(pop_white2016.shape)

# 2017
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

pop_white2017 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2017["geoid"] = pop_white2017["state"] + pop_white2017["county"] + \
	pop_white2017["tract"] + pop_white2017["block group"]
# Rename pop_white column
pop_white2017 = pop_white2017.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2017["year"] = 2017
# Keep relevant columns
pop_white2017 = pop_white2017.loc[:, ["geoid", "pop_white", "year"]]
pop_white2017.head()
print(pop_white2017.shape)

# 2018
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

pop_white2018 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2018["geoid"] = pop_white2018["state"] + pop_white2018["county"] + \
	pop_white2018["tract"] + pop_white2018["block group"]
# Rename pop_white column
pop_white2018 = pop_white2018.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2018["year"] = 2018
# Keep relevant columns
pop_white2018 = pop_white2018.loc[:, ["geoid", "pop_white", "year"]]
pop_white2018.head()
print(pop_white2018.shape)

# 2019
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

pop_white2019 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2019["geoid"] = pop_white2019["state"] + pop_white2019["county"] + \
	pop_white2019["tract"] + pop_white2019["block group"]
# Rename pop_white column
pop_white2019 = pop_white2019.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2019["year"] = 2019
# Keep relevant columns
pop_white2019 = pop_white2019.loc[:, ["geoid", "pop_white", "year"]]
pop_white2019.head()
print(pop_white2019.shape)

# 2020
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2020)))

pop_white2020 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2020["geoid"] = pop_white2020["state"] + pop_white2020["county"] + \
	pop_white2020["tract"] + pop_white2020["block group"]
# Rename pop_white column
pop_white2020 = pop_white2020.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2020["year"] = 2020
# Keep relevant columns
pop_white2020 = pop_white2020.loc[:, ["geoid", "pop_white", "year"]]
pop_white2020.head()
print(pop_white2020.shape)

# 2021
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2021)))

pop_white2021 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2021["geoid"] = pop_white2021["state"] + pop_white2021["county"] + \
	pop_white2021["tract"] + pop_white2021["block group"]
# Rename pop_white column
pop_white2021 = pop_white2021.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2021["year"] = 2021
# Keep relevant columns
pop_white2021 = pop_white2021.loc[:, ["geoid", "pop_white", "year"]]
pop_white2021.head()
print(pop_white2021.shape)

# 2022
pop_white_by_state = []
for i in fips_list:
	pop_white_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2022)))

pop_white2022 = pd.concat(pop_white_by_state, ignore_index=True)

# Create geoid column
pop_white2022["geoid"] = pop_white2022["state"] + pop_white2022["county"] + \
	pop_white2022["tract"] + pop_white2022["block group"]
# Rename pop_white column
pop_white2022 = pop_white2022.rename(columns={'B03002_003E': 'pop_white'})
# Add year column
pop_white2022["year"] = 2022
# Keep relevant columns
pop_white2022 = pop_white2022.loc[:, ["geoid", "pop_white", "year"]]
pop_white2022.head()
print(pop_white2022.shape)

# Get land area for each Census block group
print("Done getting non-Hispanic white population in each Census block...")
print("Now getting land area for each Census block...")
if aland_need_to_pull == True:
	# 2013
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2013))[["GEOID", "ALAND"]])

	aland2013 = pd.concat(aland_by_state, ignore_index=True)
	aland2013.columns = ["geoid", "aland"]
	aland2013["year"] = 2013
	# Convert to square miles
	aland2013['aland'] = 3.861 * (10**(-7)) * aland2013['aland']
	aland2013.head()
	aland2013.shape
	aland2013.to_csv("./intermediate/aland2013.csv")

	# 2014
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2014))[["GEOID", "ALAND"]])

	aland2014 = pd.concat(aland_by_state, ignore_index=True)
	aland2014.columns = ["geoid", "aland"]
	aland2014["year"] = 2014
	# Convert to square miles
	aland2014['aland'] = 3.861 * (10**(-7)) * aland2014['aland']
	aland2014.head()
	aland2014.shape
	aland2014.to_csv("./intermediate/aland2014.csv")

	# 2015
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2015))[["GEOID", "ALAND"]])

	aland2015 = pd.concat(aland_by_state, ignore_index=True)
	aland2015.columns = ["geoid", "aland"]
	aland2015["year"] = 2015
	# Convert to square miles
	aland2015['aland'] = 3.861 * (10**(-7)) * aland2015['aland']
	aland2015.head()
	aland2015.shape
	aland2015.to_csv("./intermediate/aland2015.csv")

	# 2016
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2016))[["GEOID", "ALAND"]])

	aland2016 = pd.concat(aland_by_state, ignore_index=True)
	aland2016.columns = ["geoid", "aland"]
	aland2016["year"] = 2016
	# Convert to square miles
	aland2016['aland'] = 3.861 * (10**(-7)) * aland2016['aland']
	aland2016.head()
	aland2016.shape
	aland2016.to_csv("./intermediate/aland2016.csv")

	# 2017
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2017))[["GEOID", "ALAND"]])

	aland2017 = pd.concat(aland_by_state, ignore_index=True)
	aland2017.columns = ["geoid", "aland"]
	aland2017["year"] = 2017
	# Convert to square miles
	aland2017['aland'] = 3.861 * (10**(-7)) * aland2017['aland']
	aland2017.head()
	aland2017.shape
	aland2017.to_csv("./intermediate/aland2017.csv")

	# 2018
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2018))[["GEOID", "ALAND"]])

	aland2018 = pd.concat(aland_by_state, ignore_index=True)
	aland2018.columns = ["geoid", "aland"]
	aland2018["year"] = 2018
	# Convert to square miles
	aland2018['aland'] = 3.861 * (10**(-7)) * aland2018['aland']
	aland2018.head()
	aland2018.shape
	aland2018.to_csv("./intermediate/aland2018.csv")

	# 2019
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2019))[["GEOID", "ALAND"]])

	aland2019 = pd.concat(aland_by_state, ignore_index=True)
	aland2019.columns = ["geoid", "aland"]
	aland2019["year"] = 2019
	# Convert to square miles
	aland2019['aland'] = 3.861 * (10**(-7)) * aland2019['aland']
	aland2019.head()
	aland2019.shape
	aland2019.to_csv("./intermediate/aland2019.csv")

	# 2020
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2020))[["GEOID", "ALAND"]])

	aland2020 = pd.concat(aland_by_state, ignore_index=True)
	aland2020.columns = ["geoid", "aland"]
	aland2020["year"] = 2020
	# Convert to square miles
	aland2020['aland'] = 3.861 * (10**(-7)) * aland2020['aland']
	aland2020.head()
	aland2020.shape
	aland2020.to_csv("./intermediate/aland2020.csv")

	# 2021
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2021))[["GEOID", "ALAND"]])

	aland2021 = pd.concat(aland_by_state, ignore_index=True)
	aland2021.columns = ["geoid", "aland"]
	aland2021["year"] = 2021
	# Convert to square miles
	aland2021['aland'] = 3.861 * (10**(-7)) * aland2021['aland']
	aland2021.head()
	aland2021.shape
	aland2021.to_csv("./intermediate/aland2021.csv")

	# 2022
	aland_by_state = []
	for i in fips_list:
		aland_by_state.append(pd.DataFrame(pygris.block_groups(
			state=i, cb=True, year=2022))[["GEOID", "ALAND"]])

	aland2022 = pd.concat(aland_by_state, ignore_index=True)
	aland2022.columns = ["geoid", "aland"]
	aland2022["year"] = 2022
	# Convert to square miles
	aland2022['aland'] = 3.861 * (10**(-7)) * aland2022['aland']
	aland2022.head()
	aland2022.shape
	aland2022.to_csv("./intermediate/aland2022.csv")
else:
	aland2013 = pd.read_csv(
		"./intermediate/aland2013.csv")[["geoid", "aland", "year"]]
	aland2013['geoid'] = aland2013['geoid'].astype(str).apply(add_leading_zero)

	aland2014 = pd.read_csv(
		"./intermediate/aland2014.csv")[["geoid", "aland", "year"]]
	aland2014['geoid'] = aland2014['geoid'].astype(str).apply(add_leading_zero)

	aland2015 = pd.read_csv(
		"./intermediate/aland2015.csv")[["geoid", "aland", "year"]]
	aland2015['geoid'] = aland2015['geoid'].astype(str).apply(add_leading_zero)

	aland2016 = pd.read_csv(
		"./intermediate/aland2016.csv")[["geoid", "aland", "year"]]
	aland2016['geoid'] = aland2016['geoid'].astype(str).apply(add_leading_zero)

	aland2017 = pd.read_csv(
		"./intermediate/aland2017.csv")[["geoid", "aland", "year"]]
	aland2017['geoid'] = aland2017['geoid'].astype(str).apply(add_leading_zero)

	aland2018 = pd.read_csv(
		"./intermediate/aland2018.csv")[["geoid", "aland", "year"]]
	aland2018['geoid'] = aland2018['geoid'].astype(str).apply(add_leading_zero)

	aland2019 = pd.read_csv(
		"./intermediate/aland2019.csv")[["geoid", "aland", "year"]]
	aland2019['geoid'] = aland2019['geoid'].astype(str).apply(add_leading_zero)

	aland2020 = pd.read_csv(
		"./intermediate/aland2020.csv")[["geoid", "aland", "year"]]
	aland2020['geoid'] = aland2020['geoid'].astype(str).apply(add_leading_zero)

	aland2021 = pd.read_csv(
		"./intermediate/aland2021.csv")[["geoid", "aland", "year"]]
	aland2021['geoid'] = aland2021['geoid'].astype(str).apply(add_leading_zero)

	aland2022 = pd.read_csv(
		"./intermediate/aland2022.csv")[["geoid", "aland", "year"]]
	aland2022['geoid'] = aland2022['geoid'].astype(str).apply(add_leading_zero)

print("Done getting land area for each Census block...")
print("Now getting number of households in each Census block...")
# Get number of households for each Census block group
# 2013
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

hh2013 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2013["geoid"] = hh2013["state"] + hh2013["county"] + \
	hh2013["tract"] + hh2013["block group"]
# Rename households column
hh2013 = hh2013.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2013["year"] = 2013
# Keep relevant columns
hh2013 = hh2013.loc[:, ["geoid", "households", "year"]]
hh2013.head()
print(hh2013.shape)

# 2014
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

hh2014 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2014["geoid"] = hh2014["state"] + hh2014["county"] + \
	hh2014["tract"] + hh2014["block group"]
# Rename households column
hh2014 = hh2014.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2014["year"] = 2014
# Keep relevant columns
hh2014 = hh2014.loc[:, ["geoid", "households", "year"]]
hh2014.head()
print(hh2014.shape)

# 2015
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

hh2015 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2015["geoid"] = hh2015["state"] + hh2015["county"] + \
	hh2015["tract"] + hh2015["block group"]
# Rename households column
hh2015 = hh2015.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2015["year"] = 2015
# Keep relevant columns
hh2015 = hh2015.loc[:, ["geoid", "households", "year"]]
hh2015.head()
print(hh2015.shape)

# 2016
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

hh2016 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2016["geoid"] = hh2016["state"] + hh2016["county"] + \
	hh2016["tract"] + hh2016["block group"]
# Rename households column
hh2016 = hh2016.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2016["year"] = 2016
# Keep relevant columns
hh2016 = hh2016.loc[:, ["geoid", "households", "year"]]
hh2016.head()
print(hh2016.shape)

# 2017
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

hh2017 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2017["geoid"] = hh2017["state"] + hh2017["county"] + \
	hh2017["tract"] + hh2017["block group"]
# Rename households column
hh2017 = hh2017.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2017["year"] = 2017
# Keep relevant columns
hh2017 = hh2017.loc[:, ["geoid", "households", "year"]]
hh2017.head()
print(hh2017.shape)

# 2018
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

hh2018 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2018["geoid"] = hh2018["state"] + hh2018["county"] + \
	hh2018["tract"] + hh2018["block group"]
# Rename households column
hh2018 = hh2018.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2018["year"] = 2018
# Keep relevant columns
hh2018 = hh2018.loc[:, ["geoid", "households", "year"]]
hh2018.head()
print(hh2018.shape)

# 2019
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

hh2019 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2019["geoid"] = hh2019["state"] + hh2019["county"] + \
	hh2019["tract"] + hh2019["block group"]
# Rename households column
hh2019 = hh2019.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2019["year"] = 2019
# Keep relevant columns
hh2019 = hh2019.loc[:, ["geoid", "households", "year"]]
hh2019.head()
print(hh2019.shape)

# 2020
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2020)))

hh2020 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2020["geoid"] = hh2020["state"] + hh2020["county"] + \
	hh2020["tract"] + hh2020["block group"]
# Rename households column
hh2020 = hh2020.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2020["year"] = 2020
# Keep relevant columns
hh2020 = hh2020.loc[:, ["geoid", "households", "year"]]
hh2020.head()
print(hh2020.shape)

# 2021
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2021)))

hh2021 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2021["geoid"] = hh2021["state"] + hh2021["county"] + \
	hh2021["tract"] + hh2021["block group"]
# Rename households column
hh2021 = hh2021.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2021["year"] = 2021
# Keep relevant columns
hh2021 = hh2021.loc[:, ["geoid", "households", "year"]]
hh2021.head()
print(hh2021.shape)

# 2022
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2022)))

hh2022 = pd.concat(hh_by_state, ignore_index=True)

# Create geoid column
hh2022["geoid"] = hh2022["state"] + hh2022["county"] + \
	hh2022["tract"] + hh2022["block group"]
# Rename households column
hh2022 = hh2022.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2022["year"] = 2022
# Keep relevant columns
hh2022 = hh2022.loc[:, ["geoid", "households", "year"]]
hh2022.head()
print(hh2022.shape)

print("Done getting number of households for each Census block...")
print("Now getting median income in each Census block...")
# Median Income by Census block group
# Code: B19013E
# 2013
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

median_inc2013 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2013["geoid"] = median_inc2013["state"] + median_inc2013["county"] + \
	median_inc2013["tract"] + median_inc2013["block group"]
# Rename median income column
median_inc2013 = median_inc2013.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2013 Income * (2022 CPI / 2013 CPI), Apr 2022 CPI: 288.582, Apr 2013 CPI: 231.797
median_inc2013["median_income_2022dollars"] = median_inc2013["median_income"] * \
	(288.582 / 231.797)
# Add year column
median_inc2013["year"] = 2013
# Keep relevant columns
median_inc2013 = median_inc2013.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2013.head()
print(median_inc2013.shape)

# 2014
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

median_inc2014 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2014["geoid"] = median_inc2014["state"] + median_inc2014["county"] + \
	median_inc2014["tract"] + median_inc2014["block group"]
# Rename median income column
median_inc2014 = median_inc2014.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2014 Income * (2022 CPI / 2014 CPI), Apr 2022 CPI: 288.582, Apr 2014 CPI: 236.468
median_inc2014["median_income_2022dollars"] = median_inc2014["median_income"] * \
	(288.582 / 236.468)
# Add year column
median_inc2014["year"] = 2014
# Keep relevant columns
median_inc2014 = median_inc2014.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2014.head()
print(median_inc2014.shape)

# 2015
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

median_inc2015 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2015["geoid"] = median_inc2015["state"] + median_inc2015["county"] + \
	median_inc2015["tract"] + median_inc2015["block group"]
# Rename median income column
median_inc2015 = median_inc2015.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2015 Income * (2022 CPI / 2015 CPI), Apr 2022 CPI: 288.582, Apr 2015 CPI: 236.222
median_inc2015["median_income_2022dollars"] = median_inc2015["median_income"] * \
	(288.582 / 236.222)
# Add year column
median_inc2015["year"] = 2015
# Keep relevant columns
median_inc2015 = median_inc2015.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2015.head()
print(median_inc2015.shape)

# 2016
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

median_inc2016 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2016["geoid"] = median_inc2016["state"] + median_inc2016["county"] + \
	median_inc2016["tract"] + median_inc2016["block group"]
# Rename median_income column
median_inc2016 = median_inc2016.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2016 Income * (2022 CPI / 2016 CPI), Apr 2022 CPI: 288.582, Apr 2016 CPI: 238.992
median_inc2016["median_income_2022dollars"] = median_inc2016["median_income"] * \
	(288.582 / 238.992)
# Add year column
median_inc2016["year"] = 2016
# Keep relevant columns
median_inc2016 = median_inc2016.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2016.head()
print(median_inc2016.shape)

# 2017
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

median_inc2017 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2017["geoid"] = median_inc2017["state"] + median_inc2017["county"] + \
	median_inc2017["tract"] + median_inc2017["block group"]
# Rename median_income column
median_inc2017 = median_inc2017.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2017 Income * (2022 CPI / 2017 CPI), Apr 2022 CPI: 288.582, Apr 2017 CPI: 244.193
median_inc2017["median_income_2022dollars"] = median_inc2017["median_income"] * \
	(288.582 / 244.193)
# Add year column
median_inc2017["year"] = 2017
# Keep relevant columns
median_inc2017 = median_inc2017.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2017.head()
print(median_inc2017.shape)

# 2018
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

median_inc2018 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2018["geoid"] = median_inc2018["state"] + median_inc2018["county"] + \
	median_inc2018["tract"] + median_inc2018["block group"]
# Rename median_income column
median_inc2018 = median_inc2018.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2018 Income * (2022 CPI / 2018 CPI), Apr 2022 CPI: 288.582, Apr 2018 CPI: 250.227
median_inc2018["median_income_2022dollars"] = median_inc2018["median_income"] * \
	(288.582 / 250.227)
# Add year column
median_inc2018["year"] = 2018
# Keep relevant columns
median_inc2018 = median_inc2018.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2018.head()
print(median_inc2018.shape)


# 2019
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

median_inc2019 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2019["geoid"] = median_inc2019["state"] + median_inc2019["county"] + \
	median_inc2019["tract"] + median_inc2019["block group"]
# Rename median_income column
median_inc2019 = median_inc2019.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2019 Income * (2022 CPI / 2019 CPI), Apr 2022 CPI: 288.582, Apr 2019 CPI: 255.233
median_inc2019["median_income_2022dollars"] = median_inc2019["median_income"] * \
	(288.582 / 255.233)
# Add year column
median_inc2019["year"] = 2019
# Keep relevant columns
median_inc2019 = median_inc2019.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2019.head()
print(median_inc2019.shape)

# 2020
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2020)))

median_inc2020 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2020["geoid"] = median_inc2020["state"] + median_inc2020["county"] + \
	median_inc2020["tract"] + median_inc2020["block group"]
# Rename median_income column
median_inc2020 = median_inc2020.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2020 Income * (2022 CPI / 2020 CPI), Apr 2022 CPI: 288.582, Apr 2020 CPI: 256.032
median_inc2020["median_income_2022dollars"] = median_inc2020["median_income"] * \
	(288.582 / 256.032)
# Add year column
median_inc2020["year"] = 2020
# Keep relevant columns
median_inc2020 = median_inc2020.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2020.head()
print(median_inc2020.shape)

# 2021
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2021)))

median_inc2021 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2021["geoid"] = median_inc2021["state"] + median_inc2021["county"] + \
	median_inc2021["tract"] + median_inc2021["block group"]
# Rename median_income column
median_inc2021 = median_inc2021.rename(
	columns={'B19013_001E': 'median_income'})
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2021 Income * (2022 CPI / 2021 CPI), Apr 2022 CPI: 288.582, Apr 2021 CPI: 266.625
median_inc2021["median_income_2022dollars"] = median_inc2021["median_income"] * \
	(288.582 / 266.625)
# Add year column
median_inc2021["year"] = 2021
# Keep relevant columns
median_inc2021 = median_inc2021.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2021.head()
print(median_inc2021.shape)

# 2022
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2022)))

median_inc2022 = pd.concat(median_inc_by_state, ignore_index=True)

# Create geoid column
median_inc2022["geoid"] = median_inc2022["state"] + median_inc2022["county"] + \
	median_inc2022["tract"] + median_inc2022["block group"]
# Rename median_income column
median_inc2022 = median_inc2022.rename(
	columns={'B19013_001E': 'median_income_2022dollars'})
# Add year column
median_inc2022["year"] = 2022
# Keep relevant columns
median_inc2022 = median_inc2022.loc[:, [
	"geoid", "median_income_2022dollars", "year"]]
median_inc2022.head()
print(median_inc2022.shape)

print("Done getting median income in each Census block...")
print("Now combining it all together...")
# Combine Data Together
# Population, Households, Median Income, White Population, Land Area

# 2013
pop2013.shape
hh2013.shape
median_inc2013.shape
pop_white2013.shape
aland2013.shape

acs2013 = pop2013.merge(hh2013, on=['geoid', 'year'], how='left').merge(median_inc2013, on=['geoid', 'year'], how='left').merge(
	pop_white2013, on=['geoid', 'year'], how="left").merge(aland2013, on=['geoid', 'year'],  how="left")
acs2013['pop_den'] = acs2013["population"] / acs2013["aland"]
acs2013['hh_den'] = acs2013["households"] / acs2013["aland"]
acs2013['pct_minority'] = 1 - (acs2013["pop_white"] / acs2013["population"])
acs2013 = acs2013[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2013.head()
acs2013.shape
acs2013.to_csv("./outputs/acs2013.csv", index = False)

# 2014
pop2014.shape
hh2014.shape
median_inc2014.shape
pop_white2014.shape
aland2014.shape

acs2014 = pop2014.merge(hh2014, on=['geoid', 'year'], how='left').merge(median_inc2014, on=['geoid', 'year'], how='left').merge(
	pop_white2014, on=['geoid', 'year'], how="left").merge(aland2014, on=['geoid', 'year'],  how="left")
acs2014['pop_den'] = acs2014["population"] / acs2014["aland"]
acs2014['hh_den'] = acs2014["households"] / acs2014["aland"]
acs2014['pct_minority'] = 1 - (acs2014["pop_white"] / acs2014["population"])
acs2014 = acs2014[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2014.head()
acs2014.shape
acs2014.to_csv("./outputs/acs2014.csv", index = False)

# 2015
pop2015.shape
hh2015.shape
median_inc2015.shape
pop_white2015.shape
aland2015.shape

acs2015 = pop2015.merge(hh2015, on=['geoid', 'year'], how='left').merge(median_inc2015, on=['geoid', 'year'], how='left').merge(
	pop_white2015, on=['geoid', 'year'], how="left").merge(aland2015, on=['geoid', 'year'],  how="left")
acs2015['pop_den'] = acs2015["population"] / acs2015["aland"]
acs2015['hh_den'] = acs2015["households"] / acs2015["aland"]
acs2015['pct_minority'] = 1 - (acs2015["pop_white"] / acs2015["population"])
acs2015 = acs2015[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2015.head()
acs2015.shape
acs2015.to_csv("./outputs/acs2015.csv", index = False)

# 2016
pop2016.shape
hh2016.shape
median_inc2016.shape
pop_white2016.shape
aland2016.shape

acs2016 = pop2016.merge(hh2016, on=['geoid', 'year'], how='left').merge(median_inc2016, on=['geoid', 'year'], how='left').merge(
	pop_white2016, on=['geoid', 'year'], how="left").merge(aland2016, on=['geoid', 'year'],  how="left")
acs2016['pop_den'] = acs2016["population"] / acs2016["aland"]
acs2016['hh_den'] = acs2016["households"] / acs2016["aland"]
acs2016['pct_minority'] = 1 - (acs2016["pop_white"] / acs2016["population"])
acs2016 = acs2016[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2016.head()
acs2016.shape
acs2016.to_csv("./outputs/acs2016.csv", index = False)

# 2017
pop2017.shape
hh2017.shape
median_inc2017.shape
pop_white2017.shape
aland2017.shape

acs2017 = pop2017.merge(hh2017, on=['geoid', 'year'], how='left').merge(median_inc2017, on=['geoid', 'year'], how='left').merge(
	pop_white2017, on=['geoid', 'year'], how="left").merge(aland2017, on=['geoid', 'year'],  how="left")
acs2017['pop_den'] = acs2017["population"] / acs2017["aland"]
acs2017['hh_den'] = acs2017["households"] / acs2017["aland"]
acs2017['pct_minority'] = 1 - (acs2017["pop_white"] / acs2017["population"])
acs2017 = acs2017[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2017.head()
acs2017.shape
acs2017.to_csv("./outputs/acs2017.csv", index = False)

# 2018
pop2018.shape
hh2018.shape
median_inc2018.shape
pop_white2018.shape
aland2018.shape

acs2018 = pop2018.merge(hh2018, on=['geoid', 'year'], how='left').merge(median_inc2018, on=['geoid', 'year'], how='left').merge(
	pop_white2018, on=['geoid', 'year'], how="left").merge(aland2018, on=['geoid', 'year'],  how="left")
acs2018['pop_den'] = acs2018["population"] / acs2018["aland"]
acs2018['hh_den'] = acs2018["households"] / acs2018["aland"]
acs2018['pct_minority'] = 1 - (acs2018["pop_white"] / acs2018["population"])
acs2018 = acs2018[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2018.head()
acs2018.shape
acs2018.to_csv("./outputs/acs2018.csv", index = False)

# 2019
pop2019.shape
hh2019.shape
median_inc2019.shape
pop_white2019.shape
aland2019.shape

acs2019 = pop2019.merge(hh2019, on=['geoid', 'year'], how='left').merge(median_inc2019, on=['geoid', 'year'], how='left').merge(
	pop_white2019, on=['geoid', 'year'], how="left").merge(aland2019, on=['geoid', 'year'],  how="left")
acs2019['pop_den'] = acs2019["population"] / acs2019["aland"]
acs2019['hh_den'] = acs2019["households"] / acs2019["aland"]
acs2019['pct_minority'] = 1 - (acs2019["pop_white"] / acs2019["population"])
acs2019 = acs2019[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2019.head()
acs2019.shape
acs2019.to_csv("./outputs/acs2019.csv", index = False)

# 2020
pop2020.shape
hh2020.shape
median_inc2020.shape
pop_white2020.shape
aland2020.shape

acs2020 = pop2020.merge(hh2020, on=['geoid', 'year'], how='left').merge(median_inc2020, on=['geoid', 'year'], how='left').merge(
	pop_white2020, on=['geoid', 'year'], how="left").merge(aland2020, on=['geoid', 'year'],  how="left")
acs2020['pop_den'] = acs2020["population"] / acs2020["aland"]
acs2020['hh_den'] = acs2020["households"] / acs2020["aland"]
acs2020['pct_minority'] = 1 - (acs2020["pop_white"] / acs2020["population"])
acs2020 = acs2020[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2020.head()
acs2020.shape
acs2020.to_csv("./outputs/acs2020.csv", index = False)

# 2021
pop2021.shape
hh2021.shape
median_inc2021.shape
pop_white2021.shape
aland2021.shape

acs2021 = pop2021.merge(hh2021, on=['geoid', 'year'], how='left').merge(median_inc2021, on=['geoid', 'year'], how='left').merge(
	pop_white2021, on=['geoid', 'year'], how="left").merge(aland2021, on=['geoid', 'year'],  how="left")
acs2021['pop_den'] = acs2021["population"] / acs2021["aland"]
acs2021['hh_den'] = acs2021["households"] / acs2021["aland"]
acs2021['pct_minority'] = 1 - (acs2021["pop_white"] / acs2021["population"])
acs2021 = acs2021[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2021.head()
acs2021.shape
acs2021.to_csv("./outputs/acs2021.csv", index = False)

# 2022
pop2022.shape
hh2022.shape
median_inc2022.shape
pop_white2022.shape
aland2022.shape

acs2022 = pop2022.merge(hh2022, on=['geoid', 'year'], how='left').merge(median_inc2022, on=['geoid', 'year'], how='left').merge(
	pop_white2022, on=['geoid', 'year'], how="left").merge(aland2022, on=['geoid', 'year'],  how="left")
acs2022['pop_den'] = acs2022["population"] / acs2022["aland"]
acs2022['hh_den'] = acs2022["households"] / acs2022["aland"]
acs2022['pct_minority'] = 1 - (acs2022["pop_white"] / acs2022["population"])
acs2022 = acs2022[["geoid", "aland", "hh_den", "pop_den", "median_income_2022dollars", "pct_minority", "year"]]
acs2022.head()
acs2022.shape
acs2022.to_csv("./outputs/acs2022.csv", index = False)

print("All done, check outputs for files...")

