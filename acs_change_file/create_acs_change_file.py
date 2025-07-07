# Create ACS Change File
# This gives us Land Area, HH Density, Population Density, Percent of 0-Veh HHs, Median Income, and Percent Minority
# at the block group level by year, from 2005 to 2022.

# Load Libraries
import os
import glob
import numpy as np
import pandas as pd
import random
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

# Read 2000 BGP to 2010 BG crosswalk
nhgis_bgp2000_bg2010 = pd.read_csv("./nhgis_bgp2000_bg2010.csv")

# Read 2020 BG to 2010 BG crosswalk
nhgis_bg2020_bg2010 = pd.read_csv("./nhgis_bg2020_bg2010.csv")

# Do you need to pull files?
need_to_pull = False

# Census API Key
api_key = "4049ee84e96e784c0042da45d81f95514d53b7fd"
c = Census(api_key)

## NHGIS API Key
os.environ["IPUMS_API_KEY"] = "59cba10d8a5da536fc06b59d23a1e21a315f492fa8cf28e6b302798b"
IPUMS_API_KEY = os.environ.get("IPUMS_API_KEY")
ipums = IpumsApiClient(IPUMS_API_KEY)
DOWNLOAD_DIR = Path("D:/neeco/rdc_census/acs_change_file/intermediate/")

# Helper Functions
def add_leading_zero(value):
	if len(value) == 11:
		return '0' + value
	else:
		return value
	
def tract_add_trailing_zero(value):
	if len(value) == 4:
		return value + '00' 
	elif len(value) == 5:
		return value + '0' 
	else:
		return value
	
def county_add_leading_zero(value):
	if len(value) == 1:
		return '00' + value
	elif len(value) == 2:
		return '0' + value
	else:
		return value
	
def state_add_leading_zero(value):
	if len(value) == 1:
		return '0' + value
	else:
		return value
	
def year_bg_interpolate(df, bg_col, year_col, value_col):
    """
    Interpolates missing values in a DataFrame by year for each block group.

    Args:
        df (pd.DataFrame): The DataFrame with the data.
        bg_col (str): The name of the block group column.
        year_col (str): The name of the year column.
        value_col (str): The name of the value column.

    Returns:
        pd.DataFrame: A new DataFrame with interpolated values.
    """
    # Ensure the year column is numeric
    df[year_col] = pd.to_numeric(df[year_col], errors='coerce')
    
    # Pivot the table, interpolate, and unstack
    interpolated_df = (
        df.pivot(index=year_col, columns=bg_col, values=value_col)
        .reindex(range(df[year_col].min(), df[year_col].max() + 1))
        .interpolate('index')
        .unstack(-1)
		.reset_index(name=value_col)
    )
    
    return interpolated_df

# Weighted average households function
def weighted_average_hh(group):
  """
  Calculates the weighted average of median income for a given group.
  """
  # Multiply each value by its corresponding weight.
  weighted_sum = (group['median_income'] * group['households']).sum() 
  # Sum the weights.
  total_weight = group['households'].sum()
  # Calculate the weighted average.
  return weighted_sum / total_weight

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

# Edit crosswalk files
## 2000 BGP to 2010 BG crosswalk
nhgis_bgp2000_bg2010["bg2010ge"] = nhgis_bgp2000_bg2010["bg2010ge"].astype(str).apply(add_leading_zero)
## 2020 BG to 2010 BG crosswalk
nhgis_bg2020_bg2010["bg2020ge"] = nhgis_bg2020_bg2010["bg2020ge"].astype(str).apply(add_leading_zero)
nhgis_bg2020_bg2010["bg2010ge"] = nhgis_bg2020_bg2010["bg2010ge"].astype(str).apply(add_leading_zero)

# Connecticut Crosswalk
ct_xwalk2022 = pd.read_csv("https://github.com/CT-Data-Collaborative/2022-block-crosswalk/raw/refs/heads/main/2022blockcrosswalk.csv")
ct_xwalk2022['bg2022_ct'] = ct_xwalk2022['block_fips_2022'].astype(str).str[:-3].apply(add_leading_zero)
ct_xwalk2022['bg2020'] = ct_xwalk2022['block_fips_2020'].astype(str).str[:-3].apply(add_leading_zero)
ct_xwalk2022 = ct_xwalk2022[['bg2020', 'bg2022_ct']]
ct_xwalk2022 = ct_xwalk2022.drop_duplicates()
print(len(ct_xwalk2022['bg2020'].unique()))
print(len(ct_xwalk2022['bg2022_ct'].unique()))

# Total Population
# Code: B01003
# 2000 uses 2000 US Census
# 2001-2008 is interpolated
# 2009-2012 uses IPUMS NHGIS API Python library
# 2013-2022 uses Census API Python library
print("Getting total population by Census block group...")

# 2000
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2000_SF3b", data_tables=["NP001A"], geog_levels=["blck_grp_090"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "pop2000")
	csv_file = glob.glob(intermediate_dir + "pop2000/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "pop2000/pop2000_load.csv")
else:
	pass

pop2000_load = pd.read_csv(intermediate_dir + "pop2000/pop2000_load.csv")
pop2000_load = pop2000_load[["GISJOIN", "HAK001", "STUSAB"]]
pop2000_load.columns = ["GISJOIN", "pop_bgp2000", "state"]
print("Population before crosswalk:", pop2000_load['pop_bgp2000'].sum())
pop2000 = pop2000_load[pop2000_load['state'].isin(states_list)]
pop2000 = pop2000[["GISJOIN", "pop_bgp2000"]]

pop2000 = pd.merge(pop2000, nhgis_bgp2000_bg2010, how='left', left_on='GISJOIN', right_on='bgp2000gj')
pop2000['population'] = pop2000['pop_bgp2000'] * pop2000['wt_pop'] 
pop2000 = pop2000[["bg2010ge", "population"]]
pop2000 = pop2000.groupby('bg2010ge', as_index=False).sum()
pop2000.columns = ["bg2010", "population"]
pop2000["year"] = 2000
print(pop2000.head())
print(pop2000.shape)
print("Population after crosswalk:", round(pop2000['population'].sum()))

# 2010
if need_to_pull:
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
else:
	pass

pop2010_load = pd.read_csv(intermediate_dir + "pop2010/pop2010_load.csv")
pop2010_load = pop2010_load[["GEOID", "JMAE001", "STUSAB"]]
pop2010_load.columns = ["bg2010", "population", "state"]
pop2010_load["bg2010"] = pop2010_load["bg2010"].str[-12:]
pop2010 = pop2010_load[pop2010_load['state'].isin(states_list)]
pop2010 = pop2010[["bg2010", "population"]]
pop2010["year"] = 2010
print(pop2010.head())
print(pop2010.shape)

# Interpolate to get 2001 to 2009
## Concatenate dataframes
pop2000and2010 = pd.DataFrame(pd.concat([pop2000, pop2010]))
## Get population from 2001 to 2009
pop2000to2010 = year_bg_interpolate(pop2000and2010, "bg2010", "year", "population")
## Filter to get population for each year
pop2001 = pop2000to2010[pop2000to2010["year"] == 2001]
print(pop2001.head())
print(pop2001.shape)

pop2002 = pop2000to2010[pop2000to2010["year"] == 2002]
print(pop2002.head())
print(pop2002.shape)

pop2003 = pop2000to2010[pop2000to2010["year"] == 2003]
print(pop2003.head())
print(pop2003.shape)

pop2004 = pop2000to2010[pop2000to2010["year"] == 2004]
print(pop2004.head())
print(pop2004.shape)

pop2005 = pop2000to2010[pop2000to2010["year"] == 2005]
print(pop2005.head())
print(pop2005.shape)

pop2006 = pop2000to2010[pop2000to2010["year"] == 2006]
print(pop2006.head())
print(pop2006.shape)

pop2007 = pop2000to2010[pop2000to2010["year"] == 2007]
print(pop2007.head())
print(pop2007.shape)

pop2008 = pop2000to2010[pop2000to2010["year"] == 2008]
print(pop2008.head())
print(pop2008.shape)

pop2009 = pop2000to2010[pop2000to2010["year"] == 2009]
print(pop2009.head())
print(pop2009.shape)


# 2011
if need_to_pull:
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

else:
	pass

pop2011_load = pd.read_csv(intermediate_dir + "pop2011/pop2011_load.csv")
pop2011_load = pop2011_load[["GEOID", "MNTE001", "STUSAB"]]
pop2011_load.columns = ["bg2010", "population", "state"]
pop2011_load["bg2010"] = pop2011_load["bg2010"].str[-12:]
pop2011 = pop2011_load[pop2011_load['state'].isin(states_list)]
pop2011 = pop2011[["bg2010", "population"]]
pop2011["year"] = 2011
print(pop2011.head())
print(pop2011.shape)

# 2012
if need_to_pull:
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
else:
	pass

pop2012_load = pd.read_csv(intermediate_dir + "pop2012/pop2012_load.csv")
pop2012_load = pop2012_load[["GEOID", "QSPE001", "STUSAB"]]
pop2012_load.columns = ["bg2010", "population", "state"]
pop2012_load["bg2010"] = pop2012_load["bg2010"].str[-12:]
pop2012 = pop2012_load[pop2012_load['state'].isin(states_list)]
pop2012 = pop2012[["bg2010", "population"]]
pop2012["year"] = 2012
print(pop2012.head())
print(pop2012.shape)

# 2013
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

pop2013 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2010 column
pop2013["bg2010"] = pop2013["state"] + pop2013["county"] + \
	pop2013["tract"] + pop2013["block group"]
# Rename population column
pop2013 = pop2013.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2013["year"] = 2013
# Keep relevant columns
pop2013 = pop2013.loc[:, ["bg2010", "population", "year"]]
print(pop2013.head())
print(pop2013.shape)

# 2014
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

pop2014 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2010 column
pop2014["bg2010"] = pop2014["state"] + pop2014["county"] + \
	pop2014["tract"] + pop2014["block group"]
# Rename population column
pop2014 = pop2014.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2014["year"] = 2014
# Keep relevant columns
pop2014 = pop2014.loc[:, ["bg2010", "population", "year"]]
print(pop2014.head())
print(pop2014.shape)

# 2015
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

pop2015 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2010 column
pop2015["bg2010"] = pop2015["state"] + pop2015["county"] + \
	pop2015["tract"] + pop2015["block group"]
# Rename population column
pop2015 = pop2015.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2015["year"] = 2015
# Keep relevant columns
pop2015 = pop2015.loc[:, ["bg2010", "population", "year"]]
print(pop2015.head())
print(pop2015.shape)

# 2016
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

pop2016 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2010 column
pop2016["bg2010"] = pop2016["state"] + pop2016["county"] + \
	pop2016["tract"] + pop2016["block group"]
# Rename population column
pop2016 = pop2016.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2016["year"] = 2016
# Keep relevant columns
pop2016 = pop2016.loc[:, ["bg2010", "population", "year"]]
print(pop2016.head())
print(pop2016.shape)

# 2017
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

pop2017 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2010 column
pop2017["bg2010"] = pop2017["state"] + pop2017["county"] + \
	pop2017["tract"] + pop2017["block group"]
# Rename population column
pop2017 = pop2017.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2017["year"] = 2017
# Keep relevant columns
pop2017 = pop2017.loc[:, ["bg2010", "population", "year"]]
print(pop2017.head())
print(pop2017.shape)

# 2018
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

pop2018 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2010 column
pop2018["bg2010"] = pop2018["state"] + pop2018["county"] + \
	pop2018["tract"] + pop2018["block group"]
# Rename population column
pop2018 = pop2018.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2018["year"] = 2018
# Keep relevant columns
pop2018 = pop2018.loc[:, ["bg2010", "population", "year"]]
print(pop2018.head())
print(pop2018.shape)

# 2019
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

pop2019 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2010 column
pop2019["bg2010"] = pop2019["state"] + pop2019["county"] + \
	pop2019["tract"] + pop2019["block group"]
# Rename population column
pop2019 = pop2019.rename(columns={'B01003_001E': 'population'})
# Add year column
pop2019["year"] = 2019
# Keep relevant columns
pop2019 = pop2019.loc[:, ["bg2010", "population", "year"]]
print(pop2019.head())
print(pop2019.shape)

# 2020 to 2022: Convert 2020 BGs to 2010 BGs using crosswalks
# 2020
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2020)))

pop2020 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2020 column
pop2020["bg2020"] = pop2020["state"] + pop2020["county"] + \
	pop2020["tract"] + pop2020["block group"]
# Rename population column
pop2020 = pop2020.rename(columns={'B01003_001E': 'pop2020_bg20'})
# Keep relevant columns
pop2020 = pop2020.loc[:, ["bg2020", "pop2020_bg20"]]

# Join with crosswalk 
print("Population before crosswalk:", pop2020['pop2020_bg20'].sum())
pop2020 = pd.merge(pop2020, nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
pop2020['population'] = pop2020['pop2020_bg20'] * pop2020['wt_pop']
pop2020 = pop2020[["bg2010ge", "population"]]

pop2020  = pop2020.groupby('bg2010ge', as_index=False).sum()
pop2020.columns = ['bg2010', 'population']
# Add year column
pop2020["year"] = 2020

print(pop2020.head())
print(pop2020.shape)
print("Population after crosswalk:", round(pop2020['population'].sum()))


# 2021
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2021)))

pop2021 = pd.concat(pop_by_state, ignore_index=True)

# Create bg2020 column
pop2021["bg2020"] = pop2021["state"] + pop2021["county"] + \
	pop2021["tract"] + pop2021["block group"]
# Rename population column
pop2021 = pop2021.rename(columns={'B01003_001E': 'pop2021_bg20'})
# Keep relevant columns
pop2021 = pop2021.loc[:, ["bg2020", "pop2021_bg20"]]

# Join with crosswalk 
print("Population before crosswalk:", pop2021['pop2021_bg20'].sum())
pop2021 = pd.merge(pop2021, nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
pop2021['population'] = pop2021['pop2021_bg20'] * pop2021['wt_pop']
pop2021 = pop2021[["bg2010ge", "population"]]

pop2021  = pop2021.groupby('bg2010ge', as_index=False).sum()
pop2021.columns = ['bg2010', 'population']
# Add year column
pop2021["year"] = 2021

print(pop2021.head())
print(pop2021.shape)
print("Population before crosswalk:", round(pop2021['population'].sum()))

# 2022
pop_by_state = []
for i in fips_list:
	pop_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B01003_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2022)))

pop2022_load = pd.concat(pop_by_state, ignore_index=True)

# Create bg2020 column
pop2022_load["bg2020"] = pop2022_load["state"] + pop2022_load["county"] + \
	pop2022_load["tract"] + pop2022_load["block group"]
# Rename population column
pop2022_load = pop2022_load.rename(columns={'B01003_001E': 'pop2022_bg20'})
# Keep relevant columns
pop2022_load = pop2022_load.loc[:, ["state", "bg2020", "pop2022_bg20"]]
pop2022_ct = pop2022_load[pop2022_load['state']== "09"]
pop2022_no_ct = pop2022_load[pop2022_load['state'] != "09"]

pop2022_ct.columns = ['state', 'bg2022', 'pop2022_bg20']
pop2022_ct = pd.merge(pop2022_ct, ct_xwalk2022, how = "left", left_on = "bg2022", right_on = "bg2022_ct")
pop2022_ct = pop2022_ct[["state", "bg2020", "pop2022_bg20"]]

pop2022 = pd.concat([pop2022_ct, pop2022_no_ct])

# Join with crosswalk 
print("Population before crosswalk:", pop2022['pop2022_bg20'].sum())
pop2022 = pd.merge(pop2022, nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
pop2022['population'] = pop2022['pop2022_bg20'] * pop2022['wt_pop']
pop2022 = pop2022[["bg2010ge", "population"]]

pop2022  = pop2022.groupby('bg2010ge', as_index=False).sum()
pop2022.columns = ['bg2010', 'population']
# Add year column
pop2022["year"] = 2022

print(pop2022.head())
print(pop2022.shape)
print("Population after crosswalk:", round(pop2022['population'].sum()))

print("Done getting total population by Census block group...")
print("Now getting non-Hispanic white population by Census block group...")

# Total White Population (Non-Hispanic)
# Code: B030002
# 2000
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2000_SF3b", data_tables=["NP007B"], geog_levels=["blck_grp_090"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "pop_nhwhite2000")
	csv_file = glob.glob(intermediate_dir + "pop_nhwhite2000/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "pop_nhwhite2000/pop_nhwhite2000_load.csv")
else:
	pass

pop_nhwhite2000_load = pd.read_csv(intermediate_dir + "pop_nhwhite2000/pop_nhwhite2000_load.csv")


pop_nhwhite2000_load = pop_nhwhite2000_load[["GISJOIN", "HAR001", "STUSAB"]]
pop_nhwhite2000_load.columns = ["GISJOIN", "pop_nhwhite", "state"]
pop_nhwhite2000 = pop_nhwhite2000_load[pop_nhwhite2000_load['state'].isin(states_list)]
pop_nhwhite2000 = pop_nhwhite2000[["GISJOIN", "pop_nhwhite"]]

# Join on crosswalk
print("Non-Hispanic white population before crosswalk:", pop_nhwhite2000['pop_nhwhite'].sum())
pop_nhwhite2000 = pd.merge(pop_nhwhite2000, nhgis_bgp2000_bg2010, how='left', left_on='GISJOIN', right_on='bgp2000gj')
pop_nhwhite2000['pop_nhwhite'] = pop_nhwhite2000['pop_nhwhite'] * pop_nhwhite2000['wt_pop'] 
pop_nhwhite2000 = pop_nhwhite2000[["bg2010ge", "pop_nhwhite"]]
pop_nhwhite2000  = pop_nhwhite2000.groupby('bg2010ge', as_index=False).sum()
pop_nhwhite2000.columns = ["bg2010", "pop_nhwhite"]
pop_nhwhite2000["year"] = 2000
print(pop_nhwhite2000.head())
print(pop_nhwhite2000.shape)
print("Non-Hispanic white population after crosswalk:", round(pop_nhwhite2000['pop_nhwhite'].sum()))


# 2010
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2006_2010_ACS5a", data_tables=["B03002"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "pop_nhwhite2010")
	csv_file = glob.glob(intermediate_dir + "pop_nhwhite2010/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "pop_nhwhite2010/pop_nhwhite2010_load.csv")
else:
	pass

pop_nhwhite2010_load = pd.read_csv(intermediate_dir + "pop_nhwhite2010/pop_nhwhite2010_load.csv")
pop_nhwhite2010_load = pop_nhwhite2010_load[["GEOID", "JMJE003", "STUSAB"]]
pop_nhwhite2010_load.columns = ["bg2010", "pop_nhwhite", "state"]
pop_nhwhite2010_load["bg2010"] = pop_nhwhite2010_load["bg2010"].str[-12:]
pop_nhwhite2010 = pop_nhwhite2010_load[pop_nhwhite2010_load['state'].isin(states_list)]
pop_nhwhite2010 = pop_nhwhite2010[["bg2010", "pop_nhwhite"]]
pop_nhwhite2010["year"] = 2010
print(pop_nhwhite2010.head())
print(pop_nhwhite2010.shape)

# Interpolate to get 2001 to 2009
## Concatenate dataframes
pop_nhwhite2000and2009 = pd.DataFrame(pd.concat([pop_nhwhite2000, pop_nhwhite2010]))
## Get pop_nhwhiteulation from 2001 to 2009
pop_nhwhite2000to2010 = year_bg_interpolate(pop_nhwhite2000and2009, "bg2010", "year", "pop_nhwhite")
## Filter to get pop_nhwhiteulation for each year
pop_nhwhite2001 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2001]
print(pop_nhwhite2001.head())
print(pop_nhwhite2001.shape)
print(pop_nhwhite2001['pop_nhwhite'].sum())

pop_nhwhite2002 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2002]
print(pop_nhwhite2002.head())
print(pop_nhwhite2002.shape)
print(pop_nhwhite2002['pop_nhwhite'].sum())

pop_nhwhite2003 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2003]
print(pop_nhwhite2003.head())
print(pop_nhwhite2003.shape)
print(pop_nhwhite2003['pop_nhwhite'].sum())

pop_nhwhite2004 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2004]
print(pop_nhwhite2004.head())
print(pop_nhwhite2004.shape)
print(pop_nhwhite2004['pop_nhwhite'].sum())

pop_nhwhite2005 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2005]
print(pop_nhwhite2005.head())
print(pop_nhwhite2005.shape)
print(pop_nhwhite2005['pop_nhwhite'].sum())

pop_nhwhite2006 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2006]
print(pop_nhwhite2006.head())
print(pop_nhwhite2006.shape)
print(pop_nhwhite2006['pop_nhwhite'].sum())

pop_nhwhite2007 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2007]
print(pop_nhwhite2007.head())
print(pop_nhwhite2007.shape)
print(pop_nhwhite2007['pop_nhwhite'].sum())

pop_nhwhite2008 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2008]
print(pop_nhwhite2008.head())
print(pop_nhwhite2008.shape)
print(pop_nhwhite2008['pop_nhwhite'].sum())

pop_nhwhite2009 = pop_nhwhite2000to2010[pop_nhwhite2000to2010["year"] == 2009]
print(pop_nhwhite2009.head())
print(pop_nhwhite2009.shape)
print(pop_nhwhite2009['pop_nhwhite'].sum())

# 2011
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2007_2011_ACS5a", data_tables=["B03002"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "pop_nhwhite2011")
	csv_file = glob.glob(intermediate_dir + "pop_nhwhite2011/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "pop_nhwhite2011/pop_nhwhite2011_load.csv")
else:
	pass
pop_nhwhite2011_load = pd.read_csv(intermediate_dir + "pop_nhwhite2011/pop_nhwhite2011_load.csv")
pop_nhwhite2011_load = pop_nhwhite2011_load[["GEOID", "MN2E003", "STUSAB"]]
pop_nhwhite2011_load.columns = ["bg2010", "pop_nhwhite", "state"]
pop_nhwhite2011_load["bg2010"] = pop_nhwhite2011_load["bg2010"].str[-12:]
pop_nhwhite2011 = pop_nhwhite2011_load[pop_nhwhite2011_load['state'].isin(states_list)]
pop_nhwhite2011 = pop_nhwhite2011[["bg2010", "pop_nhwhite"]]
pop_nhwhite2011["year"] = 2011
print(pop_nhwhite2011.head())
print(pop_nhwhite2011.shape)

# 2012
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2008_2012_ACS5a", data_tables=["B03002"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "pop_nhwhite2012")
	csv_file = glob.glob(intermediate_dir + "pop_nhwhite2012/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "pop_nhwhite2012/pop_nhwhite2012_load.csv")
else:
	pass
pop_nhwhite2012_load = pd.read_csv(intermediate_dir + "pop_nhwhite2012/pop_nhwhite2012_load.csv")
pop_nhwhite2012_load = pop_nhwhite2012_load[["GEOID", "QSYE003", "STUSAB"]]
pop_nhwhite2012_load.columns = ["bg2010", "pop_nhwhite", "state"]
pop_nhwhite2012_load["bg2010"] = pop_nhwhite2012_load["bg2010"].str[-12:]
pop_nhwhite2012 = pop_nhwhite2012_load[pop_nhwhite2012_load['state'].isin(states_list)]
pop_nhwhite2012 = pop_nhwhite2012[["bg2010", "pop_nhwhite"]]
pop_nhwhite2012["year"] = 2012
print(pop_nhwhite2012.head())
print(pop_nhwhite2012.shape)

# 2013
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

pop_nhwhite2013 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2010 column
pop_nhwhite2013["bg2010"] = pop_nhwhite2013["state"] + pop_nhwhite2013["county"] + \
	pop_nhwhite2013["tract"] + pop_nhwhite2013["block group"]
# Rename pop_nhwhite column
pop_nhwhite2013 = pop_nhwhite2013.rename(columns={'B03002_003E': 'pop_nhwhite'})
# Add year column
pop_nhwhite2013["year"] = 2013
# Keep relevant columns
pop_nhwhite2013 = pop_nhwhite2013.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2013.head())
print(pop_nhwhite2013.shape)

# 2014
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

pop_nhwhite2014 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2010 column
pop_nhwhite2014["bg2010"] = pop_nhwhite2014["state"] + pop_nhwhite2014["county"] + \
	pop_nhwhite2014["tract"] + pop_nhwhite2014["block group"]
# Rename pop_nhwhite column
pop_nhwhite2014 = pop_nhwhite2014.rename(columns={'B03002_003E': 'pop_nhwhite'})
# Add year column
pop_nhwhite2014["year"] = 2014
# Keep relevant columns
pop_nhwhite2014 = pop_nhwhite2014.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2014.head())
print(pop_nhwhite2014.shape)

# 2015
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

pop_nhwhite2015 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2010 column
pop_nhwhite2015["bg2010"] = pop_nhwhite2015["state"] + pop_nhwhite2015["county"] + \
	pop_nhwhite2015["tract"] + pop_nhwhite2015["block group"]
# Rename pop_nhwhite column
pop_nhwhite2015 = pop_nhwhite2015.rename(columns={'B03002_003E': 'pop_nhwhite'})
# Add year column
pop_nhwhite2015["year"] = 2015
# Keep relevant columns
pop_nhwhite2015 = pop_nhwhite2015.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2015.head())
print(pop_nhwhite2015.shape)

# 2016
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

pop_nhwhite2016 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2010 column
pop_nhwhite2016["bg2010"] = pop_nhwhite2016["state"] + pop_nhwhite2016["county"] + \
	pop_nhwhite2016["tract"] + pop_nhwhite2016["block group"]
# Rename pop_nhwhite column
pop_nhwhite2016 = pop_nhwhite2016.rename(columns={'B03002_003E': 'pop_nhwhite'})
# Add year column
pop_nhwhite2016["year"] = 2016
# Keep relevant columns
pop_nhwhite2016 = pop_nhwhite2016.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2016.head())
print(pop_nhwhite2016.shape)

# 2017
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

pop_nhwhite2017 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2010 column
pop_nhwhite2017["bg2010"] = pop_nhwhite2017["state"] + pop_nhwhite2017["county"] + \
	pop_nhwhite2017["tract"] + pop_nhwhite2017["block group"]
# Rename pop_nhwhite column
pop_nhwhite2017 = pop_nhwhite2017.rename(columns={'B03002_003E': 'pop_nhwhite'})
# Add year column
pop_nhwhite2017["year"] = 2017
# Keep relevant columns
pop_nhwhite2017 = pop_nhwhite2017.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2017.head())
print(pop_nhwhite2017.shape)

# 2018
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

pop_nhwhite2018 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2010 column
pop_nhwhite2018["bg2010"] = pop_nhwhite2018["state"] + pop_nhwhite2018["county"] + \
	pop_nhwhite2018["tract"] + pop_nhwhite2018["block group"]
# Rename pop_nhwhite column
pop_nhwhite2018 = pop_nhwhite2018.rename(columns={'B03002_003E': 'pop_nhwhite'})
# Add year column
pop_nhwhite2018["year"] = 2018
# Keep relevant columns
pop_nhwhite2018 = pop_nhwhite2018.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2018.head())
print(pop_nhwhite2018.shape)

# 2019
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

pop_nhwhite2019 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2010 column
pop_nhwhite2019["bg2010"] = pop_nhwhite2019["state"] + pop_nhwhite2019["county"] + \
	pop_nhwhite2019["tract"] + pop_nhwhite2019["block group"]
# Rename pop_nhwhite column
pop_nhwhite2019 = pop_nhwhite2019.rename(columns={'B03002_003E': 'pop_nhwhite'})
# Add year column
pop_nhwhite2019["year"] = 2019
# Keep relevant columns
pop_nhwhite2019 = pop_nhwhite2019.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2019.head())
print(pop_nhwhite2019.shape)

# 2020 to 2022: Convert 2020 BGs to 2010 BGs using crosswalks
# 2020
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2020)))

pop_nhwhite2020 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2020 column
pop_nhwhite2020["bg2020"] = pop_nhwhite2020["state"] + pop_nhwhite2020["county"] + \
	pop_nhwhite2020["tract"] + pop_nhwhite2020["block group"]
# Rename pop_nhwhite column
pop_nhwhite2020 = pop_nhwhite2020.rename(columns={'B03002_003E': 'pop2020nhwhite_bg20'})

# Join with crosswalk 
print("Non-Hispanic white population before crosswalk:", pop_nhwhite2020['pop2020nhwhite_bg20'].sum())
pop_nhwhite2020  = pd.merge(pop_nhwhite2020 , nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
pop_nhwhite2020['pop_nhwhite'] = pop_nhwhite2020['pop2020nhwhite_bg20'] * pop_nhwhite2020 ['wt_pop']
pop_nhwhite2020 = pop_nhwhite2020[["bg2010ge", "pop_nhwhite"]]

pop_nhwhite2020 = pop_nhwhite2020.groupby('bg2010ge', as_index=False).sum()
pop_nhwhite2020.columns = ['bg2010', 'pop_nhwhite']

# Add year column
pop_nhwhite2020["year"] = 2020
# Keep relevant columns
pop_nhwhite2020 = pop_nhwhite2020.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2020.head())
print(pop_nhwhite2020.shape)
print("Non-Hispanic white population after crosswalk:", round(pop_nhwhite2020['pop_nhwhite'].sum()))


# 2021
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2021)))

pop_nhwhite2021 = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2020 column
pop_nhwhite2021["bg2020"] = pop_nhwhite2021["state"] + pop_nhwhite2021["county"] + \
	pop_nhwhite2021["tract"] + pop_nhwhite2021["block group"]
# Rename pop_nhwhite column
pop_nhwhite2021 = pop_nhwhite2021.rename(columns={'B03002_003E': 'pop2021nhwhite_bg20'})

# Join with crosswalk 
print("Non-Hispanic white population before crosswalk:", pop_nhwhite2021['pop2021nhwhite_bg20'].sum())
pop_nhwhite2021  = pd.merge(pop_nhwhite2021 , nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
pop_nhwhite2021['pop_nhwhite'] = pop_nhwhite2021['pop2021nhwhite_bg20'] * pop_nhwhite2021 ['wt_pop']
pop_nhwhite2021 = pop_nhwhite2021[["bg2010ge", "pop_nhwhite"]]

pop_nhwhite2021 = pop_nhwhite2021.groupby('bg2010ge', as_index=False).sum()
pop_nhwhite2021.columns = ['bg2010', 'pop_nhwhite']

# Add year column
pop_nhwhite2021["year"] = 2021
# Keep relevant columns
pop_nhwhite2021 = pop_nhwhite2021.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2021.head())
print(pop_nhwhite2021.shape)
print("Non-Hispanic white population after crosswalk:", round(pop_nhwhite2021['pop_nhwhite'].sum()))

# 2022
pop_nhwhite_by_state = []
for i in fips_list:
	pop_nhwhite_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B03002_003E'), state_fips=i, county_fips='*', blockgroup='*', year=2022)))

pop_nhwhite2022_load = pd.concat(pop_nhwhite_by_state, ignore_index=True)

# Create bg2020 column
pop_nhwhite2022_load["bg2020"] = pop_nhwhite2022_load["state"] + pop_nhwhite2022_load["county"] + \
	pop_nhwhite2022_load["tract"] + pop_nhwhite2022_load["block group"]
# Rename pop_nhwhite column
pop_nhwhite2022_load = pop_nhwhite2022_load.rename(columns={'B03002_003E': 'pop2022nhwhite_bg20'})

pop_nhwhite2022_ct = pop_nhwhite2022_load[["state", "bg2020", "pop2022nhwhite_bg20"]][pop_nhwhite2022_load['state'] == "09"]
pop_nhwhite2022_no_ct = pop_nhwhite2022_load[["state", "bg2020", "pop2022nhwhite_bg20"]][pop_nhwhite2022_load['state'] != "09"]

pop_nhwhite2022_ct.columns = ["state", "bg2022", "pop2022_nhwhite_bg20"]
pop_nhwhite2022_ct = pd.merge(pop_nhwhite2022_ct, ct_xwalk2022, how = "left", left_on = "bg2022", right_on = "bg2022_ct")
pop_nhwhite2022_ct = pop_nhwhite2022_ct[["state", "bg2020", "pop2022_nhwhite_bg20"]]

pop_nhwhite2022 = pd.concat([pop_nhwhite2022_no_ct, pop_nhwhite2022_ct])
# Join with crosswalk 
print("Non-Hispanic white population before crosswalk:", pop_nhwhite2022['pop2022nhwhite_bg20'].sum())
pop_nhwhite2022  = pd.merge(pop_nhwhite2022, nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
pop_nhwhite2022['pop_nhwhite'] = pop_nhwhite2022['pop2022nhwhite_bg20'] * pop_nhwhite2022 ['wt_pop']
pop_nhwhite2022 = pop_nhwhite2022[["bg2010ge", "pop_nhwhite"]]

pop_nhwhite2022 = pop_nhwhite2022.groupby('bg2010ge', as_index=False).sum()
pop_nhwhite2022.columns = ['bg2010', 'pop_nhwhite']

# Add year column
pop_nhwhite2022["year"] = 2022
# Keep relevant columns
pop_nhwhite2022 = pop_nhwhite2022.loc[:, ["bg2010", "pop_nhwhite", "year"]]
print(pop_nhwhite2022.head())
print(pop_nhwhite2022.shape)
print("Non-Hispanic white population after crosswalk:", round(pop_nhwhite2022['pop_nhwhite'].sum()))


# Get land area for each Census block group
print("Done getting non-Hispanic white population in each Census block group...")
print("Now getting land area for each Census block group...")

# 2010 Census Block Group Areas
aland_load = []
for i in fips_list:
	aland_load.append(pd.DataFrame(pygris.block_groups(state=i, cb=True, cache=True, year = 2010)))
aland2010_load = pd.concat(aland_load)
aland2010_load = aland2010_load.reset_index()
aland2010 = aland2010_load[["GEO_ID", "CENSUSAREA"]]
aland2010["bg2010"] = aland2010["GEO_ID"].str[-12:]
aland2010["area_sqmi"] = aland2010["CENSUSAREA"]
aland2010 = aland2010[["bg2010", "area_sqmi"]]
aland2010["area_acres"] = aland2010["area_sqmi"] * 640
print(aland2010.head())
print(aland2010.shape)


# Get number of households for each Census block group
# Code: B11001
# 2000
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2000_SF3b", data_tables=["NP010A"], geog_levels=["blck_grp_090"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "hh2000")
	csv_file = glob.glob(intermediate_dir + "hh2000/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "hh2000/hh2000_load.csv")
else:
	pass

hh2000_load = pd.read_csv(intermediate_dir + "hh2000/hh2000_load.csv")
# hh2000_load["state"] = hh2000_load["STATEA"].astype(str).str.zfill(2)
# hh2000_load["county"] = hh2000_load["COUNTYA"].astype(str).str.zfill(3)
# hh2000_load["tract"] = hh2000_load["TRACTA"].astype(str).str.zfill(6)
# hh2000_load["block_group"] = hh2000_load["BLCK_GRPA"].astype(str).str.zfill(1)
# hh2000_load["bg2000"] = hh2000_load["state"] + hh2000_load["county"] + hh2000_load["tract"] + hh2000_load["block_group"]
hh2000_load = hh2000_load[["GISJOIN", "HA2001", "STUSAB"]]
hh2000_load.columns = ["GISJOIN", "hh_bgp2000", "state"]
hh2000 = hh2000_load[hh2000_load['state'].isin(states_list)]
hh2000 = hh2000[["GISJOIN", "hh_bgp2000"]]
## Convert to 2010 block groups
print("Households before crosswalk:", round(hh2000['hh_bgp2000'].sum()))
hh2000 = pd.merge(hh2000, nhgis_bgp2000_bg2010, how='left', left_on='GISJOIN', right_on='bgp2000gj')
hh2000['households'] = hh2000['hh_bgp2000'] * hh2000['wt_hh'] 
hh2000 = hh2000[["bg2010ge", "households"]]
hh2000 = hh2000.groupby('bg2010ge', as_index=False).sum()
hh2000.columns = ["bg2010", "households"]

hh2000["year"] = 2000
print(hh2000.head())
print(hh2000.shape)
print("Households after crosswalk:", round(hh2000['households'].sum()))

# 2010
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2006_2010_ACS5a", data_tables=["B11001"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "hh2010")
	csv_file = glob.glob(intermediate_dir + "hh2010/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "hh2010/hh2010_load.csv")

else:
	pass
hh2010_load = pd.read_csv(intermediate_dir + "hh2010/hh2010_load.csv")
hh2010_load = hh2010_load[["GEOID", "JM5E001", "STUSAB"]]
hh2010_load.columns = ["bg2010", "households", "state"]
hh2010_load["bg2010"] = hh2010_load["bg2010"].str[-12:]
hh2010 = hh2010_load[hh2010_load['state'].isin(states_list)]
hh2010 = hh2010[["bg2010", "households"]]
hh2010["year"] = 2010
print(hh2010.head())
print(hh2010.shape)

# Interpolate to get 2001 to 2009
## Concatenate dataframes
hh2000and2010 = pd.DataFrame(pd.concat([hh2000, hh2010]))
## Get households from 2001 to 2009
hh2000to2010 = year_bg_interpolate(hh2000and2010, "bg2010", "year", "households")
## Filter to get households for each year
hh2001 = hh2000to2010[hh2000to2010["year"] == 2001]
print(hh2001.head())
print(hh2001.shape)

hh2002 = hh2000to2010[hh2000to2010["year"] == 2002]
print(hh2002.head())
print(hh2002.shape)

hh2003 = hh2000to2010[hh2000to2010["year"] == 2003]
print(hh2003.head())
print(hh2003.shape)

hh2004 = hh2000to2010[hh2000to2010["year"] == 2004]
print(hh2004.head())
print(hh2004.shape)

hh2005 = hh2000to2010[hh2000to2010["year"] == 2005]
print(hh2005.head())
print(hh2005.shape)

hh2006 = hh2000to2010[hh2000to2010["year"] == 2006]
print(hh2006.head())
print(hh2006.shape)

hh2007 = hh2000to2010[hh2000to2010["year"] == 2007]
print(hh2007.head())
print(hh2007.shape)

hh2008 = hh2000to2010[hh2000to2010["year"] == 2008]
print(hh2008.head())
print(hh2008.shape)

hh2009 = hh2000to2010[hh2000to2010["year"] == 2009]
print(hh2009.head())
print(hh2009.shape)


# 2011
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2007_2011_ACS5a", data_tables=["B11001"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "hh2011")
	csv_file = glob.glob(intermediate_dir + "hh2011/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "hh2011/hh2011_load.csv")
else:
	pass
hh2011_load = pd.read_csv(intermediate_dir + "hh2011/hh2011_load.csv")
hh2011_load = hh2011_load[["GEOID", "MOOE001", "STUSAB"]]
hh2011_load.columns = ["bg2010", "households", "state"]
hh2011_load["bg2010"] = hh2011_load["bg2010"].str[-12:]
hh2011 = hh2011_load[hh2011_load['state'].isin(states_list)]
hh2011 = hh2011[["bg2010", "households"]]
hh2011["year"] = 2011
print(hh2011.head())
print(hh2011.shape)

# 2012
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2008_2012_ACS5a", data_tables=["B11001"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "hh2012")
	csv_file = glob.glob(intermediate_dir + "hh2012/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "hh2012/hh2012_load.csv")
else:
	pass
hh2012_load = pd.read_csv(intermediate_dir + "hh2012/hh2012_load.csv")
hh2012_load = hh2012_load[["GEOID", "QTME001", "STUSAB"]]
hh2012_load.columns = ["bg2010", "households", "state"]
hh2012_load["bg2010"] = hh2012_load["bg2010"].str[-12:]
hh2012 = hh2012_load[hh2012_load['state'].isin(states_list)]
hh2012 = hh2012[["bg2010", "households"]]
hh2012["year"] = 2012
print(hh2012.head())
print(hh2012.shape)


# 2013
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

hh2013 = pd.concat(hh_by_state, ignore_index=True)

# Create bg2010 column
hh2013["bg2010"] = hh2013["state"] + hh2013["county"] + \
	hh2013["tract"] + hh2013["block group"]
# Rename households column
hh2013 = hh2013.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2013["year"] = 2013
# Keep relevant columns
hh2013 = hh2013.loc[:, ["bg2010", "households", "year"]]
print(hh2013.head())
print(hh2013.shape)

# 2014
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

hh2014 = pd.concat(hh_by_state, ignore_index=True)

# Create bg2010 column
hh2014["bg2010"] = hh2014["state"] + hh2014["county"] + \
	hh2014["tract"] + hh2014["block group"]
# Rename households column
hh2014 = hh2014.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2014["year"] = 2014
# Keep relevant columns
hh2014 = hh2014.loc[:, ["bg2010", "households", "year"]]
print(hh2014.head())
print(hh2014.shape)

# 2015
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

hh2015 = pd.concat(hh_by_state, ignore_index=True)

# Create bg2010 column
hh2015["bg2010"] = hh2015["state"] + hh2015["county"] + \
	hh2015["tract"] + hh2015["block group"]
# Rename households column
hh2015 = hh2015.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2015["year"] = 2015
# Keep relevant columns
hh2015 = hh2015.loc[:, ["bg2010", "households", "year"]]
print(hh2015.head())
print(hh2015.shape)

# 2016
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

hh2016 = pd.concat(hh_by_state, ignore_index=True)

# Create bg2010 column
hh2016["bg2010"] = hh2016["state"] + hh2016["county"] + \
	hh2016["tract"] + hh2016["block group"]
# Rename households column
hh2016 = hh2016.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2016["year"] = 2016
# Keep relevant columns
hh2016 = hh2016.loc[:, ["bg2010", "households", "year"]]
print(hh2016.head())
print(hh2016.shape)

# 2017
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

hh2017 = pd.concat(hh_by_state, ignore_index=True)

# Create bg2010 column
hh2017["bg2010"] = hh2017["state"] + hh2017["county"] + \
	hh2017["tract"] + hh2017["block group"]
# Rename households column
hh2017 = hh2017.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2017["year"] = 2017
# Keep relevant columns
hh2017 = hh2017.loc[:, ["bg2010", "households", "year"]]
print(hh2017.head())
print(hh2017.shape)

# 2018
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

hh2018 = pd.concat(hh_by_state, ignore_index=True)

# Create bg2010 column
hh2018["bg2010"] = hh2018["state"] + hh2018["county"] + \
	hh2018["tract"] + hh2018["block group"]
# Rename households column
hh2018 = hh2018.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2018["year"] = 2018
# Keep relevant columns
hh2018 = hh2018.loc[:, ["bg2010", "households", "year"]]
print(hh2018.head())
print(hh2018.shape)

# 2019
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

hh2019 = pd.concat(hh_by_state, ignore_index=True)

# Create bg2010 column
hh2019["bg2010"] = hh2019["state"] + hh2019["county"] + \
	hh2019["tract"] + hh2019["block group"]
# Rename households column
hh2019 = hh2019.rename(columns={'B11001_001E': 'households'})
# Add year column
hh2019["year"] = 2019
# Keep relevant columns
hh2019 = hh2019.loc[:, ["bg2010", "households", "year"]]
print(hh2019.head())
print(hh2019.shape)

# 2020
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2020)))

hh2020_load = pd.concat(hh_by_state, ignore_index=True)

# Create bg2020 column
hh2020_load["bg2020"] = hh2020_load["state"] + hh2020_load["county"] + \
	hh2020_load["tract"] + hh2020_load["block group"]
# Rename households column
hh2020_load = hh2020_load.rename(columns={'B11001_001E': 'hh2020_bg20'})
# Keep relevant columns
hh2020_load = hh2020_load.loc[:, ["bg2020", "hh2020_bg20"]]
# Join with crosswalk 
print("Households before crosswalk:", round(hh2020_load["hh2020_bg20"].sum()))
hh2020 = pd.merge(hh2020_load, nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
hh2020['households'] = hh2020['hh2020_bg20'] * hh2020['wt_hh']
hh2020 = hh2020[["bg2010ge", "households"]]
hh2020 = hh2020.groupby('bg2010ge', as_index=False).sum()
hh2020.columns = ['bg2010', 'households']

# Add year column
hh2020["year"] = 2020

print(hh2020.head())
print(hh2020.shape)
print("Households after crosswalk:", round(hh2020["households"].sum()))

# 2021
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2021)))

hh2021_load = pd.concat(hh_by_state, ignore_index=True)

# Create bg2020 column
hh2021_load["bg2020"] = hh2021_load["state"] + hh2021_load["county"] + \
	hh2021_load["tract"] + hh2021_load["block group"]
# Rename households column
hh2021_load = hh2021_load.rename(columns={'B11001_001E': 'hh2021_bg20'})
# Keep relevant columns
hh2021_load = hh2021_load.loc[:, ["bg2020", "hh2021_bg20"]]
# Join with crosswalk 
print("Households before crosswalk:", round(hh2021_load["hh2021_bg20"].sum()))
hh2021 = pd.merge(hh2021_load, nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
hh2021['households'] = hh2021['hh2021_bg20'] * hh2021['wt_hh']
hh2021 = hh2021[["bg2010ge", "households"]]
hh2021 = hh2021.groupby('bg2010ge', as_index=False).sum()
hh2021.columns = ['bg2010', 'households']

# Add year column
hh2021["year"] = 2021

print(hh2021.head())
print(hh2021.shape)
print("Households after crosswalk:", round(hh2021["households"].sum()))

# 2022
hh_by_state = []
for i in fips_list:
	hh_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B11001_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2022)))

hh2022_load = pd.concat(hh_by_state, ignore_index=True)

# Create bg2020 column
hh2022_load["bg2020"] = hh2022_load["state"] + hh2022_load["county"] + \
	hh2022_load["tract"] + hh2022_load["block group"]
# Rename households column
hh2022_load = hh2022_load.rename(columns={'B11001_001E': 'hh2022_bg20'})
# Keep relevant columns
hh2022_load = hh2022_load.loc[:, ["state", "bg2020", "hh2022_bg20"]]
hh2022_ct = hh2022_load[hh2022_load['state'] == "09"]
hh2022_no_ct = hh2022_load[hh2022_load['state'] != "09"]

hh2022_ct.columns = ['state', 'bg2022', 'hh2022_bg20']
hh2022_ct = pd.merge(hh2022_ct, ct_xwalk2022, how = "left", left_on = "bg2022", right_on = "bg2022_ct")
hh2022_ct = hh2022_ct[["state", "bg2020", "hh2022_bg20"]]

hh2022_load = pd.concat([hh2022_no_ct, hh2022_ct])

# Join with crosswalk 
print("Households before crosswalk:", round(hh2022_load["hh2022_bg20"].sum()))
hh2022 = pd.merge(hh2022_load, nhgis_bg2020_bg2010, how = 'left', left_on = 'bg2020', right_on = 'bg2020ge')
hh2022['households'] = hh2022['hh2022_bg20'] * hh2022['wt_hh']
hh2022 = hh2022[["bg2010ge", "households"]]
hh2022 = hh2022.groupby('bg2010ge', as_index=False).sum()
hh2022.columns = ['bg2010', 'households']

# Add year column
hh2022["year"] = 2022

print(hh2022.head())
print(hh2022.shape)
print("Households after crosswalk:", round(hh2022["households"].sum()))


print("Done getting number of households for each Census block group...")

# Median Income by Census block group
print("Now getting median income in each Census block group...")
## Get count data, match on GISJOIN, then get median from distribution of binned data.
## https://forum.ipums.org/t/using-medians-instead-of-count-w-geographic-crosswalks/4269/6
# Code: B19013E
# 2000
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2000_SF3b", data_tables=["NP053A"], geog_levels=["blck_grp_090"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "median_inc2000")
	csv_file = glob.glob(intermediate_dir + "median_inc2000/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "median_inc2000/median_inc2000_load.csv")
else:
	pass

median_inc2000_load = pd.read_csv(intermediate_dir + "median_inc2000/median_inc2000_load.csv")
median_inc2000_load = median_inc2000_load[["GISJOIN", "HF6001"]]

## Join on crosswalk 
median_inc2000 = pd.merge(median_inc2000_load, nhgis_bgp2000_bg2010, how='left', left_on='GISJOIN', right_on='bgp2000gj')
median_inc2000 = pd.merge(median_inc2000, hh2000_load, how='left', left_on='GISJOIN', right_on='GISJOIN')
## Multiply by hh weight
# median_inc2000.update(median_inc2000.filter(regex='^HF5').mul(median_inc2000['wt_hh'], axis=0))
## Convert bg2010ge to string
median_inc2000['bg2010ge'] = median_inc2000['bg2010ge'].astype(str).apply(add_leading_zero)
median_inc2000 = median_inc2000[['bg2010ge', 'HF6001', 'hh_bgp2000', 'wt_hh']]
median_inc2000['households'] = median_inc2000['hh_bgp2000'] * median_inc2000['wt_hh']
median_inc2000 = median_inc2000[['bg2010ge', 'HF6001', 'households']]
median_inc2000.columns = ['bg2010', 'median_income', 'households']

median_inc2000 = median_inc2000.groupby('bg2010').apply(lambda group: 
    # Check if bg2010 is unique within the group.
    group['median_income'].iloc[0] if len(group['bg2010']) == 1 
    # If not unique, calculate the weighted average.
    else weighted_average_hh(group)
)

median_inc2000 = pd.DataFrame(median_inc2000).reset_index()
median_inc2000.columns = ['bg2010', 'median_income']

# Adjust to 2022 Dollars
# CPI: 2022 Income = 2009 Income * (2022 CPI / 2000 CPI), Apr 2022 CPI: 288.582, Apr 2000 CPI: 170.900
median_inc2000["median_income_2022dollars"] = median_inc2000['median_income'] * (288.582 / 170.900)
median_inc2000["year"] = 2000
#median_inc2000 = median_inc2000[["bg2010", "median_income_2022dollars", "year"]]
print(median_inc2000.head())
print(median_inc2000.shape)

# ## Then group by 2010 block groups
# median_inc2000 = median_inc2000.groupby('bg2010ge', as_index= False)[income_columns].sum()
# ## Rename income columns
# median_inc2000.columns = ["bg2010", "5000", "12500", "17500", "22500",
# 						  "27500", "32500", "37500", "42500",
# 						  "47500", "55000", "65000", "87500", 
# 						  "112500", "137500", "175000", "200001"]
# ## Round values so that they are integers
# numeric_cols = median_inc2000.select_dtypes(include=['number'])
# median_inc2000[numeric_cols.columns] = numeric_cols.round().astype('Int64')

# # Extract bin ranges and their counts
# median_inc2000_nobg = median_inc2000.iloc[:, 1:]
# bins = [int(col) for col in median_inc2000_nobg.columns]
# counts = []
# for i in range(0, len(median_inc2000_nobg)):
# 	counts.append(median_inc2000_nobg.iloc[i:i+1].values.flatten())

# # Create list of income values based on the bins
# income_list = []
# for i in range(0, len(counts)):
# 	income_values = []
# 	for j, bin_val in enumerate(bins):
# 		income_values.extend([bin_val] * counts[i][j])
# 	income_list.append(income_values)

# # For each income list, find the median.
# median_income_list = []
# for i in range(0, len(income_list)):
# 	median_income_list.append(np.median(income_list[i]))

# median_inc2000['median_income'] = median_income_list
# median_inc2000 = median_inc2000[["bg2010", "median_income"]]

# 2010
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2006_2010_ACS5a", data_tables=["B19013"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "median_inc2010")
	csv_file = glob.glob(intermediate_dir + "median_inc2010/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "median_inc2010/median_inc2010_load.csv")
else:
	pass

median_inc2010_load = pd.read_csv(intermediate_dir + "median_inc2010/median_inc2010_load.csv")
median_inc2010_load = median_inc2010_load[["GEOID", "JOIE001", "STUSAB"]]
median_inc2010_load.columns = ["bg2010", "median_income", "state"]
median_inc2010_load["bg2010"] = median_inc2010_load["bg2010"].str[-12:]
median_inc2010 = median_inc2010_load[median_inc2010_load['state'].isin(states_list)]
median_inc2010 = median_inc2010[["bg2010", "median_income"]]
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2010 Income * (2022 CPI / 2010 CPI), Apr 2022 CPI: 288.582, Apr 2010 CPI: 217.403
median_inc2010['median_income'] = pd.to_numeric(median_inc2010['median_income'], errors='coerce')
median_inc2010["median_income_2022dollars"] = median_inc2010['median_income'] * (288.582 / 217.403)
median_inc2010["year"] = 2010
# median_inc2010 = median_inc2010.loc[:, [
# 	"bg2010", "median_income_2022dollars", "year"]]
print(median_inc2010.head())
print(median_inc2010.shape)

# Interpolate to get 2001 to 2009
## Concatenate dataframes
median_inc2000and2010 = pd.DataFrame(pd.concat([median_inc2000, median_inc2010]))
## Get income from 2001 to 2009
median_inc2000to2010 = year_bg_interpolate(median_inc2000and2010, "bg2010", "year", "median_income_2022dollars")
median_inc2000to2010_unadj = year_bg_interpolate(median_inc2000and2010, "bg2010", "year", "median_income")
median_inc2000to2010 = pd.merge(median_inc2000to2010, median_inc2000to2010_unadj, left_on=['bg2010', 'year'], right_on=['bg2010', 'year'], how='left')
## Filter to get median_inculation for each year
median_inc2001 = median_inc2000to2010[median_inc2000to2010["year"] == 2001]
print(median_inc2001.head())
print(median_inc2001.shape)

median_inc2002 = median_inc2000to2010[median_inc2000to2010["year"] == 2002]
print(median_inc2002.head())
print(median_inc2002.shape)

median_inc2003 = median_inc2000to2010[median_inc2000to2010["year"] == 2003]
print(median_inc2003.head())
print(median_inc2003.shape)

median_inc2004 = median_inc2000to2010[median_inc2000to2010["year"] == 2004]
print(median_inc2004.head())
print(median_inc2004.shape)

median_inc2005 = median_inc2000to2010[median_inc2000to2010["year"] == 2005]
print(median_inc2005.head())
print(median_inc2005.shape)

median_inc2006 = median_inc2000to2010[median_inc2000to2010["year"] == 2006]
print(median_inc2006.head())
print(median_inc2006.shape)

median_inc2007 = median_inc2000to2010[median_inc2000to2010["year"] == 2007]
print(median_inc2007.head())
print(median_inc2007.shape)

median_inc2008 = median_inc2000to2010[median_inc2000to2010["year"] == 2008]
print(median_inc2008.head())
print(median_inc2008.shape)

median_inc2009 = median_inc2000to2010[median_inc2000to2010["year"] == 2009]
print(median_inc2009.head())
print(median_inc2009.shape)

# 2011
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2007_2011_ACS5a", data_tables=["B19013"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "median_inc2011")
	csv_file = glob.glob(intermediate_dir + "median_inc2011/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "median_inc2011/median_inc2011_load.csv")
else:
	pass

median_inc2011_load = pd.read_csv(intermediate_dir + "median_inc2011/median_inc2011_load.csv")
median_inc2011_load = median_inc2011_load[["GEOID", "MP1E001", "STUSAB"]]
median_inc2011_load.columns = ["bg2010", "median_income", "state"]
median_inc2011_load["bg2010"] = median_inc2011_load["bg2010"].str[-12:]
median_inc2011 = median_inc2011_load[median_inc2011_load['state'].isin(states_list)]
median_inc2011 = median_inc2011[["bg2010", "median_income"]]
# Adjust to 2022 Dollars
# CPI: 2022 Income = 2011 Income * (2022 CPI / 2011 CPI), Apr 2022 CPI: 288.582, Apr 2011 CPI: 224.093
median_inc2011['median_income'] = pd.to_numeric(median_inc2011['median_income'], errors='coerce')
median_inc2011["median_income_2022dollars"] = median_inc2011['median_income'] * (288.582 / 224.093)
median_inc2011["year"] = 2011
median_inc2011 = median_inc2011.loc[:, [
	"bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2011.head())
print(median_inc2011.shape)

# 2012
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2008_2012_ACS5a", data_tables=["B19013"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "median_inc2012")
	csv_file = glob.glob(intermediate_dir + "median_inc2012/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "median_inc2012/median_inc2012_load.csv")
else:
	pass
median_inc2012_load = pd.read_csv(intermediate_dir + "median_inc2012/median_inc2012_load.csv")
median_inc2012_load = median_inc2012_load[["GEOID", "QU1E001", "STUSAB"]]
median_inc2012_load.columns = ["bg2010", "median_income", "state"]
median_inc2012_load["bg2010"] = median_inc2012_load["bg2010"].str[-12:]
median_inc2012 = median_inc2012_load[median_inc2012_load['state'].isin(states_list)]
median_inc2012 = median_inc2012[["bg2010", "median_income"]]

# Adjust to 2022 Dollars
# CPI: 2022 Income = 2012 Income * (2022 CPI / 2012 CPI), Apr 2022 CPI: 288.582, Apr 2012 CPI: 229.187
median_inc2012['median_income'] = pd.to_numeric(median_inc2012['median_income'], errors='coerce')
median_inc2012["median_income_2022dollars"] = median_inc2012['median_income'] * (288.582 / 229.187)
median_inc2012["year"] = 2012
median_inc2012 = median_inc2012.loc[:, [
	"bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2012.head())
print(median_inc2012.shape)

# 2013
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2013)))

median_inc2013 = pd.concat(median_inc_by_state, ignore_index=True)

# Create bg2010 column
median_inc2013["bg2010"] = median_inc2013["state"] + median_inc2013["county"] + \
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
	"bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2013.head())
print(median_inc2013.shape)

# 2014
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2014)))

median_inc2014 = pd.concat(median_inc_by_state, ignore_index=True)

# Create bg2010 column
median_inc2014["bg2010"] = median_inc2014["state"] + median_inc2014["county"] + \
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
	"bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2014.head())
print(median_inc2014.shape)

# 2015
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2015)))

median_inc2015 = pd.concat(median_inc_by_state, ignore_index=True)

# Create bg2010 column
median_inc2015["bg2010"] = median_inc2015["state"] + median_inc2015["county"] + \
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
	"bg2010","median_income", "median_income_2022dollars", "year"]]
print(median_inc2015.head())
print(median_inc2015.shape)

# 2016
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2016)))

median_inc2016 = pd.concat(median_inc_by_state, ignore_index=True)

# Create bg2010 column
median_inc2016["bg2010"] = median_inc2016["state"] + median_inc2016["county"] + \
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
	"bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2016.head())
print(median_inc2016.shape)

# 2017
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2017)))

median_inc2017 = pd.concat(median_inc_by_state, ignore_index=True)

# Create bg2010 column
median_inc2017["bg2010"] = median_inc2017["state"] + median_inc2017["county"] + \
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
	"bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2017.head())
print(median_inc2017.shape)

# 2018
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2018)))

median_inc2018 = pd.concat(median_inc_by_state, ignore_index=True)

# Create bg2010 column
median_inc2018["bg2010"] = median_inc2018["state"] + median_inc2018["county"] + \
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
	"bg2010","median_income", "median_income_2022dollars", "year"]]
print(median_inc2018.head())
print(median_inc2018.shape)


# 2019
median_inc_by_state = []
for i in fips_list:
	median_inc_by_state.append(pd.DataFrame(c.acs5.state_county_blockgroup(
		('NAME', 'B19013_001E'), state_fips=i, county_fips='*', blockgroup='*', year=2019)))

median_inc2019 = pd.concat(median_inc_by_state, ignore_index=True)

# Create bg2010 column
median_inc2019["bg2010"] = median_inc2019["state"] + median_inc2019["county"] + \
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
	"bg2010","median_income", "median_income_2022dollars", "year"]]
print(median_inc2019.head())
print(median_inc2019.shape)

# 2020
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2016_2020_ACS5a", data_tables=["B19013"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "median_inc2020")
	csv_file = glob.glob(intermediate_dir + "median_inc2020/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "median_inc2020/median_inc2020_load.csv")
else:
	pass

median_inc2020_load = pd.read_csv(intermediate_dir + "median_inc2020/median_inc2020_load.csv")
median_inc2020 = pd.merge(median_inc2020_load, nhgis_bg2020_bg2010, how='left', left_on='GISJOIN', right_on='bg2020gj')
median_inc2020 = pd.merge(median_inc2020, hh2020_load, how = "left", left_on = "bg2020ge", right_on = "bg2020")
median_inc2020 = median_inc2020[median_inc2020['STUSAB'].isin(states_list)]
## Convert bg2010ge to string
median_inc2020['bg2010ge'] = median_inc2020['bg2010ge'].astype(str).apply(add_leading_zero)
median_inc2020 = median_inc2020[['bg2010ge', 'AMR8E001', 'hh2020_bg20', 'wt_hh']]
median_inc2020['households'] = median_inc2020['hh2020_bg20'] * median_inc2020['wt_hh']
median_inc2020 = median_inc2020[['bg2010ge', 'AMR8E001', 'households']]
median_inc2020.columns = ['bg2010', 'median_income', 'households']


median_inc2020 = median_inc2020.groupby('bg2010').apply(lambda group: 
    # Check if bg2010 is unique within the group.
    group['median_income'].iloc[0] if len(group['bg2010']) == 1 
    # If not unique, calculate the weighted average.
    else weighted_average_hh(group)
)

median_inc2020 = pd.DataFrame(median_inc2020).reset_index()
median_inc2020.columns = ['bg2010', 'median_income']

median_inc2020["year"] = 2020

# Adjust to 2022 Dollars
# CPI: 2022 Income = 2009 Income * (2022 CPI / 2020 CPI), Apr 2022 CPI: 288.582, Apr 2020 CPI: 256.032
median_inc2020["median_income_2022dollars"] = median_inc2020['median_income'] * (288.582 / 256.032)
median_inc2020["year"] = 2020
median_inc2020 = median_inc2020[["bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2020.head())
print(median_inc2020.shape)

# income_columns = [col for col in median_inc2020_load if col.startswith('AMR7E')]
# income_columns = income_columns[1:]
# use_columns = ["GISJOIN", "STUSAB"] + income_columns
# median_inc2020_load = median_inc2020_load[use_columns]
# median_inc2020_load = median_inc2020_load[median_inc2020_load['STUSAB'].isin(states_list)]

# ## Join on crosswalk 
# median_inc2020 = pd.merge(median_inc2020_load, nhgis_bg2020_bg2010, how='left', left_on='GISJOIN', right_on='bg2020gj')
# ## Multiply by hh weight
# median_inc2020.update(median_inc2020.filter(regex='^AMR7E').mul(median_inc2020['wt_hh'], axis=0))
# ## Convert bg2010ge to string
# median_inc2020['bg2010ge'] = median_inc2020['bg2010ge'].astype(str).apply(add_leading_zero)
# ## Then group by 2010 block groups
# median_inc2020 = median_inc2020.groupby('bg2010ge', as_index= False)[income_columns].sum()
# ## Rename income columns
# median_inc2020.columns = ["bg2010", "5000", "12500", "17500", "22500",
# 						  "27500", "32500", "37500", "42500",
# 						  "47500", "55000", "65000", "87500", 
# 						  "112500", "137500", "175000", "200001"]
# ## Round values so that they are integers
# numeric_cols = median_inc2020.select_dtypes(include=['number'])
# median_inc2020[numeric_cols.columns] = numeric_cols.round().astype('Int64')

# # Extract bin ranges and their counts
# median_inc2020_nobg = median_inc2020.iloc[:, 1:]
# bins = [int(col) for col in median_inc2020_nobg.columns]
# counts = []
# for i in range(0, len(median_inc2020_nobg)):
# 	counts.append(median_inc2020_nobg.iloc[i:i+1].values.flatten())

# # Create list of income values based on the bins
# income_list = []
# for i in range(0, len(counts)):
# 	income_values = []
# 	for j, bin_val in enumerate(bins):
# 		income_values.extend([bin_val] * counts[i][j])
# 	income_list.append(income_values)

# # For each income list, find the median.
# median_income_list = []
# for i in range(0, len(income_list)):
# 	median_income_list.append(np.median(income_list[i]))

# median_inc2020['median_income'] = median_income_list
# median_inc2020 = median_inc2020[["bg2010", "median_income"]]

# 2021
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2017_2021_ACS5a", data_tables=["B19013"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "median_inc2021")
	csv_file = glob.glob(intermediate_dir + "median_inc2021/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "median_inc2021/median_inc2021_load.csv")
else:
	pass

median_inc2021_load = pd.read_csv(intermediate_dir + "median_inc2021/median_inc2021_load.csv")

## Join on crosswalk 
median_inc2021 = pd.merge(median_inc2021_load, nhgis_bg2020_bg2010, how='left', left_on='GISJOIN', right_on='bg2020gj')
median_inc2021 = pd.merge(median_inc2021, hh2021_load, how = "left", left_on = "bg2020ge", right_on = "bg2020")
median_inc2021 = median_inc2021[median_inc2021['STUSAB'].isin(states_list)]
## Convert bg2010ge to string
median_inc2021['bg2010ge'] = median_inc2021['bg2010ge'].astype(str).apply(add_leading_zero)
median_inc2021 = median_inc2021[['bg2010ge', 'AOQIE001', 'hh2021_bg20', 'wt_hh']]
median_inc2021['households'] = median_inc2021['hh2021_bg20'] * median_inc2021['wt_hh']
median_inc2021 = median_inc2021[['bg2010ge', 'AOQIE001', 'households']]
median_inc2021.columns = ['bg2010', 'median_income', 'households']

median_inc2021 = median_inc2021.groupby('bg2010').apply(lambda group: 
    # Check if bg2010 is unique within the group.
    group['median_income'].iloc[0] if len(group['bg2010']) == 1 
    # If not unique, calculate the weighted average.
    else weighted_average_hh(group)
)

median_inc2021 = pd.DataFrame(median_inc2021).reset_index()
median_inc2021.columns = ['bg2010', 'median_income']

median_inc2021["year"] = 2021

# Adjust to 2022 Dollars
# CPI: 2022 Income = 2021 Income * (2022 CPI / 2021 CPI), Apr 2022 CPI: 288.582, Apr 2021 CPI: 266.625
median_inc2021["median_income_2022dollars"] = median_inc2021["median_income"] * \
	(288.582 / 266.625)
# Add year column
median_inc2021["year"] = 2021
# Keep relevant columns
median_inc2021 = median_inc2021.loc[:, [
	"bg2010", "median_income", "median_income_2022dollars", "year"]]
print(median_inc2021.head())
print(median_inc2021.shape)

# 2022
if need_to_pull:
	extract = AggregateDataExtract(
	collection="nhgis",
	datasets=[
		Dataset(name="2018_2022_ACS5a", data_tables=["B19013"], geog_levels=["blck_grp"])
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

	os.rename(latest_file_no_zip, intermediate_dir + "median_inc2022")
	csv_file = glob.glob(intermediate_dir + "median_inc2022/" "*.csv")
	os.rename(csv_file[0], intermediate_dir + "median_inc2022/median_inc2022_load.csv")
else:
	pass

median_inc2022_load = pd.read_csv(intermediate_dir + "median_inc2022/median_inc2022_load.csv")
median_inc2022 = median_inc2022_load[median_inc2022_load['STUSAB'].isin(states_list)]

median_inc2022_ct = median_inc2022[median_inc2022['STUSAB'] == 'CT']
median_inc2022_ct['bg2022'] = median_inc2022_ct['TL_GEO_ID'].astype(str).apply(add_leading_zero)
median_inc2022_ct = pd.merge(median_inc2022_ct, ct_xwalk2022, how = "left", left_on = "bg2022", right_on = "bg2022_ct")
median_inc2022_ct = median_inc2022_ct.drop(['bg2022', 'bg2022_ct'], axis = 1)

median_inc2022_no_ct = median_inc2022[median_inc2022['STUSAB'] != 'CT']
median_inc2022_no_ct['bg2020'] = median_inc2022_no_ct['TL_GEO_ID'].astype(str).apply(add_leading_zero)

median_inc2022_load = pd.concat([median_inc2022_no_ct, median_inc2022_ct])

## Join on crosswalk 
median_inc2022 = pd.merge(median_inc2022_load, nhgis_bg2020_bg2010, how='left', left_on='bg2020', right_on='bg2020ge')
median_inc2022 = pd.merge(median_inc2022, hh2022_load, how='left', left_on='bg2020', right_on='bg2020')

## Convert bg2010ge to string
median_inc2022['bg2010ge'] = median_inc2022['bg2010ge'].astype(str).apply(add_leading_zero)
median_inc2022 = median_inc2022[['bg2010ge', 'AQP6E001', 'wt_hh', 'hh2022_bg20']]
median_inc2022['households'] = median_inc2022['hh2022_bg20'] * median_inc2022['wt_hh']
median_inc2022 = median_inc2022[['bg2010ge', 'AQP6E001', 'households']]
median_inc2022.columns = ['bg2010', 'median_income', 'households']

median_inc2022 = median_inc2022.groupby('bg2010').apply(lambda group: 
    # Check if bg2010 is unique within the group.
    group['median_income'].iloc[0] if len(group['bg2010']) == 1 
    # If not unique, calculate the weighted average.
    else weighted_average_hh(group)
)

median_inc2022 = pd.DataFrame(median_inc2022).reset_index()
median_inc2022.columns = ['bg2010', 'median_income']

median_inc2022 = median_inc2022[["bg2010", "median_income"]]
median_inc2022['median_income_2022dollars'] = median_inc2022["median_income"]

median_inc2022['year'] = 2022

print(median_inc2022.head())
print(median_inc2022.shape)


### Examine Median Income
median_inc_all = pd.DataFrame(pd.concat([median_inc2000to2010, median_inc2011, median_inc2012,
median_inc2013, median_inc2014, median_inc2015, median_inc2016, median_inc2017, median_inc2018,
median_inc2019, median_inc2020, median_inc2021, median_inc2022]))

# Replace NA with 0
median_inc_all[median_inc_all[["median_income_2022dollars", "median_income"]] < 0] = np.nan

# Sort the DataFrame by 'id' and 'year'
median_inc_all = median_inc_all.sort_values(by=['bg2010', 'year'])

# Find the bgs with NaN
median_inc_all_nan = median_inc_all[median_inc_all['median_income'].isna()]
bg_nan = median_inc_all_nan['bg2010'].unique()
median_inc_all_nan = median_inc_all[median_inc_all['bg2010'].isin(bg_nan)]
# Interpolate
median_inc_interpolated = median_inc_all_nan.groupby('bg2010').apply(lambda group: group.interpolate())
median_inc_interpolated = median_inc_interpolated.reset_index(drop = True)

# Find the bgs with no NaN
median_inc_all_no_nan = ~median_inc_all['bg2010'].isin(bg_nan)
median_inc_all_no_nan = median_inc_all[median_inc_all_no_nan]

# Stack them together
median_inc_all_final = pd.concat([median_inc_interpolated, median_inc_all_no_nan])
median_inc_all_final = median_inc_all_final.sort_values(by=['bg2010', 'year'])

median_inc_all_ex = median_inc_all_final[median_inc_all_final['bg2010'] == random.choice(bg_nan)]
print(median_inc_all_ex)

# Now break out by year
median_inc2000 = median_inc_all_final[median_inc_all_final["year"] == 2000]
print(median_inc2000.head())
print(median_inc2000.shape)

median_inc2001 = median_inc_all_final[median_inc_all_final["year"] == 2001]
print(median_inc2001.head())
print(median_inc2001.shape)

median_inc2002 = median_inc_all_final[median_inc_all_final["year"] == 2002]
print(median_inc2002.head())
print(median_inc2002.shape)

median_inc2003 = median_inc_all_final[median_inc_all_final["year"] == 2003]
print(median_inc2003.head())
print(median_inc2003.shape)

median_inc2004 = median_inc_all_final[median_inc_all_final["year"] == 2004]
print(median_inc2004.head())
print(median_inc2004.shape)

median_inc2005 = median_inc_all_final[median_inc_all_final["year"] == 2005]
print(median_inc2005.head())
print(median_inc2005.shape)

median_inc2006 = median_inc_all_final[median_inc_all_final["year"] == 2006]
print(median_inc2006.head())
print(median_inc2006.shape)

median_inc2007 = median_inc_all_final[median_inc_all_final["year"] == 2007]
print(median_inc2007.head())
print(median_inc2007.shape)

median_inc2008 = median_inc_all_final[median_inc_all_final["year"] == 2008]
print(median_inc2008.head())
print(median_inc2008.shape)

median_inc2009 = median_inc_all_final[median_inc_all_final["year"] == 2009]
print(median_inc2009.head())
print(median_inc2009.shape)

median_inc2010 = median_inc_all_final[median_inc_all_final["year"] == 2010]
print(median_inc2010.head())
print(median_inc2010.shape)

median_inc2011 = median_inc_all_final[median_inc_all_final["year"] == 2011]
print(median_inc2011.head())
print(median_inc2011.shape)

median_inc2012 = median_inc_all_final[median_inc_all_final["year"] == 2012]
print(median_inc2012.head())
print(median_inc2012.shape)

median_inc2013 = median_inc_all_final[median_inc_all_final["year"] == 2013]
print(median_inc2013.head())
print(median_inc2013.shape)

median_inc2014 = median_inc_all_final[median_inc_all_final["year"] == 2014]
print(median_inc2014.head())
print(median_inc2014.shape)

median_inc2015 = median_inc_all_final[median_inc_all_final["year"] == 2015]
print(median_inc2015.head())
print(median_inc2015.shape)

median_inc2016 = median_inc_all_final[median_inc_all_final["year"] == 2016]
print(median_inc2016.head())
print(median_inc2016.shape)

median_inc2017 = median_inc_all_final[median_inc_all_final["year"] == 2017]
print(median_inc2017.head())
print(median_inc2017.shape)

median_inc2018 = median_inc_all_final[median_inc_all_final["year"] == 2018]
print(median_inc2018.head())
print(median_inc2018.shape)

median_inc2019 = median_inc_all_final[median_inc_all_final["year"] == 2019]
print(median_inc2019.head())
print(median_inc2019.shape)

median_inc2020 = median_inc_all_final[median_inc_all_final["year"] == 2020]
print(median_inc2020.head())
print(median_inc2020.shape)

median_inc2021 = median_inc_all_final[median_inc_all_final["year"] == 2021]
print(median_inc2021.head())
print(median_inc2021.shape)

median_inc2022 = median_inc_all_final[median_inc_all_final["year"] == 2022]
print(median_inc2022.head())
print(median_inc2022.shape)


print("Done getting median income in each Census block group...")
print("Now combining it all together...")
# Combine Data Together
# Population, Households, Median Income, White Population, Land Area
# 2000
pop2000.shape
hh2000.shape
median_inc2000.shape
pop_nhwhite2000.shape
aland2010.shape

acs2000 = pop2000.merge(hh2000, on=['bg2010', 'year'], how='left').merge(median_inc2000, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2000, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2000['pop_den_acre'] = acs2000["population"] / acs2000["area_acres"]
acs2000['hh_den_acre'] = acs2000["households"] / acs2000["area_acres"]
acs2000['pct_minority'] = 1 - (acs2000["pop_nhwhite"] / acs2000["population"])
acs2000 = acs2000[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2000.head())
print(acs2000.shape)
acs2000.to_csv("./outputs/acs2000.csv", index = False)

# 2001
pop2001.shape
hh2001.shape
median_inc2001.shape
pop_nhwhite2001.shape
aland2010.shape

acs2001 = pop2001.merge(hh2001, on=['bg2010', 'year'], how='left').merge(median_inc2001, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2001, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2001['pop_den_acre'] = acs2001["population"] / acs2001["area_acres"]
acs2001['hh_den_acre'] = acs2001["households"] / acs2001["area_acres"]
acs2001['pct_minority'] = 1 - (acs2001["pop_nhwhite"] / acs2001["population"])
acs2001 = acs2001[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2001.head())
print(acs2001.shape)
acs2001.to_csv("./outputs/acs2001.csv", index = False)

# 2002
pop2002.shape
hh2002.shape
median_inc2002.shape
pop_nhwhite2002.shape
aland2010.shape

acs2002 = pop2002.merge(hh2002, on=['bg2010', 'year'], how='left').merge(median_inc2002, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2002, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2002['pop_den_acre'] = acs2002["population"] / acs2002["area_acres"]
acs2002['hh_den_acre'] = acs2002["households"] / acs2002["area_acres"]
acs2002['pct_minority'] = 1 - (acs2002["pop_nhwhite"] / acs2002["population"])
acs2002 = acs2002[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2002.head())
print(acs2002.shape)
acs2002.to_csv("./outputs/acs2002.csv", index = False)

# 2003
pop2003.shape
hh2003.shape
median_inc2003.shape
pop_nhwhite2003.shape
aland2010.shape

acs2003 = pop2003.merge(hh2003, on=['bg2010', 'year'], how='left').merge(median_inc2003, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2003, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2003['pop_den_acre'] = acs2003["population"] / acs2003["area_acres"]
acs2003['hh_den_acre'] = acs2003["households"] / acs2003["area_acres"]
acs2003['pct_minority'] = 1 - (acs2003["pop_nhwhite"] / acs2003["population"])
acs2003 = acs2003[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2003.head())
print(acs2003.shape)
acs2003.to_csv("./outputs/acs2003.csv", index = False)

# 2004
pop2004.shape
hh2004.shape
median_inc2004.shape
pop_nhwhite2004.shape
aland2010.shape

acs2004 = pop2004.merge(hh2004, on=['bg2010', 'year'], how='left').merge(median_inc2004, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2004, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2004['pop_den_acre'] = acs2004["population"] / acs2004["area_acres"]
acs2004['hh_den_acre'] = acs2004["households"] / acs2004["area_acres"]
acs2004['pct_minority'] = 1 - (acs2004["pop_nhwhite"] / acs2004["population"])
acs2004 = acs2004[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2004.head())
print(acs2004.shape)
acs2004.to_csv("./outputs/acs2004.csv", index = False)

# 2005
pop2005.shape
hh2005.shape
median_inc2005.shape
pop_nhwhite2005.shape
aland2010.shape

acs2005 = pop2005.merge(hh2005, on=['bg2010', 'year'], how='left').merge(median_inc2005, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2005, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2005['pop_den_acre'] = acs2005["population"] / acs2005["area_acres"]
acs2005['hh_den_acre'] = acs2005["households"] / acs2005["area_acres"]
acs2005['pct_minority'] = 1 - (acs2005["pop_nhwhite"] / acs2005["population"])
acs2005 = acs2005[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2005.head())
print(acs2005.shape)
acs2005.to_csv("./outputs/acs2005.csv", index = False)

# 2006
pop2006.shape
hh2006.shape
median_inc2006.shape
pop_nhwhite2006.shape
aland2010.shape

acs2006 = pop2006.merge(hh2006, on=['bg2010', 'year'], how='left').merge(median_inc2006, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2006, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2006['pop_den_acre'] = acs2006["population"] / acs2006["area_acres"]
acs2006['hh_den_acre'] = acs2006["households"] / acs2006["area_acres"]
acs2006['pct_minority'] = 1 - (acs2006["pop_nhwhite"] / acs2006["population"])
acs2006 = acs2006[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2006.head())
print(acs2006.shape)
acs2006.to_csv("./outputs/acs2006.csv", index = False)

# 2007
pop2007.shape
hh2007.shape
median_inc2007.shape
pop_nhwhite2007.shape
aland2010.shape

acs2007 = pop2007.merge(hh2007, on=['bg2010', 'year'], how='left').merge(median_inc2007, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2007, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2007['pop_den_acre'] = acs2007["population"] / acs2007["area_acres"]
acs2007['hh_den_acre'] = acs2007["households"] / acs2007["area_acres"]
acs2007['pct_minority'] = 1 - (acs2007["pop_nhwhite"] / acs2007["population"])
acs2007 = acs2007[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2007.head())
print(acs2007.shape)
acs2007.to_csv("./outputs/acs2007.csv", index = False)

# 2008
pop2008.shape
hh2008.shape
median_inc2008.shape
pop_nhwhite2008.shape
aland2010.shape

acs2008 = pop2008.merge(hh2008, on=['bg2010', 'year'], how='left').merge(median_inc2008, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2008, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2008['pop_den_acre'] = acs2008["population"] / acs2008["area_acres"]
acs2008['hh_den_acre'] = acs2008["households"] / acs2008["area_acres"]
acs2008['pct_minority'] = 1 - (acs2008["pop_nhwhite"] / acs2008["population"])
acs2008 = acs2008[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2008.head())
print(acs2008.shape)
acs2008.to_csv("./outputs/acs2008.csv", index = False)

# 2009
pop2009.shape
hh2009.shape
median_inc2009.shape
pop_nhwhite2009.shape
aland2010.shape

acs2009 = pop2009.merge(hh2009, on=['bg2010', 'year'], how='left').merge(median_inc2009, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2009, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2009['pop_den_acre'] = acs2009["population"] / acs2009["area_acres"]
acs2009['hh_den_acre'] = acs2009["households"] / acs2009["area_acres"]
acs2009['pct_minority'] = 1 - (acs2009["pop_nhwhite"] / acs2009["population"])
acs2009 = acs2009[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2009.head())
print(acs2009.shape)
acs2009.to_csv("./outputs/acs2009.csv", index = False)

# 2010
pop2010.shape
hh2010.shape
median_inc2010.shape
pop_nhwhite2010.shape
aland2010.shape

acs2010 = pop2010.merge(hh2010, on=['bg2010', 'year'], how='left').merge(median_inc2010, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2010, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2010['pop_den_acre'] = acs2010["population"] / acs2010["area_acres"]
acs2010['hh_den_acre'] = acs2010["households"] / acs2010["area_acres"]
acs2010['pct_minority'] = 1 - (acs2010["pop_nhwhite"] / acs2010["population"])
acs2010 = acs2010[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2010.head())
print(acs2010.shape)
acs2010.to_csv("./outputs/acs2010.csv", index = False)

# 2011
pop2011.shape
hh2011.shape
median_inc2011.shape
pop_nhwhite2011.shape
aland2010.shape

acs2011 = pop2011.merge(hh2011, on=['bg2010', 'year'], how='left').merge(median_inc2011, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2011, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2011['pop_den_acre'] = acs2011["population"] / acs2011["area_acres"]
acs2011['hh_den_acre'] = acs2011["households"] / acs2011["area_acres"]
acs2011['pct_minority'] = 1 - (acs2011["pop_nhwhite"] / acs2011["population"])
acs2011 = acs2011[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2011.head())
print(acs2011.shape)
acs2011.to_csv("./outputs/acs2011.csv", index = False)

# 2012
pop2012.shape
hh2012.shape
median_inc2012.shape
pop_nhwhite2012.shape
aland2010.shape

acs2012 = pop2012.merge(hh2012, on=['bg2010', 'year'], how='left').merge(median_inc2012, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2012, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2012['pop_den_acre'] = acs2012["population"] / acs2012["area_acres"]
acs2012['hh_den_acre'] = acs2012["households"] / acs2012["area_acres"]
acs2012['pct_minority'] = 1 - (acs2012["pop_nhwhite"] / acs2012["population"])
acs2012 = acs2012[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2012.head())
print(acs2012.shape)
acs2012.to_csv("./outputs/acs2012.csv", index = False)

# 2013
pop2013.shape
hh2013.shape
median_inc2013.shape
pop_nhwhite2013.shape
aland2010.shape

acs2013 = pop2013.merge(hh2013, on=['bg2010', 'year'], how='left').merge(median_inc2013, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2013, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2013['pop_den_acre'] = acs2013["population"] / acs2013["area_acres"]
acs2013['hh_den_acre'] = acs2013["households"] / acs2013["area_acres"]
acs2013['pct_minority'] = 1 - (acs2013["pop_nhwhite"] / acs2013["population"])
acs2013 = acs2013[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2013.head())
print(acs2013.shape)
acs2013.to_csv("./outputs/acs2013.csv", index = False)

# 2014
pop2014.shape
hh2014.shape
median_inc2014.shape
pop_nhwhite2014.shape
aland2010.shape

acs2014 = pop2014.merge(hh2014, on=['bg2010', 'year'], how='left').merge(median_inc2014, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2014, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2014['pop_den_acre'] = acs2014["population"] / acs2014["area_acres"]
acs2014['hh_den_acre'] = acs2014["households"] / acs2014["area_acres"]
acs2014['pct_minority'] = 1 - (acs2014["pop_nhwhite"] / acs2014["population"])
acs2014 = acs2014[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2014.head())
print(acs2014.shape)
acs2014.to_csv("./outputs/acs2014.csv", index = False)

# 2015
pop2015.shape
hh2015.shape
median_inc2015.shape
pop_nhwhite2015.shape
aland2010.shape

acs2015 = pop2015.merge(hh2015, on=['bg2010', 'year'], how='left').merge(median_inc2015, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2015, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2015['pop_den_acre'] = acs2015["population"] / acs2015["area_acres"]
acs2015['hh_den_acre'] = acs2015["households"] / acs2015["area_acres"]
acs2015['pct_minority'] = 1 - (acs2015["pop_nhwhite"] / acs2015["population"])
acs2015 = acs2015[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2015.head())
print(acs2015.shape)
acs2015.to_csv("./outputs/acs2015.csv", index = False)

# 2016
pop2016.shape
hh2016.shape
median_inc2016.shape
pop_nhwhite2016.shape
aland2010.shape

acs2016 = pop2016.merge(hh2016, on=['bg2010', 'year'], how='left').merge(median_inc2016, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2016, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2016['pop_den_acre'] = acs2016["population"] / acs2016["area_acres"]
acs2016['hh_den_acre'] = acs2016["households"] / acs2016["area_acres"]
acs2016['pct_minority'] = 1 - (acs2016["pop_nhwhite"] / acs2016["population"])
acs2016 = acs2016[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre", "median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2016.head())
print(acs2016.shape)
acs2016.to_csv("./outputs/acs2016.csv", index = False)

# 2017
pop2017.shape
hh2017.shape
median_inc2017.shape
pop_nhwhite2017.shape
aland2010.shape

acs2017 = pop2017.merge(hh2017, on=['bg2010', 'year'], how='left').merge(median_inc2017, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2017, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2017['pop_den_acre'] = acs2017["population"] / acs2017["area_acres"]
acs2017['hh_den_acre'] = acs2017["households"] / acs2017["area_acres"]
acs2017['pct_minority'] = 1 - (acs2017["pop_nhwhite"] / acs2017["population"])
acs2017 = acs2017[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre", "median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2017.head())
print(acs2017.shape)
acs2017.to_csv("./outputs/acs2017.csv", index = False)

# 2018
pop2018.shape
hh2018.shape
median_inc2018.shape
pop_nhwhite2018.shape
aland2010.shape

acs2018 = pop2018.merge(hh2018, on=['bg2010', 'year'], how='left').merge(median_inc2018, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2018, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2018['pop_den_acre'] = acs2018["population"] / acs2018["area_acres"]
acs2018['hh_den_acre'] = acs2018["households"] / acs2018["area_acres"]
acs2018['pct_minority'] = 1 - (acs2018["pop_nhwhite"] / acs2018["population"])
acs2018 = acs2018[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre", "median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2018.head())
print(acs2018.shape)
acs2018.to_csv("./outputs/acs2018.csv", index = False)

# 2019
pop2019.shape
hh2019.shape
median_inc2019.shape
pop_nhwhite2019.shape
aland2010.shape

acs2019 = pop2019.merge(hh2019, on=['bg2010', 'year'], how='left').merge(median_inc2019, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2019, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2019['pop_den_acre'] = acs2019["population"] / acs2019["area_acres"]
acs2019['hh_den_acre'] = acs2019["households"] / acs2019["area_acres"]
acs2019['pct_minority'] = 1 - (acs2019["pop_nhwhite"] / acs2019["population"])
acs2019 = acs2019[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre", "median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2019.head())
print(acs2019.shape)
acs2019.to_csv("./outputs/acs2019.csv", index = False)

# 2020
pop2020.shape
hh2020.shape
median_inc2020.shape
pop_nhwhite2020.shape
aland2010.shape

acs2020 = pop2020.merge(hh2020, on=['bg2010', 'year'], how='left').merge(median_inc2020, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2020, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2020['pop_den_acre'] = acs2020["population"] / acs2020["area_acres"]
acs2020['hh_den_acre'] = acs2020["households"] / acs2020["area_acres"]
acs2020['pct_minority'] = 1 - (acs2020["pop_nhwhite"] / acs2020["population"])
acs2020 = acs2020[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre","median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2020.head())
print(acs2020.shape)
acs2020.to_csv("./outputs/acs2020.csv", index = False)

# 2021
pop2021.shape
hh2021.shape
median_inc2021.shape
pop_nhwhite2021.shape
aland2010.shape

acs2021 = pop2021.merge(hh2021, on=['bg2010', 'year'], how='left').merge(median_inc2021, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2021, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2021['pop_den_acre'] = acs2021["population"] / acs2021["area_acres"]
acs2021['hh_den_acre'] = acs2021["households"] / acs2021["area_acres"]
acs2021['pct_minority'] = 1 - (acs2021["pop_nhwhite"] / acs2021["population"])
acs2021 = acs2021[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre", "median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2021.head())
print(acs2021.shape)
acs2021.to_csv("./outputs/acs2021.csv", index = False)

# 2022
pop2022.shape
hh2022.shape
median_inc2022.shape
pop_nhwhite2022.shape
aland2010.shape

acs2022 = pop2022.merge(hh2022, on=['bg2010', 'year'], how='left').merge(median_inc2022, on=['bg2010', 'year'], how='left').merge(
	pop_nhwhite2022, on=['bg2010', 'year'], how="left").merge(aland2010, on=['bg2010'],  how="left")
acs2022['pop_den_acre'] = acs2022["population"] / acs2022["area_acres"]
acs2022['hh_den_acre'] = acs2022["households"] / acs2022["area_acres"]
acs2022['pct_minority'] = 1 - (acs2022["pop_nhwhite"] / acs2022["population"])
acs2022 = acs2022[["bg2010", "area_sqmi", "area_acres", "hh_den_acre", "pop_den_acre", "median_income", "median_income_2022dollars", "pct_minority", "year"]]
print(acs2022.head())
print(acs2022.shape)
acs2022.to_csv("./outputs/acs2022.csv", index = False)

print("All done, check outputs for files...")
