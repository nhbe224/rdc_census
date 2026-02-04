# Get jobs within 30 minutes of driving by Census tract

# Load Libraries
import os
import numpy as np
import pandas as pd
import census
import us
from census import Census
import geopandas as gpd
from us import states
import pygris
import requests
import io
import zipfile
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Set working directory
os.chdir('D:/neeco/rdc_census/travel_times/')

# Read in tract to tract travel times
travel_times_load = pd.read_stata("../to_rdc/travel_times.dta")

# Read in crosswalk files
nhgis_blk2000_tr2010 = pd.read_csv("./inputs/nhgis_blk2000_tr2010.csv")
nhgis_blk2020_tr2010 = pd.read_csv("./inputs/nhgis_blk2020_tr2010.csv")

# Helper functions
def add_leading_zero(value):
	if len(value) == 10 or len(value) == 14:
		return '0' + value
	else:
		return value
	
# Edit crosswalk files
nhgis_blk2000_tr2010['blk2000ge'] = nhgis_blk2000_tr2010['blk2000ge'].astype(str).apply(add_leading_zero)
nhgis_blk2000_tr2010['tr2010ge'] = nhgis_blk2000_tr2010['tr2010ge'].astype(str).apply(add_leading_zero)

nhgis_blk2020_tr2010['blk2020ge'] = nhgis_blk2020_tr2010['blk2020ge'].astype(str).apply(add_leading_zero)
nhgis_blk2020_tr2010['tr2010ge'] = nhgis_blk2020_tr2010['tr2010ge'].astype(str).apply(add_leading_zero)

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

# States list (lowercase, needed for pasting into URL)
states_list = list(fips_lookup["state"])
states_list = [state.lower() for state in states_list]
states_list

# Clean travel times data so it's 30 minutes and under
travel_times = travel_times_load[['home_tr2010', 'work_tr2010', 'minutes']]
travel_times = travel_times[travel_times['minutes'] <= 30]
travel_times['home_tr2010'] = travel_times['home_tr2010'].astype(str).apply(add_leading_zero)
travel_times['work_tr2010'] = travel_times['work_tr2010'].astype(str).apply(add_leading_zero)
travel_times.to_csv("./travel_times_for_m2.csv", index = False)

# This only goes from 2002 to 2023
# 2002
print("Getting 2002 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2002.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2002 = emp_load.reset_index()

## Calculate employment by industry
emp_load2002["blk2000"] = emp_load2002["w_geocode"]
emp_load2002["emp_tot"] = emp_load2002["C000"]

## Select columns
emp_load2002 = emp_load2002[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2002['blk2000'] = emp_load2002['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2002 = pd.merge(emp_load2002, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2002 = emp2002[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2002[emp_columns].multiply(emp2002["weight"], axis="index")

## And sum within 2010 tracts
emp2002 = emp2002.groupby("tr2010ge", as_index=False).sum()
emp2002 = emp2002.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2002 = emp2002.drop(["weight"], axis = 1)

# Merge with travel times
emp2002 = pd.merge(travel_times, emp2002, left_on='work_tr2010', right_on='tr2010', how='left')
emp2002 = emp2002[["home_tr2010", "emp_tot"]]
emp2002 = emp2002.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2002['year'] = 2002
emp2002.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2002.head())

# 2003
print("Getting 2003 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2003.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2003 = emp_load.reset_index()

## Calculate employment by industry
emp_load2003["blk2000"] = emp_load2003["w_geocode"]
emp_load2003["emp_tot"] = emp_load2003["C000"]

## Select columns
emp_load2003 = emp_load2003[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2003['blk2000'] = emp_load2003['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2003 = pd.merge(emp_load2003, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2003 = emp2003[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2003[emp_columns].multiply(emp2003["weight"], axis="index")

## And sum within 2010 tracts
emp2003 = emp2003.groupby("tr2010ge", as_index=False).sum()
emp2003 = emp2003.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2003 = emp2003.drop(["weight"], axis = 1)

# Merge with travel times
emp2003 = pd.merge(travel_times, emp2003, left_on='work_tr2010', right_on='tr2010', how='left')
emp2003 = emp2003[["home_tr2010", "emp_tot"]]
emp2003 = emp2003.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2003['year'] = 2003
emp2003.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2003.head())


# 2004
print("Getting 2004 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2004.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2004 = emp_load.reset_index()

## Calculate employment by industry
emp_load2004["blk2000"] = emp_load2004["w_geocode"]
emp_load2004["emp_tot"] = emp_load2004["C000"]

## Select columns
emp_load2004 = emp_load2004[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2004['blk2000'] = emp_load2004['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2004 = pd.merge(emp_load2004, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2004 = emp2004[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2004[emp_columns].multiply(emp2004["weight"], axis="index")

## And sum within 2010 tracts
emp2004 = emp2004.groupby("tr2010ge", as_index=False).sum()
emp2004 = emp2004.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2004 = emp2004.drop(["weight"], axis = 1)

# Merge with travel times
emp2004 = pd.merge(travel_times, emp2004, left_on='work_tr2010', right_on='tr2010', how='left')
emp2004 = emp2004[["home_tr2010", "emp_tot"]]
emp2004 = emp2004.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2004['year'] = 2004
emp2004.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2004.head())

# 2005
print("Getting 2005 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2005.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2005 = emp_load.reset_index()

## Calculate employment by industry
emp_load2005["blk2000"] = emp_load2005["w_geocode"]
emp_load2005["emp_tot"] = emp_load2005["C000"]

## Select columns
emp_load2005 = emp_load2005[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2005['blk2000'] = emp_load2005['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2005 = pd.merge(emp_load2005, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2005 = emp2005[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2005[emp_columns].multiply(emp2005["weight"], axis="index")

## And sum within 2010 tracts
emp2005 = emp2005.groupby("tr2010ge", as_index=False).sum()
emp2005 = emp2005.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2005 = emp2005.drop(["weight"], axis = 1)

# Merge with travel times
emp2005 = pd.merge(travel_times, emp2005, left_on='work_tr2010', right_on='tr2010', how='left')
emp2005 = emp2005[["home_tr2010", "emp_tot"]]
emp2005 = emp2005.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2005['year'] = 2005
emp2005.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2005.head())


# 2006
print("Getting 2006 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2006.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2006 = emp_load.reset_index()

## Calculate employment by industry
emp_load2006["blk2000"] = emp_load2006["w_geocode"]
emp_load2006["emp_tot"] = emp_load2006["C000"]

## Select columns
emp_load2006 = emp_load2006[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2006['blk2000'] = emp_load2006['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2006 = pd.merge(emp_load2006, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2006 = emp2006[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2006[emp_columns].multiply(emp2006["weight"], axis="index")

## And sum within 2010 tracts
emp2006 = emp2006.groupby("tr2010ge", as_index=False).sum()
emp2006 = emp2006.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2006 = emp2006.drop(["weight"], axis = 1)

# Merge with travel times
emp2006 = pd.merge(travel_times, emp2006, left_on='work_tr2010', right_on='tr2010', how='left')
emp2006 = emp2006[["home_tr2010", "emp_tot"]]
emp2006 = emp2006.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2006['year'] = 2006
emp2006.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2006.head())


# 2007
print("Getting 2007 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2007.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2007 = emp_load.reset_index()

## Calculate employment by industry
emp_load2007["blk2000"] = emp_load2007["w_geocode"]
emp_load2007["emp_tot"] = emp_load2007["C000"]

## Select columns
emp_load2007 = emp_load2007[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2007['blk2000'] = emp_load2007['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2007 = pd.merge(emp_load2007, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2007 = emp2007[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2007[emp_columns].multiply(emp2007["weight"], axis="index")

## And sum within 2010 tracts
emp2007 = emp2007.groupby("tr2010ge", as_index=False).sum()
emp2007 = emp2007.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2007 = emp2007.drop(["weight"], axis = 1)

# Merge with travel times
emp2007 = pd.merge(travel_times, emp2007, left_on='work_tr2010', right_on='tr2010', how='left')
emp2007 = emp2007[["home_tr2010", "emp_tot"]]
emp2007 = emp2007.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2007['year'] = 2007
emp2007.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2007.head())


# 2008
print("Getting 2008 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2008.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2008 = emp_load.reset_index()

## Calculate employment by industry
emp_load2008["blk2000"] = emp_load2008["w_geocode"]
emp_load2008["emp_tot"] = emp_load2008["C000"]

## Select columns
emp_load2008 = emp_load2008[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2008['blk2000'] = emp_load2008['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2008 = pd.merge(emp_load2008, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2008 = emp2008[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2008[emp_columns].multiply(emp2008["weight"], axis="index")

## And sum within 2010 tracts
emp2008 = emp2008.groupby("tr2010ge", as_index=False).sum()
emp2008 = emp2008.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2008 = emp2008.drop(["weight"], axis = 1)

# Merge with travel times
emp2008 = pd.merge(travel_times, emp2008, left_on='work_tr2010', right_on='tr2010', how='left')
emp2008 = emp2008[["home_tr2010", "emp_tot"]]
emp2008 = emp2008.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2008['year'] = 2008
emp2008.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2008.head())


# 2009
print("Getting 2009 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES5/' + i + '/wac/' + i + '_wac_S000_JT00_2009.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2009 = emp_load.reset_index()

## Calculate employment by industry
emp_load2009["blk2000"] = emp_load2009["w_geocode"]
emp_load2009["emp_tot"] = emp_load2009["C000"]

## Select columns
emp_load2009 = emp_load2009[["blk2000", "emp_tot"]]

## Convert blk2000 to string
emp_load2009['blk2000'] = emp_load2009['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2009 = pd.merge(emp_load2009, nhgis_blk2000_tr2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2009 = emp2009[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2009[emp_columns].multiply(emp2009["weight"], axis="index")

## And sum within 2010 tracts
emp2009 = emp2009.groupby("tr2010ge", as_index=False).sum()
emp2009 = emp2009.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2009 = emp2009.drop(["weight"], axis = 1)

# Merge with travel times
emp2009 = pd.merge(travel_times, emp2009, left_on='work_tr2010', right_on='tr2010', how='left')
emp2009 = emp2009[["home_tr2010", "emp_tot"]]
emp2009 = emp2009.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2009['year'] = 2009
emp2009.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2009.head())

# 2010
print("Getting 2010 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2010.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2010 = emp_load.reset_index()

## Calculate employment by industry
emp_load2010["blk2010"] = emp_load2010["w_geocode"]
emp_load2010["emp_tot"] = emp_load2010["C000"]

## Rename columns
emp_load2010 = emp_load2010[["blk2010", "emp_tot"]]

## Convert blk2010 to string
emp_load2010['blk2010'] = emp_load2010['blk2010'].astype(str)
## Remove last three characters
emp_load2010['tr2010'] = emp_load2010['blk2010'].str[:-4]
emp2010 = emp_load2010[['tr2010', 'emp_tot']]
## Add leading zeros
emp2010['tr2010'] = emp2010['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2010 = emp2010.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2010 = pd.merge(travel_times, emp2010, left_on='work_tr2010', right_on='tr2010', how='left')
emp2010 = emp2010[["home_tr2010", "emp_tot"]]
emp2010 = emp2010.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2010['year'] = 2010
emp2010.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2010)


# 2011
print("Getting 2011 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2011.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2011 = emp_load.reset_index()

## Calculate employment by industry
emp_load2011["blk2010"] = emp_load2011["w_geocode"]
emp_load2011["emp_tot"] = emp_load2011["C000"]

## Rename columns
emp_load2011 = emp_load2011[["blk2010", "emp_tot"]]

## Convert blk2011 to string
emp_load2011['blk2010'] = emp_load2011['blk2010'].astype(str)
## Remove last three characters
emp_load2011['tr2010'] = emp_load2011['blk2010'].str[:-4]
emp2011 = emp_load2011[['tr2010', 'emp_tot']]
## Add leading zeros
emp2011['tr2010'] = emp2011['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2011 = emp2011.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2011 = pd.merge(travel_times, emp2011, left_on='work_tr2010', right_on='tr2010', how='left')
emp2011 = emp2011[["home_tr2010", "emp_tot"]]
emp2011 = emp2011.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2011['year'] = 2011
emp2011.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2011)

# 2012
print("Getting 2012 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2012.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2012 = emp_load.reset_index()

## Calculate employment by industry
emp_load2012["blk2010"] = emp_load2012["w_geocode"]
emp_load2012["emp_tot"] = emp_load2012["C000"]

## Rename columns
emp_load2012 = emp_load2012[["blk2010", "emp_tot"]]

## Convert blk2012 to string
emp_load2012['blk2010'] = emp_load2012['blk2010'].astype(str)
## Remove last three characters
emp_load2012['tr2010'] = emp_load2012['blk2010'].str[:-4]
emp2012 = emp_load2012[['tr2010', 'emp_tot']]
## Add leading zeros
emp2012['tr2010'] = emp2012['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2012 = emp2012.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2012 = pd.merge(travel_times, emp2012, left_on='work_tr2010', right_on='tr2010', how='left')
emp2012 = emp2012[["home_tr2010", "emp_tot"]]
emp2012 = emp2012.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2012['year'] = 2012
emp2012.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2012)

# 2013
print("Getting 2013 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2013.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2013 = emp_load.reset_index()

## Calculate employment by industry
emp_load2013["blk2010"] = emp_load2013["w_geocode"]
emp_load2013["emp_tot"] = emp_load2013["C000"]

## Rename columns
emp_load2013 = emp_load2013[["blk2010", "emp_tot"]]

## Convert blk2013 to string
emp_load2013['blk2010'] = emp_load2013['blk2010'].astype(str)
## Remove last three characters
emp_load2013['tr2010'] = emp_load2013['blk2010'].str[:-4]
emp2013 = emp_load2013[['tr2010', 'emp_tot']]
## Add leading zeros
emp2013['tr2010'] = emp2013['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2013 = emp2013.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2013 = pd.merge(travel_times, emp2013, left_on='work_tr2010', right_on='tr2010', how='left')
emp2013 = emp2013[["home_tr2010", "emp_tot"]]
emp2013 = emp2013.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2013['year'] = 2013
emp2013.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2013)

# 2014
print("Getting 2014 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2014.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2014 = emp_load.reset_index()

## Calculate employment by industry
emp_load2014["blk2010"] = emp_load2014["w_geocode"]
emp_load2014["emp_tot"] = emp_load2014["C000"]

## Rename columns
emp_load2014 = emp_load2014[["blk2010", "emp_tot"]]

## Convert blk2014 to string
emp_load2014['blk2010'] = emp_load2014['blk2010'].astype(str)
## Remove last three characters
emp_load2014['tr2010'] = emp_load2014['blk2010'].str[:-4]
emp2014 = emp_load2014[['tr2010', 'emp_tot']]
## Add leading zeros
emp2014['tr2010'] = emp2014['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2014 = emp2014.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2014 = pd.merge(travel_times, emp2014, left_on='work_tr2010', right_on='tr2010', how='left')
emp2014 = emp2014[["home_tr2010", "emp_tot"]]
emp2014 = emp2014.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2014['year'] = 2014
emp2014.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2014)


# 2015
print("Getting 2015 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2015.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2015 = emp_load.reset_index()

## Calculate employment by industry
emp_load2015["blk2010"] = emp_load2015["w_geocode"]
emp_load2015["emp_tot"] = emp_load2015["C000"]

## Rename columns
emp_load2015 = emp_load2015[["blk2010", "emp_tot"]]

## Convert blk2015 to string
emp_load2015['blk2010'] = emp_load2015['blk2010'].astype(str)
## Remove last three characters
emp_load2015['tr2010'] = emp_load2015['blk2010'].str[:-4]
emp2015 = emp_load2015[['tr2010', 'emp_tot']]
## Add leading zeros
emp2015['tr2010'] = emp2015['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2015 = emp2015.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2015 = pd.merge(travel_times, emp2015, left_on='work_tr2010', right_on='tr2010', how='left')
emp2015 = emp2015[["home_tr2010", "emp_tot"]]
emp2015 = emp2015.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2015['year'] = 2015
emp2015.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2015)


# 2016
print("Getting 2016 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2016.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2016 = emp_load.reset_index()

## Calculate employment by industry
emp_load2016["blk2010"] = emp_load2016["w_geocode"]
emp_load2016["emp_tot"] = emp_load2016["C000"]

## Rename columns
emp_load2016 = emp_load2016[["blk2010", "emp_tot"]]

## Convert blk2016 to string
emp_load2016['blk2010'] = emp_load2016['blk2010'].astype(str)
## Remove last three characters
emp_load2016['tr2010'] = emp_load2016['blk2010'].str[:-4]
emp2016 = emp_load2016[['tr2010', 'emp_tot']]
## Add leading zeros
emp2016['tr2010'] = emp2016['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2016 = emp2016.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2016 = pd.merge(travel_times, emp2016, left_on='work_tr2010', right_on='tr2010', how='left')
emp2016 = emp2016[["home_tr2010", "emp_tot"]]
emp2016 = emp2016.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2016['year'] = 2016
emp2016.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2016)

# 2017
print("Getting 2017 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2017.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2017 = emp_load.reset_index()

## Calculate employment by industry
emp_load2017["blk2010"] = emp_load2017["w_geocode"]
emp_load2017["emp_tot"] = emp_load2017["C000"]

## Rename columns
emp_load2017 = emp_load2017[["blk2010", "emp_tot"]]

## Convert blk2017 to string
emp_load2017['blk2010'] = emp_load2017['blk2010'].astype(str)
## Remove last three characters
emp_load2017['tr2010'] = emp_load2017['blk2010'].str[:-4]
emp2017 = emp_load2017[['tr2010', 'emp_tot']]
## Add leading zeros
emp2017['tr2010'] = emp2017['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2017 = emp2017.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2017 = pd.merge(travel_times, emp2017, left_on='work_tr2010', right_on='tr2010', how='left')
emp2017 = emp2017[["home_tr2010", "emp_tot"]]
emp2017 = emp2017.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2017['year'] = 2017
emp2017.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2017)


# 2018
print("Getting 2018 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2018.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2018 = emp_load.reset_index()

## Calculate employment by industry
emp_load2018["blk2010"] = emp_load2018["w_geocode"]
emp_load2018["emp_tot"] = emp_load2018["C000"]

## Rename columns
emp_load2018 = emp_load2018[["blk2010", "emp_tot"]]

## Convert blk2018 to string
emp_load2018['blk2010'] = emp_load2018['blk2010'].astype(str)
## Remove last three characters
emp_load2018['tr2010'] = emp_load2018['blk2010'].str[:-4]
emp2018 = emp_load2018[['tr2010', 'emp_tot']]
## Add leading zeros
emp2018['tr2010'] = emp2018['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2018 = emp2018.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2018 = pd.merge(travel_times, emp2018, left_on='work_tr2010', right_on='tr2010', how='left')
emp2018 = emp2018[["home_tr2010", "emp_tot"]]
emp2018 = emp2018.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2018['year'] = 2018
emp2018.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2018)


# 2019
print("Getting 2019 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2019.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2019 = emp_load.reset_index()

## Calculate employment by industry
emp_load2019["blk2010"] = emp_load2019["w_geocode"]
emp_load2019["emp_tot"] = emp_load2019["C000"]

## Rename columns
emp_load2019 = emp_load2019[["blk2010", "emp_tot"]]

## Convert blk2019 to string
emp_load2019['blk2010'] = emp_load2019['blk2010'].astype(str)
## Remove last three characters
emp_load2019['tr2010'] = emp_load2019['blk2010'].str[:-4]
emp2019 = emp_load2019[['tr2010', 'emp_tot']]
## Add leading zeros
emp2019['tr2010'] = emp2019['tr2010'].apply(add_leading_zero)
## Group by tr2010 and sum emp_tot to get total employment by tract
emp2019 = emp2019.groupby('tr2010').sum().reset_index()

# Merge with travel times
emp2019 = pd.merge(travel_times, emp2019, left_on='work_tr2010', right_on='tr2010', how='left')
emp2019 = emp2019[["home_tr2010", "emp_tot"]]
emp2019 = emp2019.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2019['year'] = 2019
emp2019.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2019)


# 2020
print("Getting 2020 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES8/' + i + '/wac/' + i + '_wac_S000_JT00_2020.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2020 = emp_load.reset_index()

## Calculate employment by industry
emp_load2020["blk2020"] = emp_load2020["w_geocode"]
emp_load2020["emp_tot"] = emp_load2020["C000"]

## Select columns
emp_load2020 = emp_load2020[["blk2020", "emp_tot"]]

## Convert blk2020 to string
emp_load2020['blk2020'] = emp_load2020['blk2020'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2020 = pd.merge(emp_load2020, nhgis_blk2020_tr2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2020 = emp2020[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2020[emp_columns].multiply(emp2020["weight"], axis="index")

## And sum within 2010 tracts
emp2020 = emp2020.groupby("tr2010ge", as_index=False).sum()
emp2020 = emp2020.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2020 = emp2020.drop(["weight"], axis = 1)

# Merge with travel times
emp2020 = pd.merge(travel_times, emp2020, left_on='work_tr2010', right_on='tr2010', how='left')
emp2020 = emp2020[["home_tr2010", "emp_tot"]]
emp2020 = emp2020.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2020['year'] = 2020
emp2020.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2020)

# 2021
print("Getting 2021 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES8/' + i + '/wac/' + i + '_wac_S000_JT00_2021.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2021 = emp_load.reset_index()

## Calculate employment by industry
emp_load2021["blk2020"] = emp_load2021["w_geocode"]
emp_load2021["emp_tot"] = emp_load2021["C000"]

## Select columns
emp_load2021 = emp_load2021[["blk2020", "emp_tot"]]

## Convert blk2020 to string
emp_load2021['blk2020'] = emp_load2021['blk2020'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2021 = pd.merge(emp_load2021, nhgis_blk2020_tr2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2021 = emp2021[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2021[emp_columns].multiply(emp2021["weight"], axis="index")

## And sum within 2010 tracts
emp2021 = emp2021.groupby("tr2010ge", as_index=False).sum()
emp2021 = emp2021.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2021 = emp2021.drop(["weight"], axis = 1)

# Merge with travel times
emp2021 = pd.merge(travel_times, emp2021, left_on='work_tr2010', right_on='tr2010', how='left')
emp2021 = emp2021[["home_tr2010", "emp_tot"]]
emp2021 = emp2021.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2021['year'] = 2021
emp2021.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2021)

# 2022
print("Getting 2022 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES8/' + i + '/wac/' + i + '_wac_S000_JT00_2022.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2022 = emp_load.reset_index()

## Calculate employment by industry
emp_load2022["blk2020"] = emp_load2022["w_geocode"]
emp_load2022["emp_tot"] = emp_load2022["C000"]

## Select columns
emp_load2022 = emp_load2022[["blk2020", "emp_tot"]]

## Convert blk2020 to string
emp_load2022['blk2020'] = emp_load2022['blk2020'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2022 = pd.merge(emp_load2022, nhgis_blk2020_tr2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2022 = emp2022[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2022[emp_columns].multiply(emp2022["weight"], axis="index")

## And sum within 2010 tracts
emp2022 = emp2022.groupby("tr2010ge", as_index=False).sum()
emp2022 = emp2022.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2022 = emp2022.drop(["weight"], axis = 1)

# Merge with travel times
emp2022 = pd.merge(travel_times, emp2022, left_on='work_tr2010', right_on='tr2010', how='left')
emp2022 = emp2022[["home_tr2010", "emp_tot"]]
emp2022 = emp2022.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2022['year'] = 2022
emp2022.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2022)

# 2023
print("Getting 2023 employment data...")
emp_load = []
for i in states_list:
	try:
		url = 'https://lehd.ces.census.gov/data/lodes/LODES8/' + i + '/wac/' + i + '_wac_S000_JT00_2023.csv.gz' 
		response = requests.get(url)
		content = response.content
		df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
		df['state_abb'] = i.upper()
		emp_load.append(df)
	except:
		pass

emp_load = pd.concat(emp_load)
emp_load2023 = emp_load.reset_index()

## Calculate employment by industry
emp_load2023["blk2020"] = emp_load2023["w_geocode"]
emp_load2023["emp_tot"] = emp_load2023["C000"]

## Select columns
emp_load2023 = emp_load2023[["blk2020", "emp_tot"]]

## Convert blk2020 to string
emp_load2023['blk2020'] = emp_load2023['blk2020'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2023 = pd.merge(emp_load2023, nhgis_blk2020_tr2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2023 = emp2023[["tr2010ge", "emp_tot", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2023[emp_columns].multiply(emp2023["weight"], axis="index")

## And sum within 2010 tracts
emp2023 = emp2023.groupby("tr2010ge", as_index=False).sum()
emp2023 = emp2023.rename(columns={'tr2010ge': 'tr2010'})

## Drop weight column
emp2023 = emp2023.drop(["weight"], axis = 1)

# Merge with travel times
emp2023 = pd.merge(travel_times, emp2023, left_on='work_tr2010', right_on='tr2010', how='left')
emp2023 = emp2023[["home_tr2010", "emp_tot"]]
emp2023 = emp2023.groupby("home_tr2010", as_index=False).sum()

## Add year column
emp2023['year'] = 2023
emp2023.columns = ['tr2010', 'auto_jobs30_m2', 'year']
print(emp2023)


# Concatenate all employemnt data
empAll = pd.concat([emp2005, emp2006, emp2007, emp2008, emp2009,
					 emp2010, emp2011, emp2012, emp2013,
					 emp2014, emp2015, emp2016, emp2017,
					 emp2018, emp2019, emp2020, emp2021, 
					 emp2022, emp2023], ignore_index=True)


# Write out data
empAll.to_stata("./outputs/auto_jobs30_m2.dta", write_index=False)
empAll.to_stata("../to_rdc/auto_jobs30_m2.dta", write_index=False)