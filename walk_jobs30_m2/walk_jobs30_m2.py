# Create file that has number of jobs within 30 minutes by 2010 block groups.
# This is Measure 2, not from AAA.

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
os.chdir('D:/neeco/rdc_census/walk_jobs30_m2/')

# Read in crosswalk files
nhgis_blk2000_bg2010 = pd.read_csv("./inputs/nhgis_blk2000_bg2010.csv")
nhgis_blk2020_bg2010 = pd.read_csv("./inputs/nhgis_blk2020_bg2010.csv")

# Read in walk distance files
walk_distance_load = pd.read_csv("./inputs/sf12010blkgrpdistance1miles.csv")

# Helper functions
def add_leading_zero(value):
	if len(value) == 11 or len(value) == 14:
		return '0' + value
	else:
		return value
	
# Edit crosswalk files
nhgis_blk2000_bg2010['blk2000ge'] = nhgis_blk2000_bg2010['blk2000ge'].astype(str).apply(add_leading_zero)
nhgis_blk2000_bg2010['bg2010ge'] = nhgis_blk2000_bg2010['bg2010ge'].astype(str).apply(add_leading_zero)

nhgis_blk2020_bg2010['blk2020ge'] = nhgis_blk2020_bg2010['blk2020ge'].astype(str).apply(add_leading_zero)
nhgis_blk2020_bg2010['bg2010ge'] = nhgis_blk2020_bg2010['bg2010ge'].astype(str).apply(add_leading_zero)

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

# Clean walk distances
walk_distance_load['from_bg2010'] = walk_distance_load['tract1'].astype(str) + walk_distance_load['blkgrp1'].astype(str)
walk_distance_load['to_bg2010'] = walk_distance_load['tract2'].astype(str) + walk_distance_load['blkgrp2'].astype(str)
walk_distance_load['from_bg2010'] = walk_distance_load['from_bg2010'].astype(str).apply(add_leading_zero)
walk_distance_load['to_bg2010'] = walk_distance_load['to_bg2010'].astype(str).apply(add_leading_zero)

walk_distance = walk_distance_load[["from_bg2010", "to_bg2010", "mi_to_blkgrp"]]
walk_distance.columns = ["from_bg2010", "to_bg2010", "miles"]

# This only goes from 2002 to 2022
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
emp2002 = pd.merge(emp_load2002, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2002 = emp2002[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2002[emp_columns].multiply(emp2002["weight"], axis="index")

## And sum within 2010 block groups
emp2002 = emp2002.groupby("bg2010ge", as_index=False).sum()
emp2002 = emp2002.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2002 = emp2002.drop(["weight"], axis = 1)

# Merge with walk distance
walk2002_m2 = pd.merge(walk_distance, emp2002, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2002_m2 = walk2002_m2.dropna()

walk2002_m2 = walk2002_m2[["from_bg2010", "emp_tot"]]
walk2002_m2 = walk2002_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2002_m2['year'] = 2002
print(walk2002_m2)

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
emp2003 = pd.merge(emp_load2003, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2003 = emp2003[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2003[emp_columns].multiply(emp2003["weight"], axis="index")

## And sum within 2010 block groups
emp2003 = emp2003.groupby("bg2010ge", as_index=False).sum()
emp2003 = emp2003.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2003 = emp2003.drop(["weight"], axis = 1)

# Merge with walk distance
walk2003_m2 = pd.merge(walk_distance, emp2003, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2003_m2 = walk2003_m2.dropna()

walk2003_m2 = walk2003_m2[["from_bg2010", "emp_tot"]]
walk2003_m2 = walk2003_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2003_m2['year'] = 2003
print(walk2003_m2)

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
emp2004 = pd.merge(emp_load2004, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2004 = emp2004[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2004[emp_columns].multiply(emp2004["weight"], axis="index")

## And sum within 2010 block groups
emp2004 = emp2004.groupby("bg2010ge", as_index=False).sum()
emp2004 = emp2004.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2004 = emp2004.drop(["weight"], axis = 1)

# Merge with walk distance
walk2004_m2 = pd.merge(walk_distance, emp2004, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2004_m2 = walk2004_m2.dropna()

walk2004_m2 = walk2004_m2[["from_bg2010", "emp_tot"]]
walk2004_m2 = walk2004_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2004_m2['year'] = 2004
print(walk2004_m2)

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
emp2005 = pd.merge(emp_load2005, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2005 = emp2005[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2005[emp_columns].multiply(emp2005["weight"], axis="index")

## And sum within 2010 block groups
emp2005 = emp2005.groupby("bg2010ge", as_index=False).sum()
emp2005 = emp2005.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2005 = emp2005.drop(["weight"], axis = 1)

# Merge with walk distance
walk2005_m2 = pd.merge(walk_distance, emp2005, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2005_m2 = walk2005_m2.dropna()

walk2005_m2 = walk2005_m2[["from_bg2010", "emp_tot"]]
walk2005_m2 = walk2005_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2005_m2['year'] = 2005
print(walk2005_m2)

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
emp2006 = pd.merge(emp_load2006, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2006 = emp2006[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2006[emp_columns].multiply(emp2006["weight"], axis="index")

## And sum within 2010 block groups
emp2006 = emp2006.groupby("bg2010ge", as_index=False).sum()
emp2006 = emp2006.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2006 = emp2006.drop(["weight"], axis = 1)

# Merge with walk distance
walk2006_m2 = pd.merge(walk_distance, emp2006, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2006_m2 = walk2006_m2.dropna()

walk2006_m2 = walk2006_m2[["from_bg2010", "emp_tot"]]
walk2006_m2 = walk2006_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2006_m2['year'] = 2006
print(walk2006_m2)

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
emp2007 = pd.merge(emp_load2007, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2007 = emp2007[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2007[emp_columns].multiply(emp2007["weight"], axis="index")

## And sum within 2010 block groups
emp2007 = emp2007.groupby("bg2010ge", as_index=False).sum()
emp2007 = emp2007.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2007 = emp2007.drop(["weight"], axis = 1)

# Merge with walk distance
walk2007_m2 = pd.merge(walk_distance, emp2007, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2007_m2 = walk2007_m2.dropna()

walk2007_m2 = walk2007_m2[["from_bg2010", "emp_tot"]]
walk2007_m2 = walk2007_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2007_m2['year'] = 2007
print(walk2007_m2)

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
emp2008 = pd.merge(emp_load2008, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2008 = emp2008[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2008[emp_columns].multiply(emp2008["weight"], axis="index")

## And sum within 2010 block groups
emp2008 = emp2008.groupby("bg2010ge", as_index=False).sum()
emp2008 = emp2008.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2008 = emp2008.drop(["weight"], axis = 1)

# Merge with walk distance
walk2008_m2 = pd.merge(walk_distance, emp2008, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2008_m2 = walk2008_m2.dropna()

walk2008_m2 = walk2008_m2[["from_bg2010", "emp_tot"]]
walk2008_m2 = walk2008_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2008_m2['year'] = 2008
print(walk2008_m2)

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
emp2009 = pd.merge(emp_load2009, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2009 = emp2009[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2009[emp_columns].multiply(emp2009["weight"], axis="index")

## And sum within 2010 block groups
emp2009 = emp2009.groupby("bg2010ge", as_index=False).sum()
emp2009 = emp2009.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2009 = emp2009.drop(["weight"], axis = 1)

# Merge with walk distance
walk2009_m2 = pd.merge(walk_distance, emp2009, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2009_m2 = walk2009_m2.dropna()

walk2009_m2 = walk2009_m2[["from_bg2010", "emp_tot"]]
walk2009_m2 = walk2009_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2009_m2['year'] = 2009
print(walk2009_m2)

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
emp_load2010["bg2010"] = emp_load2010["w_geocode"]
emp_load2010["emp_tot"] = emp_load2010["C000"]

## Make it a string
emp_load2010['bg2010'] = emp_load2010['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2010['bg2010'] = emp_load2010['bg2010'].str[:-3]

emp2010 = emp_load2010[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2010_m2 = pd.merge(walk_distance, emp2010, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2010_m2 = walk2010_m2.dropna()

walk2010_m2 = walk2010_m2[["from_bg2010", "emp_tot"]]
walk2010_m2 = walk2010_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2010_m2['year'] = 2010
print(walk2010_m2)

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
emp_load2011["bg2010"] = emp_load2011["w_geocode"]
emp_load2011["emp_tot"] = emp_load2011["C000"]

## Make it a string
emp_load2011['bg2010'] = emp_load2011['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2011['bg2010'] = emp_load2011['bg2010'].str[:-3]

emp2011 = emp_load2011[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2011_m2 = pd.merge(walk_distance, emp2011, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2011_m2 = walk2011_m2.dropna()

walk2011_m2 = walk2011_m2[["from_bg2010", "emp_tot"]]
walk2011_m2 = walk2011_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2011_m2['year'] = 2011
print(walk2011_m2)

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
emp_load2012["bg2010"] = emp_load2012["w_geocode"]
emp_load2012["emp_tot"] = emp_load2012["C000"]

## Make it a string
emp_load2012['bg2010'] = emp_load2012['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2012['bg2010'] = emp_load2012['bg2010'].str[:-3]

emp2012 = emp_load2012[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2012_m2 = pd.merge(walk_distance, emp2012, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2012_m2 = walk2012_m2.dropna()

walk2012_m2 = walk2012_m2[["from_bg2010", "emp_tot"]]
walk2012_m2 = walk2012_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2012_m2['year'] = 2012
print(walk2012_m2)

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
emp_load2013["bg2010"] = emp_load2013["w_geocode"]
emp_load2013["emp_tot"] = emp_load2013["C000"]

## Make it a string
emp_load2013['bg2010'] = emp_load2013['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2013['bg2010'] = emp_load2013['bg2010'].str[:-3]

emp2013 = emp_load2013[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2013_m2 = pd.merge(walk_distance, emp2013, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2013_m2 = walk2013_m2.dropna()

walk2013_m2 = walk2013_m2[["from_bg2010", "emp_tot"]]
walk2013_m2 = walk2013_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2013_m2['year'] = 2013
print(walk2013_m2)


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
emp_load2014["bg2010"] = emp_load2014["w_geocode"]
emp_load2014["emp_tot"] = emp_load2014["C000"]

## Make it a string
emp_load2014['bg2010'] = emp_load2014['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2014['bg2010'] = emp_load2014['bg2010'].str[:-3]

emp2014 = emp_load2014[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2014_m2 = pd.merge(walk_distance, emp2014, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2014_m2 = walk2014_m2.dropna()

walk2014_m2 = walk2014_m2[["from_bg2010", "emp_tot"]]
walk2014_m2 = walk2014_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2014_m2['year'] = 2014
print(walk2014_m2)


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
emp_load2015["bg2010"] = emp_load2015["w_geocode"]
emp_load2015["emp_tot"] = emp_load2015["C000"]

## Make it a string
emp_load2015['bg2010'] = emp_load2015['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2015['bg2010'] = emp_load2015['bg2010'].str[:-3]

emp2015 = emp_load2015[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2015_m2 = pd.merge(walk_distance, emp2015, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2015_m2 = walk2015_m2.dropna()

walk2015_m2 = walk2015_m2[["from_bg2010", "emp_tot"]]
walk2015_m2 = walk2015_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2015_m2['year'] = 2015
print(walk2015_m2)


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
emp_load2016["bg2010"] = emp_load2016["w_geocode"]
emp_load2016["emp_tot"] = emp_load2016["C000"]

## Make it a string
emp_load2016['bg2010'] = emp_load2016['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2016['bg2010'] = emp_load2016['bg2010'].str[:-3]

emp2016 = emp_load2016[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2016_m2 = pd.merge(walk_distance, emp2016, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2016_m2 = walk2016_m2.dropna()

walk2016_m2 = walk2016_m2[["from_bg2010", "emp_tot"]]
walk2016_m2 = walk2016_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2016_m2['year'] = 2016
print(walk2016_m2)


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
emp_load2017["bg2010"] = emp_load2017["w_geocode"]
emp_load2017["emp_tot"] = emp_load2017["C000"]

## Make it a string
emp_load2017['bg2010'] = emp_load2017['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2017['bg2010'] = emp_load2017['bg2010'].str[:-3]

emp2017 = emp_load2017[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2017_m2 = pd.merge(walk_distance, emp2017, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2017_m2 = walk2017_m2.dropna()

walk2017_m2 = walk2017_m2[["from_bg2010", "emp_tot"]]
walk2017_m2 = walk2017_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2017_m2['year'] = 2017
print(walk2017_m2)


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
emp_load2018["bg2010"] = emp_load2018["w_geocode"]
emp_load2018["emp_tot"] = emp_load2018["C000"]

## Make it a string
emp_load2018['bg2010'] = emp_load2018['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2018['bg2010'] = emp_load2018['bg2010'].str[:-3]

emp2018 = emp_load2018[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2018_m2 = pd.merge(walk_distance, emp2018, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2018_m2 = walk2018_m2.dropna()

walk2018_m2 = walk2018_m2[["from_bg2010", "emp_tot"]]
walk2018_m2 = walk2018_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2018_m2['year'] = 2018
print(walk2018_m2)


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
emp_load2019["bg2010"] = emp_load2019["w_geocode"]
emp_load2019["emp_tot"] = emp_load2019["C000"]

## Make it a string
emp_load2019['bg2010'] = emp_load2019['bg2010'].astype(str).apply(add_leading_zero)
## Remove last three characters
emp_load2019['bg2010'] = emp_load2019['bg2010'].str[:-3]

emp2019 = emp_load2019[["bg2010", "emp_tot"]]

# Merge with walk distance
walk2019_m2 = pd.merge(walk_distance, emp2019, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2019_m2 = walk2019_m2.dropna()

walk2019_m2 = walk2019_m2[["from_bg2010", "emp_tot"]]
walk2019_m2 = walk2019_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2019_m2['year'] = 2019
print(walk2019_m2)

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
emp2020 = pd.merge(emp_load2020, nhgis_blk2020_bg2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2020 = emp2020[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2020[emp_columns].multiply(emp2020["weight"], axis="index")

## And sum within 2010 block groups
emp2020 = emp2020.groupby("bg2010ge", as_index=False).sum()
emp2020 = emp2020.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2020 = emp2020.drop(["weight"], axis = 1)

# Merge with walk distance
walk2020_m2 = pd.merge(walk_distance, emp2020, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2020_m2 = walk2020_m2.dropna()

walk2020_m2 = walk2020_m2[["from_bg2010", "emp_tot"]]
walk2020_m2 = walk2020_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2020_m2['year'] = 2020
print(walk2020_m2)

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
emp2021 = pd.merge(emp_load2021, nhgis_blk2020_bg2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2021 = emp2021[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2021[emp_columns].multiply(emp2021["weight"], axis="index")

## And sum within 2010 block groups
emp2021 = emp2021.groupby("bg2010ge", as_index=False).sum()
emp2021 = emp2021.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2021 = emp2021.drop(["weight"], axis = 1)

# Merge with walk distance
walk2021_m2 = pd.merge(walk_distance, emp2021, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2021_m2 = walk2021_m2.dropna()

walk2021_m2 = walk2021_m2[["from_bg2010", "emp_tot"]]
walk2021_m2 = walk2021_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2021_m2['year'] = 2021
print(walk2021_m2)

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
emp2022 = pd.merge(emp_load2022, nhgis_blk2020_bg2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2022 = emp2022[["bg2010ge", "emp_tot",  "weight"]]
## Multiply by weight
emp_columns = ["emp_tot"]
emp2022[emp_columns].multiply(emp2022["weight"], axis="index")

## And sum within 2010 block groups
emp2022 = emp2022.groupby("bg2010ge", as_index=False).sum()
emp2022 = emp2022.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2022 = emp2022.drop(["weight"], axis = 1)

# Merge with walk distance
walk2022_m2 = pd.merge(walk_distance, emp2022, left_on='to_bg2010', right_on='bg2010', how='left')

# Drop NAs
walk2022_m2 = walk2022_m2.dropna()

walk2022_m2 = walk2022_m2[["from_bg2010", "emp_tot"]]
walk2022_m2 = walk2022_m2.groupby("from_bg2010", as_index=False).sum()

# Add year
walk2022_m2['year'] = 2022
print(walk2022_m2)

# Concatenate
walkAll = pd.concat([walk2002_m2, walk2003_m2, walk2004_m2, walk2005_m2,
					 walk2006_m2, walk2007_m2, walk2008_m2, walk2009_m2,
					 walk2010_m2, walk2011_m2, walk2012_m2, walk2013_m2,
					 walk2014_m2, walk2015_m2, walk2016_m2, walk2017_m2,
					 walk2018_m2, walk2019_m2, walk2020_m2, walk2021_m2, walk2022_m2], ignore_index=True)

walkAll.columns = ["bg2010", "walk_jobs30_m2", "year"]

# Write to Stata