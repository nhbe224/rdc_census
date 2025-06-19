# Create LEHD Change File
# Gives us employment density, Employment Entropy (Use EPA Definition),
# Employment and Housing Entropy, Employment + Household Density (from ACS), and Employment + Population Density (from ACS)

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
os.chdir('D:/neeco/rdc_census/lehd_change_file/')

# Read in crosswalk files
nhgis_blk2000_bg2010 = pd.read_csv("./nhgis_blk2000_bg2010.csv")
nhgis_blk2020_bg2010 = pd.read_csv("./nhgis_blk2020_bg2010.csv")

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
emp_load2002["emp_retail"] = emp_load2002["CNS07"]
emp_load2002["emp_office"] = emp_load2002["CNS09"] + emp_load2002["CNS10"] + emp_load2002["CNS11"] + emp_load2002["CNS13"] + emp_load2002["CNS20"]
emp_load2002["emp_industrial"] =  emp_load2002["CNS01"] + emp_load2002["CNS02"] + emp_load2002["CNS03"] + emp_load2002["CNS04"] + emp_load2002["CNS05"] + emp_load2002["CNS06"] + emp_load2002["CNS08"]
emp_load2002["emp_service"] = emp_load2002["CNS12"] + emp_load2002["CNS14"] + emp_load2002["CNS15"] + emp_load2002["CNS16"] + emp_load2002["CNS19"]
emp_load2002["emp_entertain"] =  emp_load2002["CNS17"] + emp_load2002["CNS18"]																	

## Select columns
emp_load2002 = emp_load2002[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2002['blk2000'] = emp_load2002['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2002 = pd.merge(emp_load2002, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2002 = emp2002[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2002[emp_columns].multiply(emp2002["weight"], axis="index")

## And sum within 2010 block groups
emp2002 = emp2002.groupby("bg2010ge", as_index=False).sum()
emp2002 = emp2002.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2002 = emp2002.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2002[emp_columns] += 1

## Calculate employment entropy
emp2002["E"] = ((emp2002["emp_retail"]/emp2002["emp_tot"])*np.log(emp2002["emp_retail"]/emp2002["emp_tot"])) + \
((emp2002["emp_office"]/emp2002["emp_tot"])*np.log(emp2002["emp_office"]/emp2002["emp_tot"]))  + \
((emp2002["emp_industrial"]/emp2002["emp_tot"])*np.log(emp2002["emp_industrial"]/emp2002["emp_tot"])) + \
((emp2002["emp_service"]/emp2002["emp_tot"])*np.log(emp2002["emp_service"]/emp2002["emp_tot"])) + \
((emp2002["emp_entertain"]/emp2002["emp_tot"])*np.log(emp2002["emp_entertain"]/emp2002["emp_tot"]))

emp2002["emp_entropy"] = -emp2002["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2002 = pd.read_csv("../acs_change_file/outputs/acs2002.csv")
acs2002["bg2010"] = acs2002['bg2010'].astype(str).apply(add_leading_zero)
acs2002["hh"]  = acs2002["area_acres"] * acs2002["hh_den_acre"]

emp_hh2002 = pd.merge(emp2002, acs2002, on = "bg2010", how = "left")

emp_hh2002["total_activity"] = emp_hh2002["hh"] + emp_hh2002["emp_tot"]
emp_hh2002["A"] = ((emp_hh2002["hh"] / emp_hh2002["total_activity"])*np.log(emp_hh2002["hh"] /emp_hh2002["total_activity"])) + \
((emp_hh2002["emp_retail"] / emp_hh2002["total_activity"])*np.log(emp_hh2002["emp_retail"] /emp_hh2002["total_activity"]))  + \
((emp_hh2002["emp_office"] / emp_hh2002["total_activity"])*np.log(emp_hh2002["emp_office"] /emp_hh2002["total_activity"]))  + \
((emp_hh2002["emp_industrial"] / emp_hh2002["total_activity"])*np.log(emp_hh2002["emp_industrial"] /emp_hh2002["total_activity"]))  + \
((emp_hh2002["emp_service"] / emp_hh2002["total_activity"])*np.log(emp_hh2002["emp_service"] /emp_hh2002["total_activity"]))  + \
((emp_hh2002["emp_entertain"] / emp_hh2002["total_activity"])*np.log(emp_hh2002["emp_entertain"] /emp_hh2002["total_activity"])) 

emp_hh2002["emp_hh_entropy"] = -emp_hh2002["A"]/np.log(6)

## Employment density
emp_hh2002["emp_den_acre"] = emp_hh2002["emp_tot"] / emp_hh2002["area_acres"]
## Employment and household density
emp_hh2002["emp_hh_den_acre"] = (emp_hh2002["emp_tot"] + emp_hh2002["hh"]) / emp_hh2002["area_acres"]
## Employment and population density
emp_hh2002["pop"] = emp_hh2002["pop_den_acre"] * emp_hh2002["area_acres"]
emp_hh2002["emp_pop_den_acre"] = (emp_hh2002["emp_tot"] + emp_hh2002["pop"]) / emp_hh2002["area_acres"]

## Add year
lehd2002 = emp_hh2002[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2002.head())
print(lehd2002.shape)
lehd2002.to_csv("./outputs/lehd2002.csv", index = False)

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
emp_load2003["emp_retail"] = emp_load2003["CNS07"]
emp_load2003["emp_office"] = emp_load2003["CNS09"] + emp_load2003["CNS10"] + emp_load2003["CNS11"] + emp_load2003["CNS13"] + emp_load2003["CNS20"]
emp_load2003["emp_industrial"] =  emp_load2003["CNS01"] + emp_load2003["CNS02"] + emp_load2003["CNS03"] + emp_load2003["CNS04"] + emp_load2003["CNS05"] + emp_load2003["CNS06"] + emp_load2003["CNS08"]
emp_load2003["emp_service"] = emp_load2003["CNS12"] + emp_load2003["CNS14"] + emp_load2003["CNS15"] + emp_load2003["CNS16"] + emp_load2003["CNS19"]
emp_load2003["emp_entertain"] =  emp_load2003["CNS17"] + emp_load2003["CNS18"]																	

## Select columns
emp_load2003 = emp_load2003[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2003['blk2000'] = emp_load2003['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2003 = pd.merge(emp_load2003, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2003 = emp2003[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2003[emp_columns].multiply(emp2003["weight"], axis="index")

## And sum within 2010 block groups
emp2003 = emp2003.groupby("bg2010ge", as_index=False).sum()
emp2003 = emp2003.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2003 = emp2003.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2003[emp_columns] += 1

## Calculate employment entropy
emp2003["E"] = ((emp2003["emp_retail"]/emp2003["emp_tot"])*np.log(emp2003["emp_retail"]/emp2003["emp_tot"])) + \
((emp2003["emp_office"]/emp2003["emp_tot"])*np.log(emp2003["emp_office"]/emp2003["emp_tot"]))  + \
((emp2003["emp_industrial"]/emp2003["emp_tot"])*np.log(emp2003["emp_industrial"]/emp2003["emp_tot"])) + \
((emp2003["emp_service"]/emp2003["emp_tot"])*np.log(emp2003["emp_service"]/emp2003["emp_tot"])) + \
((emp2003["emp_entertain"]/emp2003["emp_tot"])*np.log(emp2003["emp_entertain"]/emp2003["emp_tot"]))

emp2003["emp_entropy"] = -emp2003["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2003 = pd.read_csv("../acs_change_file/outputs/acs2003.csv")
acs2003["bg2010"] = acs2003['bg2010'].astype(str).apply(add_leading_zero)
acs2003["hh"]  = acs2003["area_acres"] * acs2003["hh_den_acre"]

emp_hh2003 = pd.merge(emp2003, acs2003, on = "bg2010", how = "left")

emp_hh2003["total_activity"] = emp_hh2003["hh"] + emp_hh2003["emp_tot"]
emp_hh2003["A"] = ((emp_hh2003["hh"] / emp_hh2003["total_activity"])*np.log(emp_hh2003["hh"] /emp_hh2003["total_activity"])) + \
((emp_hh2003["emp_retail"] / emp_hh2003["total_activity"])*np.log(emp_hh2003["emp_retail"] /emp_hh2003["total_activity"]))  + \
((emp_hh2003["emp_office"] / emp_hh2003["total_activity"])*np.log(emp_hh2003["emp_office"] /emp_hh2003["total_activity"]))  + \
((emp_hh2003["emp_industrial"] / emp_hh2003["total_activity"])*np.log(emp_hh2003["emp_industrial"] /emp_hh2003["total_activity"]))  + \
((emp_hh2003["emp_service"] / emp_hh2003["total_activity"])*np.log(emp_hh2003["emp_service"] /emp_hh2003["total_activity"]))  + \
((emp_hh2003["emp_entertain"] / emp_hh2003["total_activity"])*np.log(emp_hh2003["emp_entertain"] /emp_hh2003["total_activity"])) 

emp_hh2003["emp_hh_entropy"] = -emp_hh2003["A"]/np.log(6)

## Employment density
emp_hh2003["emp_den_acre"] = emp_hh2003["emp_tot"] / emp_hh2003["area_acres"]
## Employment and household density
emp_hh2003["emp_hh_den_acre"] = (emp_hh2003["emp_tot"] + emp_hh2003["hh"]) / emp_hh2003["area_acres"]
## Employment and population density
emp_hh2003["pop"] = emp_hh2003["pop_den_acre"] * emp_hh2003["area_acres"]
emp_hh2003["emp_pop_den_acre"] = (emp_hh2003["emp_tot"] + emp_hh2003["pop"]) / emp_hh2003["area_acres"]

## Add year
lehd2003 = emp_hh2003[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2003.head())
print(lehd2003.shape)
lehd2003.to_csv("./outputs/lehd2003.csv", index = False)

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
emp_load2004["emp_retail"] = emp_load2004["CNS07"]
emp_load2004["emp_office"] = emp_load2004["CNS09"] + emp_load2004["CNS10"] + emp_load2004["CNS11"] + emp_load2004["CNS13"] + emp_load2004["CNS20"]
emp_load2004["emp_industrial"] =  emp_load2004["CNS01"] + emp_load2004["CNS02"] + emp_load2004["CNS03"] + emp_load2004["CNS04"] + emp_load2004["CNS05"] + emp_load2004["CNS06"] + emp_load2004["CNS08"]
emp_load2004["emp_service"] = emp_load2004["CNS12"] + emp_load2004["CNS14"] + emp_load2004["CNS15"] + emp_load2004["CNS16"] + emp_load2004["CNS19"]
emp_load2004["emp_entertain"] =  emp_load2004["CNS17"] + emp_load2004["CNS18"]																	

## Select columns
emp_load2004 = emp_load2004[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2004['blk2000'] = emp_load2004['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2004 = pd.merge(emp_load2004, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2004 = emp2004[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2004[emp_columns].multiply(emp2004["weight"], axis="index")

## And sum within 2010 block groups
emp2004 = emp2004.groupby("bg2010ge", as_index=False).sum()
emp2004 = emp2004.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2004 = emp2004.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2004[emp_columns] += 1

## Calculate employment entropy
emp2004["E"] = ((emp2004["emp_retail"]/emp2004["emp_tot"])*np.log(emp2004["emp_retail"]/emp2004["emp_tot"])) + \
((emp2004["emp_office"]/emp2004["emp_tot"])*np.log(emp2004["emp_office"]/emp2004["emp_tot"]))  + \
((emp2004["emp_industrial"]/emp2004["emp_tot"])*np.log(emp2004["emp_industrial"]/emp2004["emp_tot"])) + \
((emp2004["emp_service"]/emp2004["emp_tot"])*np.log(emp2004["emp_service"]/emp2004["emp_tot"])) + \
((emp2004["emp_entertain"]/emp2004["emp_tot"])*np.log(emp2004["emp_entertain"]/emp2004["emp_tot"]))

emp2004["emp_entropy"] = -emp2004["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2004 = pd.read_csv("../acs_change_file/outputs/acs2004.csv")
acs2004["bg2010"] = acs2004['bg2010'].astype(str).apply(add_leading_zero)
acs2004["hh"]  = acs2004["area_acres"] * acs2004["hh_den_acre"]

emp_hh2004 = pd.merge(emp2004, acs2004, on = "bg2010", how = "left")

emp_hh2004["total_activity"] = emp_hh2004["hh"] + emp_hh2004["emp_tot"]
emp_hh2004["A"] = ((emp_hh2004["hh"] / emp_hh2004["total_activity"])*np.log(emp_hh2004["hh"] /emp_hh2004["total_activity"])) + \
((emp_hh2004["emp_retail"] / emp_hh2004["total_activity"])*np.log(emp_hh2004["emp_retail"] /emp_hh2004["total_activity"]))  + \
((emp_hh2004["emp_office"] / emp_hh2004["total_activity"])*np.log(emp_hh2004["emp_office"] /emp_hh2004["total_activity"]))  + \
((emp_hh2004["emp_industrial"] / emp_hh2004["total_activity"])*np.log(emp_hh2004["emp_industrial"] /emp_hh2004["total_activity"]))  + \
((emp_hh2004["emp_service"] / emp_hh2004["total_activity"])*np.log(emp_hh2004["emp_service"] /emp_hh2004["total_activity"]))  + \
((emp_hh2004["emp_entertain"] / emp_hh2004["total_activity"])*np.log(emp_hh2004["emp_entertain"] /emp_hh2004["total_activity"])) 

emp_hh2004["emp_hh_entropy"] = -emp_hh2004["A"]/np.log(6)

## Employment density
emp_hh2004["emp_den_acre"] = emp_hh2004["emp_tot"] / emp_hh2004["area_acres"]
## Employment and household density
emp_hh2004["emp_hh_den_acre"] = (emp_hh2004["emp_tot"] + emp_hh2004["hh"]) / emp_hh2004["area_acres"]
## Employment and population density
emp_hh2004["pop"] = emp_hh2004["pop_den_acre"] * emp_hh2004["area_acres"]
emp_hh2004["emp_pop_den_acre"] = (emp_hh2004["emp_tot"] + emp_hh2004["pop"]) / emp_hh2004["area_acres"]

## Add year
lehd2004 = emp_hh2004[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2004.head())
print(lehd2004.shape)
lehd2004.to_csv("./outputs/lehd2004.csv", index = False)

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
emp_load2005["emp_retail"] = emp_load2005["CNS07"]
emp_load2005["emp_office"] = emp_load2005["CNS09"] + emp_load2005["CNS10"] + emp_load2005["CNS11"] + emp_load2005["CNS13"] + emp_load2005["CNS20"]
emp_load2005["emp_industrial"] =  emp_load2005["CNS01"] + emp_load2005["CNS02"] + emp_load2005["CNS03"] + emp_load2005["CNS04"] + emp_load2005["CNS05"] + emp_load2005["CNS06"] + emp_load2005["CNS08"]
emp_load2005["emp_service"] = emp_load2005["CNS12"] + emp_load2005["CNS14"] + emp_load2005["CNS15"] + emp_load2005["CNS16"] + emp_load2005["CNS19"]
emp_load2005["emp_entertain"] =  emp_load2005["CNS17"] + emp_load2005["CNS18"]																	

## Select columns
emp_load2005 = emp_load2005[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2005['blk2000'] = emp_load2005['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2005 = pd.merge(emp_load2005, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2005 = emp2005[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2005[emp_columns].multiply(emp2005["weight"], axis="index")

## And sum within 2010 block groups
emp2005 = emp2005.groupby("bg2010ge", as_index=False).sum()
emp2005 = emp2005.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2005 = emp2005.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2005[emp_columns] += 1

## Calculate employment entropy
emp2005["E"] = ((emp2005["emp_retail"]/emp2005["emp_tot"])*np.log(emp2005["emp_retail"]/emp2005["emp_tot"])) + \
((emp2005["emp_office"]/emp2005["emp_tot"])*np.log(emp2005["emp_office"]/emp2005["emp_tot"]))  + \
((emp2005["emp_industrial"]/emp2005["emp_tot"])*np.log(emp2005["emp_industrial"]/emp2005["emp_tot"])) + \
((emp2005["emp_service"]/emp2005["emp_tot"])*np.log(emp2005["emp_service"]/emp2005["emp_tot"])) + \
((emp2005["emp_entertain"]/emp2005["emp_tot"])*np.log(emp2005["emp_entertain"]/emp2005["emp_tot"]))

emp2005["emp_entropy"] = -emp2005["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2005 = pd.read_csv("../acs_change_file/outputs/acs2005.csv")
acs2005["bg2010"] = acs2005['bg2010'].astype(str).apply(add_leading_zero)
acs2005["hh"]  = acs2005["area_acres"] * acs2005["hh_den_acre"]

emp_hh2005 = pd.merge(emp2005, acs2005, on = "bg2010", how = "left")

emp_hh2005["total_activity"] = emp_hh2005["hh"] + emp_hh2005["emp_tot"]
emp_hh2005["A"] = ((emp_hh2005["hh"] / emp_hh2005["total_activity"])*np.log(emp_hh2005["hh"] /emp_hh2005["total_activity"])) + \
((emp_hh2005["emp_retail"] / emp_hh2005["total_activity"])*np.log(emp_hh2005["emp_retail"] /emp_hh2005["total_activity"]))  + \
((emp_hh2005["emp_office"] / emp_hh2005["total_activity"])*np.log(emp_hh2005["emp_office"] /emp_hh2005["total_activity"]))  + \
((emp_hh2005["emp_industrial"] / emp_hh2005["total_activity"])*np.log(emp_hh2005["emp_industrial"] /emp_hh2005["total_activity"]))  + \
((emp_hh2005["emp_service"] / emp_hh2005["total_activity"])*np.log(emp_hh2005["emp_service"] /emp_hh2005["total_activity"]))  + \
((emp_hh2005["emp_entertain"] / emp_hh2005["total_activity"])*np.log(emp_hh2005["emp_entertain"] /emp_hh2005["total_activity"])) 

emp_hh2005["emp_hh_entropy"] = -emp_hh2005["A"]/np.log(6)

## Employment density
emp_hh2005["emp_den_acre"] = emp_hh2005["emp_tot"] / emp_hh2005["area_acres"]
## Employment and household density
emp_hh2005["emp_hh_den_acre"] = (emp_hh2005["emp_tot"] + emp_hh2005["hh"]) / emp_hh2005["area_acres"]
## Employment and population density
emp_hh2005["pop"] = emp_hh2005["pop_den_acre"] * emp_hh2005["area_acres"]
emp_hh2005["emp_pop_den_acre"] = (emp_hh2005["emp_tot"] + emp_hh2005["pop"]) / emp_hh2005["area_acres"]

## Add year
lehd2005 = emp_hh2005[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2005.head())
print(lehd2005.shape)
lehd2005.to_csv("./outputs/lehd2005.csv", index = False)

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
emp_load2006["emp_retail"] = emp_load2006["CNS07"]
emp_load2006["emp_office"] = emp_load2006["CNS09"] + emp_load2006["CNS10"] + emp_load2006["CNS11"] + emp_load2006["CNS13"] + emp_load2006["CNS20"]
emp_load2006["emp_industrial"] =  emp_load2006["CNS01"] + emp_load2006["CNS02"] + emp_load2006["CNS03"] + emp_load2006["CNS04"] + emp_load2006["CNS05"] + emp_load2006["CNS06"] + emp_load2006["CNS08"]
emp_load2006["emp_service"] = emp_load2006["CNS12"] + emp_load2006["CNS14"] + emp_load2006["CNS15"] + emp_load2006["CNS16"] + emp_load2006["CNS19"]
emp_load2006["emp_entertain"] =  emp_load2006["CNS17"] + emp_load2006["CNS18"]																	

## Select columns
emp_load2006 = emp_load2006[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2006['blk2000'] = emp_load2006['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2006 = pd.merge(emp_load2006, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2006 = emp2006[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2006[emp_columns].multiply(emp2006["weight"], axis="index")

## And sum within 2010 block groups
emp2006 = emp2006.groupby("bg2010ge", as_index=False).sum()
emp2006 = emp2006.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2006 = emp2006.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2006[emp_columns] += 1

## Calculate employment entropy
emp2006["E"] = ((emp2006["emp_retail"]/emp2006["emp_tot"])*np.log(emp2006["emp_retail"]/emp2006["emp_tot"])) + \
((emp2006["emp_office"]/emp2006["emp_tot"])*np.log(emp2006["emp_office"]/emp2006["emp_tot"]))  + \
((emp2006["emp_industrial"]/emp2006["emp_tot"])*np.log(emp2006["emp_industrial"]/emp2006["emp_tot"])) + \
((emp2006["emp_service"]/emp2006["emp_tot"])*np.log(emp2006["emp_service"]/emp2006["emp_tot"])) + \
((emp2006["emp_entertain"]/emp2006["emp_tot"])*np.log(emp2006["emp_entertain"]/emp2006["emp_tot"]))

emp2006["emp_entropy"] = -emp2006["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2006 = pd.read_csv("../acs_change_file/outputs/acs2006.csv")
acs2006["bg2010"] = acs2006['bg2010'].astype(str).apply(add_leading_zero)
acs2006["hh"]  = acs2006["area_acres"] * acs2006["hh_den_acre"]

emp_hh2006 = pd.merge(emp2006, acs2006, on = "bg2010", how = "left")

emp_hh2006["total_activity"] = emp_hh2006["hh"] + emp_hh2006["emp_tot"]
emp_hh2006["A"] = ((emp_hh2006["hh"] / emp_hh2006["total_activity"])*np.log(emp_hh2006["hh"] /emp_hh2006["total_activity"])) + \
((emp_hh2006["emp_retail"] / emp_hh2006["total_activity"])*np.log(emp_hh2006["emp_retail"] /emp_hh2006["total_activity"]))  + \
((emp_hh2006["emp_office"] / emp_hh2006["total_activity"])*np.log(emp_hh2006["emp_office"] /emp_hh2006["total_activity"]))  + \
((emp_hh2006["emp_industrial"] / emp_hh2006["total_activity"])*np.log(emp_hh2006["emp_industrial"] /emp_hh2006["total_activity"]))  + \
((emp_hh2006["emp_service"] / emp_hh2006["total_activity"])*np.log(emp_hh2006["emp_service"] /emp_hh2006["total_activity"]))  + \
((emp_hh2006["emp_entertain"] / emp_hh2006["total_activity"])*np.log(emp_hh2006["emp_entertain"] /emp_hh2006["total_activity"])) 

emp_hh2006["emp_hh_entropy"] = -emp_hh2006["A"]/np.log(6)

## Employment density
emp_hh2006["emp_den_acre"] = emp_hh2006["emp_tot"] / emp_hh2006["area_acres"]
## Employment and household density
emp_hh2006["emp_hh_den_acre"] = (emp_hh2006["emp_tot"] + emp_hh2006["hh"]) / emp_hh2006["area_acres"]
## Employment and population density
emp_hh2006["pop"] = emp_hh2006["pop_den_acre"] * emp_hh2006["area_acres"]
emp_hh2006["emp_pop_den_acre"] = (emp_hh2006["emp_tot"] + emp_hh2006["pop"]) / emp_hh2006["area_acres"]

## Add year
lehd2006 = emp_hh2006[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2006.head())
print(lehd2006.shape)
lehd2006.to_csv("./outputs/lehd2006.csv", index = False)

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
emp_load2007["emp_retail"] = emp_load2007["CNS07"]
emp_load2007["emp_office"] = emp_load2007["CNS09"] + emp_load2007["CNS10"] + emp_load2007["CNS11"] + emp_load2007["CNS13"] + emp_load2007["CNS20"]
emp_load2007["emp_industrial"] =  emp_load2007["CNS01"] + emp_load2007["CNS02"] + emp_load2007["CNS03"] + emp_load2007["CNS04"] + emp_load2007["CNS05"] + emp_load2007["CNS06"] + emp_load2007["CNS08"]
emp_load2007["emp_service"] = emp_load2007["CNS12"] + emp_load2007["CNS14"] + emp_load2007["CNS15"] + emp_load2007["CNS16"] + emp_load2007["CNS19"]
emp_load2007["emp_entertain"] =  emp_load2007["CNS17"] + emp_load2007["CNS18"]																	

## Select columns
emp_load2007 = emp_load2007[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2007['blk2000'] = emp_load2007['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2007 = pd.merge(emp_load2007, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2007 = emp2007[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2007[emp_columns].multiply(emp2007["weight"], axis="index")

## And sum within 2010 block groups
emp2007 = emp2007.groupby("bg2010ge", as_index=False).sum()
emp2007 = emp2007.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2007 = emp2007.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2007[emp_columns] += 1

## Calculate employment entropy
emp2007["E"] = ((emp2007["emp_retail"]/emp2007["emp_tot"])*np.log(emp2007["emp_retail"]/emp2007["emp_tot"])) + \
((emp2007["emp_office"]/emp2007["emp_tot"])*np.log(emp2007["emp_office"]/emp2007["emp_tot"]))  + \
((emp2007["emp_industrial"]/emp2007["emp_tot"])*np.log(emp2007["emp_industrial"]/emp2007["emp_tot"])) + \
((emp2007["emp_service"]/emp2007["emp_tot"])*np.log(emp2007["emp_service"]/emp2007["emp_tot"])) + \
((emp2007["emp_entertain"]/emp2007["emp_tot"])*np.log(emp2007["emp_entertain"]/emp2007["emp_tot"]))

emp2007["emp_entropy"] = -emp2007["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2007 = pd.read_csv("../acs_change_file/outputs/acs2007.csv")
acs2007["bg2010"] = acs2007['bg2010'].astype(str).apply(add_leading_zero)
acs2007["hh"]  = acs2007["area_acres"] * acs2007["hh_den_acre"]

emp_hh2007 = pd.merge(emp2007, acs2007, on = "bg2010", how = "left")

emp_hh2007["total_activity"] = emp_hh2007["hh"] + emp_hh2007["emp_tot"]
emp_hh2007["A"] = ((emp_hh2007["hh"] / emp_hh2007["total_activity"])*np.log(emp_hh2007["hh"] /emp_hh2007["total_activity"])) + \
((emp_hh2007["emp_retail"] / emp_hh2007["total_activity"])*np.log(emp_hh2007["emp_retail"] /emp_hh2007["total_activity"]))  + \
((emp_hh2007["emp_office"] / emp_hh2007["total_activity"])*np.log(emp_hh2007["emp_office"] /emp_hh2007["total_activity"]))  + \
((emp_hh2007["emp_industrial"] / emp_hh2007["total_activity"])*np.log(emp_hh2007["emp_industrial"] /emp_hh2007["total_activity"]))  + \
((emp_hh2007["emp_service"] / emp_hh2007["total_activity"])*np.log(emp_hh2007["emp_service"] /emp_hh2007["total_activity"]))  + \
((emp_hh2007["emp_entertain"] / emp_hh2007["total_activity"])*np.log(emp_hh2007["emp_entertain"] /emp_hh2007["total_activity"])) 

emp_hh2007["emp_hh_entropy"] = -emp_hh2007["A"]/np.log(6)

## Employment density
emp_hh2007["emp_den_acre"] = emp_hh2007["emp_tot"] / emp_hh2007["area_acres"]
## Employment and household density
emp_hh2007["emp_hh_den_acre"] = (emp_hh2007["emp_tot"] + emp_hh2007["hh"]) / emp_hh2007["area_acres"]
## Employment and population density
emp_hh2007["pop"] = emp_hh2007["pop_den_acre"] * emp_hh2007["area_acres"]
emp_hh2007["emp_pop_den_acre"] = (emp_hh2007["emp_tot"] + emp_hh2007["pop"]) / emp_hh2007["area_acres"]

## Add year
lehd2007 = emp_hh2007[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2007.head())
print(lehd2007.shape)
lehd2007.to_csv("./outputs/lehd2007.csv", index = False)

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
emp_load2008["emp_retail"] = emp_load2008["CNS07"]
emp_load2008["emp_office"] = emp_load2008["CNS09"] + emp_load2008["CNS10"] + emp_load2008["CNS11"] + emp_load2008["CNS13"] + emp_load2008["CNS20"]
emp_load2008["emp_industrial"] =  emp_load2008["CNS01"] + emp_load2008["CNS02"] + emp_load2008["CNS03"] + emp_load2008["CNS04"] + emp_load2008["CNS05"] + emp_load2008["CNS06"] + emp_load2008["CNS08"]
emp_load2008["emp_service"] = emp_load2008["CNS12"] + emp_load2008["CNS14"] + emp_load2008["CNS15"] + emp_load2008["CNS16"] + emp_load2008["CNS19"]
emp_load2008["emp_entertain"] =  emp_load2008["CNS17"] + emp_load2008["CNS18"]																	

## Select columns
emp_load2008 = emp_load2008[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2008['blk2000'] = emp_load2008['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2008 = pd.merge(emp_load2008, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2008 = emp2008[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2008[emp_columns].multiply(emp2008["weight"], axis="index")

## And sum within 2010 block groups
emp2008 = emp2008.groupby("bg2010ge", as_index=False).sum()
emp2008 = emp2008.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2008 = emp2008.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2008[emp_columns] += 1

## Calculate employment entropy
emp2008["E"] = ((emp2008["emp_retail"]/emp2008["emp_tot"])*np.log(emp2008["emp_retail"]/emp2008["emp_tot"])) + \
((emp2008["emp_office"]/emp2008["emp_tot"])*np.log(emp2008["emp_office"]/emp2008["emp_tot"]))  + \
((emp2008["emp_industrial"]/emp2008["emp_tot"])*np.log(emp2008["emp_industrial"]/emp2008["emp_tot"])) + \
((emp2008["emp_service"]/emp2008["emp_tot"])*np.log(emp2008["emp_service"]/emp2008["emp_tot"])) + \
((emp2008["emp_entertain"]/emp2008["emp_tot"])*np.log(emp2008["emp_entertain"]/emp2008["emp_tot"]))

emp2008["emp_entropy"] = -emp2008["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2008 = pd.read_csv("../acs_change_file/outputs/acs2008.csv")
acs2008["bg2010"] = acs2008['bg2010'].astype(str).apply(add_leading_zero)
acs2008["hh"]  = acs2008["area_acres"] * acs2008["hh_den_acre"]

emp_hh2008 = pd.merge(emp2008, acs2008, on = "bg2010", how = "left")

emp_hh2008["total_activity"] = emp_hh2008["hh"] + emp_hh2008["emp_tot"]
emp_hh2008["A"] = ((emp_hh2008["hh"] / emp_hh2008["total_activity"])*np.log(emp_hh2008["hh"] /emp_hh2008["total_activity"])) + \
((emp_hh2008["emp_retail"] / emp_hh2008["total_activity"])*np.log(emp_hh2008["emp_retail"] /emp_hh2008["total_activity"]))  + \
((emp_hh2008["emp_office"] / emp_hh2008["total_activity"])*np.log(emp_hh2008["emp_office"] /emp_hh2008["total_activity"]))  + \
((emp_hh2008["emp_industrial"] / emp_hh2008["total_activity"])*np.log(emp_hh2008["emp_industrial"] /emp_hh2008["total_activity"]))  + \
((emp_hh2008["emp_service"] / emp_hh2008["total_activity"])*np.log(emp_hh2008["emp_service"] /emp_hh2008["total_activity"]))  + \
((emp_hh2008["emp_entertain"] / emp_hh2008["total_activity"])*np.log(emp_hh2008["emp_entertain"] /emp_hh2008["total_activity"])) 

emp_hh2008["emp_hh_entropy"] = -emp_hh2008["A"]/np.log(6)

## Employment density
emp_hh2008["emp_den_acre"] = emp_hh2008["emp_tot"] / emp_hh2008["area_acres"]
## Employment and household density
emp_hh2008["emp_hh_den_acre"] = (emp_hh2008["emp_tot"] + emp_hh2008["hh"]) / emp_hh2008["area_acres"]
## Employment and population density
emp_hh2008["pop"] = emp_hh2008["pop_den_acre"] * emp_hh2008["area_acres"]
emp_hh2008["emp_pop_den_acre"] = (emp_hh2008["emp_tot"] + emp_hh2008["pop"]) / emp_hh2008["area_acres"]

## Add year
lehd2008 = emp_hh2008[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2008.head())
print(lehd2008.shape)
lehd2008.to_csv("./outputs/lehd2008.csv", index = False)

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
emp_load2009["emp_retail"] = emp_load2009["CNS07"]
emp_load2009["emp_office"] = emp_load2009["CNS09"] + emp_load2009["CNS10"] + emp_load2009["CNS11"] + emp_load2009["CNS13"] + emp_load2009["CNS20"]
emp_load2009["emp_industrial"] =  emp_load2009["CNS01"] + emp_load2009["CNS02"] + emp_load2009["CNS03"] + emp_load2009["CNS04"] + emp_load2009["CNS05"] + emp_load2009["CNS06"] + emp_load2009["CNS08"]
emp_load2009["emp_service"] = emp_load2009["CNS12"] + emp_load2009["CNS14"] + emp_load2009["CNS15"] + emp_load2009["CNS16"] + emp_load2009["CNS19"]
emp_load2009["emp_entertain"] =  emp_load2009["CNS17"] + emp_load2009["CNS18"]																	

## Select columns
emp_load2009 = emp_load2009[["blk2000", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2000 to string
emp_load2009['blk2000'] = emp_load2009['blk2000'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2009 = pd.merge(emp_load2009, nhgis_blk2000_bg2010, how='left', left_on = "blk2000", right_on = "blk2000ge")
emp2009 = emp2009[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2009[emp_columns].multiply(emp2009["weight"], axis="index")

## And sum within 2010 block groups
emp2009 = emp2009.groupby("bg2010ge", as_index=False).sum()
emp2009 = emp2009.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2009 = emp2009.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2009[emp_columns] += 1

## Calculate employment entropy
emp2009["E"] = ((emp2009["emp_retail"]/emp2009["emp_tot"])*np.log(emp2009["emp_retail"]/emp2009["emp_tot"])) + \
((emp2009["emp_office"]/emp2009["emp_tot"])*np.log(emp2009["emp_office"]/emp2009["emp_tot"]))  + \
((emp2009["emp_industrial"]/emp2009["emp_tot"])*np.log(emp2009["emp_industrial"]/emp2009["emp_tot"])) + \
((emp2009["emp_service"]/emp2009["emp_tot"])*np.log(emp2009["emp_service"]/emp2009["emp_tot"])) + \
((emp2009["emp_entertain"]/emp2009["emp_tot"])*np.log(emp2009["emp_entertain"]/emp2009["emp_tot"]))

emp2009["emp_entropy"] = -emp2009["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2009 = pd.read_csv("../acs_change_file/outputs/acs2009.csv")
acs2009["bg2010"] = acs2009['bg2010'].astype(str).apply(add_leading_zero)
acs2009["hh"]  = acs2009["area_acres"] * acs2009["hh_den_acre"]

emp_hh2009 = pd.merge(emp2009, acs2009, on = "bg2010", how = "left")

emp_hh2009["total_activity"] = emp_hh2009["hh"] + emp_hh2009["emp_tot"]
emp_hh2009["A"] = ((emp_hh2009["hh"] / emp_hh2009["total_activity"])*np.log(emp_hh2009["hh"] /emp_hh2009["total_activity"])) + \
((emp_hh2009["emp_retail"] / emp_hh2009["total_activity"])*np.log(emp_hh2009["emp_retail"] /emp_hh2009["total_activity"]))  + \
((emp_hh2009["emp_office"] / emp_hh2009["total_activity"])*np.log(emp_hh2009["emp_office"] /emp_hh2009["total_activity"]))  + \
((emp_hh2009["emp_industrial"] / emp_hh2009["total_activity"])*np.log(emp_hh2009["emp_industrial"] /emp_hh2009["total_activity"]))  + \
((emp_hh2009["emp_service"] / emp_hh2009["total_activity"])*np.log(emp_hh2009["emp_service"] /emp_hh2009["total_activity"]))  + \
((emp_hh2009["emp_entertain"] / emp_hh2009["total_activity"])*np.log(emp_hh2009["emp_entertain"] /emp_hh2009["total_activity"])) 

emp_hh2009["emp_hh_entropy"] = -emp_hh2009["A"]/np.log(6)

## Employment density
emp_hh2009["emp_den_acre"] = emp_hh2009["emp_tot"] / emp_hh2009["area_acres"]
## Employment and household density
emp_hh2009["emp_hh_den_acre"] = (emp_hh2009["emp_tot"] + emp_hh2009["hh"]) / emp_hh2009["area_acres"]
## Employment and population density
emp_hh2009["pop"] = emp_hh2009["pop_den_acre"] * emp_hh2009["area_acres"]
emp_hh2009["emp_pop_den_acre"] = (emp_hh2009["emp_tot"] + emp_hh2009["pop"]) / emp_hh2009["area_acres"]

## Add year
lehd2009 = emp_hh2009[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2009.head())
print(lehd2009.shape)
lehd2009.to_csv("./outputs/lehd2009.csv", index = False)

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
emp_load2010["emp_retail"] = emp_load2010["CNS07"]
emp_load2010["emp_office"] = emp_load2010["CNS09"] + emp_load2010["CNS10"] + emp_load2010["CNS11"] + emp_load2010["CNS13"] + emp_load2010["CNS20"]
emp_load2010["emp_industrial"] =  emp_load2010["CNS01"] + emp_load2010["CNS02"] + emp_load2010["CNS03"] + emp_load2010["CNS04"] + emp_load2010["CNS05"] + emp_load2010["CNS06"] + emp_load2010["CNS08"]
emp_load2010["emp_service"] = emp_load2010["CNS12"] + emp_load2010["CNS14"] + emp_load2010["CNS15"] + emp_load2010["CNS16"] + emp_load2010["CNS19"]
emp_load2010["emp_entertain"] =  emp_load2010["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2010 = emp_load2010[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2010['bg2010'] = emp_load2010['bg2010'].astype(str)
## Remove last three characters
emp_load2010['bg2010'] = emp_load2010['bg2010'].str[:-3]
## Add leading zeros
emp_load2010['bg2010'] = emp_load2010['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2010 = emp_load2010.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2010 += 1

## Calculate employment entropy
emp2010["E"] = ((emp2010["emp_retail"]/emp2010["emp_tot"])*np.log(emp2010["emp_retail"]/emp2010["emp_tot"])) + \
((emp2010["emp_office"]/emp2010["emp_tot"])*np.log(emp2010["emp_office"]/emp2010["emp_tot"]))  + \
((emp2010["emp_industrial"]/emp2010["emp_tot"])*np.log(emp2010["emp_industrial"]/emp2010["emp_tot"])) + \
((emp2010["emp_service"]/emp2010["emp_tot"])*np.log(emp2010["emp_service"]/emp2010["emp_tot"])) + \
((emp2010["emp_entertain"]/emp2010["emp_tot"])*np.log(emp2010["emp_entertain"]/emp2010["emp_tot"]))

emp2010["emp_entropy"] = -emp2010["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2010 = pd.read_csv("../acs_change_file/outputs/acs2010.csv")
acs2010["bg2010"] = acs2010['bg2010'].astype(str).apply(add_leading_zero)
acs2010["hh"]  = acs2010["area_acres"] * acs2010["hh_den_acre"]

emp_hh2010 = pd.merge(emp2010, acs2010, on = "bg2010", how = "left")

emp_hh2010["total_activity"] = emp_hh2010["hh"] + emp_hh2010["emp_tot"]
emp_hh2010["A"] = ((emp_hh2010["hh"] / emp_hh2010["total_activity"])*np.log(emp_hh2010["hh"] /emp_hh2010["total_activity"])) + \
((emp_hh2010["emp_retail"] / emp_hh2010["total_activity"])*np.log(emp_hh2010["emp_retail"] /emp_hh2010["total_activity"]))  + \
((emp_hh2010["emp_office"] / emp_hh2010["total_activity"])*np.log(emp_hh2010["emp_office"] /emp_hh2010["total_activity"]))  + \
((emp_hh2010["emp_industrial"] / emp_hh2010["total_activity"])*np.log(emp_hh2010["emp_industrial"] /emp_hh2010["total_activity"]))  + \
((emp_hh2010["emp_service"] / emp_hh2010["total_activity"])*np.log(emp_hh2010["emp_service"] /emp_hh2010["total_activity"]))  + \
((emp_hh2010["emp_entertain"] / emp_hh2010["total_activity"])*np.log(emp_hh2010["emp_entertain"] /emp_hh2010["total_activity"])) 

emp_hh2010["emp_hh_entropy"] = -emp_hh2010["A"]/np.log(6)

## Employment density
emp_hh2010["emp_den_acre"] = emp_hh2010["emp_tot"] / emp_hh2010["area_acres"]
## Employment and household density
emp_hh2010["emp_hh_den_acre"] = (emp_hh2010["emp_tot"] + emp_hh2010["hh"]) / emp_hh2010["area_acres"]
## Employment and population density
emp_hh2010["pop"] = emp_hh2010["pop_den_acre"] * emp_hh2010["area_acres"]
emp_hh2010["emp_pop_den_acre"] = (emp_hh2010["emp_tot"] + emp_hh2010["pop"]) / emp_hh2010["area_acres"]

## Add year
lehd2010 = emp_hh2010[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2010.head())
print(lehd2010.shape)
lehd2010.to_csv("./outputs/lehd2010.csv", index = False)

# 2011
print("Getting 2011 employment data...")
emp_load = []
for i in states_list:
	url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2011.csv.gz' 
	response = requests.get(url)
	content = response.content
	df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
	df['state_abb'] = i.upper()
	emp_load.append(df)

emp_load = pd.concat(emp_load)
emp_load2011 = emp_load.reset_index()

## Calculate employment by industry
emp_load2011["bg2010"] = emp_load2011["w_geocode"]
emp_load2011["emp_tot"] = emp_load2011["C000"]
emp_load2011["emp_retail"] = emp_load2011["CNS07"]
emp_load2011["emp_office"] = emp_load2011["CNS09"] + emp_load2011["CNS10"] + emp_load2011["CNS11"] + emp_load2011["CNS13"] + emp_load2011["CNS20"]
emp_load2011["emp_industrial"] =  emp_load2011["CNS01"] + emp_load2011["CNS02"] + emp_load2011["CNS03"] + emp_load2011["CNS04"] + emp_load2011["CNS05"] + emp_load2011["CNS06"] + emp_load2011["CNS08"]
emp_load2011["emp_service"] = emp_load2011["CNS12"] + emp_load2011["CNS14"] + emp_load2011["CNS15"] + emp_load2011["CNS16"] + emp_load2011["CNS19"]
emp_load2011["emp_entertain"] =  emp_load2011["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2011 = emp_load2011[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2011['bg2010'] = emp_load2011['bg2010'].astype(str)
## Remove last three characters
emp_load2011['bg2010'] = emp_load2011['bg2010'].str[:-3]
## Add leading zeros
emp_load2011['bg2010'] = emp_load2011['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2011 = emp_load2011.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2011 += 1

## Calculate employment entropy
emp2011["E"] = ((emp2011["emp_retail"]/emp2011["emp_tot"])*np.log(emp2011["emp_retail"]/emp2011["emp_tot"])) + \
((emp2011["emp_office"]/emp2011["emp_tot"])*np.log(emp2011["emp_office"]/emp2011["emp_tot"]))  + \
((emp2011["emp_industrial"]/emp2011["emp_tot"])*np.log(emp2011["emp_industrial"]/emp2011["emp_tot"])) + \
((emp2011["emp_service"]/emp2011["emp_tot"])*np.log(emp2011["emp_service"]/emp2011["emp_tot"])) + \
((emp2011["emp_entertain"]/emp2011["emp_tot"])*np.log(emp2011["emp_entertain"]/emp2011["emp_tot"]))

emp2011["emp_entropy"] = -emp2011["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2011 = pd.read_csv("../acs_change_file/outputs/acs2011.csv")
acs2011["bg2010"] = acs2011['bg2010'].astype(str).apply(add_leading_zero)
acs2011["hh"]  = acs2011["area_acres"] * acs2011["hh_den_acre"]

emp_hh2011 = pd.merge(emp2011, acs2011, on = "bg2010", how = "left")

emp_hh2011["total_activity"] = emp_hh2011["hh"] + emp_hh2011["emp_tot"]
emp_hh2011["A"] = ((emp_hh2011["hh"] / emp_hh2011["total_activity"])*np.log(emp_hh2011["hh"] /emp_hh2011["total_activity"])) + \
((emp_hh2011["emp_retail"] / emp_hh2011["total_activity"])*np.log(emp_hh2011["emp_retail"] /emp_hh2011["total_activity"]))  + \
((emp_hh2011["emp_office"] / emp_hh2011["total_activity"])*np.log(emp_hh2011["emp_office"] /emp_hh2011["total_activity"]))  + \
((emp_hh2011["emp_industrial"] / emp_hh2011["total_activity"])*np.log(emp_hh2011["emp_industrial"] /emp_hh2011["total_activity"]))  + \
((emp_hh2011["emp_service"] / emp_hh2011["total_activity"])*np.log(emp_hh2011["emp_service"] /emp_hh2011["total_activity"]))  + \
((emp_hh2011["emp_entertain"] / emp_hh2011["total_activity"])*np.log(emp_hh2011["emp_entertain"] /emp_hh2011["total_activity"])) 

emp_hh2011["emp_hh_entropy"] = -emp_hh2011["A"]/np.log(6)

## Employment density
emp_hh2011["emp_den_acre"] = emp_hh2011["emp_tot"] / emp_hh2011["area_acres"]
## Employment and household density
emp_hh2011["emp_hh_den_acre"] = (emp_hh2011["emp_tot"] + emp_hh2011["hh"]) / emp_hh2011["area_acres"]
## Employment and population density
emp_hh2011["pop"] = emp_hh2011["pop_den_acre"] * emp_hh2011["area_acres"]
emp_hh2011["emp_pop_den_acre"] = (emp_hh2011["emp_tot"] + emp_hh2011["pop"]) / emp_hh2011["area_acres"]

## Add year
lehd2011 = emp_hh2011[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2011.head())
print(lehd2011.shape)
lehd2011.to_csv("./outputs/lehd2011.csv", index = False)

# 2012
print("Getting 2012 employment data...")
emp_load = []
for i in states_list:
	url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2012.csv.gz' 
	response = requests.get(url)
	content = response.content
	df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
	df['state_abb'] = i.upper()
	emp_load.append(df)

emp_load = pd.concat(emp_load)
emp_load2012 = emp_load.reset_index()

## Calculate employment by industry
emp_load2012["bg2010"] = emp_load2012["w_geocode"]
emp_load2012["emp_tot"] = emp_load2012["C000"]
emp_load2012["emp_retail"] = emp_load2012["CNS07"]
emp_load2012["emp_office"] = emp_load2012["CNS09"] + emp_load2012["CNS10"] + emp_load2012["CNS11"] + emp_load2012["CNS13"] + emp_load2012["CNS20"]
emp_load2012["emp_industrial"] =  emp_load2012["CNS01"] + emp_load2012["CNS02"] + emp_load2012["CNS03"] + emp_load2012["CNS04"] + emp_load2012["CNS05"] + emp_load2012["CNS06"] + emp_load2012["CNS08"]
emp_load2012["emp_service"] = emp_load2012["CNS12"] + emp_load2012["CNS14"] + emp_load2012["CNS15"] + emp_load2012["CNS16"] + emp_load2012["CNS19"]
emp_load2012["emp_entertain"] =  emp_load2012["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2012 = emp_load2012[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2012['bg2010'] = emp_load2012['bg2010'].astype(str)
## Remove last three characters
emp_load2012['bg2010'] = emp_load2012['bg2010'].str[:-3]
## Add leading zeros
emp_load2012['bg2010'] = emp_load2012['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2012 = emp_load2012.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2012 += 1

## Calculate employment entropy
emp2012["E"] = ((emp2012["emp_retail"]/emp2012["emp_tot"])*np.log(emp2012["emp_retail"]/emp2012["emp_tot"])) + \
((emp2012["emp_office"]/emp2012["emp_tot"])*np.log(emp2012["emp_office"]/emp2012["emp_tot"]))  + \
((emp2012["emp_industrial"]/emp2012["emp_tot"])*np.log(emp2012["emp_industrial"]/emp2012["emp_tot"])) + \
((emp2012["emp_service"]/emp2012["emp_tot"])*np.log(emp2012["emp_service"]/emp2012["emp_tot"])) + \
((emp2012["emp_entertain"]/emp2012["emp_tot"])*np.log(emp2012["emp_entertain"]/emp2012["emp_tot"]))

emp2012["emp_entropy"] = -emp2012["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2012 = pd.read_csv("../acs_change_file/outputs/acs2012.csv")
acs2012["bg2010"] = acs2012['bg2010'].astype(str).apply(add_leading_zero)
acs2012["hh"]  = acs2012["area_acres"] * acs2012["hh_den_acre"]

emp_hh2012 = pd.merge(emp2012, acs2012, on = "bg2010", how = "left")

emp_hh2012["total_activity"] = emp_hh2012["hh"] + emp_hh2012["emp_tot"]
emp_hh2012["A"] = ((emp_hh2012["hh"] / emp_hh2012["total_activity"])*np.log(emp_hh2012["hh"] /emp_hh2012["total_activity"])) + \
((emp_hh2012["emp_retail"] / emp_hh2012["total_activity"])*np.log(emp_hh2012["emp_retail"] /emp_hh2012["total_activity"]))  + \
((emp_hh2012["emp_office"] / emp_hh2012["total_activity"])*np.log(emp_hh2012["emp_office"] /emp_hh2012["total_activity"]))  + \
((emp_hh2012["emp_industrial"] / emp_hh2012["total_activity"])*np.log(emp_hh2012["emp_industrial"] /emp_hh2012["total_activity"]))  + \
((emp_hh2012["emp_service"] / emp_hh2012["total_activity"])*np.log(emp_hh2012["emp_service"] /emp_hh2012["total_activity"]))  + \
((emp_hh2012["emp_entertain"] / emp_hh2012["total_activity"])*np.log(emp_hh2012["emp_entertain"] /emp_hh2012["total_activity"])) 

emp_hh2012["emp_hh_entropy"] = -emp_hh2012["A"]/np.log(6)

## Employment density
emp_hh2012["emp_den_acre"] = emp_hh2012["emp_tot"] / emp_hh2012["area_acres"]
## Employment and household density
emp_hh2012["emp_hh_den_acre"] = (emp_hh2012["emp_tot"] + emp_hh2012["hh"]) / emp_hh2012["area_acres"]
## Employment and population density
emp_hh2012["pop"] = emp_hh2012["pop_den_acre"] * emp_hh2012["area_acres"]
emp_hh2012["emp_pop_den_acre"] = (emp_hh2012["emp_tot"] + emp_hh2012["pop"]) / emp_hh2012["area_acres"]

## Add year
lehd2012 = emp_hh2012[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2012.head())
print(lehd2012.shape)
lehd2012.to_csv("./outputs/lehd2012.csv", index = False)

# 2013
print("Getting 2013 employment data...")
emp_load = []
for i in states_list:
	url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2013.csv.gz' 
	response = requests.get(url)
	content = response.content
	df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
	df['state_abb'] = i.upper()
	emp_load.append(df)

emp_load = pd.concat(emp_load)
emp_load2013 = emp_load.reset_index()

## Calculate employment by industry
emp_load2013["bg2010"] = emp_load2013["w_geocode"]
emp_load2013["emp_tot"] = emp_load2013["C000"]
emp_load2013["emp_retail"] = emp_load2013["CNS07"]
emp_load2013["emp_office"] = emp_load2013["CNS09"] + emp_load2013["CNS10"] + emp_load2013["CNS11"] + emp_load2013["CNS13"] + emp_load2013["CNS20"]
emp_load2013["emp_industrial"] =  emp_load2013["CNS01"] + emp_load2013["CNS02"] + emp_load2013["CNS03"] + emp_load2013["CNS04"] + emp_load2013["CNS05"] + emp_load2013["CNS06"] + emp_load2013["CNS08"]
emp_load2013["emp_service"] = emp_load2013["CNS12"] + emp_load2013["CNS14"] + emp_load2013["CNS15"] + emp_load2013["CNS16"] + emp_load2013["CNS19"]
emp_load2013["emp_entertain"] =  emp_load2013["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2013 = emp_load2013[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2013['bg2010'] = emp_load2013['bg2010'].astype(str)
## Remove last three characters
emp_load2013['bg2010'] = emp_load2013['bg2010'].str[:-3]
## Add leading zeros
emp_load2013['bg2010'] = emp_load2013['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2013 = emp_load2013.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2013 += 1

## Calculate employment entropy
emp2013["E"] = ((emp2013["emp_retail"]/emp2013["emp_tot"])*np.log(emp2013["emp_retail"]/emp2013["emp_tot"])) + \
((emp2013["emp_office"]/emp2013["emp_tot"])*np.log(emp2013["emp_office"]/emp2013["emp_tot"]))  + \
((emp2013["emp_industrial"]/emp2013["emp_tot"])*np.log(emp2013["emp_industrial"]/emp2013["emp_tot"])) + \
((emp2013["emp_service"]/emp2013["emp_tot"])*np.log(emp2013["emp_service"]/emp2013["emp_tot"])) + \
((emp2013["emp_entertain"]/emp2013["emp_tot"])*np.log(emp2013["emp_entertain"]/emp2013["emp_tot"]))

emp2013["emp_entropy"] = -emp2013["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2013 = pd.read_csv("../acs_change_file/outputs/acs2013.csv")
acs2013["bg2010"] = acs2013['bg2010'].astype(str).apply(add_leading_zero)
acs2013["hh"]  = acs2013["area_acres"] * acs2013["hh_den_acre"]

emp_hh2013 = pd.merge(emp2013, acs2013, on = "bg2010", how = "left")

emp_hh2013["total_activity"] = emp_hh2013["hh"] + emp_hh2013["emp_tot"]
emp_hh2013["A"] = ((emp_hh2013["hh"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["hh"] /emp_hh2013["total_activity"])) + \
((emp_hh2013["emp_retail"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_retail"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_office"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_office"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_industrial"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_industrial"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_service"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_service"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_entertain"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_entertain"] /emp_hh2013["total_activity"])) 

emp_hh2013["emp_hh_entropy"] = -emp_hh2013["A"]/np.log(6)

## Employment density
emp_hh2013["emp_den_acre"] = emp_hh2013["emp_tot"] / emp_hh2013["area_acres"]
## Employment and household density
emp_hh2013["emp_hh_den_acre"] = (emp_hh2013["emp_tot"] + emp_hh2013["hh"]) / emp_hh2013["area_acres"]
## Employment and population density
emp_hh2013["pop"] = emp_hh2013["pop_den_acre"] * emp_hh2013["area_acres"]
emp_hh2013["emp_pop_den_acre"] = (emp_hh2013["emp_tot"] + emp_hh2013["pop"]) / emp_hh2013["area_acres"]

## Add year
lehd2013 = emp_hh2013[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2013.head())
print(lehd2013.shape)
lehd2013.to_csv("./outputs/lehd2013.csv", index = False)

# 2014
print("Getting 2014 employment data...")
emp_load = []
for i in states_list:
	url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2014.csv.gz' 
	response = requests.get(url)
	content = response.content
	df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
	df['state_abb'] = i.upper()
	emp_load.append(df)

emp_load = pd.concat(emp_load)
emp_load2014 = emp_load.reset_index()

## Calculate employment by industry
emp_load2014["bg2010"] = emp_load2014["w_geocode"]
emp_load2014["emp_tot"] = emp_load2014["C000"]
emp_load2014["emp_retail"] = emp_load2014["CNS07"]
emp_load2014["emp_office"] = emp_load2014["CNS09"] + emp_load2014["CNS10"] + emp_load2014["CNS11"] + emp_load2014["CNS13"] + emp_load2014["CNS20"]
emp_load2014["emp_industrial"] =  emp_load2014["CNS01"] + emp_load2014["CNS02"] + emp_load2014["CNS03"] + emp_load2014["CNS04"] + emp_load2014["CNS05"] + emp_load2014["CNS06"] + emp_load2014["CNS08"]
emp_load2014["emp_service"] = emp_load2014["CNS12"] + emp_load2014["CNS14"] + emp_load2014["CNS15"] + emp_load2014["CNS16"] + emp_load2014["CNS19"]
emp_load2014["emp_entertain"] =  emp_load2014["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2014 = emp_load2014[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2014['bg2010'] = emp_load2014['bg2010'].astype(str)
## Remove last three characters
emp_load2014['bg2010'] = emp_load2014['bg2010'].str[:-3]
## Add leading zeros
emp_load2014['bg2010'] = emp_load2014['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2014 = emp_load2014.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2014 += 1

## Calculate employment entropy
emp2014["E"] = ((emp2014["emp_retail"]/emp2014["emp_tot"])*np.log(emp2014["emp_retail"]/emp2014["emp_tot"])) + \
((emp2014["emp_office"]/emp2014["emp_tot"])*np.log(emp2014["emp_office"]/emp2014["emp_tot"]))  + \
((emp2014["emp_industrial"]/emp2014["emp_tot"])*np.log(emp2014["emp_industrial"]/emp2014["emp_tot"])) + \
((emp2014["emp_service"]/emp2014["emp_tot"])*np.log(emp2014["emp_service"]/emp2014["emp_tot"])) + \
((emp2014["emp_entertain"]/emp2014["emp_tot"])*np.log(emp2014["emp_entertain"]/emp2014["emp_tot"]))

emp2014["emp_entropy"] = -emp2014["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2014 = pd.read_csv("../acs_change_file/outputs/acs2014.csv")
acs2014["bg2010"] = acs2014['bg2010'].astype(str).apply(add_leading_zero)
acs2014["hh"]  = acs2014["area_acres"] * acs2014["hh_den_acre"]

emp_hh2014 = pd.merge(emp2014, acs2014, on = "bg2010", how = "left")

emp_hh2014["total_activity"] = emp_hh2014["hh"] + emp_hh2014["emp_tot"]
emp_hh2014["A"] = ((emp_hh2014["hh"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["hh"] /emp_hh2014["total_activity"])) + \
((emp_hh2014["emp_retail"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_retail"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_office"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_office"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_industrial"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_industrial"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_service"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_service"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_entertain"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_entertain"] /emp_hh2014["total_activity"])) 

emp_hh2014["emp_hh_entropy"] = -emp_hh2014["A"]/np.log(6)

## Employment density
emp_hh2014["emp_den_acre"] = emp_hh2014["emp_tot"] / emp_hh2014["area_acres"]
## Employment and household density
emp_hh2014["emp_hh_den_acre"] = (emp_hh2014["emp_tot"] + emp_hh2014["hh"]) / emp_hh2014["area_acres"]
## Employment and population density
emp_hh2014["pop"] = emp_hh2014["pop_den_acre"] * emp_hh2014["area_acres"]
emp_hh2014["emp_pop_den_acre"] = (emp_hh2014["emp_tot"] + emp_hh2014["pop"]) / emp_hh2014["area_acres"]

## Add year
lehd2014 = emp_hh2014[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2014.head())
print(lehd2014.shape)
lehd2014.to_csv("./outputs/lehd2014.csv", index = False)

# 2015
print("Getting 2015 employment data...")
emp_load = []
for i in states_list:
	url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2015.csv.gz' 
	response = requests.get(url)
	content = response.content
	df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
	df['state_abb'] = i.upper()
	emp_load.append(df)

emp_load = pd.concat(emp_load)
emp_load2015 = emp_load.reset_index()

## Calculate employment by industry
emp_load2015["bg2010"] = emp_load2015["w_geocode"]
emp_load2015["emp_tot"] = emp_load2015["C000"]
emp_load2015["emp_retail"] = emp_load2015["CNS07"]
emp_load2015["emp_office"] = emp_load2015["CNS09"] + emp_load2015["CNS10"] + emp_load2015["CNS11"] + emp_load2015["CNS13"] + emp_load2015["CNS20"]
emp_load2015["emp_industrial"] =  emp_load2015["CNS01"] + emp_load2015["CNS02"] + emp_load2015["CNS03"] + emp_load2015["CNS04"] + emp_load2015["CNS05"] + emp_load2015["CNS06"] + emp_load2015["CNS08"]
emp_load2015["emp_service"] = emp_load2015["CNS12"] + emp_load2015["CNS14"] + emp_load2015["CNS15"] + emp_load2015["CNS16"] + emp_load2015["CNS19"]
emp_load2015["emp_entertain"] =  emp_load2015["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2015 = emp_load2015[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2015['bg2010'] = emp_load2015['bg2010'].astype(str)
## Remove last three characters
emp_load2015['bg2010'] = emp_load2015['bg2010'].str[:-3]
## Add leading zeros
emp_load2015['bg2010'] = emp_load2015['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2015 = emp_load2015.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2015 += 1

## Calculate employment entropy
emp2015["E"] = ((emp2015["emp_retail"]/emp2015["emp_tot"])*np.log(emp2015["emp_retail"]/emp2015["emp_tot"])) + \
((emp2015["emp_office"]/emp2015["emp_tot"])*np.log(emp2015["emp_office"]/emp2015["emp_tot"]))  + \
((emp2015["emp_industrial"]/emp2015["emp_tot"])*np.log(emp2015["emp_industrial"]/emp2015["emp_tot"])) + \
((emp2015["emp_service"]/emp2015["emp_tot"])*np.log(emp2015["emp_service"]/emp2015["emp_tot"])) + \
((emp2015["emp_entertain"]/emp2015["emp_tot"])*np.log(emp2015["emp_entertain"]/emp2015["emp_tot"]))

emp2015["emp_entropy"] = -emp2015["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2015 = pd.read_csv("../acs_change_file/outputs/acs2015.csv")
acs2015["bg2010"] = acs2015['bg2010'].astype(str).apply(add_leading_zero)
acs2015["hh"]  = acs2015["area_acres"] * acs2015["hh_den_acre"]

emp_hh2015 = pd.merge(emp2015, acs2015, on = "bg2010", how = "left")

emp_hh2015["total_activity"] = emp_hh2015["hh"] + emp_hh2015["emp_tot"]
emp_hh2015["A"] = ((emp_hh2015["hh"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["hh"] /emp_hh2015["total_activity"])) + \
((emp_hh2015["emp_retail"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_retail"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_office"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_office"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_industrial"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_industrial"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_service"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_service"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_entertain"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_entertain"] /emp_hh2015["total_activity"])) 

emp_hh2015["emp_hh_entropy"] = -emp_hh2015["A"]/np.log(6)

## Employment density
emp_hh2015["emp_den_acre"] = emp_hh2015["emp_tot"] / emp_hh2015["area_acres"]
## Employment and household density
emp_hh2015["emp_hh_den_acre"] = (emp_hh2015["emp_tot"] + emp_hh2015["hh"]) / emp_hh2015["area_acres"]
## Employment and population density
emp_hh2015["pop"] = emp_hh2015["pop_den_acre"] * emp_hh2015["area_acres"]
emp_hh2015["emp_pop_den_acre"] = (emp_hh2015["emp_tot"] + emp_hh2015["pop"]) / emp_hh2015["area_acres"]

## Add year
lehd2015 = emp_hh2015[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2015.head())
print(lehd2015.shape)
lehd2015.to_csv("./outputs/lehd2015.csv", index = False)

# 2016
print("Getting 2016 employment data...")
emp_load = []
for i in states_list:
	url = 'https://lehd.ces.census.gov/data/lodes/LODES7/' + i + '/wac/' + i + '_wac_S000_JT00_2016.csv.gz' 
	response = requests.get(url)
	content = response.content
	df = pd.read_csv(io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"')
	df['state_abb'] = i.upper()
	emp_load.append(df)

emp_load = pd.concat(emp_load)
emp_load2016 = emp_load.reset_index()

## Calculate employment by industry
emp_load2016["bg2010"] = emp_load2016["w_geocode"]
emp_load2016["emp_tot"] = emp_load2016["C000"]
emp_load2016["emp_retail"] = emp_load2016["CNS07"]
emp_load2016["emp_office"] = emp_load2016["CNS09"] + emp_load2016["CNS10"] + emp_load2016["CNS11"] + emp_load2016["CNS13"] + emp_load2016["CNS20"]
emp_load2016["emp_industrial"] =  emp_load2016["CNS01"] + emp_load2016["CNS02"] + emp_load2016["CNS03"] + emp_load2016["CNS04"] + emp_load2016["CNS05"] + emp_load2016["CNS06"] + emp_load2016["CNS08"]
emp_load2016["emp_service"] = emp_load2016["CNS12"] + emp_load2016["CNS14"] + emp_load2016["CNS15"] + emp_load2016["CNS16"] + emp_load2016["CNS19"]
emp_load2016["emp_entertain"] =  emp_load2016["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2016 = emp_load2016[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2016['bg2010'] = emp_load2016['bg2010'].astype(str)
## Remove last three characters
emp_load2016['bg2010'] = emp_load2016['bg2010'].str[:-3]
## Add leading zeros
emp_load2016['bg2010'] = emp_load2016['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2016 = emp_load2016.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2016 += 1

## Calculate employment entropy
emp2016["E"] = ((emp2016["emp_retail"]/emp2016["emp_tot"])*np.log(emp2016["emp_retail"]/emp2016["emp_tot"])) + \
((emp2016["emp_office"]/emp2016["emp_tot"])*np.log(emp2016["emp_office"]/emp2016["emp_tot"]))  + \
((emp2016["emp_industrial"]/emp2016["emp_tot"])*np.log(emp2016["emp_industrial"]/emp2016["emp_tot"])) + \
((emp2016["emp_service"]/emp2016["emp_tot"])*np.log(emp2016["emp_service"]/emp2016["emp_tot"])) + \
((emp2016["emp_entertain"]/emp2016["emp_tot"])*np.log(emp2016["emp_entertain"]/emp2016["emp_tot"]))

emp2016["emp_entropy"] = -emp2016["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2016 = pd.read_csv("../acs_change_file/outputs/acs2016.csv")
acs2016["bg2010"] = acs2016['bg2010'].astype(str).apply(add_leading_zero)
acs2016["hh"]  = acs2016["area_acres"] * acs2016["hh_den_acre"]

emp_hh2016 = pd.merge(emp2016, acs2016, on = "bg2010", how = "left")

emp_hh2016["total_activity"] = emp_hh2016["hh"] + emp_hh2016["emp_tot"]
emp_hh2016["A"] = ((emp_hh2016["hh"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["hh"] /emp_hh2016["total_activity"])) + \
((emp_hh2016["emp_retail"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_retail"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_office"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_office"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_industrial"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_industrial"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_service"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_service"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_entertain"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_entertain"] /emp_hh2016["total_activity"])) 

emp_hh2016["emp_hh_entropy"] = -emp_hh2016["A"]/np.log(6)

## Employment density
emp_hh2016["emp_den_acre"] = emp_hh2016["emp_tot"] / emp_hh2016["area_acres"]
## Employment and household density
emp_hh2016["emp_hh_den_acre"] = (emp_hh2016["emp_tot"] + emp_hh2016["hh"]) / emp_hh2016["area_acres"]
## Employment and population density
emp_hh2016["pop"] = emp_hh2016["pop_den_acre"] * emp_hh2016["area_acres"]
emp_hh2016["emp_pop_den_acre"] = (emp_hh2016["emp_tot"] + emp_hh2016["pop"]) / emp_hh2016["area_acres"]

## Add year
lehd2016 = emp_hh2016[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2016.head())
print(lehd2016.shape)
lehd2016.to_csv("./outputs/lehd2016.csv", index = False)

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
emp_load2017["emp_retail"] = emp_load2017["CNS07"]
emp_load2017["emp_office"] = emp_load2017["CNS09"] + emp_load2017["CNS10"] + emp_load2017["CNS11"] + emp_load2017["CNS13"] + emp_load2017["CNS20"]
emp_load2017["emp_industrial"] =  emp_load2017["CNS01"] + emp_load2017["CNS02"] + emp_load2017["CNS03"] + emp_load2017["CNS04"] + emp_load2017["CNS05"] + emp_load2017["CNS06"] + emp_load2017["CNS08"]
emp_load2017["emp_service"] = emp_load2017["CNS12"] + emp_load2017["CNS14"] + emp_load2017["CNS15"] + emp_load2017["CNS16"] + emp_load2017["CNS19"]
emp_load2017["emp_entertain"] =  emp_load2017["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2017 = emp_load2017[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2017['bg2010'] = emp_load2017['bg2010'].astype(str)
## Remove last three characters
emp_load2017['bg2010'] = emp_load2017['bg2010'].str[:-3]
## Add leading zeros
emp_load2017['bg2010'] = emp_load2017['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2017 = emp_load2017.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2017 += 1

## Calculate employment entropy
emp2017["E"] = ((emp2017["emp_retail"]/emp2017["emp_tot"])*np.log(emp2017["emp_retail"]/emp2017["emp_tot"])) + \
((emp2017["emp_office"]/emp2017["emp_tot"])*np.log(emp2017["emp_office"]/emp2017["emp_tot"]))  + \
((emp2017["emp_industrial"]/emp2017["emp_tot"])*np.log(emp2017["emp_industrial"]/emp2017["emp_tot"])) + \
((emp2017["emp_service"]/emp2017["emp_tot"])*np.log(emp2017["emp_service"]/emp2017["emp_tot"])) + \
((emp2017["emp_entertain"]/emp2017["emp_tot"])*np.log(emp2017["emp_entertain"]/emp2017["emp_tot"]))

emp2017["emp_entropy"] = -emp2017["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2017 = pd.read_csv("../acs_change_file/outputs/acs2017.csv")
acs2017["bg2010"] = acs2017['bg2010'].astype(str).apply(add_leading_zero)
acs2017["hh"]  = acs2017["area_acres"] * acs2017["hh_den_acre"]

emp_hh2017 = pd.merge(emp2017, acs2017, on = "bg2010", how = "left")

emp_hh2017["total_activity"] = emp_hh2017["hh"] + emp_hh2017["emp_tot"]
emp_hh2017["A"] = ((emp_hh2017["hh"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["hh"] /emp_hh2017["total_activity"])) + \
((emp_hh2017["emp_retail"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_retail"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_office"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_office"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_industrial"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_industrial"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_service"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_service"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_entertain"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_entertain"] /emp_hh2017["total_activity"])) 

emp_hh2017["emp_hh_entropy"] = -emp_hh2017["A"]/np.log(6)

## Employment density
emp_hh2017["emp_den_acre"] = emp_hh2017["emp_tot"] / emp_hh2017["area_acres"]
## Employment and household density
emp_hh2017["emp_hh_den_acre"] = (emp_hh2017["emp_tot"] + emp_hh2017["hh"]) / emp_hh2017["area_acres"]
## Employment and population density
emp_hh2017["pop"] = emp_hh2017["pop_den_acre"] * emp_hh2017["area_acres"]
emp_hh2017["emp_pop_den_acre"] = (emp_hh2017["emp_tot"] + emp_hh2017["pop"]) / emp_hh2017["area_acres"]

## Add year
lehd2017 = emp_hh2017[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2017.head())
print(lehd2017.shape)
lehd2017.to_csv("./outputs/lehd2017.csv", index = False)

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
emp_load2018["emp_retail"] = emp_load2018["CNS07"]
emp_load2018["emp_office"] = emp_load2018["CNS09"] + emp_load2018["CNS10"] + emp_load2018["CNS11"] + emp_load2018["CNS13"] + emp_load2018["CNS20"]
emp_load2018["emp_industrial"] =  emp_load2018["CNS01"] + emp_load2018["CNS02"] + emp_load2018["CNS03"] + emp_load2018["CNS04"] + emp_load2018["CNS05"] + emp_load2018["CNS06"] + emp_load2018["CNS08"]
emp_load2018["emp_service"] = emp_load2018["CNS12"] + emp_load2018["CNS14"] + emp_load2018["CNS15"] + emp_load2018["CNS16"] + emp_load2018["CNS19"]
emp_load2018["emp_entertain"] =  emp_load2018["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2018 = emp_load2018[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2018['bg2010'] = emp_load2018['bg2010'].astype(str)
## Remove last three characters
emp_load2018['bg2010'] = emp_load2018['bg2010'].str[:-3]
## Add leading zeros
emp_load2018['bg2010'] = emp_load2018['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2018 = emp_load2018.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2018 += 1

## Calculate employment entropy
emp2018["E"] = ((emp2018["emp_retail"]/emp2018["emp_tot"])*np.log(emp2018["emp_retail"]/emp2018["emp_tot"])) + \
((emp2018["emp_office"]/emp2018["emp_tot"])*np.log(emp2018["emp_office"]/emp2018["emp_tot"]))  + \
((emp2018["emp_industrial"]/emp2018["emp_tot"])*np.log(emp2018["emp_industrial"]/emp2018["emp_tot"])) + \
((emp2018["emp_service"]/emp2018["emp_tot"])*np.log(emp2018["emp_service"]/emp2018["emp_tot"])) + \
((emp2018["emp_entertain"]/emp2018["emp_tot"])*np.log(emp2018["emp_entertain"]/emp2018["emp_tot"]))

emp2018["emp_entropy"] = -emp2018["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2018 = pd.read_csv("../acs_change_file/outputs/acs2018.csv")
acs2018["bg2010"] = acs2018['bg2010'].astype(str).apply(add_leading_zero)
acs2018["hh"]  = acs2018["area_acres"] * acs2018["hh_den_acre"]

emp_hh2018 = pd.merge(emp2018, acs2018, on = "bg2010", how = "left")

emp_hh2018["total_activity"] = emp_hh2018["hh"] + emp_hh2018["emp_tot"]
emp_hh2018["A"] = ((emp_hh2018["hh"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["hh"] /emp_hh2018["total_activity"])) + \
((emp_hh2018["emp_retail"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_retail"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_office"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_office"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_industrial"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_industrial"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_service"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_service"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_entertain"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_entertain"] /emp_hh2018["total_activity"])) 

emp_hh2018["emp_hh_entropy"] = -emp_hh2018["A"]/np.log(6)

## Employment density
emp_hh2018["emp_den_acre"] = emp_hh2018["emp_tot"] / emp_hh2018["area_acres"]
## Employment and household density
emp_hh2018["emp_hh_den_acre"] = (emp_hh2018["emp_tot"] + emp_hh2018["hh"]) / emp_hh2018["area_acres"]
## Employment and population density
emp_hh2018["pop"] = emp_hh2018["pop_den_acre"] * emp_hh2018["area_acres"]
emp_hh2018["emp_pop_den_acre"] = (emp_hh2018["emp_tot"] + emp_hh2018["pop"]) / emp_hh2018["area_acres"]

## Add year
lehd2018 = emp_hh2018[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2018.head())
print(lehd2018.shape)
lehd2018.to_csv("./outputs/lehd2018.csv", index = False)

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
emp_load2019["emp_retail"] = emp_load2019["CNS07"]
emp_load2019["emp_office"] = emp_load2019["CNS09"] + emp_load2019["CNS10"] + emp_load2019["CNS11"] + emp_load2019["CNS13"] + emp_load2019["CNS20"]
emp_load2019["emp_industrial"] =  emp_load2019["CNS01"] + emp_load2019["CNS02"] + emp_load2019["CNS03"] + emp_load2019["CNS04"] + emp_load2019["CNS05"] + emp_load2019["CNS06"] + emp_load2019["CNS08"]
emp_load2019["emp_service"] = emp_load2019["CNS12"] + emp_load2019["CNS14"] + emp_load2019["CNS15"] + emp_load2019["CNS16"] + emp_load2019["CNS19"]
emp_load2019["emp_entertain"] =  emp_load2019["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2019 = emp_load2019[["bg2010", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert bg2010 to string
emp_load2019['bg2010'] = emp_load2019['bg2010'].astype(str)
## Remove last three characters
emp_load2019['bg2010'] = emp_load2019['bg2010'].str[:-3]
## Add leading zeros
emp_load2019['bg2010'] = emp_load2019['bg2010'].apply(add_leading_zero)
## Group by bg2010 and sum emp_tot to get total employment by block group
emp2019 = emp_load2019.groupby('bg2010').sum()
## Add 1 to every value for ln purposes
emp2019 += 1

## Calculate employment entropy
emp2019["E"] = ((emp2019["emp_retail"]/emp2019["emp_tot"])*np.log(emp2019["emp_retail"]/emp2019["emp_tot"])) + \
((emp2019["emp_office"]/emp2019["emp_tot"])*np.log(emp2019["emp_office"]/emp2019["emp_tot"]))  + \
((emp2019["emp_industrial"]/emp2019["emp_tot"])*np.log(emp2019["emp_industrial"]/emp2019["emp_tot"])) + \
((emp2019["emp_service"]/emp2019["emp_tot"])*np.log(emp2019["emp_service"]/emp2019["emp_tot"])) + \
((emp2019["emp_entertain"]/emp2019["emp_tot"])*np.log(emp2019["emp_entertain"]/emp2019["emp_tot"]))

emp2019["emp_entropy"] = -emp2019["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2019 = pd.read_csv("../acs_change_file/outputs/acs2019.csv")
acs2019["bg2010"] = acs2019['bg2010'].astype(str).apply(add_leading_zero)
acs2019["hh"]  = acs2019["area_acres"] * acs2019["hh_den_acre"]

emp_hh2019 = pd.merge(emp2019, acs2019, on = "bg2010", how = "left")

emp_hh2019["total_activity"] = emp_hh2019["hh"] + emp_hh2019["emp_tot"]
emp_hh2019["A"] = ((emp_hh2019["hh"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["hh"] /emp_hh2019["total_activity"])) + \
((emp_hh2019["emp_retail"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_retail"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_office"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_office"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_industrial"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_industrial"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_service"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_service"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_entertain"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_entertain"] /emp_hh2019["total_activity"])) 

emp_hh2019["emp_hh_entropy"] = -emp_hh2019["A"]/np.log(6)

## Employment density
emp_hh2019["emp_den_acre"] = emp_hh2019["emp_tot"] / emp_hh2019["area_acres"]
## Employment and household density
emp_hh2019["emp_hh_den_acre"] = (emp_hh2019["emp_tot"] + emp_hh2019["hh"]) / emp_hh2019["area_acres"]
## Employment and population density
emp_hh2019["pop"] = emp_hh2019["pop_den_acre"] * emp_hh2019["area_acres"]
emp_hh2019["emp_pop_den_acre"] = (emp_hh2019["emp_tot"] + emp_hh2019["pop"]) / emp_hh2019["area_acres"]

## Add year
lehd2019 = emp_hh2019[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2019.head())
print(lehd2019.shape)
lehd2019.to_csv("./outputs/lehd2019.csv", index = False)

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
emp_load2020["emp_retail"] = emp_load2020["CNS07"]
emp_load2020["emp_office"] = emp_load2020["CNS09"] + emp_load2020["CNS10"] + emp_load2020["CNS11"] + emp_load2020["CNS13"] + emp_load2020["CNS20"]
emp_load2020["emp_industrial"] =  emp_load2020["CNS01"] + emp_load2020["CNS02"] + emp_load2020["CNS03"] + emp_load2020["CNS04"] + emp_load2020["CNS05"] + emp_load2020["CNS06"] + emp_load2020["CNS08"]
emp_load2020["emp_service"] = emp_load2020["CNS12"] + emp_load2020["CNS14"] + emp_load2020["CNS15"] + emp_load2020["CNS16"] + emp_load2020["CNS19"]
emp_load2020["emp_entertain"] =  emp_load2020["CNS17"] + emp_load2020["CNS18"]																	

## Select columns
emp_load2020 = emp_load2020[["blk2020", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2020 to string
emp_load2020['blk2020'] = emp_load2020['blk2020'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2020 = pd.merge(emp_load2020, nhgis_blk2020_bg2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2020 = emp2020[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2020[emp_columns].multiply(emp2020["weight"], axis="index")

## And sum within 2020 block groups
emp2020 = emp2020.groupby("bg2010ge", as_index=False).sum()
emp2020 = emp2020.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2020 = emp2020.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2020[emp_columns] += 1

## Calculate employment entropy
emp2020["E"] = ((emp2020["emp_retail"]/emp2020["emp_tot"])*np.log(emp2020["emp_retail"]/emp2020["emp_tot"])) + \
((emp2020["emp_office"]/emp2020["emp_tot"])*np.log(emp2020["emp_office"]/emp2020["emp_tot"]))  + \
((emp2020["emp_industrial"]/emp2020["emp_tot"])*np.log(emp2020["emp_industrial"]/emp2020["emp_tot"])) + \
((emp2020["emp_service"]/emp2020["emp_tot"])*np.log(emp2020["emp_service"]/emp2020["emp_tot"])) + \
((emp2020["emp_entertain"]/emp2020["emp_tot"])*np.log(emp2020["emp_entertain"]/emp2020["emp_tot"]))

emp2020["emp_entropy"] = -emp2020["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2020 = pd.read_csv("../acs_change_file/outputs/acs2020.csv")
acs2020["bg2010"] = acs2020['bg2010'].astype(str).apply(add_leading_zero)
acs2020["hh"]  = acs2020["area_acres"] * acs2020["hh_den_acre"]

emp_hh2020 = pd.merge(emp2020, acs2020, on = "bg2010", how = "left")

emp_hh2020["total_activity"] = emp_hh2020["hh"] + emp_hh2020["emp_tot"]
emp_hh2020["A"] = ((emp_hh2020["hh"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["hh"] /emp_hh2020["total_activity"])) + \
((emp_hh2020["emp_retail"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_retail"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_office"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_office"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_industrial"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_industrial"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_service"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_service"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_entertain"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_entertain"] /emp_hh2020["total_activity"])) 

emp_hh2020["emp_hh_entropy"] = -emp_hh2020["A"]/np.log(6)

## Employment density
emp_hh2020["emp_den_acre"] = emp_hh2020["emp_tot"] / emp_hh2020["area_acres"]
## Employment and household density
emp_hh2020["emp_hh_den_acre"] = (emp_hh2020["emp_tot"] + emp_hh2020["hh"]) / emp_hh2020["area_acres"]
## Employment and population density
emp_hh2020["pop"] = emp_hh2020["pop_den_acre"] * emp_hh2020["area_acres"]
emp_hh2020["emp_pop_den_acre"] = (emp_hh2020["emp_tot"] + emp_hh2020["pop"]) / emp_hh2020["area_acres"]

## Add year
lehd2020 = emp_hh2020[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2020.head())
print(lehd2020.shape)
lehd2020.to_csv("./outputs/lehd2020.csv", index = False)

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
emp_load2021["emp_retail"] = emp_load2021["CNS07"]
emp_load2021["emp_office"] = emp_load2021["CNS09"] + emp_load2021["CNS10"] + emp_load2021["CNS11"] + emp_load2021["CNS13"] + emp_load2021["CNS20"]
emp_load2021["emp_industrial"] =  emp_load2021["CNS01"] + emp_load2021["CNS02"] + emp_load2021["CNS03"] + emp_load2021["CNS04"] + emp_load2021["CNS05"] + emp_load2021["CNS06"] + emp_load2021["CNS08"]
emp_load2021["emp_service"] = emp_load2021["CNS12"] + emp_load2021["CNS14"] + emp_load2021["CNS15"] + emp_load2021["CNS16"] + emp_load2021["CNS19"]
emp_load2021["emp_entertain"] =  emp_load2021["CNS17"] + emp_load2021["CNS18"]																	

## Select columns
emp_load2021 = emp_load2021[["blk2020", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2020 to string
emp_load2021['blk2020'] = emp_load2021['blk2020'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2021 = pd.merge(emp_load2021, nhgis_blk2020_bg2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2021 = emp2021[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2021[emp_columns].multiply(emp2021["weight"], axis="index")

## And sum within 2021 block groups
emp2021 = emp2021.groupby("bg2010ge", as_index=False).sum()
emp2021 = emp2021.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2021 = emp2021.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2021[emp_columns] += 1

## Calculate employment entropy
emp2021["E"] = ((emp2021["emp_retail"]/emp2021["emp_tot"])*np.log(emp2021["emp_retail"]/emp2021["emp_tot"])) + \
((emp2021["emp_office"]/emp2021["emp_tot"])*np.log(emp2021["emp_office"]/emp2021["emp_tot"]))  + \
((emp2021["emp_industrial"]/emp2021["emp_tot"])*np.log(emp2021["emp_industrial"]/emp2021["emp_tot"])) + \
((emp2021["emp_service"]/emp2021["emp_tot"])*np.log(emp2021["emp_service"]/emp2021["emp_tot"])) + \
((emp2021["emp_entertain"]/emp2021["emp_tot"])*np.log(emp2021["emp_entertain"]/emp2021["emp_tot"]))

emp2021["emp_entropy"] = -emp2021["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2021 = pd.read_csv("../acs_change_file/outputs/acs2021.csv")
acs2021["bg2010"] = acs2021['bg2010'].astype(str).apply(add_leading_zero)
acs2021["hh"]  = acs2021["area_acres"] * acs2021["hh_den_acre"]

emp_hh2021 = pd.merge(emp2021, acs2021, on = "bg2010", how = "left")

emp_hh2021["total_activity"] = emp_hh2021["hh"] + emp_hh2021["emp_tot"]
emp_hh2021["A"] = ((emp_hh2021["hh"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["hh"] /emp_hh2021["total_activity"])) + \
((emp_hh2021["emp_retail"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_retail"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_office"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_office"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_industrial"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_industrial"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_service"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_service"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_entertain"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_entertain"] /emp_hh2021["total_activity"])) 

emp_hh2021["emp_hh_entropy"] = -emp_hh2021["A"]/np.log(6)

## Employment density
emp_hh2021["emp_den_acre"] = emp_hh2021["emp_tot"] / emp_hh2021["area_acres"]
## Employment and household density
emp_hh2021["emp_hh_den_acre"] = (emp_hh2021["emp_tot"] + emp_hh2021["hh"]) / emp_hh2021["area_acres"]
## Employment and population density
emp_hh2021["pop"] = emp_hh2021["pop_den_acre"] * emp_hh2021["area_acres"]
emp_hh2021["emp_pop_den_acre"] = (emp_hh2021["emp_tot"] + emp_hh2021["pop"]) / emp_hh2021["area_acres"]

## Add year
lehd2021 = emp_hh2021[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2021.head())
print(lehd2021.shape)
lehd2021.to_csv("./outputs/lehd2021.csv", index = False)

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
emp_load2022["emp_retail"] = emp_load2022["CNS07"]
emp_load2022["emp_office"] = emp_load2022["CNS09"] + emp_load2022["CNS10"] + emp_load2022["CNS11"] + emp_load2022["CNS13"] + emp_load2022["CNS20"]
emp_load2022["emp_industrial"] =  emp_load2022["CNS01"] + emp_load2022["CNS02"] + emp_load2022["CNS03"] + emp_load2022["CNS04"] + emp_load2022["CNS05"] + emp_load2022["CNS06"] + emp_load2022["CNS08"]
emp_load2022["emp_service"] = emp_load2022["CNS12"] + emp_load2022["CNS14"] + emp_load2022["CNS15"] + emp_load2022["CNS16"] + emp_load2022["CNS19"]
emp_load2022["emp_entertain"] =  emp_load2022["CNS17"] + emp_load2022["CNS18"]																	

## Select columns
emp_load2022 = emp_load2022[["blk2020", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert blk2020 to string
emp_load2022['blk2020'] = emp_load2022['blk2020'].astype(str).apply(add_leading_zero)

## Join on crosswalks and aggregate
emp2022 = pd.merge(emp_load2022, nhgis_blk2020_bg2010, how='left', left_on = "blk2020", right_on = "blk2020ge")
emp2022 = emp2022[["bg2010ge", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain", "weight"]]
## Multiply by weight
emp_columns = ["emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]
emp2022[emp_columns].multiply(emp2022["weight"], axis="index")

## And sum within 2022 block groups
emp2022 = emp2022.groupby("bg2010ge", as_index=False).sum()
emp2022 = emp2022.rename(columns={'bg2010ge': 'bg2010'})

## Drop weight column
emp2022 = emp2022.drop(["weight"], axis = 1)

## Add 1 to every value for ln purposes
emp2022[emp_columns] += 1

## Calculate employment entropy
emp2022["E"] = ((emp2022["emp_retail"]/emp2022["emp_tot"])*np.log(emp2022["emp_retail"]/emp2022["emp_tot"])) + \
((emp2022["emp_office"]/emp2022["emp_tot"])*np.log(emp2022["emp_office"]/emp2022["emp_tot"]))  + \
((emp2022["emp_industrial"]/emp2022["emp_tot"])*np.log(emp2022["emp_industrial"]/emp2022["emp_tot"])) + \
((emp2022["emp_service"]/emp2022["emp_tot"])*np.log(emp2022["emp_service"]/emp2022["emp_tot"])) + \
((emp2022["emp_entertain"]/emp2022["emp_tot"])*np.log(emp2022["emp_entertain"]/emp2022["emp_tot"]))

emp2022["emp_entropy"] = -emp2022["E"]/np.log(5)

## Calculate employment and housing entropy
## Join with housing info from ACS
acs2022 = pd.read_csv("../acs_change_file/outputs/acs2022.csv")
acs2022["bg2010"] = acs2022['bg2010'].astype(str).apply(add_leading_zero)
acs2022["hh"]  = acs2022["area_acres"] * acs2022["hh_den_acre"]

emp_hh2022 = pd.merge(emp2022, acs2022, on = "bg2010", how = "left")

emp_hh2022["total_activity"] = emp_hh2022["hh"] + emp_hh2022["emp_tot"]
emp_hh2022["A"] = ((emp_hh2022["hh"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["hh"] /emp_hh2022["total_activity"])) + \
((emp_hh2022["emp_retail"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_retail"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_office"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_office"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_industrial"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_industrial"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_service"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_service"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_entertain"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_entertain"] /emp_hh2022["total_activity"])) 

emp_hh2022["emp_hh_entropy"] = -emp_hh2022["A"]/np.log(6)

## Employment density
emp_hh2022["emp_den_acre"] = emp_hh2022["emp_tot"] / emp_hh2022["area_acres"]
## Employment and household density
emp_hh2022["emp_hh_den_acre"] = (emp_hh2022["emp_tot"] + emp_hh2022["hh"]) / emp_hh2022["area_acres"]
## Employment and population density
emp_hh2022["pop"] = emp_hh2022["pop_den_acre"] * emp_hh2022["area_acres"]
emp_hh2022["emp_pop_den_acre"] = (emp_hh2022["emp_tot"] + emp_hh2022["pop"]) / emp_hh2022["area_acres"]

## Add year
lehd2022 = emp_hh2022[["bg2010", "emp_den_acre", "emp_entropy", "emp_hh_entropy", "emp_hh_den_acre", "emp_pop_den_acre", "year"]]
print(lehd2022.head())
print(lehd2022.shape)
lehd2022.to_csv("./outputs/lehd2022.csv", index = False)

print("All done, check outputs folder!")