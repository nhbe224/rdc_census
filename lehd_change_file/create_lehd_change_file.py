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
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Set working directory
os.chdir('D:/neeco/rdc_census/lehd_change_file/')

# Helper functions
def add_leading_zero(value):
	if len(value) == 11:
		return '0' + value
	else:
		return value

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
emp_load2013["geoid"] = emp_load2013["w_geocode"]
emp_load2013["emp_tot"] = emp_load2013["C000"]
emp_load2013["emp_retail"] = emp_load2013["CNS07"]
emp_load2013["emp_office"] = emp_load2013["CNS09"] + emp_load2013["CNS10"] + emp_load2013["CNS11"] + emp_load2013["CNS13"] + emp_load2013["CNS20"]
emp_load2013["emp_industrial"] =  emp_load2013["CNS01"] + emp_load2013["CNS02"] + emp_load2013["CNS03"] + emp_load2013["CNS04"] + emp_load2013["CNS05"] + emp_load2013["CNS06"] + emp_load2013["CNS08"]
emp_load2013["emp_service"] = emp_load2013["CNS12"] + emp_load2013["CNS14"] + emp_load2013["CNS15"] + emp_load2013["CNS16"] + emp_load2013["CNS19"]
emp_load2013["emp_entertain"] =  emp_load2013["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2013= emp_load2013[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2013['geoid'] = emp_load2013['geoid'].astype(str)
## Remove last three characters
emp_load2013['geoid'] = emp_load2013['geoid'].str[:-3]
## Add leading zeros
emp_load2013['geoid'] = emp_load2013['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2013 = emp_load2013.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2013 = emp2013.replace(0, 0.001)

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
acs2013["geoid"] = acs2013['geoid'].astype(str).apply(add_leading_zero)
acs2013["hh"]  = acs2013["aland"] * acs2013["hh_den"]

emp_hh2013 = pd.merge(emp2013, acs2013, on = "geoid", how = "left")

emp_hh2013["total_activity"] = emp_hh2013["hh"] + emp_hh2013["emp_tot"]
emp_hh2013["A"] = ((emp_hh2013["hh"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["hh"] /emp_hh2013["total_activity"])) + \
((emp_hh2013["emp_retail"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_retail"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_office"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_office"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_industrial"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_industrial"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_service"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_service"] /emp_hh2013["total_activity"]))  + \
((emp_hh2013["emp_entertain"] / emp_hh2013["total_activity"])*np.log(emp_hh2013["emp_entertain"] /emp_hh2013["total_activity"])) 

emp_hh2013["emp_hh_entropy"] = -emp_hh2013["A"]/np.log(5)

## Employment density
emp_hh2013["emp_den"] = emp_hh2013["emp_tot"] / emp_hh2013["aland"]
## Employment and household density
emp_hh2013["emp_hh_den"] = emp_hh2013["emp_tot"] + emp_hh2013["hh_den"]
## Employment and population density
emp_hh2013["emp_pop_den"] = emp_hh2013["emp_tot"] + emp_hh2013["pop_den"]

## Add year
lehd2013 = emp_hh2013[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2014["geoid"] = emp_load2014["w_geocode"]
emp_load2014["emp_tot"] = emp_load2014["C000"]
emp_load2014["emp_retail"] = emp_load2014["CNS07"]
emp_load2014["emp_office"] = emp_load2014["CNS09"] + emp_load2014["CNS10"] + emp_load2014["CNS11"] + emp_load2014["CNS13"] + emp_load2014["CNS20"]
emp_load2014["emp_industrial"] =  emp_load2014["CNS01"] + emp_load2014["CNS02"] + emp_load2014["CNS03"] + emp_load2014["CNS04"] + emp_load2014["CNS05"] + emp_load2014["CNS06"] + emp_load2014["CNS08"]
emp_load2014["emp_service"] = emp_load2014["CNS12"] + emp_load2014["CNS14"] + emp_load2014["CNS15"] + emp_load2014["CNS16"] + emp_load2014["CNS19"]
emp_load2014["emp_entertain"] =  emp_load2014["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2014= emp_load2014[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2014['geoid'] = emp_load2014['geoid'].astype(str)
## Remove last three characters
emp_load2014['geoid'] = emp_load2014['geoid'].str[:-3]
## Add leading zeros
emp_load2014['geoid'] = emp_load2014['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2014 = emp_load2014.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2014 = emp2014.replace(0, 0.001)

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
acs2014["geoid"] = acs2014['geoid'].astype(str).apply(add_leading_zero)
acs2014["hh"]  = acs2014["aland"] * acs2014["hh_den"]

emp_hh2014 = pd.merge(emp2014, acs2014, on = "geoid", how = "left")

emp_hh2014["total_activity"] = emp_hh2014["hh"] + emp_hh2014["emp_tot"]
emp_hh2014["A"] = ((emp_hh2014["hh"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["hh"] /emp_hh2014["total_activity"])) + \
((emp_hh2014["emp_retail"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_retail"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_office"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_office"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_industrial"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_industrial"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_service"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_service"] /emp_hh2014["total_activity"]))  + \
((emp_hh2014["emp_entertain"] / emp_hh2014["total_activity"])*np.log(emp_hh2014["emp_entertain"] /emp_hh2014["total_activity"])) 

emp_hh2014["emp_hh_entropy"] = -emp_hh2014["A"]/np.log(5)

## Employment density
emp_hh2014["emp_den"] = emp_hh2014["emp_tot"] / emp_hh2014["aland"]
## Employment and household density
emp_hh2014["emp_hh_den"] = emp_hh2014["emp_tot"] + emp_hh2014["hh_den"]
## Employment and population density
emp_hh2014["emp_pop_den"] = emp_hh2014["emp_tot"] + emp_hh2014["pop_den"]

## Add year
lehd2014 = emp_hh2014[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2015["geoid"] = emp_load2015["w_geocode"]
emp_load2015["emp_tot"] = emp_load2015["C000"]
emp_load2015["emp_retail"] = emp_load2015["CNS07"]
emp_load2015["emp_office"] = emp_load2015["CNS09"] + emp_load2015["CNS10"] + emp_load2015["CNS11"] + emp_load2015["CNS13"] + emp_load2015["CNS20"]
emp_load2015["emp_industrial"] =  emp_load2015["CNS01"] + emp_load2015["CNS02"] + emp_load2015["CNS03"] + emp_load2015["CNS04"] + emp_load2015["CNS05"] + emp_load2015["CNS06"] + emp_load2015["CNS08"]
emp_load2015["emp_service"] = emp_load2015["CNS12"] + emp_load2015["CNS14"] + emp_load2015["CNS15"] + emp_load2015["CNS16"] + emp_load2015["CNS19"]
emp_load2015["emp_entertain"] =  emp_load2015["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2015= emp_load2015[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2015['geoid'] = emp_load2015['geoid'].astype(str)
## Remove last three characters
emp_load2015['geoid'] = emp_load2015['geoid'].str[:-3]
## Add leading zeros
emp_load2015['geoid'] = emp_load2015['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2015 = emp_load2015.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2015 = emp2015.replace(0, 0.001)

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
acs2015["geoid"] = acs2015['geoid'].astype(str).apply(add_leading_zero)
acs2015["hh"]  = acs2015["aland"] * acs2015["hh_den"]

emp_hh2015 = pd.merge(emp2015, acs2015, on = "geoid", how = "left")

emp_hh2015["total_activity"] = emp_hh2015["hh"] + emp_hh2015["emp_tot"]
emp_hh2015["A"] = ((emp_hh2015["hh"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["hh"] /emp_hh2015["total_activity"])) + \
((emp_hh2015["emp_retail"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_retail"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_office"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_office"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_industrial"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_industrial"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_service"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_service"] /emp_hh2015["total_activity"]))  + \
((emp_hh2015["emp_entertain"] / emp_hh2015["total_activity"])*np.log(emp_hh2015["emp_entertain"] /emp_hh2015["total_activity"])) 

emp_hh2015["emp_hh_entropy"] = -emp_hh2015["A"]/np.log(5)

## Employment density
emp_hh2015["emp_den"] = emp_hh2015["emp_tot"] / emp_hh2015["aland"]
## Employment and household density
emp_hh2015["emp_hh_den"] = emp_hh2015["emp_tot"] + emp_hh2015["hh_den"]
## Employment and population density
emp_hh2015["emp_pop_den"] = emp_hh2015["emp_tot"] + emp_hh2015["pop_den"]

## Add year
lehd2015 = emp_hh2015[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2016["geoid"] = emp_load2016["w_geocode"]
emp_load2016["emp_tot"] = emp_load2016["C000"]
emp_load2016["emp_retail"] = emp_load2016["CNS07"]
emp_load2016["emp_office"] = emp_load2016["CNS09"] + emp_load2016["CNS10"] + emp_load2016["CNS11"] + emp_load2016["CNS13"] + emp_load2016["CNS20"]
emp_load2016["emp_industrial"] =  emp_load2016["CNS01"] + emp_load2016["CNS02"] + emp_load2016["CNS03"] + emp_load2016["CNS04"] + emp_load2016["CNS05"] + emp_load2016["CNS06"] + emp_load2016["CNS08"]
emp_load2016["emp_service"] = emp_load2016["CNS12"] + emp_load2016["CNS14"] + emp_load2016["CNS15"] + emp_load2016["CNS16"] + emp_load2016["CNS19"]
emp_load2016["emp_entertain"] =  emp_load2016["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2016= emp_load2016[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2016['geoid'] = emp_load2016['geoid'].astype(str)
## Remove last three characters
emp_load2016['geoid'] = emp_load2016['geoid'].str[:-3]
## Add leading zeros
emp_load2016['geoid'] = emp_load2016['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2016 = emp_load2016.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2016 = emp2016.replace(0, 0.001)

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
acs2016["geoid"] = acs2016['geoid'].astype(str).apply(add_leading_zero)
acs2016["hh"]  = acs2016["aland"] * acs2016["hh_den"]

emp_hh2016 = pd.merge(emp2016, acs2016, on = "geoid", how = "left")

emp_hh2016["total_activity"] = emp_hh2016["hh"] + emp_hh2016["emp_tot"]
emp_hh2016["A"] = ((emp_hh2016["hh"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["hh"] /emp_hh2016["total_activity"])) + \
((emp_hh2016["emp_retail"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_retail"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_office"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_office"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_industrial"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_industrial"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_service"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_service"] /emp_hh2016["total_activity"]))  + \
((emp_hh2016["emp_entertain"] / emp_hh2016["total_activity"])*np.log(emp_hh2016["emp_entertain"] /emp_hh2016["total_activity"])) 

emp_hh2016["emp_hh_entropy"] = -emp_hh2016["A"]/np.log(5)

## Employment density
emp_hh2016["emp_den"] = emp_hh2016["emp_tot"] / emp_hh2016["aland"]
## Employment and household density
emp_hh2016["emp_hh_den"] = emp_hh2016["emp_tot"] + emp_hh2016["hh_den"]
## Employment and population density
emp_hh2016["emp_pop_den"] = emp_hh2016["emp_tot"] + emp_hh2016["pop_den"]

## Add year
lehd2016 = emp_hh2016[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2017["geoid"] = emp_load2017["w_geocode"]
emp_load2017["emp_tot"] = emp_load2017["C000"]
emp_load2017["emp_retail"] = emp_load2017["CNS07"]
emp_load2017["emp_office"] = emp_load2017["CNS09"] + emp_load2017["CNS10"] + emp_load2017["CNS11"] + emp_load2017["CNS13"] + emp_load2017["CNS20"]
emp_load2017["emp_industrial"] =  emp_load2017["CNS01"] + emp_load2017["CNS02"] + emp_load2017["CNS03"] + emp_load2017["CNS04"] + emp_load2017["CNS05"] + emp_load2017["CNS06"] + emp_load2017["CNS08"]
emp_load2017["emp_service"] = emp_load2017["CNS12"] + emp_load2017["CNS14"] + emp_load2017["CNS15"] + emp_load2017["CNS16"] + emp_load2017["CNS19"]
emp_load2017["emp_entertain"] =  emp_load2017["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2017= emp_load2017[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2017['geoid'] = emp_load2017['geoid'].astype(str)
## Remove last three characters
emp_load2017['geoid'] = emp_load2017['geoid'].str[:-3]
## Add leading zeros
emp_load2017['geoid'] = emp_load2017['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2017 = emp_load2017.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2017 = emp2017.replace(0, 0.001)

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
acs2017["geoid"] = acs2017['geoid'].astype(str).apply(add_leading_zero)
acs2017["hh"]  = acs2017["aland"] * acs2017["hh_den"]

emp_hh2017 = pd.merge(emp2017, acs2017, on = "geoid", how = "left")

emp_hh2017["total_activity"] = emp_hh2017["hh"] + emp_hh2017["emp_tot"]
emp_hh2017["A"] = ((emp_hh2017["hh"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["hh"] /emp_hh2017["total_activity"])) + \
((emp_hh2017["emp_retail"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_retail"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_office"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_office"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_industrial"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_industrial"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_service"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_service"] /emp_hh2017["total_activity"]))  + \
((emp_hh2017["emp_entertain"] / emp_hh2017["total_activity"])*np.log(emp_hh2017["emp_entertain"] /emp_hh2017["total_activity"])) 

emp_hh2017["emp_hh_entropy"] = -emp_hh2017["A"]/np.log(5)

## Employment density
emp_hh2017["emp_den"] = emp_hh2017["emp_tot"] / emp_hh2017["aland"]
## Employment and household density
emp_hh2017["emp_hh_den"] = emp_hh2017["emp_tot"] + emp_hh2017["hh_den"]
## Employment and population density
emp_hh2017["emp_pop_den"] = emp_hh2017["emp_tot"] + emp_hh2017["pop_den"]

## Add year
lehd2017 = emp_hh2017[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2018["geoid"] = emp_load2018["w_geocode"]
emp_load2018["emp_tot"] = emp_load2018["C000"]
emp_load2018["emp_retail"] = emp_load2018["CNS07"]
emp_load2018["emp_office"] = emp_load2018["CNS09"] + emp_load2018["CNS10"] + emp_load2018["CNS11"] + emp_load2018["CNS13"] + emp_load2018["CNS20"]
emp_load2018["emp_industrial"] =  emp_load2018["CNS01"] + emp_load2018["CNS02"] + emp_load2018["CNS03"] + emp_load2018["CNS04"] + emp_load2018["CNS05"] + emp_load2018["CNS06"] + emp_load2018["CNS08"]
emp_load2018["emp_service"] = emp_load2018["CNS12"] + emp_load2018["CNS14"] + emp_load2018["CNS15"] + emp_load2018["CNS16"] + emp_load2018["CNS19"]
emp_load2018["emp_entertain"] =  emp_load2018["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2018= emp_load2018[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2018['geoid'] = emp_load2018['geoid'].astype(str)
## Remove last three characters
emp_load2018['geoid'] = emp_load2018['geoid'].str[:-3]
## Add leading zeros
emp_load2018['geoid'] = emp_load2018['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2018 = emp_load2018.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2018 = emp2018.replace(0, 0.001)

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
acs2018["geoid"] = acs2018['geoid'].astype(str).apply(add_leading_zero)
acs2018["hh"]  = acs2018["aland"] * acs2018["hh_den"]

emp_hh2018 = pd.merge(emp2018, acs2018, on = "geoid", how = "left")

emp_hh2018["total_activity"] = emp_hh2018["hh"] + emp_hh2018["emp_tot"]
emp_hh2018["A"] = ((emp_hh2018["hh"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["hh"] /emp_hh2018["total_activity"])) + \
((emp_hh2018["emp_retail"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_retail"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_office"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_office"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_industrial"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_industrial"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_service"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_service"] /emp_hh2018["total_activity"]))  + \
((emp_hh2018["emp_entertain"] / emp_hh2018["total_activity"])*np.log(emp_hh2018["emp_entertain"] /emp_hh2018["total_activity"])) 

emp_hh2018["emp_hh_entropy"] = -emp_hh2018["A"]/np.log(5)

## Employment density
emp_hh2018["emp_den"] = emp_hh2018["emp_tot"] / emp_hh2018["aland"]
## Employment and household density
emp_hh2018["emp_hh_den"] = emp_hh2018["emp_tot"] + emp_hh2018["hh_den"]
## Employment and population density
emp_hh2018["emp_pop_den"] = emp_hh2018["emp_tot"] + emp_hh2018["pop_den"]

## Add year
lehd2018 = emp_hh2018[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2019["geoid"] = emp_load2019["w_geocode"]
emp_load2019["emp_tot"] = emp_load2019["C000"]
emp_load2019["emp_retail"] = emp_load2019["CNS07"]
emp_load2019["emp_office"] = emp_load2019["CNS09"] + emp_load2019["CNS10"] + emp_load2019["CNS11"] + emp_load2019["CNS13"] + emp_load2019["CNS20"]
emp_load2019["emp_industrial"] =  emp_load2019["CNS01"] + emp_load2019["CNS02"] + emp_load2019["CNS03"] + emp_load2019["CNS04"] + emp_load2019["CNS05"] + emp_load2019["CNS06"] + emp_load2019["CNS08"]
emp_load2019["emp_service"] = emp_load2019["CNS12"] + emp_load2019["CNS14"] + emp_load2019["CNS15"] + emp_load2019["CNS16"] + emp_load2019["CNS19"]
emp_load2019["emp_entertain"] =  emp_load2019["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2019= emp_load2019[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2019['geoid'] = emp_load2019['geoid'].astype(str)
## Remove last three characters
emp_load2019['geoid'] = emp_load2019['geoid'].str[:-3]
## Add leading zeros
emp_load2019['geoid'] = emp_load2019['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2019 = emp_load2019.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2019 = emp2019.replace(0, 0.001)

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
acs2019["geoid"] = acs2019['geoid'].astype(str).apply(add_leading_zero)
acs2019["hh"]  = acs2019["aland"] * acs2019["hh_den"]

emp_hh2019 = pd.merge(emp2019, acs2019, on = "geoid", how = "left")

emp_hh2019["total_activity"] = emp_hh2019["hh"] + emp_hh2019["emp_tot"]
emp_hh2019["A"] = ((emp_hh2019["hh"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["hh"] /emp_hh2019["total_activity"])) + \
((emp_hh2019["emp_retail"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_retail"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_office"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_office"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_industrial"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_industrial"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_service"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_service"] /emp_hh2019["total_activity"]))  + \
((emp_hh2019["emp_entertain"] / emp_hh2019["total_activity"])*np.log(emp_hh2019["emp_entertain"] /emp_hh2019["total_activity"])) 

emp_hh2019["emp_hh_entropy"] = -emp_hh2019["A"]/np.log(5)

## Employment density
emp_hh2019["emp_den"] = emp_hh2019["emp_tot"] / emp_hh2019["aland"]
## Employment and household density
emp_hh2019["emp_hh_den"] = emp_hh2019["emp_tot"] + emp_hh2019["hh_den"]
## Employment and population density
emp_hh2019["emp_pop_den"] = emp_hh2019["emp_tot"] + emp_hh2019["pop_den"]

## Add year
lehd2019 = emp_hh2019[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2020["geoid"] = emp_load2020["w_geocode"]
emp_load2020["emp_tot"] = emp_load2020["C000"]
emp_load2020["emp_retail"] = emp_load2020["CNS07"]
emp_load2020["emp_office"] = emp_load2020["CNS09"] + emp_load2020["CNS10"] + emp_load2020["CNS11"] + emp_load2020["CNS13"] + emp_load2020["CNS20"]
emp_load2020["emp_industrial"] =  emp_load2020["CNS01"] + emp_load2020["CNS02"] + emp_load2020["CNS03"] + emp_load2020["CNS04"] + emp_load2020["CNS05"] + emp_load2020["CNS06"] + emp_load2020["CNS08"]
emp_load2020["emp_service"] = emp_load2020["CNS12"] + emp_load2020["CNS14"] + emp_load2020["CNS15"] + emp_load2020["CNS16"] + emp_load2020["CNS19"]
emp_load2020["emp_entertain"] =  emp_load2020["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2020= emp_load2020[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2020['geoid'] = emp_load2020['geoid'].astype(str)
## Remove last three characters
emp_load2020['geoid'] = emp_load2020['geoid'].str[:-3]
## Add leading zeros
emp_load2020['geoid'] = emp_load2020['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2020 = emp_load2020.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2020 = emp2020.replace(0, 0.001)

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
acs2020["geoid"] = acs2020['geoid'].astype(str).apply(add_leading_zero)
acs2020["hh"]  = acs2020["aland"] * acs2020["hh_den"]

emp_hh2020 = pd.merge(emp2020, acs2020, on = "geoid", how = "left")

emp_hh2020["total_activity"] = emp_hh2020["hh"] + emp_hh2020["emp_tot"]
emp_hh2020["A"] = ((emp_hh2020["hh"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["hh"] /emp_hh2020["total_activity"])) + \
((emp_hh2020["emp_retail"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_retail"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_office"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_office"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_industrial"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_industrial"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_service"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_service"] /emp_hh2020["total_activity"]))  + \
((emp_hh2020["emp_entertain"] / emp_hh2020["total_activity"])*np.log(emp_hh2020["emp_entertain"] /emp_hh2020["total_activity"])) 

emp_hh2020["emp_hh_entropy"] = -emp_hh2020["A"]/np.log(5)

## Employment density
emp_hh2020["emp_den"] = emp_hh2020["emp_tot"] / emp_hh2020["aland"]
## Employment and household density
emp_hh2020["emp_hh_den"] = emp_hh2020["emp_tot"] + emp_hh2020["hh_den"]
## Employment and population density
emp_hh2020["emp_pop_den"] = emp_hh2020["emp_tot"] + emp_hh2020["pop_den"]

## Add year
lehd2020 = emp_hh2020[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2021["geoid"] = emp_load2021["w_geocode"]
emp_load2021["emp_tot"] = emp_load2021["C000"]
emp_load2021["emp_retail"] = emp_load2021["CNS07"]
emp_load2021["emp_office"] = emp_load2021["CNS09"] + emp_load2021["CNS10"] + emp_load2021["CNS11"] + emp_load2021["CNS13"] + emp_load2021["CNS20"]
emp_load2021["emp_industrial"] =  emp_load2021["CNS01"] + emp_load2021["CNS02"] + emp_load2021["CNS03"] + emp_load2021["CNS04"] + emp_load2021["CNS05"] + emp_load2021["CNS06"] + emp_load2021["CNS08"]
emp_load2021["emp_service"] = emp_load2021["CNS12"] + emp_load2021["CNS14"] + emp_load2021["CNS15"] + emp_load2021["CNS16"] + emp_load2021["CNS19"]
emp_load2021["emp_entertain"] =  emp_load2021["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2021= emp_load2021[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2021['geoid'] = emp_load2021['geoid'].astype(str)
## Remove last three characters
emp_load2021['geoid'] = emp_load2021['geoid'].str[:-3]
## Add leading zeros
emp_load2021['geoid'] = emp_load2021['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2021 = emp_load2021.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2021 = emp2021.replace(0, 0.001)

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
acs2021["geoid"] = acs2021['geoid'].astype(str).apply(add_leading_zero)
acs2021["hh"]  = acs2021["aland"] * acs2021["hh_den"]

emp_hh2021 = pd.merge(emp2021, acs2021, on = "geoid", how = "left")

emp_hh2021["total_activity"] = emp_hh2021["hh"] + emp_hh2021["emp_tot"]
emp_hh2021["A"] = ((emp_hh2021["hh"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["hh"] /emp_hh2021["total_activity"])) + \
((emp_hh2021["emp_retail"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_retail"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_office"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_office"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_industrial"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_industrial"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_service"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_service"] /emp_hh2021["total_activity"]))  + \
((emp_hh2021["emp_entertain"] / emp_hh2021["total_activity"])*np.log(emp_hh2021["emp_entertain"] /emp_hh2021["total_activity"])) 

emp_hh2021["emp_hh_entropy"] = -emp_hh2021["A"]/np.log(5)

## Employment density
emp_hh2021["emp_den"] = emp_hh2021["emp_tot"] / emp_hh2021["aland"]
## Employment and household density
emp_hh2021["emp_hh_den"] = emp_hh2021["emp_tot"] + emp_hh2021["hh_den"]
## Employment and population density
emp_hh2021["emp_pop_den"] = emp_hh2021["emp_tot"] + emp_hh2021["pop_den"]

## Add year
lehd2021 = emp_hh2021[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
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
emp_load2022["geoid"] = emp_load2022["w_geocode"]
emp_load2022["emp_tot"] = emp_load2022["C000"]
emp_load2022["emp_retail"] = emp_load2022["CNS07"]
emp_load2022["emp_office"] = emp_load2022["CNS09"] + emp_load2022["CNS10"] + emp_load2022["CNS11"] + emp_load2022["CNS13"] + emp_load2022["CNS20"]
emp_load2022["emp_industrial"] =  emp_load2022["CNS01"] + emp_load2022["CNS02"] + emp_load2022["CNS03"] + emp_load2022["CNS04"] + emp_load2022["CNS05"] + emp_load2022["CNS06"] + emp_load2022["CNS08"]
emp_load2022["emp_service"] = emp_load2022["CNS12"] + emp_load2022["CNS14"] + emp_load2022["CNS15"] + emp_load2022["CNS16"] + emp_load2022["CNS19"]
emp_load2022["emp_entertain"] =  emp_load2022["CNS17"] + emp_load["CNS18"]																	

## Rename columns
emp_load2022= emp_load2022[["geoid", "emp_tot", "emp_retail", "emp_office", "emp_industrial", "emp_service", "emp_entertain"]]

## Convert geoid to string
emp_load2022['geoid'] = emp_load2022['geoid'].astype(str)
## Remove last three characters
emp_load2022['geoid'] = emp_load2022['geoid'].str[:-3]
## Add leading zeros
emp_load2022['geoid'] = emp_load2022['geoid'].apply(add_leading_zero)
## Group by geoid and sum emp_tot to get total employment by block group
emp2022 = emp_load2022.groupby('geoid').sum()
## Replace 0 values with 0.001 for ln purposes
emp2022 = emp2022.replace(0, 0.001)

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
acs2022["geoid"] = acs2022['geoid'].astype(str).apply(add_leading_zero)
acs2022["hh"]  = acs2022["aland"] * acs2022["hh_den"]

emp_hh2022 = pd.merge(emp2022, acs2022, on = "geoid", how = "left")

emp_hh2022["total_activity"] = emp_hh2022["hh"] + emp_hh2022["emp_tot"]
emp_hh2022["A"] = ((emp_hh2022["hh"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["hh"] /emp_hh2022["total_activity"])) + \
((emp_hh2022["emp_retail"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_retail"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_office"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_office"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_industrial"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_industrial"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_service"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_service"] /emp_hh2022["total_activity"]))  + \
((emp_hh2022["emp_entertain"] / emp_hh2022["total_activity"])*np.log(emp_hh2022["emp_entertain"] /emp_hh2022["total_activity"])) 

emp_hh2022["emp_hh_entropy"] = -emp_hh2022["A"]/np.log(5)

## Employment density
emp_hh2022["emp_den"] = emp_hh2022["emp_tot"] / emp_hh2022["aland"]
## Employment and household density
emp_hh2022["emp_hh_den"] = emp_hh2022["emp_tot"] + emp_hh2022["hh_den"]
## Employment and population density
emp_hh2022["emp_pop_den"] = emp_hh2022["emp_tot"] + emp_hh2022["pop_den"]

## Add year
lehd2022 = emp_hh2022[["geoid", "emp_den", "emp_entropy", "emp_hh_entropy", "emp_hh_den", "emp_pop_den", "year"]]
print(lehd2022.head())
print(lehd2022.shape)
lehd2022.to_csv("./outputs/lehd2022.csv", index = False)

print("All done, check outputs folder!")