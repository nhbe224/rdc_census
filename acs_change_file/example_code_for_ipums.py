# Create ACS Change File
# This gives us Land Area, HH Density, Population Density, Percent of 0-Veh HHs, Median Income, and Percent Minority
# at the block group level by year, from 2005 to 2022.

# Load Libraries
import os
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
from ipumspy import IpumsApiClient, AggregateDataExtract, MicrodataExtract, Dataset, DatasetMetadata, readers, ddi
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Set working directory
os.chdir('D:/neeco/rdc_census/acs_change_file/')

## NHGIS API Key
ipums_key = "59cba10d8a5da536fc06b59d23a1e21a315f492fa8cf28e6b302798b"
IPUMS_API_KEY = os.environ.get(ipums_key)
ipums = IpumsApiClient(IPUMS_API_KEY)

# Get population by block group
# Start with total population
# 2012 and before uses IPUMS NHGIS API Python library
# 2013-2022 uses Census API Python library

# TEST
extract = AggregateDataExtract(
   collection="nhgis",
   description="An NHGIS example extract",
   datasets=[
      Dataset(name="2000_SF1a", data_tables=["NP001A", "NP031A"], geog_levels=["state"])
   ],
)
ipums.submit_extract(extract)