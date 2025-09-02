Notes

* ACS Files

  * 2000 is from US Census, 2010-2022 from 5 year ACS.
  * 2001 to 2009 interpolated.
  * Geography is 2010 BG





More Notes

acs\_ALLto2023.dta: External ACS data from 2005-2023



auto\_ALL: From Access Across America, weighted average of jobs within 30 minutes of driving at 2010 block group level. Available in 2018, 2021, 2022 for select areas.



travel\_times\_v3.dta: Driving times from 2010 Census tract origins to destinations for all pairs within 150 miles of each other.



auto\_jobs30\_m2.dta: This starts with travel\_times\_v3.dta and is merged with LEHD-LODES WAC, which measures employment changes year by year, giving us the opportunity to measure how access to jobs changes more broadly. Basically, the driving time remains static, but the number of jobs changes year to year. Available from 2005-2022. 



bike\_ALL: From Access Across America, weighted average of jobs within 30 minutes of biking at 2010 block group level. Available in 2019, 2021, 2022 for select areas.



lehd\_ALL.dta: External LEHD data from 2005-2022.



sld\_ALL.dta: EPA Smart Location Database data. Available in 2010 and 2018.



traffic\_volumes.dta: Average daily traffic per intersection, average daily highway traffic per intersection, average daily non-highway traffic per intersection at the Census tract level for several years.



transit\_ALL.dta: From Access Across America, weighted average of jobs within 30 minutes of transit at 2010 block group level. Available in 2014-2019, 2021, 2022 for select areas.



walk\_ALL.dta: From Access Across America, weighted average of jobs within 30 minutes of walking at 2010 block group level. Available in 2014 and 2022 for select areas.



walk\_jobs30\_m2.dta: Same idea as auto\_jobs\_30\_m2.dta, but we start with walking distances found in this database (https://www.nber.org/research/data/block-group-distance-database). We then match on block group level employment.







\*\_m2 means measure 2 (in that we already have another measure of it elsewhere).





