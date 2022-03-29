#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 09:32:28 2022

@author: danielsutter
"""


import requests
import pandas as pd
import yaml
import logging
import restapi as oxrest
# from requests import HTTPError
# import json
import time
from pyairtable import Api, Base, Table
# import base64
# import datetime as dt

requests.packages.urllib3.disable_warnings()
logging.basicConfig(filename=('./main.log'), filemode='w', level=logging.DEBUG, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

with open("./creds.yml", 'r') as stream:
    allcreds = yaml.safe_load(stream)
durocreds = allcreds['oxide_duro']
reports = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Reports/'

"""
# =============================================================================
# ORDER OF THIS SCRIPT-BUILD BOM TOP DOWN
# # 1.0 QUERY DURO API FOR RACK PARENT PRODUCT DETAILS
# # 2.0 UNPACK GET QUERY DETAILS AND PULL CHILD COMPONENTS
# # 3.0 REQUERY API FOR CHILD COMPONENT DETAILS (REPEAT 1 AND 2)
# # 4.0 APPEND RESULTS TO DATAFRAME AND BUILD BOM FILE
# # 5.0 MULTIPLY ASSEMBLY QUANTITIES TOP DOWN TO GET TOTAL BOM QTY PER PN
# # 6.0 STOP AND IMPORT NEW ORCAD FILE TO DURO
# # # 6.1 COPY CPN MPN FROM DURO TO DATA FOLDER
# # # 6.2 PULL LATEST ORCAD CSV, CONVERT TO XLSX, AND DROP IN DATA FOLDER
# # # 6.3 RUN SCRIPT AND ADD CPNS FOR ANY "PN NEEDED" ROWS
# # # RERUN 6.1 AND 6.3 UNTIL ALL CPNS CREATED
# # 7.0 RERUN FIRST 1-5 LINES
# # 8.0 MERGE FILES AND IDENTIFY ADDED, MODIFIED, OR DROPPED PNS
# # 9.0 COMMUNICATE CHANGES TO BROADER PROCUREMENT TEAM
# # 
# TO RUN THIS FILE
# # 10.0 SET uploadcomplete = FALSE
# # 11.0 RUN SCRIPT
# # 12.0 COMPLETE STEP 6 ABOVE
# # 13.0 SET uploadcomplete = TRUE
# # 14.0 RERUN SCRIPT
# =============================================================================
"""

cols_to_move = ['queryPN',
                'parent',
                'cpn',
                'name',
                'category',
                'level',
                'procurement',
                'quantity',
                'extqty']

duroid = '992-0000005' #ENTER PN HERE

uploadcomplete = False #TRUE IF UPLOAD TO DURO IS ALREADY DONE, FALSE IF NOT

# In[BUILD BOM FUNCTION]
"""
PULL DURO EXISTING BOM
"""

logging.debug("START PULL DURO EXISTING BOM")
csv = reports + duroid + 'oldbom.csv'

if uploadcomplete == False:
    oldbom = oxrest.buildbom(duroid, durocreds)
    oldbom = oldbom[cols_to_move]
    # GROUP BY CPN AND SUM REQUIRED QUANTITIES WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
    oldbom = oldbom.groupby(['cpn'], as_index=False).agg(lambda x : x.sum() if x.dtype=='float64' else ', '.join(x))
    oldbom = oldbom.drop(columns=['queryPN', 'category', 'procurement'])
    

    oldbom.to_csv(csv, index=False)
    print("PULLED OLD BOM FROM DURO")
else:
    oldbom = pd.read_csv(csv)
    print("SKIPPED DURO AND PULLED OLDBOM FROM GDRIVE")

logging.debug("FINISH PULL DURO EXISTING BOM")

# In[EXIT SCRIPT AND RUN CJ R BOM IMPORT PROCESS]
"""

STOP AT THIS POINT - EXIT SCRIPT AND RUN CJ R BOM IMPORT PROCESS
WHEN ORCAD OUTPUT HAS BEEN IMPORTED TO DURO,
RETURN AND RUN REMAINDER OF SCRIPT

"""

# In[PULL DURO NEW BOM ]
"""
PULL DURO NEW BOM 
"""

if uploadcomplete == True:
    logging.debug("FINISH PULL DURO NEW BOM ")
    
    newbom = oxrest.buildbom(duroid, durocreds)
    newbom = newbom[cols_to_move]
    newbom = newbom.groupby(['cpn'], as_index=False).agg(lambda x : x.sum() if x.dtype=='float64' else ', '.join(x))
    newbom = newbom.drop(columns=['queryPN', 'category', 'procurement'])
    
    csv = reports + duroid + 'newbom.csv'
    newbom.to_csv(csv, index=False)
    
    logging.debug("FINISH PULL DURO NEW BOM ")
    
    # In[COMPARE OLD AND NEW BOM]
    """
    COMPARE OLD AND NEW BOM
    CREATE DATAFRAME OF CHANGES AND PRINT TO CSV
    """
    
    
    plusminus = newbom.merge(oldbom, 'outer', 'cpn')
    plusminus['diff'] = ''
    plusminus['diff'] = plusminus.apply(lambda x: 'Same' if x['extqty_x'] == x['extqty_y'] else 'Diff', axis=1)
    
    addsdrops = plusminus[plusminus['extqty_x']!=plusminus['extqty_y']].copy()
    addsdrops.loc[addsdrops['parent_x'].isnull(), 'parent_x'] = 'Dropped PN'
    addsdrops.loc[addsdrops['parent_y'].isnull(), 'parent_y'] = 'Added PN'
    
    
    csv = reports + duroid + 'adddrop.csv'
    addsdrops.to_csv(csv, index=False)
    csv = reports + duroid + 'adddrop' + time.strftime("%Y%m%d-%H%M%S") + '.csv'
    addsdrops.to_csv(csv, index=False)
    csv = reports + duroid + 'BOM.Comparison.Analysis' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')
    
    # Write each dataframe to a different worksheet.
    addsdrops.to_excel(writer, sheet_name='AddsandDrops')
    plusminus.to_excel(writer, sheet_name='FullBOMComparison', index=False)
    oldbom.to_excel(writer, sheet_name='PreviousBOM', index=False)
    newbom.to_excel(writer, sheet_name='UpdatedBOM', index=False)
    
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()