#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 13 14:47:13 2022

@author: danielsutter
"""

import logging
# import math
import time
# import datetime as dt
# import base64

# import json
import yaml
import pandas as pd
# from pyairtable import Table
import requests

import restapi as oxrest
import stdcost as sc

requests.packages.urllib3.disable_warnings()
logging.basicConfig(filename=('./main.log'),
                    filemode='w',
                    level=logging.DEBUG, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

with open("./creds.yml", 'r') as stream:
    allcreds = yaml.safe_load(stream)
durocreds = allcreds['oxide_duro']
histup = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/HistUpdates/'

gims = True  # True if gims need to be added separately
"""
# =============================================================================
# # ORDER OF THIS SCRIPT-BUILD BOM TOP DOWN
# # QUERY DURO API FOR RACK PARENT PRODUCT DETAILS
# # UNPACK GET QUERY DETAILS AND PULL CHILD COMPONENTS
# # REQUERY API FOR CHILD COMPONENT DETAILS (REPEAT 1 AND 2)
# # APPEND RESULTS TO DATAFRAME AND BUILD BOM FILE
# # MULTIPLY ASSEMBLY QUANTITIES TOP DOWN TO GET TOTAL BOM QTY PER PN
# # IMPORT AND RUN stdcost.main() FROM stdcost.py
# # IDENTIFY WHICH BOM COMPONENTS DON'T MATCH TO AN MPN AND HAVE NO COST FOR LATER REVIEW
# # MERGE THE BOM AND MPN PROCUREMENT FILE ON CPN
# # ROLL COST FROM LOWER LEVEL CPNs UP TO HIGHER LEVEL PARENT PNS, FINISHING WITH QUERY PN
# # SAVE RESULTS TO OPS AUTO/REPORTS
# =============================================================================
"""

# In[BUILD BOM FUNCTION]
"""
PULL DURO EXISTING BOM
"""

logging.debug("START PULL DURO EXISTING BOM")

duroid = '999-0000014'
durobom_get = oxrest.buildbom(duroid, durocreds)
# GROUP BY CPN AND SUM REQUIRED QUANTITIES WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN

# REDUCE THE DURO BOM DF TO ONLY THE NEEDED DATA
cols_to_move = ['queryPN',
                'parent',
                'cpn',
                'name',
                'category',
                'level',
                'procurement',
                'quantity',
                'extqty']
durobom = durobom_get[cols_to_move].copy()

# IF GIMLET IS STORED IN DURO AS A PRODUCT, IT WILL NEED TO BE APPENDED TO THE RACK QUERY RESULTS
# THIS CAN BE REMOVED IF/WHEN DURO UPDATES TO ALLOW PRODUCTS TO BE CHILDREN OF OTHER PRODUCTS
if gims is True:
    gimid = '999-0000010'
    durobom_get = oxrest.buildbom(gimid, durocreds)
    x10 = durobom_get[cols_to_move].copy()

    x10['level'] = x10['level'].apply(lambda x: x+1)
    x10['quantity'] = x10['quantity'].apply(lambda x: x*32)
    x10['extqty'] = x10['extqty'].apply(lambda x: x*32)
    x10.at[0, 'parent'] = duroid
    durobom = pd.concat([durobom, x10], axis=0)

hash1 = pd.util.hash_pandas_object(durobom).sum()
logging.debug(str(hash1) + " - durobom hash")

logging.debug("FINISH HASH DURBOM TO COMPARE VALUES")

# In[CPN MPN KEY AND STD COST FROM stdcost.py]
"""
CPN MPN KEY AND STD COST FROM stdcost.py
"""
logging.debug("START CPN MPN KEY AND STD COST FROM stdcost.py")

mpn_proc = sc.main()
mpn_missing_proc = mpn_proc[mpn_proc['proc mpn'] == '-'].copy()

logging.debug("FINISH CPN MPN KEY AND STD COST FROM stdcost.py")

# In[BOM TO MPN_PROC MERGE]
"""
BOM TO MPN_CPN MERGE
"""
# MERGING DURO BOM AND THE MPN AND PROCUREMENT DF
durobom_mpn_proc = durobom.merge(mpn_proc, 'left', 'cpn')

# CAPTURING CPNS THAT DIDN'T MATCH WITH AN MPN
durobom_missing_mpn = durobom_mpn_proc[durobom_mpn_proc['mpn'].isnull()]
durobom_missing_mpn = durobom_missing_mpn[durobom_missing_mpn['category'].notnull(
)]
durobom_missing_mpn = durobom_missing_mpn.loc[(durobom_missing_mpn['procurement'] == "Buy") |
                                              (~durobom_missing_mpn['category'].str.contains("Assembly"))]
durobom_missing_mpn = durobom_missing_mpn.loc[(durobom_missing_mpn['procurement'] == "Buy") |
                                              (~durobom_missing_mpn['category'].str.contains("BOM"))]
durobom_missing_mpn = durobom_missing_mpn.loc[(durobom_missing_mpn['procurement'] == "Buy") |
                                              (~durobom_missing_mpn['category'].str.contains("Printed"))]
durobom_missing_mpn = durobom_missing_mpn.loc[(durobom_missing_mpn['procurement'] == "Buy") |
                                              (~durobom_missing_mpn['name'].str.contains("SPARE"))]
durobom_missing_mpn = durobom_missing_mpn.append(
    durobom_mpn_proc[durobom_mpn_proc['cpn'].isin(mpn_missing_proc['cpn'])])
durobom_missing_mpn = durobom_missing_mpn.loc[(durobom_missing_mpn['Std Cost'].isnull()) |
                                              (durobom_missing_mpn['Std Cost'] == 0)]
durobom_missing_mpn = durobom_missing_mpn.append(
    durobom_mpn_proc[durobom_mpn_proc['Std Cost'] == 0])
durobom_missing_mpn = durobom_missing_mpn.drop_duplicates(subset=['cpn'])

# CLEANING COLUMNS FOR A SUCCESSFUL MERGE
durobom_mpn_proc['mpn'] = durobom_mpn_proc['mpn'].fillna('-')
durobom_mpn_proc['proc mpn'] = durobom_mpn_proc['proc mpn'].fillna('-')
durobom_mpn_proc_rolledcost = durobom_mpn_proc.fillna(0)
# COMPUTING THE ROLLED COST
durobom_mpn_proc_rolledcost['rolled_cost'] = durobom_mpn_proc_rolledcost.apply(
    lambda x: x['extqty'] * x['Std Cost'], axis=1)
# STORING THE BOM LEVELS TO A DF TO USE IN THE NEXT LOOP
lvls = durobom_mpn_proc_rolledcost['level'].drop_duplicates(
).sort_values(ascending=False).reset_index(drop=True)
lvls = lvls[:-1]

lwrlvl = []
rolledcost = []
# STARTING FROM THE BOTTOM UP
# GROUP AND SUM ROLLED COST BY PARENT
# WRITE THAT SUMMED ROLLED COST TO PARENT CPN
# AND THEN MOVE UP A BOM LEVEL
for level in lvls:
    # level = 1 #DEBUG ONLY
    print(level)
    # FILTER FOR CPNS = LEVEL
    lwrlvl = durobom_mpn_proc_rolledcost[durobom_mpn_proc_rolledcost['level'] == level].copy(
    )
    # DROP DUPLICATES
    lwrlvl = lwrlvl.drop_duplicates(keep='first')
    # GROUP BY PARENT AND SUM IF AN INT, CONCAT ALL STRINGS
    rolledcost = lwrlvl.groupby(['parent'], as_index=False).agg(
        lambda x: x.sum() if x.dtype == 'float64' else ', '.join(x))
    # TRIM TO PARENT AND ROLLED COST, RENAME PARE AND EC
    rolledcost = rolledcost[['parent', 'rolled_cost']].rename(
        columns={'parent': 'par', 'rolled_cost': 'ec'})
    # MERGE WITH DURO MPN PROC ROLLEDCOST DF
    durobom_mpn_proc_rolledcost = durobom_mpn_proc_rolledcost.merge(
        rolledcost, 'left', left_on='cpn', right_on='par')
    # FILL NANS FROM THE JOIN WITH A -
    durobom_mpn_proc_rolledcost['par'] = durobom_mpn_proc_rolledcost['par'].fillna('-')
    # ITERATE THROUGH DF AND WHERE PAR != "-", WRITE EC TO ROLLEDCOST
    for i in range(len(durobom_mpn_proc_rolledcost['cpn'])):
        # print(i)
        if (durobom_mpn_proc_rolledcost.at[i, 'par'] != '-'):
            print("pn notnull " +
                  durobom_mpn_proc_rolledcost.at[i, 'cpn'] + " " + str(i))
            # MAY NEED TO ADD THIS IF ASSEMBLIES END UP WITH SOME COST
            durobom_mpn_proc_rolledcost.at[i, 'rolled_cost'] = durobom_mpn_proc_rolledcost.at[i, 'ec'] + \
                durobom_mpn_proc_rolledcost.at[i, 'rolled_cost']
    # DROP PAR AND EC COLS AND DO IT AGAIN
    durobom_mpn_proc_rolledcost = durobom_mpn_proc_rolledcost.drop(columns=[
                                                                   'par', 'ec'])

# Create a Pandas Excel writer using XlsxWriter as the engine.
reports = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Reports/'
csv = reports + 'DuroBOMRolledCost' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
writer = pd.ExcelWriter(csv, engine='xlsxwriter')

# Write each dataframe to a different worksheet.
durobom_mpn_proc_rolledcost.to_excel(
    writer, sheet_name='DuroBOMRolledAnalysis')
durobom_missing_mpn.to_excel(
    writer, sheet_name='DuroBOM.MissingMPNs.ORCost', index=False)

# Close the Pandas Excel writer and output the Excel file.
writer.save()
