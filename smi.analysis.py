# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 15:48:21 2019

@author: sutter.d
"""

# =============================================================================
# import pandas.io.json as json

# import requests
# import matplotlib.pyplot as plt
# import numpy as np
# import seaborn as sns
# import pbixrefresher
# =============================================================================

import pandas as pd
import datetime as dt
import time
import glob
import os
from dsfcns import etl

wipstatus = ['Not Ordered',
             'Ordered',
             'Draft',
             'At Mav',
             'Alteration Requested',
             'BR Submitted',
             'Coupa Alt Awaiting Approval',
             'Ordered - Alteration Pending',
             'Prepare For Pick Up',
             'Pick-up Confirmed']

shipstatus = ['Pick-up Confirmed',
              'Customs',
              'Delivered',
              'Shipped']

# In[SUPPLY CHAIN ABC ANALYSIS]

# =============================================================================
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.WWT/*')  # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# 
# wwt = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# 
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.Vz/*')  # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# 
# vz = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# 
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/SWAP/*')  # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# 
# swap = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# 
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/Core/*')  # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# 
# core = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# 
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/Build/*')  # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# 
# bau = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# 
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.CBTS/*')  # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# 
# cbts = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# 
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.WWT.2/*')  # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# 
# wwt2 = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# =============================================================================

server = 'GADC-GEMSQL001\GEMSP101'
database = 'GEMInterface'
username = 'GEMInterface_User'
password = 'GEMUser2016'

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# In[]

# VERIZON CAAS QUERY
cursor.execute("exec sp_GetESMData 2426")
esm2426 = cursor.fetchall()

y = etl(esm2426)
vz = y

# BUILD QUERY
cursor.execute("exec sp_GetESMData 2425") 
esm2425 = cursor.fetchall()

y = etl(esm2425)
bau = y

# SWAP QUERY
cursor.execute("exec sp_GetESMData 2427") 
esm2427 = cursor.fetchall()

y = etl(esm2427)
swap = y


# WWT CAAS QUERY
cursor.execute("exec sp_GetESMData 2428") 
esm2428 = cursor.fetchall()

y = etl(esm2428)
wwt = y

cursor.execute("exec sp_GetESMData 2602") 
esm2602 = cursor.fetchall()

y = etl(esm2602)
wwt2 = y

# CBTS CAAS QUERY
cursor.execute("exec sp_GetESMData 2601") 
esm2601 = cursor.fetchall()

y = etl(esm2601)
cbts = y

# CORE QUERY
cursor.execute("exec sp_GetESMData 2510") 
esm2510 = cursor.fetchall()

y = etl(esm2510)
core = y

# In[]

def frmt(x):

    xcols = x.columns
    xcols = xcols.str.replace("\n", "")
    x.columns = xcols
    
    return x

esmlst = [wwt, vz, swap, core, bau, wwt2, cbts]

for i in esmlst:
    i = frmt(i)

wwt['program'] = 'CaaS.WWT'
vz['program'] = 'CaaS.Vz'
swap['program'] = 'SWAP'
core['program'] = 'CORE'
bau['program'] = 'BAU'
wwt2['program'] = 'CaaS.WWT'
cbts['program'] = 'CaaS.CBTS'

abc = pd.concat([wwt, vz, swap, core, bau, wwt2, cbts], sort=True)
abc['P&G Part #'] = abc['P&G Part #'].apply(str)


dempvt = abc[abc['Plant'] != 'Global Plant DO NOT SHIP-GBS']
dempvt = dempvt[dempvt['Qty'] != 0]
dempvt = dempvt[dempvt['CAP/EXP'] != 'EXP']
dempvt = dempvt.groupby(['P&G Part #']).agg('sum').reset_index(['P&G Part #'])
dempvt = dempvt.sort_values(by=['P&G Part #'])


dempvt = dempvt[['P&G Part #',
                 'Qty']]

dempvt = dempvt.rename(columns = {'Qty': 'Demand Qty'})

# In[CAAS SMI ANALYSIS]

smi = abc[abc['Plant'] == 'Global Plant DO NOT SHIP-GBS']
smi = pd.concat([smi[smi['program'] == 'CaaS.WWT'], smi[smi['program'] == 'CaaS.Vz'], smi[smi['program'] == 'CaaS.CBTS']], sort=True)
smi = smi[smi['Unit Status'] == 'Ordered']
smi = smi[smi['CAP/EXP'] != 'EXP']
smipvt = smi.groupby(['P&G Part #']).agg('sum').reset_index(['P&G Part #'])
smipvt = smipvt.sort_values(by=['P&G Part #'])

smipvt = pd.merge(smipvt, dempvt, how = 'left', on = 'P&G Part #')
smipvt = smipvt.fillna(0)
smipvt['SMI Ratio'] = smipvt['Qty'] / smipvt['Demand Qty']
desc = abc[['P&G Part #', 'Description']]
desc = desc.drop_duplicates(subset=['P&G Part #'], keep='first')
smipvt = pd.merge(smipvt, desc, how = 'left', on = 'P&G Part #')
                  
                       
smicols = ['P&G Part #',
           'Description',
           'PO Total Price (USD)',
           'Qty',
           'Demand Qty',
           'SMI Ratio',]

smipvt = smipvt[smicols]

# In[]
smisplit = smi.groupby(['P&G Part #', 'program']).agg('sum').reset_index(['P&G Part #', 'program'])

smicols = ['P&G Part #',
           'program',
           'PO Total Price (USD)',
           'Qty']


smisplit = smisplit[smicols]
smisplit = pd.concat([smisplit, smisplit.pivot(columns='program', values='Qty')], axis=1, sort=True)
smisplit = smisplit.rename(columns={'CaaS.WWT':'WWT.Qty', 'CaaS.Vz':'Vz.Qty'})
smisplit = pd.concat([smisplit, smisplit.pivot(columns='program', values='PO Total Price (USD)')], axis=1, sort=True)
smisplit = smisplit[['P&G Part #',
                     'CaaS.WWT',
                     'WWT.Qty',
                     'CaaS.Vz',
                     'Vz.Qty']]
smisplit = smisplit.groupby('P&G Part #').agg('sum').reset_index('P&G Part #')

smipvt = pd.merge(smipvt, smisplit, how='left', on='P&G Part #')
#smipvt['CaaS.WWT'] = smipvt['CaaS.WWT'].fillna(0)
#smipvt['CaaS.Vz'] = smipvt['CaaS.Vz'].fillna(0)
smipvt = smipvt.sort_values(by=['SMI Ratio', 'PO Total Price (USD)'], ascending=False)                  
                  
smipvt.to_csv('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Metrics/02-Documents/SMI.CaaS.analysis' + time.strftime("%Y%m%d-%H%M%S") + '.csv', index=False)
smipvt.to_csv('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Metrics/SMI.CaaS.analysis.csv', index=False)
