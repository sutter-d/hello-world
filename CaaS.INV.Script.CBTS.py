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
import pyodbc
from dsfcns import esmwwt, esmvz, invwwt, etl
import dsfcns

dr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/"
invdr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Inventory/"
abcdr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Inventory/Part Classification/"
smidr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Inventory/SMI/"
vzohdr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Inventory/On Hand/Vz OH/"
wwtohdr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Inventory/On Hand/WWT OH/"
oodr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Inventory/Open Order/"

wipstatus = dsfcns.wipstatus

shipstatus = dsfcns.shipstatus

list_of_files = glob.glob(oodr + "*")  # * means all if need specific format then *.csv
latest_oo = max(list_of_files, key=os.path.getctime)

list_of_files = glob.glob(wwtohdr + "*")  # * means all if need specific format then *.csv
latest_oh = max(list_of_files, key=os.path.getctime)

wwt = invwwt(pd.read_excel(latest_oo, sheet_name = 1), pd.read_excel(latest_oh, header=1))


# In[]

list_of_files = glob.glob(vzohdr + "*")  # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)

vz = pd.read_excel(latest_file, 
                     sheet_name='Summary')

vz = vz[['Part Number',
             'Project',
             'Open Orders',
             'On Hand',
             'Work in Progress',
             'Shipped',
             'Region']]

vz = vz[vz['Project'].str.lower() == 'caas']


vzcols = ['Open Orders',
          'On Hand',
          'Work in Progress',
          'Shipped']
for i in vzcols:
    vz[i] = vz[i].fillna(0)

vz['Open Orders'] = vz['Open Orders'].apply(int)
vz['Part Number'] = vz['Part Number'].apply(str)
vz['Part Number'] = vz['Part Number'].str.replace(" ","")

vz['PARTIAL'] = 0
vz['SubTotal'] = vz['Open Orders'] + vz['On Hand'] 

vz = vz.rename(index=str, columns={'Region': 'Integrator',
                                   'Open Orders': 'OPEN',
                                   'On Hand': 'INVENTORY'})

vz['today'] = dt.datetime.fromtimestamp(time.time())


# In[]
inv = vz

inv = inv[['Integrator',
           'Part Number',
           'OPEN',
           'PARTIAL',
           'INVENTORY',
           'Work in Progress',
           'Shipped',
           'SubTotal']]
inv['OH Ratio'] = inv['INVENTORY']/inv['SubTotal']
inv = inv.fillna(0)
inv['Part Number'] = inv['Part Number'].apply(str)
inv['Integrator'] = inv['Integrator'].apply(str)

inv.to_csv(dr + 'GEM.INV.Report.csv', index=False)
# In[]
server = 'GADC-GEMSQL001\GEMSP101'
database = 'GEMInterface'
username = 'GEMInterface_User'
password = 'GEMUser2016'
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ' ;PWD=' + password)
cursor = cnxn.cursor()

# In[]
                       
cursor.execute("exec sp_GetESMData 2601") 
esm2601 = cursor.fetchall()
esm2601 = etl(esm2601)
cbtsesm = esmwwt(esm2601)
cbtsesm['P&G Part #'] = cbtsesm['P&G Part #'].str.strip()

desc = esm2601                            
desc = desc[['P&G Part #', 'Description']]
# In[]
esm = cbtsesm
esm = esm.fillna(0)

esm = esm.groupby(['Supplier', 'P&G Part #']).agg('sum').reset_index(['Supplier', 'P&G Part #'])

esm = pd.merge(esm, desc, 'left', 'P&G Part #')

esm['key'] = esm['Supplier'].map(str) + esm['P&G Part #'].map(str).str.replace(" ","")

esm.to_excel(invdr + 'OO.OH.hist/CaaS.CBTS.Inventory' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx', index=False)
# In[]
   
# =============================================================================
# keydf = dsfcns.keydf
#    
# #inv = pd.merge(inv,
# #               keydf,
# #               how='left',
# #               left_on='Integrator',
# #               right_on='INVkey')
# #inv['key'] = inv['ESMkey'] + inv['Part Number']
# 
# # COMMENT TO RETURN TO UNGROUPED RESULTS
# #inv= inv.groupby(['key']).agg('sum').reset_index(['key'])
# 
# esm = pd.merge(esm,
#                keydf,
#                how='left',
#                left_on='Supplier',
#                right_on='ESMkey')
# esm['key'] = esm['INVkey'] + esm['P&G Part #'].map(str).str.replace(" ","")
# temp = inv.copy()
# temp['key'] = temp['Integrator'] + temp['Part Number']
# 
# temp = temp.groupby(['key']).agg('sum').reset_index(['key'])
# 
# desc = esm[['key','P&G Part #', 'Description', 'INVkey']]
# 
# esm = esm.groupby(['key']).agg('sum').reset_index(['key'])
# esm = pd.merge(esm,
#                desc,
#                'left',
#                'key')
# # In[]
# inv = temp
# 
# comp = pd.merge(esm, inv, 'left', 'key')  # may need to change back to 'left'
# comp['GEM Total'] = comp['GDNS Qty'] + comp['GEM WIP'] + comp['GEM SHIP']
# comp['SubTotal'] = comp['OPEN'] + comp['PARTIAL'] + comp['INVENTORY'] + comp['Work in Progress'] + comp['Shipped']
# comp['Diff'] = comp['SubTotal'] - comp['GEM Total']
# comp['Percent of GEM Total'] = (comp['Diff']+comp['GEM Total'])/comp['GEM Total']
# 
# # UNCOMMENT FOR UNGROUPED RESULTS
# #comp = comp.drop(columns=['ESMkey',
# #                          'INVkey',
# #                          'key',
# #                          'Part Number',
# #                          'OH Ratio'])
# 
# #comp = comp.sort_values(by=['Supplier', 'P&G Part #'])
# comp = comp.sort_values(by=['key'])
# 
# comp = comp.rename(index=str, columns={'INVkey': 'Region',
#                                    'Open Orders': 'OPEN',
#                                    'On Hand': 'INVENTORY'})
# 
# cols = ['Region',
#         'P&G Part #',
#         'Description',
#         'GDNS Qty',
#         'GEM WIP',
#         'GEM SHIP',
#         'GEM Total',
#         'key',
# #        'Part Number', # UNCOMMENT FOR UNGROUPED RESULTS
# #        'Integrator',
#         'OPEN',
#         'PARTIAL',
#         'INVENTORY',
#         'Work in Progress',
#         'Shipped',
#         'SubTotal',
#         'Diff',
#         'Percent of GEM Total']
# 
# comp = comp[cols]
# 
# #comp = comp[comp['Supplier'] == 'World Wide Technology']
# 
# abc = pd.read_csv(abcdr + 'abc.cycle.count.csv')
# 
# comp = pd.merge(comp, abc[['P&G Part #', 'RPN abc']], 'left', 'P&G Part #')
# 
# 
# comp = comp[comp['Region'] != 'WWT NA']
# #comp['Supplier'] = comp['Supplier'].fillna(0)
# #comp = comp[comp['Supplier'] != 0]
# 
# comp = comp.drop_duplicates(subset='key', keep='first')
# 
# comp['Diff'] = comp['Diff'].fillna("Missing from Vz Reporting")
# 
# comp.to_excel(invdr + 'OO.OH.hist/CaaS.Vz.Inventory' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx', index=False)
# #comp.to_csv(invdr + 'CaaS.Vz.Inventory.csv', index=False)
# comp.to_excel(invdr + 'CaaS.Vz.Inventory.xlsx', index=False)
# =============================================================================

print("INV Script Complete")

from pandas.util import hash_pandas_object
h = hash_pandas_object(esm).sum()

print(h)
