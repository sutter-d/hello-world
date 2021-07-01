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

# In[]

list_of_files = glob.glob(oodr + "*")  # * means all if need specific format then *.csv
latest_oo = max(list_of_files, key=os.path.getctime)

list_of_files = glob.glob(wwtohdr + "*")  # * means all if need specific format then *.csv
latest_oh = max(list_of_files, key=os.path.getctime)

wwt = invwwt(pd.read_excel(latest_oo, sheet_name=0), pd.read_excel(latest_oh, header=1))


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

# =============================================================================
# Somewhere here I need to Trim the list to CaaS only and then duplicate
# this file for each program
# =============================================================================


vzcols = ['Open Orders',
          'On Hand',
          'Work in Progress',
          'Shipped']
for i in vzcols:
    vz[i] = vz[i].fillna(0)

vz['Open Orders'] = vz['Open Orders'].apply(int)
vz['Part Number'] = vz['Part Number'].apply(str)

vz['Partial'] = 0
vz['Subtotal'] = vz['Open Orders'] + vz['On Hand']

vz = vz[['Part Number',
             'On Hand',
             'Open Orders',
             'Partial',
             'Subtotal',
             'Region']]

vz['today'] = dt.datetime.fromtimestamp(time.time())

wwtcols = wwt.columns
vz.columns = wwtcols

# In[]
inv = wwt

inv = inv[['Integrator',
           'Part Number',
           'OPEN',
           'PARTIAL',
           'INVENTORY',
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

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# In[] WWT GEM DB QUERY FOR PROJECTS 2426, 2428, 2602, 2425

cursor.execute("exec sp_GetESMData 2428") 
esm2428 = cursor.fetchall()
esm2428 = etl(esm2428)

cursor.execute("exec sp_GetESMData 2602") 
esm2602 = cursor.fetchall()
esm2602 = etl(esm2602)

cursor.execute("exec sp_GetESMData 2426") 
esm2426 = cursor.fetchall()
esm2426 = etl(esm2426)

esm2426 = esm2426[esm2426['Supplier'].str.contains('World Wide Technology')]

cursor.execute("exec sp_GetESMData 2425") 
esm2425 = cursor.fetchall()
esm2425 = etl(esm2425)

esm2425 = esm2425[esm2425['Supplier'].str.contains('World Wide Technology')]

esm2428 = esm2428.append(esm2426)

esm2428 = esm2428.append(esm2602)

esm2428 = esm2428.append(esm2425)

wwtesm = esmwwt(esm2428)

desc = esm2428
desc = desc[['P&G Part #', 'Description']]
# In[]
                       
#list_of_files = glob.glob(dr + 'ESMs/CaaS.Vz/GEM-ESM-ID2426-CaaS*') # * means all if need specific format then *.csv
#latest_vz= max(list_of_files, key=os.path.getctime)
#
#vzesm = esmvz(pd.read_excel(latest_vz,
#                            sheet_name='Level3-Details',
#                            header=6))

cursor.execute("exec sp_GetESMData 2426") 
esm2426 = cursor.fetchall()
esm2426 = etl(esm2426)
esm2426 = esm2426[~esm2426['Supplier'].str.contains('World Wide Technology')]
vzesm = esmvz(esm2426)
#temp = pd.read_excel(latest_vz, sheet_name='Level3-Details', header=6)
temp = esm2426
desc = pd.concat([desc, temp[['P&G Part #', 'Description']]], sort=True)
desc = desc.drop_duplicates(subset='P&G Part #', keep='first')
# In[]

esm = pd.concat([wwtesm, vzesm], sort=True)
esm = esm.fillna(0)

esm = esm.groupby(['Supplier',
                   'P&G Part #']).agg('sum').reset_index(['Supplier',
                                                          'P&G Part #'])

esm = pd.merge(esm, desc, 'left', 'P&G Part #')

esm['key'] = esm['Supplier'].map(str) + esm['P&G Part #'].map(str)
# In[]

keydf = dsfcns.keydf

inv = pd.merge(inv,
               keydf,
               how='left',
               left_on='Integrator',
               right_on='INVkey')
inv['key'] = inv['ESMkey'] + inv['Part Number']

comp = pd.merge(esm, inv, 'outer', 'key')  # may need to change back to 'left'
comp['Diff'] = comp['SubTotal'] - comp['GDNS Qty']
comp['GEM Total'] = comp['GDNS Qty'] + comp['GEM WIP']
comp['Percent of GDNS'] = 1-(comp['Diff']/comp['GDNS Qty'])
comp = comp.drop(columns=['ESMkey',
                          'INVkey',
#                          'key',
#                          'Part Number',
                          'OH Ratio'])

comp = comp.sort_values(by=['P&G Part #', 'Integrator'])


cols = ['Supplier',
        'P&G Part #',
        'Description',
        'GDNS Qty',
        'GEM WIP',
        'GEM SHIP',
        'GEM Total',
        'key',
        'Part Number',
        'Integrator',
        'OPEN',
        'PARTIAL',
        'INVENTORY',
        'SubTotal',
        'Diff',
        'Percent of GDNS']

comp = comp[cols]

#comp = comp[comp['Supplier'] == 'World Wide Technology']

abc = pd.read_csv(abcdr + 'abc.cycle.count.csv')

comp = pd.merge(comp, abc[['P&G Part #', 'RPN abc']], 'left', 'P&G Part #')

comp = comp[comp['Supplier'].notna()]
comp = comp[comp['Supplier'].str.contains('World Wide Technology')]
temp = pd.DataFrame([latest_oo, latest_oh], columns = ['P&G Part #'])
comp = pd.DataFrame.append(comp, temp, ignore_index=True)
cols.append('RPN abc')
comp = comp[cols]
comp.to_excel(invdr + 'OO.OH.hist/CaaS.WWT.Inventory.OH.' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx', index=False)
comp.to_excel(invdr + 'CaaS.WWT.Inventory.xlsx', index=False)

print("INV Script Complete")

from pandas.util import hash_pandas_object
h = hash_pandas_object(comp).sum()

print(h)