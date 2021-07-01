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

#wipstatus = ['Not Ordered',
#             'Ordered',
#             'Draft',
#             'At Mav',
#             'Alteration Requested',
#             'BR Submitted',
#             'Coupa Alt Awaiting Approval',
#             'Ordered - Alteration Pending',
#             'Prepare For Pick Up',
#             'Pick-up Confirmed']
#
#shipstatus = ['Pick-up Confirmed',
#              'Customs',
#              'Delivered',
#              'Shipped']

wipstatus = dsfcns.wipstatus

shipstatus = dsfcns.shipstatus

list_of_files = glob.glob(oodr + "*")  # * means all if need specific format then *.csv
latest_oo = max(list_of_files, key=os.path.getctime)

list_of_files = glob.glob(wwtohdr + "*")  # * means all if need specific format then *.csv
latest_oh = max(list_of_files, key=os.path.getctime)

wwt = invwwt(pd.read_excel(latest_oo, sheet_name=1), pd.read_excel(latest_oh, header=1))

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

vz = vz[vz['Project'] == 'BAU']


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
#vz = vz[['Part Number',
#             'On Hand',
#             'Open Orders',
#             'Partial',
#             'Subtotal',
#             'Work in Progress',
#             'Shipped',
#             'Region']]

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

#inv.to_csv('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/GEM.INV.Report.csv', index=False)

# In[]
server = 'GADC-GEMSQL001\GEMSP101' 
database = 'GEMInterface' 
username = 'GEMInterface_User' 
password = 'GEMUser2016' 

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# In[]
#
#list_of_files = glob.glob(dr + 'ESMs/CaaS.WWT/GEM-ESM-ID2428-CaaS*')  # * means all if need specific format then *.csv
#latest_wwt = max(list_of_files, key=os.path.getctime)
#
#wwtesm = esmwwt(pd.read_excel(latest_wwt,
#                              sheet_name='Level3-Details',
#                              header=6))
#desc = pd.read_excel(latest_wwt, sheet_name='Level3-Details', header=6)

cursor.execute("exec sp_GetESMData 2428") 
esm2428 = cursor.fetchall()
esm2428 = etl(esm2428)
wwtesm = esmwwt(esm2428)

desc = esm2428
desc = desc[['P&G Part #', 'Description']]
# In[]
                       
#list_of_files = glob.glob(dr + 'ESMs/BUILD/GEM-ESM-ID2425-BUILD*') # * means all if need specific format then *.csv
#latest_vz= max(list_of_files, key=os.path.getctime)
#
#vzesm = esmvz(pd.read_excel(latest_vz,
#                            sheet_name='Level3-Details',
#                            header=6))
#temp = pd.read_excel(latest_vz, sheet_name='Level3-Details', header=6)
#desc = pd.concat([desc, temp[['P&G Part #', 'Description']]], sort=True)
#desc = desc.drop_duplicates(subset='P&G Part #', keep='first')
cursor.execute("exec sp_GetESMData 2425") 
esm2425 = cursor.fetchall()
esm2425 = etl(esm2425)
vzesm = esmvz(esm2425)
#temp = pd.read_excel(latest_vz, sheet_name='Level3-Details', header=6)
temp = esm2425
desc = pd.concat([desc, temp[['P&G Part #', 'Description']]], sort=True)
desc = desc.drop_duplicates(subset='P&G Part #', keep='first')
# In[]

esm = pd.concat([wwtesm, vzesm], sort=True)
#esm = vz
esm = esm.fillna(0)


esm = esm.groupby(['Supplier', 'P&G Part #']).agg('sum').reset_index(['Supplier', 'P&G Part #'])

esm = pd.merge(esm, desc, 'left', 'P&G Part #')

esm['key'] = esm['Supplier'].map(str) + esm['P&G Part #'].map(str)#.str.replace(" ","")

# In[]
   
keydf = dsfcns.keydf

inv = pd.merge(inv,
               keydf,
               how='left',
               left_on='Integrator',
               right_on='INVkey')
inv['key'] = inv['ESMkey'] + inv['Part Number']

# COMMENT TO RETURN TO UNGROUPED RESULTS
inv= inv.groupby(['key']).agg('sum').reset_index(['key'])

comp = pd.merge(esm, inv, 'left', 'key')  # may need to change back to 'left'
comp['Diff'] = comp['SubTotal'] - comp['GDNS Qty']
comp['GEM Total'] = comp['GDNS Qty'] + comp['GEM WIP']
comp['Percent of GDNS'] = 1-(comp['Diff']/comp['GDNS Qty'])

# UNCOMMENT FOR UNGROUPED RESULTS
#comp = comp.drop(columns=['ESMkey',
#                          'INVkey',
#                          'key',
#                          'Part Number',
#                          'OH Ratio'])

comp = comp.sort_values(by=['Supplier', 'P&G Part #'])


cols = ['Supplier',
        'P&G Part #',
        'Description',
        'GDNS Qty',
        'GEM WIP',
        'GEM SHIP',
        'GEM Total',
        'key',
#        'Part Number', # UNCOMMENT FOR UNGROUPED RESULTS
#        'Integrator',
        'OPEN',
        'PARTIAL',
        'INVENTORY',
        'Work in Progress',
        'Shipped',
        'SubTotal',
        'Diff',
        'Percent of GDNS']

comp = comp[cols]

#comp = comp[comp['Supplier'] == 'World Wide Technology']

abc = pd.read_csv(abcdr + 'abc.cycle.count.csv')

comp = pd.merge(comp, abc[['P&G Part #', 'RPN abc']], 'left', 'P&G Part #')


comp = comp[comp['Supplier'] != 'World Wide Technology']
comp['Supplier'] = comp['Supplier'].fillna(0)
comp = comp[comp['Supplier'] != 0]

comp['Diff'] = comp['Diff'].fillna("Missing from Vz Reporting")

comp.to_excel(invdr + 'OO.OH.hist/BUILD.Vz.Inventory' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx', index=False)
comp.to_excel(invdr + 'BUILD.Vz.Inventory.xlsx', index=False)

print("INV Script Complete")

from pandas.util import hash_pandas_object
h = hash_pandas_object(comp).sum()
print(h)
