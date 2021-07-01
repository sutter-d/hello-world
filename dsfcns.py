# -*- coding: utf-8 -*-
"""
Created on Wed May 13 12:36:16 2020

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


import datetime as dt
import time
import glob
import os
import pandas as pd
import pyodbc 

# In[REQUIRED DATA SETS FOR FUNCTIONS]

wipstatus = ['Not Ordered',
             'Ordered',
             'Draft',
             'At MAV',
             'Alteration Requested',
             'BR Submitted',
             'Coupa Alt Awaiting Approval',
             'Ordered - Alteration Pending ',
             'Prepare For Pick Up',
             'Pick Up Confirmed',
             'Pick-up Confirmed']

shipstatus = ['Pick-up Confirmed',
              'Customs',
              'Delivered',
              'Shipped']
gdns = ['Ordered',
        'Ordered - Alteration Pending ',
        'Coupa Alt Awaiting Approval',
        'Prepare For Pick Up',
        'At MAV']

oo_status = ['CLOSED',
             'CANCELLED']

data = [['WWT NA', 'World Wide Technology - USA'],
        ['CBTS', 'CINCINNATI BELL TECHNOLOGY SOLUTIONS INC'],
        ['NA/LATAM', 'VERIZON BUSINESS NETWORK SERVICES INC'],
        ['NA/LATAM', 'Procter & Gamble Distributing, LLC c/o NCR Corpora'],        
        ['NA/LATAM', 'Procter & Gamble Distributing, LLC c/o Anixter NA'],
        ['NA/LATAM', 'Procter & Gamble Distributing, LLC c/o Westcon USA'],
        ['NA/LATAM','Procter & Gamble Distributing, LLC c/o Columbus /'],
        ['Singapore', 'VERIZON COMMUNICATIONS SINGAPORE PTE LTD'], #VERIZON COMMUNICATIONS SINGAPORE PTE LTD
        ['Singapore', 'VERIZON NETHERLANDS c/o NCR Singapore'],        
        ['Singapore', 'Verizon c/o NCR Singapore'],
        ['Switzerland', 'VERIZON NETHERLANDS BV'],
        ['Netherlands', 'VERIZON NETHERLANDS BV'],
        ['Netherlands', 'Procter & Gamble Distributing, LLC c/o NCR NL'],
        ['Netherlands', 'Procter & Gamble Distributing, LLC c/o Westcon NL']]

keydf = pd.DataFrame(data, columns=['INVkey', 
                                    'ESMkey'])

# In[GEM SQL QUERY ETL]
def etl(x):
    row = x
    dataframes = list()
    cols = ['Plant',
            'Line',
            'Description',
            'P&G Part #',
            'Supplier',
            'PO Total Price (USD)',
#            'Unit Status',
            'Xcharge Date',
            'KitUnitID',
            'Qty',
            'Destination ROS Date',
            'ShipDate',
            'Unit Status',
            'Invoice1A',
            'ShipFrom',
            'ShipTo',
            'OnSiteDate',
            'PlantPO',
            'PLANTCODE',
            'ApprovalTimeStamp',
            'Quoted',
            'QuotedTimeStamp',
            'OSS_UID',
            'PONumber',
            'CapExp',
            'DateReallocated',
            'ReallocationComments',
            'TempShipNumber']

    for r in row:
        df = pd.DataFrame(list(r)).transpose()
    #    df.columns = cols
        dataframes.append(df)

    df1 = pd.concat(dataframes)
    df1.columns = cols
    df1 = df1[~df1['PO Total Price (USD)'].isnull()]
    df1['PO Total Price (USD)'] = df1['PO Total Price (USD)'].apply(float)
    df1['P&G Part #'] = df1['P&G Part #'].apply(str)
    df1['Qty'] = df1['Qty'].apply(int)
    df1 = df1[df1['CapExp']=='CAP']
#    df1 = df1.sort_values(by=['KitUnitID', 'DateReallocated'], ascending=[True, False]).drop_duplicates('KitUnitID') #Added for duplicate DateReallocated comments
    df1 = df1.sort_values(by=['Supplier', 'Plant', 'P&G Part #'])
    return df1
# In[WWT OPEN ORDER AND ON HAND ETL]


def invwwt(x, y):
    # wwtoo = pd.read_excel('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Open Order/PG Tracking Report.xlsx')
    wwtoo = x
    wwtoo = wwtoo[['Mfg Part Number',
                   'Quantity Received',
                   'Open Quantity',
                   'PO Number',
                   'Line Status']]
    #wwtoo = wwtoo[wwtoo['Line Status'] != 'CLOSED']
    wwtoo = wwtoo[~wwtoo['Line Status'].isin(oo_status)]
    wwtoo = wwtoo.rename(index=str, columns={"Mfg Part Number": "Part Number",
                                             "Open Quantity": "Qty"})
    wwtoo = wwtoo.drop(columns=['Quantity Received'])

    wwtoh = y
    # wwtoh = wwtoh[wwtoh['Configuration'] != 'Child']
    # NEW FILTER
    wwtoh = wwtoh[wwtoh['Locator'].str.contains("STOCK")]
    wwtoh = wwtoh.drop(columns=['Locator',
                                'Configuration',
                                'Item Number',
                                'WWT PO']).groupby('Item Segment2').agg('sum').reset_index('Item Segment2')
    wwtoh = wwtoh.rename(index=str, columns={"Item Segment2": "Part Number",
                                             "Sum(Quantity)": "Qty"})
    wwtoh['Line Status'] = "INVENTORY"
    wwtoh['PO Number'] = "Received"

    wwtinv = pd.concat([wwtoo, wwtoh], sort=True)

    wwtsubs = wwtinv.groupby('Part Number', as_index=False).sum()
    wwtsubs['Line Status'] = "SubTotal"
    wwtsubs['PO Number'] = "--"

    wwtinv = pd.concat([wwtinv, wwtsubs], sort=True)

    cols = ['Part Number', 'Line Status', 'PO Number', 'Qty']

    wwtinv = wwtinv[cols]
    wwtinv = wwtinv.sort_values(by=['Part Number', 'Line Status'])
    wwtinv = wwtinv.reset_index(drop=True)

    wwt = wwtinv.groupby(['Part Number',
                          'Line Status']).agg('sum').reset_index(['Part Number', 'Line Status'])

    wwt = wwt.pivot_table(index='Part Number',
                          columns='Line Status',
                          values='Qty').reset_index()

    wwt['Integrator'] = "WWT NA"

    wwt['today'] = dt.datetime.fromtimestamp(time.time())
    return wwt
# In[ESM ETL FOR WWT PROJECTS]


#list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.Vz/GEM-ESM-ID2426-CaaS*') # * means all if need specific format then *.csv
#latest_vz= max(list_of_files, key=os.path.getctime)
#x = pd.read_excel(latest_vz,
#                  sheet_name='Level3-Details',
#                  header=6)
def esmwwt(x):
    wwtesm = x
    wwtesmcols = wwtesm.columns
    wwtesmcols = wwtesmcols.str.replace("\n", "")
    wwtesm.columns = wwtesmcols
    wwtesm['P&G Part #'] = wwtesm['P&G Part #'].apply(str)
#    desc = wwtesm[['P&G Part #', 'Description']]

    wwtesmwip = wwtesm[wwtesm['Unit Status'].isin(wipstatus)].copy()
    wwtesmwip = wwtesmwip[wwtesmwip['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    wwtesmship = wwtesm[wwtesm['Unit Status'].isin(shipstatus)].copy()
    wwtesmship = wwtesmship[wwtesmship['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    wwtesm = wwtesm[wwtesm['Plant'] == 'Global Plant DO NOT SHIP-GBS']
#    wwtesm = wwtesm[wwtesm['Unit Status'] == 'Ordered']
    wwtesm = wwtesm[wwtesm['Unit Status'].isin(gdns)]

    # Global Plant DO NOT SHIP-GBS

    wwtesm['type'] = 'GDNS Qty'
    wwtesmwip['type'] = 'GEM WIP'
    wwtesmship['type'] = 'GEM SHIP'

    # Global Plant DO NOT SHIP-GBS

    wwtesm = wwtesm[['P&G Part #',
                     'Description',
                     'Supplier',
                     'Qty',
                     'type']]
    wwtesmwip = wwtesmwip[['P&G Part #',
                           'Description',
                           'Supplier',
                           'Qty',
                           'type']]
    wwtesmship = wwtesmship[['P&G Part #',
                             'Description',
                             'Supplier',
                             'Qty',
                             'type']]

    wwtesm = pd.concat([wwtesm, wwtesmwip, wwtesmship], sort=True).reset_index(drop=True)
    wwtesm = pd.concat([wwtesm, wwtesm.pivot(columns='type', values='Qty')], axis=1, sort=True)
    wwtesm = wwtesm.sort_values(by=['P&G Part #'])

    wwtesm = wwtesm.fillna(0)
    wwtesm = wwtesm.drop(columns=['Qty', 'type']).groupby(['P&G Part #', 'Supplier']).agg('sum')
    wwtesm = wwtesm.reset_index()

    return wwtesm
# In[ESM ETL FOR VZ PROJECTS]
#list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/SWAP/GEM-ESM-ID2427-SWAP*') # * means all if need specific format then *.csv
#latest_vz= max(list_of_files, key=os.path.getctime)
#x = pd.read_excel(latest_vz,
#                  sheet_name='Level3-Details',
#                  header=6)
def esmvz(x):
    vzesm = x
    vzesmcols = vzesm.columns
    vzesmcols = vzesmcols.str.replace("\n", "")
    vzesm.columns = vzesmcols
    vzesm['P&G Part #'] = vzesm['P&G Part #'].apply(str)
    desc = vzesm[['P&G Part #', 'Description']]
    desc = pd.concat([desc, vzesm[['P&G Part #', 'Description']]], sort=True)
    desc = desc.drop_duplicates(subset='P&G Part #', keep='first')
        
    vzesmwip = vzesm[vzesm['Unit Status'].isin(wipstatus)].copy()
    vzesmwip = vzesmwip[vzesmwip['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    vzesmship = vzesm[vzesm['Unit Status'].isin(shipstatus)].copy()
    vzesmship = vzesmship[vzesmship['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    vzesm = vzesm[vzesm['Plant'] == 'Global Plant DO NOT SHIP-GBS']
#    vzesm = vzesm[vzesm['Unit Status'] == 'Ordered']
    vzesm = vzesm[vzesm['Unit Status'].isin(gdns)]
    
    vzesm['type'] = 'GDNS Qty'
    vzesmwip['type'] = 'GEM WIP'
    vzesmship['type'] = 'GEM SHIP'

    # Global Plant DO NOT SHIP-GBS

    vzesm = vzesm[['P&G Part #',
                   'Description',
                   'Supplier',
                   'Qty',
                   'type']]
    vzesmwip = vzesmwip[['P&G Part #',
                         'Description',
                         'Supplier',
                         'Qty',
                         'type']]
    vzesmship = vzesmship[['P&G Part #',
                           'Description',
                           'Supplier',
                           'Qty',
                           'type']]

    data = [['VERIZON BUSINESS NETWORK SERVICES INC', 'VERIZON BUSINESS NETWORK SERVICES INC'],
            ['VERIZON COMMUNICATIONS SINGAPORE PTE LTD', 'VERIZON COMMUNICATIONS SINGAPORE PTE LTD'],
            ['VERIZON NETHERLANDS BV', 'VERIZON NETHERLANDS BV'],
            ['Procter & Gamble Distributing, LLC c/o Columbus /', 'VERIZON BUSINESS NETWORK SERVICES INC'],
            ['Verizon c/o NCR Corporation', 'VERIZON BUSINESS NETWORK SERVICES INC'],
            ['Verizon c/o NCR Netherlands', 'VERIZON NETHERLANDS BV'],
            ['Verizon c/o NCR Singapore', 'VERIZON COMMUNICATIONS SINGAPORE PTE LTD']]

    stagers = pd.DataFrame(data, columns=['OldSupplier',
                                          'NewSupplier'])

    vzesm = pd.concat([vzesm, vzesmwip, vzesmship], sort=True).reset_index(drop=True)
    vzesm = pd.concat([vzesm, vzesm.pivot(columns='type', values='Qty')], axis=1, sort=True)

    vzesm = pd.merge(vzesm, stagers, how='left', left_on='Supplier', right_on='OldSupplier')

    def stag(x):
        temp = x['NewSupplier']
        temp2 = x['Supplier']
        if temp == 0:
            return temp2
        else:
            return temp

    vzesm = vzesm.fillna(0)
    vzesm['Supp'] = vzesm.apply(stag, axis=1)
    vzesm['Supplier'] = vzesm['Supp']
    vzesm = vzesm.drop(columns=['OldSupplier', 'NewSupplier', 'Supp'])

    vzesm = vzesm.drop(columns=['Qty', 'type']).groupby(['P&G Part #', 'Supplier']).agg('sum')
    vzesm = vzesm.reset_index()
    return vzesm