#!usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:59:01 2021

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

proc_update = False

"""
# =============================================================================
# # ORDER OF THIS SCRIPT-BUILD BOM TOP DOWN
# # 1. QUERY DURO API FOR RACK PARENT PRODUCT DETAILS
# # 2. UNPACK GET QUERY DETAILS AND PULL CHILD COMPONENTS
# # 3. REQUERY API FOR CHILD COMPONENT DETAILS (REPEAT 1 AND 2)
# # 4. APPEND RESULTS TO DATAFRAME AND BUILD BOM FILE
# # 5. MULTIPLY ASSEMBLY QUANTITIES TOP DOWN TO GET TOTAL BOM QTY PER PN
# # 6. UNPACK PRODUCTION SCHEDULE FROM DURO OR QUERY FROM AIRTABLE
# # 7. MULTIPLY FORECAST BY TOTAL BOM QTY (EXTENDED QTY) TO GET TOTAL FORECASTED DEMAND
# # 8. AGGREGATE PROCUREMENT OPEN ORDER AND ON HAND INVENTORY AT THE CPN LEVEL
# # 9. AGGREGATE FORECASTED DEMAND ACROSS ALL MODELS AT CPN LEVEL
# # 10. JOIN AGGREGATED FORECAST DEMAND WITH AGGREGATED PROCURMENT DATA FOR COMPONENT FORECAST FILE
# =============================================================================
"""
# In[BUILD BOM FUNCTION]
"""
BUILD BOM FUNCTION <= MOVED TO RESTAPI TO USE IN OTHER SCRIPTS
"""

# In[FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES]
"""
FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES
"""

logging.debug("START FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES")

def prodsched(oxforecast, oxbom):
    forecast = oxforecast
    duroext = oxbom
    for x in range(len(forecast['key'])):
        # qty = forecast.at[x, 'volume']
        # key = forecast.at[x, 'key']
        print(forecast.at[x, 'key'])
        duroext[forecast.at[x,'key']] = [forecast.at[x,'volume'] * i for i in duroext['extqty']]
    
    duroext['forecastTotal'] = duroext['evt1'] + duroext['evt2'] + duroext['dvt'] + duroext['pvt'] + duroext['mp']
    
    cols_to_move = ['queryPN',
                    'parent',
                    'cpn',
                    'name',
                    'category',
                    'level',
                    'procurement',
                    'quantity',
                    'extqty',
                    'evt1',
                    'evt2',
                    'dvt',
                    'pvt',
                    'mp',
                    'forecastTotal',
                    'leadTime',
                    'ltUnits']
    
    cols = cols_to_move + [col for col in duroext.columns if col not in cols_to_move]
    duroext = duroext[cols]
    duroext['leadTime'] = duroext['leadTime'].fillna(0)
    duroext['ltUnits'] = duroext['ltUnits'].fillna("0")
    return duroext

logging.debug("FINISH FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES")

# In[FORECAST DATA PULLED FROM GDRIVE]
"""
FORECAST DATA PULLED FROM GDRIVE
"""

logging.debug("START FORECAST DATA PULLED FROM GDRIVE")

forecast_gdrive = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/Production Forecast/Oxide Production Forecast.xlsx'
forecast_get = pd.read_excel(forecast_gdrive, sheet_name='Production Forecast', header = 0)
forecast_get = forecast_get.loc[:,'CPN':'Qty']
forecast_pns = forecast_get['CPN'].drop_duplicates().reset_index(drop=True)
forecast_grouped = forecast_get.groupby(['CPN', 'Build Phase']).agg('sum').reset_index(drop=False)
forecast_grouped_pvt = forecast_grouped.pivot(index = 'Build Phase', columns='CPN', values = 'Qty').fillna(0)
# TO ITERATE THROUGH LIST OF PNS, NEED TO RUN BOM AND THEN SCHED FCNS
# forecast_get = forecast_get[forecast_get['CPN'] == duroid]
# forecast_grouped = forecast_grouped.drop(columns=['CPN']).reset_index(drop=True)
# forecast_grouped.columns = ['key', 'volume']
# forecast = forecast_grouped

logging.debug("FINISH FORECAST DATA PULLED FROM GDRIVE")
# In[USE FORECAST PNS TO PULL BOMS FROM DURO API]
"""
USE FORECAST PNS TO PULL BOMS FROM DURO API
"""

logging.debug("START USE FORECAST PNS TO PULL BOMS FROM DURO API")

durobom_forecast = []
mpn = []
for pns in forecast_pns:
    print(pns)
    duroid = pns
    forecast_grouped_pn = forecast_grouped_pvt[[duroid]].reset_index(drop=False)
    # forecast_grouped_pn = forecast_grouped_pn.drop(columns=['CPN']).reset_index(drop=True)
    forecast_grouped_pn.columns = ['key', 'volume']
    forecast = forecast_grouped_pn
    print(forecast)
    
    # ASSIGNING PN TO QUERY AND RUNNING FUNCTION TO BUILD DURO BOM
    durobom = oxrest.buildbom(duroid, durocreds)
    # SAVING DURO MPN INFO TO DF FOR LATER
    if len(mpn) == 0:
        mpn = durobom
    else:
        mpn = mpn.append(durobom)
    # RUNNING PRODUCTION SCHEDULE FUNCTION TO GET FULL BOM FORECAST
    if len(durobom_forecast) == 0:
        durobom_forecast = prodsched(forecast, durobom)
    else:
        durobom_forecast = durobom_forecast.append(prodsched(forecast, durobom))

# mpn = durobom
mpn_get = mpn
mpn = mpn.set_index(['cpn'])
mpn = oxrest.unpack(mpn['sources.manufacturers'])
mpn = pd.concat([mpn.reset_index(drop=False),oxrest.unpack(mpn['mpn'])], axis = 1)
mpn = mpn[['ind', 'key']].rename(columns = {'ind': 'cpn', 'key': 'mpn'}).drop_duplicates()
histup = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/HistUpdates/'
csv = histup + 'mpn.csv'
mpn.to_csv(csv, index=False)

manf_cpn = durobom_forecast[['cpn', 'sources.manufacturers']].set_index(['cpn'])

logging.debug("FINISH USE FORECAST PNS TO PULL BOMS FROM DURO API")

# In[COMBINING PN QUERIES AND PUSHING TO AIRTABLE]
"""
COMBINING PN QUERIES AND PUSHING TO AIRTABLE
"""

logging.debug("START COMBINING PN QUERIES AND PUSHING TO AIRTABLE")

durobom_forecast = durobom_forecast.loc[:,'queryPN':'ltUnits']
durobom_forecast = durobom_forecast.fillna('-')
durobom_forecast = durobom_forecast.reset_index(drop=False)
csv = histup + 'durobom_forecast.csv'
durobom_forecast.to_csv(csv, index=False)
hash1 = pd.util.hash_pandas_object(durobom_forecast).sum()
logging.debug(str(hash1) + " - durobom_forecast hash")


atcreds = allcreds['oxide_airtable_opsextbom']
rec_ids = oxrest.atblget(atcreds)
if rec_ids.size>0:
    rec_ids = rec_ids['id'].tolist()
    rec_ids = Table(atcreds['api_key'], atcreds['base_id'], atcreds['table_name']).batch_delete(rec_ids)

durobom_upload = durobom_forecast.to_dict(orient='records')
durobomcreate = oxrest.atblcreate(atcreds, durobom_upload)

# durobomid = '913-0000004'
# durobombom = buildbom(durobomid)

logging.debug("FINISH COMBINING PN QUERIES AND PUSHING TO AIRTABLE")

# In[IF ONLY PROCUREMENT UPDATE]
"""
---------------------------------
---------------------------------
IF ONLY PROCUREMENT UPDATE
---------------------------------
---------------------------------
"""

if proc_update == True:
    durobom_forecast = pd.read_csv('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/HistUpdates/durobom_forecast.csv')
    mpn = pd.read_csv('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/HistUpdates/mpn.csv')

# In[PULLING PROCUREMENT TRACKER FROM GDRIVE]
"""
PULLING PROCUREMENT TRACKER FROM GDRIVE
"""

logging.debug("START PULLING PROCUREMENT TRACKER FROM GDRIVE")

proc_gdrive = '/Volumes/GoogleDrive/Shared drives/Oxide Benchmark Shared/Benchmark Procurement/On Hand Inventory/Oxide Inv Receipts and Inv Tracker at Benchmark (Rochester).xlsx'
proc_get = pd.read_excel(proc_gdrive, sheet_name='Oxide Inventory Receipts',  header = 0)
proc_get['Manufacturer P/N'] = proc_get['Manufacturer P/N'].astype(str).str.strip()

proc_total = proc_get.groupby(['Manufacturer P/N']).agg('sum').reset_index()
cols = ['Manufacturer P/N',
        'Oxide Received Inventory @ Benchmark',
        'Order Qty To Go (Calculated)',
        'Qty Ordered']
proc = proc_total[cols]
proc = proc.rename(columns={'Manufacturer P/N':'proc mpn',
                            'Qty Ordered': 'TotalQty',
                            'Oxide Received Inventory @ Benchmark': 'OnHand' ,
                            'Order Qty To Go (Calculated)': 'OpenOrders'})


logging.debug("FINISH PULLING PROCUREMENT TRACKER FROM GDRIVE")

# In[UNPACKING QUOTE AND LT INFO FROM DURO BOM]
"""
UNPACKING QUOTE AND LT INFO FROM DURO BOM
"""
logging.debug("START UNPACKING QUOTE AND LT INFO FROM DURO BOM")
# UNPACK NESTED DICT SOURCES.MANUFACTURERS AND RESET INDEX
manf = oxrest.unpack(manf_cpn['sources.manufacturers']).reset_index(drop=False)
# lt = oxrest.unpack(manf['leadTime'])
# UNPACK DISTRIBUTORS COL AND RESET INDEX
dst = oxrest.unpack(manf['distributors']).reset_index(drop=False)
# UNPACK QUOTES COL, STRIP OUT ALL 1 PIECE QUOTES AND CHANGE TYPE TO INT
qts = oxrest.unpack(dst['quotes'])
qts = qts[qts['minQuantity']>1]
qts['minQuantity'] = qts['minQuantity'].astype(int)
# RENAME IND COL TO CPN, RESET AND KEEP INDEX, AND RENAME MANFIND
manf = manf.reset_index(drop=False).rename(columns = {'index':'manfind',
                                                      'ind':'cpn',
                                                      'name':'manf_name',
                                                      'description':'manf_desc'})
# DST IND COL IS KEY BACK TO MANF INDEX COL
# RENAME IND COL TO MANFIND, RESET AND KEEP INDEX AND RENAME DSTIND
dst = dst.reset_index(drop=False).rename(columns = {'index':'dstind',
                                                    'ind':'manfind',
                                                    'name': 'dst_name',
                                                    'description': 'dst_desc'})
pkg = oxrest.unpack(dst['package'])
dst = pd.concat([dst, pkg],
                axis = 1)
# QTS IND COL IS KEY BACK TO DST INDEX COL
# RENAME IND COL TO DSTIND, RESET INDEX
qts = qts.reset_index(drop=False).rename(columns = {'ind':'dstind'})
srcs = manf.merge(dst, 'left', 'manfind')
srcs = srcs.merge(qts, 'left', 'dstind')
# DROP NULL ROWS MISSING DST AND QTS INFO
srcs = srcs[srcs['minQuantity'].notnull()].reset_index(drop=True)
lt = oxrest.unpack(srcs['leadTime_y'])
srcs = pd.concat([srcs, lt],
                 axis = 1)

cols_to_move = ['cpn',
                'manf_name',
                'manf_desc',
                'dst_name',
                'dst_desc',
                'type',
                'minQuantity',
                'unitPrice',
                'units',
                'value']

srcs = srcs[cols_to_move]
srcs = srcs.drop_duplicates()

logging.debug("FINISH UNPACKING QUOTE AND LT INFO FROM DURO BOM")

# In[PULLING CPN TO MPN KEY FROM GDRIVE]
"""
PULLING CPN TO MPN KEY FROM GDRIVE
"""

logging.debug("START PULLING CPN TO MPN KEY FROM GDRIVE")

# MERGING MPN KEY AND PROCUREMENT DATA INTO ONE DF
mpn['mpn'] = mpn['mpn'].str.lower()
proc['proc mpn'] = proc['proc mpn'].astype(str).str.lower()
mpn_proc = mpn.merge(proc, 'left', left_on='mpn', right_on ='proc mpn')
mpn_proc['mpn'] = mpn_proc['mpn'].fillna('-')
mpn_proc['proc mpn'] = mpn_proc['proc mpn'].fillna('-')
mpn_proc = mpn_proc.fillna(0)
# GROUP ENTRIES BY MPN AND SUM REQUIRED QUANTITIES WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
mpn_proc = mpn_proc.groupby(['cpn'], as_index=False).agg(lambda x : x.sum() if x.dtype=='float64' else ', '.join(x))
mpn_proc['mpn'] = mpn_proc['mpn'].str.upper()
mpn_proc['proc mpn'] = mpn_proc['proc mpn'].str.upper()

csv = histup + 'mpn_proc' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
mpn_proc.to_excel(csv, index=False)

logging.debug("FINISH PULLING CPN TO MPN KEY FROM GDRIVE")

# In[MERGE CPN, MPN, PROC AND MERGE TO BOM]
"""
MERGE CPN, MPN, PROC AND MERGE TO BOM
"""

logging.debug("START MERGE CPN, MPN, PROC AND MERGE TO BOM")

durobom_forecast_mpn_proc = durobom_forecast.merge(mpn_proc, 'left', 'cpn') # DOESNT WORK WITHOUT SOME CPN TO MPN DECODER RING
reports = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Reports/'
csv = reports + 'durobom_forecast_mpn_proc' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
durobom_forecast_mpn_proc.to_excel(csv, index=False)
hash1 = pd.util.hash_pandas_object(durobom_forecast_mpn_proc).sum()
logging.debug(hash1)

logging.debug("FINISH MERGE CPN, MPN, PROC AND MERGE TO BOM")

# WRITING RESULTS TO AIRTABLE
# atcreds = allcreds['oxide_airtable_bomforecast']
# rec_ids = oxrest.atblget(atcreds)
# if rec_ids.size>0:
#     rec_ids = rec_ids['id'].tolist()
#     rec_ids = Table(atcreds['api_key'], atcreds['base_id'], atcreds['table_name']).batch_delete(rec_ids)

# cols = ['OnHand',
#         'OpenOrders',
#         'TotalQty']
# for i in cols:
#     durobom_forecast_mpn_proc[i] = durobom_forecast_mpn_proc[i].fillna(0)
# durobom_forecast_mpn_proc = durobom_forecast_mpn_proc.fillna('-')
# durobom_forecast_mpn_proc_upload = durobom_forecast_mpn_proc.to_dict(orient='records')
# durobom_forecast_mpn_proc_create = oxrest.atblcreate(atcreds, durobom_forecast_mpn_proc_upload)

# logging.debug("WROTE MERGE CPN, MPN, PROC AND MERGE TO BOM TO AIRTABLE")

# In[REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN]
"""
REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN
"""

logging.debug("START REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN")

# ASSEMBLIES AND BOMS ARE REMOVED FROM DF 
# comps_forecast = pd.DataFrame([durobom_forecast.loc[i] for i in range(len(durobom_forecast['index'])) if "Assembly" not in durobom_forecast.at[i, 'category']]).reset_index(drop=True)
# comps_forecast = pd.DataFrame([comps_forecast.loc[i] for i in range(len(comps_forecast['index'])) if "BOM" not in comps_forecast.at[i, 'category']])
comps_forecast = durobom_forecast.loc[(durobom_forecast['category'].str.contains("Cable")) | #KEEP CABLE ASSEMBLIES
                                              (~durobom_forecast['category'].str.contains("Assembly"))] #BUT GET RID OF ANYTHING OTHER ASSEMBLY
comps_forecast = comps_forecast.loc[(~comps_forecast['category'].str.contains("BOM"))]
comps_forecast = comps_forecast.loc[(~comps_forecast['category'].str.contains("Board"))]
comps_forecast = comps_forecast.loc[(~comps_forecast['name'].str.contains("SPARE"))]
comps_forecast = comps_forecast.loc[(~comps_forecast['cpn'].str.startswith("999"))]


# GROUP BY CPN AND SUM REQUIRED QUANTITIES WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
comps_forecast = comps_forecast.groupby(['cpn'], as_index=False).agg(lambda x : x.sum() if x.dtype=='float64' else ', '.join(x))
comps_forecast = comps_forecast.drop(columns=['queryPN', 'name', 'category', 'ltUnits'])

# BETTER COMPARISON BECAUSE MPN AND PROC IS GROUPED AND SUMMED TO A SINGLE CPN ENTRY BEFORE MERGE
comps_forecast_mpn_proc = comps_forecast.merge(mpn_proc, 'left', 'cpn')
comps_forecast_mpn_proc = comps_forecast_mpn_proc.merge(durobom_forecast[['cpn','name', 'category']].drop_duplicates())
# comps_forecast_mpn_proc['evtOK'] = comps_forecast_mpn_proc[['evt1', 'TotalQty']].apply(lambda x, y: 'Yes' if x > y else 'No')
def colcomp(forecast, onhand):
    if onhand > forecast:
        return 'Yes'
    else:
        return 'No'

# CREATING INDICATOR COLUMNS
# EACH COLUMN = YES IF THERE IS ENOUGH MATERIAL ORDERED, = NO IF NOT
# TO BE A YES FOR DVT, ORDER QTY > SUM(EVT, DVT) AND SO ON FOR PVT AND MP
comps_forecast_mpn_proc['evtOK'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(x['evt1']+x['evt2'], x['TotalQty']), axis=1)
comps_forecast_mpn_proc['dvtOK'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(x['evt1']+x['evt2']+x['dvt'], x['TotalQty']), axis=1)
comps_forecast_mpn_proc['pvtOK'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(x['evt1']+x['evt2']+x['dvt']+x['pvt'], x['TotalQty']), axis=1)
comps_forecast_mpn_proc['mpOK'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(x['evt1']+x['evt2']+x['dvt']+x['pvt']+x['mp'], x['TotalQty']), axis=1)
comps_forecast_mpn_proc = comps_forecast_mpn_proc.sort_values(by=['evtOK', 'dvtOK', 'pvtOK', 'mpOK'], ascending=True).reset_index(drop=True)



# FINAL FORMATTING FOR LEGIBILITY
cols_to_move = ['parent',
                'cpn',
                'name',
                'category']

cols = cols_to_move + [col for col in comps_forecast_mpn_proc.columns if col not in cols_to_move]
comps_forecast_mpn_proc = comps_forecast_mpn_proc[cols]
comps_forecast_mpn_proc['buyQty'] = comps_forecast_mpn_proc.apply(lambda x: x['forecastTotal'] - x['TotalQty'] if x['forecastTotal'] > x['TotalQty'] else 0, axis=1)

csv = reports + 'comp_forecast_mpn_proc' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
comps_forecast_mpn_proc.to_excel(csv, index=False)
hash1 = pd.util.hash_pandas_object(comps_forecast_mpn_proc).sum()
logging.debug(str(hash1) + " - comps_forecast_mpn_proc hash")


buyqty = comps_forecast_mpn_proc[['cpn', 'name', 'buyQty']]
buyqty = buyqty.merge(srcs, 'left', 'cpn')
buyqty['ordratio'] = buyqty.apply(lambda x: x['buyQty']/x['minQuantity'], axis = 1)
buyqty = buyqty[buyqty['buyQty']!=0]
buyqty_high = buyqty[buyqty['ordratio']<1]
buyqty_high = buyqty_high[['cpn', 'ordratio']].groupby(['cpn'], as_index=False).agg(max)
buyqty_low = buyqty[buyqty['ordratio']>1]
buyqty_low = buyqty_low[['cpn', 'ordratio']].groupby(['cpn'], as_index=False).agg(min)
buys = [buyqty, buyqty_high, buyqty_low]
for df in buys:
    df['key'] = ''
    df['key'] = df.apply(lambda x: x['cpn'] + str(x['ordratio']), axis=1)


buyqty_high['highlow'] = 'MOQ>NEED'
buyqty_low['highlow'] = 'NEED>MOQ'
buyqty_high = buyqty_high[['key', 'highlow']]
buyqty_low = buyqty_low[['key', 'highlow']]
buyqty_low = buyqty_low.append(buyqty_high)
buyqty = buyqty.merge(buyqty_low, 'left', 'key')
# buyqty = buyqty.loc[(buyqty['manf_name'].isnull()) |
#                     (buyqty['highlow'].notnull())]
buyqty = buyqty.loc[(buyqty['highlow'].notnull())]
# buyqty = buyqty.sort_values(by=['cpn'], ascending=True)
buyqtyind = buyqty[['cpn', 'key']].copy()
buyqtyind['sourcingInfo'] = 'Yes'
buyqtyind = buyqtyind.drop(columns=['key']).drop_duplicates()
comps_forecast_mpn_proc = comps_forecast_mpn_proc.merge(buyqtyind, 'left', 'cpn')
comps_forecast_mpn_proc['sourcingInfo'] = comps_forecast_mpn_proc['sourcingInfo'].fillna('No')

mech = pd.read_csv("./DURO-CATEGORY-REFERENCES.csv")
mech = mech[mech['Category']=="-- ELECTRICAL --"].drop(columns=['Category'])

elec_comps_forecast_mpn_proc = comps_forecast_mpn_proc[comps_forecast_mpn_proc['category'].isin(mech['Value'])]
mech_comps_forecast_mpn_proc = comps_forecast_mpn_proc[~comps_forecast_mpn_proc['category'].isin(mech['Value'])]

# In[CREATE AND NAME XLS MULTI-WORKSHEET FILE]
# CREATE AND NAME XLS MULTI-WORKSHEET FILE
csv = reports + 'Component.Forecast.Analysis' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
writer = pd.ExcelWriter(csv, engine='xlsxwriter')

# Write each dataframe to a different worksheet.
elec_comps_forecast_mpn_proc.to_excel(writer, sheet_name='Elec.Component.Analysis')
mech_comps_forecast_mpn_proc.to_excel(writer, sheet_name='Mech.Component.Analysis')
buyqty.to_excel(writer, sheet_name='DuroSourcingOptions', index=False)
bldphase = {'evt1':1,
             'evt2':2,
             'dvt':3,
             'pvt':4,
             'mp':5}
pldmnd = forecast_grouped_pvt.join(pd.DataFrame.from_dict(bldphase, orient='index'))
pldmnd = pldmnd.sort_values(by=[0]).reset_index(drop=False).drop(columns=[0])
pldmnd.to_excel(writer, sheet_name='DemandForecast', index=False)
plcpn = forecast_get[forecast_get['CPN Name'].str.contains('PLANNING')].copy()
plcpn = plcpn['CPN']
plbom = durobom_forecast_mpn_proc[['queryPN',
                   'cpn',
                   'name',
                   'category',
                   'quantity',
                   'level']].copy()
plbom = plbom[plbom['level']<2]
plbom = plbom[plbom['queryPN'].isin(plcpn)]
plbom.to_excel(writer, sheet_name='Planning.BOM.PNs', index=False)

# Close the Pandas Excel writer and output the Excel file.
writer.save()

logging.debug("FINISH REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN")

# WRITING RESULTS TO AIRTABLE
# atcreds = allcreds['oxide_airtable_compforecast']
# rec_ids = oxrest.atblget(atcreds)
# if rec_ids.size>0:
#     rec_ids = rec_ids['id'].tolist()
#     rec_ids = Table(atcreds['api_key'], atcreds['base_id'], atcreds['table_name']).batch_delete(rec_ids)

# cols = ['OnHand',
#         'OpenOrders',
#         'TotalQty']
# for i in cols:
#     comps_forecast_mpn_proc[i] = comps_forecast_mpn_proc[i].fillna(0)
# comps_forecast_mpn_proc = comps_forecast_mpn_proc.fillna('-')
# comps_forecast_mpn_proc_upload = comps_forecast_mpn_proc.to_dict(orient='records')

# comps_forecast_mpn_proc_create = oxrest.atblcreate(atcreds, comps_forecast_mpn_proc_upload)

# logging.debug("WROTE ASSEMBLIES AND GROUPING DATA BY COMPONENT PN TO AIRTABLE")
