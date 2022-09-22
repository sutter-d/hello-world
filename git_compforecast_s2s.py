#!usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:59:01 2021

@author: danielsutter
"""

import logging
import time
import datetime as dt
import sys

import yaml
import pandas as pd
import requests
import argparse

import ds_utils as ds

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
# In[vars and dfs]
"""
BUILD BOM FUNCTION <= MOVED TO RESTAPI TO USE IN OTHER SCRIPTS
"""

bldphase = {'evt1': 1,
            'evt2': 2,
            'dvt': 3,
            'pvt': 4,
            'lot1': 5,
            'lot2': 6,
            'lot3': 7,
            'lot4': 8}
prod_lots = list(bldphase.keys())

# In[FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES]
"""
FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES
"""

def prodsched(oxforecast, oxbom):
    """

    Parameters
    ----------
    oxforecast : DATAFRAME
        PRODUCTION FORECAST MAINTAINED ON GOOGLE DRIVE.
    oxbom : DATAFRAME
        DURO BOM OUPUT FROM THE BUILDBOM FCN().

    Returns
    -------
    duroext : DATAFRAME
        THIS FUNCTION RETURNS A DATAFRAME WITH EXTENDED BOM QUANTITIES.

    """
    logging.info("START FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES")
    forecast = oxforecast
    duroext = oxbom
    for x in range(len(forecast['key'])):
        logging.debug(forecast.at[x, 'key'])
        duroext[forecast.at[x, 'key']] = [
            forecast.at[x, 'volume'] * i for i in duroext['ext_qty']]

    duroext['forecast_total'] = duroext[prod_lots].sum(axis=1)

    cols_to_move = ['query_pn',
                    'parent',
                    'cpn',
                    'name',
                    'category',
                    'level',
                    'procurement',
                    'quantity',
                    'ext_qty',
                    'forecast_total',
                    'lead_time',
                    'lt_units']
    
    cols_to_move[9:9] = prod_lots

    cols = cols_to_move + \
        [col for col in duroext.columns if col not in cols_to_move]
    duroext = duroext[cols]
    duroext['lead_time'] = duroext['lead_time'].fillna(0)
    duroext['lt_units'] = duroext['lt_units'].fillna("0")
    logging.info("FINISH FORECAST LOOP FOR DIFFERENT PRODUCTION PHASES")
    return duroext

# In[MAIN FUNCTION]

def main(api_key):
    """

    Returns
    -------
    comps_forecast_mpn_proc : DATAFRAME
        COMPONENT FORECAST WITH DURO, THE PRODUCTION FORECAST, AND THE
        PROCUREMENT TRACKER AS SOURCES OF INFORMATION
    srcs : DATAFRAME
        SOURCING INFORMATION FROM OCTOPART FOR EASY REFERENCE

    manf : DATAFRAME
        MANUFACTURER NAME INFORMATION FROM OCTOPART FOR EASY REFERENCE

    """
    st = dt.datetime.now()
    histup = './data/'
    durocreds = api_key

    # if len(sys.argv) > 1:
    #     durocreds = sys.argv[1]
    #     print("CLI Input Provided")
    # else:
    #     with open("./creds.yml", 'r') as stream:
    #         allcreds = yaml.safe_load(stream)
    #     durocreds = allcreds['oxide_duro']
    #     durocreds = durocreds['api_key']
    #     print("NO CLI Input")



    # In[FORECAST DATA PULLED FROM GDRIVE]
    """
    FORECAST DATA PULLED FROM GDRIVE
    """

    logging.info("START FORECAST DATA PULLED FROM GDRIVE")

    # forecast_gdrive = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/Production Forecast/Oxide Production Forecast.xlsx'
    forecast_gdrive = './data/prod_forecast.xlsx'
    forecast_get = pd.read_excel(
        forecast_gdrive, sheet_name='OrderList', header=0)
    forecast_get = forecast_get.loc[:, 'CPN':'Qty']
    forecast_pns = forecast_get['CPN'].drop_duplicates().reset_index(drop=True)
    forecast_grouped = forecast_get.groupby(
        ['CPN', 'Build Phase']).agg('sum').reset_index(drop=False)
    forecast_grouped_pvt = forecast_grouped.pivot(index='Build Phase',
                                                  columns='CPN',
                                                  values='Qty').fillna(0)

    logging.info("FINISH FORECAST DATA PULLED FROM GDRIVE")

    # In[USE FORECAST PNS TO PULL BOMS FROM DURO API]
    """
    USE FORECAST PNS TO PULL BOMS FROM DURO API
    """

    logging.info("START USE FORECAST PNS TO PULL BOMS FROM DURO API")

    durobom_forecast = []
    mpn = []
    for pns in forecast_pns:
        logging.debug(pns)
        duroid = pns
        forecast_grouped_pn = forecast_grouped_pvt[[
            duroid]].reset_index(drop=False)
        # forecast_grouped_pn = forecast_grouped_pn.drop(columns=['CPN']).reset_index(drop=True)
        forecast_grouped_pn.columns = ['key', 'volume']
        forecast = forecast_grouped_pn
        logging.debug(forecast)

        # ASSIGNING PN TO QUERY AND RUNNING FUNCTION TO BUILD DURO BOM
        durobom = ds.s2sbuildbom(duroid, durocreds)
        # SAVING DURO MPN INFO TO DF FOR LATER
        if len(mpn) == 0:
            mpn = durobom
        else:
            mpn = mpn.append(durobom)
        # RUNNING PRODUCTION SCHEDULE FUNCTION TO GET FULL BOM FORECAST
        if len(durobom_forecast) == 0:
            durobom_forecast = prodsched(forecast, durobom)
        else:
            durobom_forecast = durobom_forecast.append(
                prodsched(forecast, durobom))

    # mpn = durobom
    mpn = mpn.set_index(['cpn'])
    mpn = ds.unpack(mpn['sources.manufacturers'])
    mpn = pd.concat([mpn.reset_index(drop=False),
                    ds.unpack(mpn['mpn'])], axis=1)
    # mpn = mpn[['ind', 'key']].rename(
    mpn = mpn[['ind', 'key', 'name']].rename(
        columns={'ind': 'cpn', 'key': 'mpn', 'name': 'manf_name'}).drop_duplicates()

    manf_cpn = durobom_forecast[['cpn', 'sources.manufacturers']].set_index([
                                                                            'cpn'])

    logging.info("FINISH USE FORECAST PNS TO PULL BOMS FROM DURO API")

    # In[COMBINING PN QUERIES AND PUSHING TO AIRTABLE]
    """
    COMBINING PN QUERIES AND PUSHING TO AIRTABLE
    """

    logging.info("START COMBINING PN QUERIES AND PUSHING TO AIRTABLE")

    durobom_forecast = durobom_forecast.loc[:, 'query_pn':'lt_units']
    durobom_forecast = durobom_forecast.fillna('-')
    durobom_forecast = durobom_forecast.reset_index(drop=False)
    csv = histup + 'durobom_forecast.csv'
    durobom_forecast.to_csv(csv, index=False)
    hash1 = pd.util.hash_pandas_object(durobom_forecast).sum()
    logging.debug(str(hash1) + " - durobom_forecast hash")

    logging.info("FINISH COMBINING PN QUERIES AND PUSHING TO AIRTABLE")

    # In[PULLING PROCUREMENT TRACKER FROM GDRIVE]
    """
    PULLING PROCUREMENT TRACKER FROM GDRIVE
    """
    # CAN'T USE stdcost.py BECAUSE THAT FILE FILTERS OUT PROCUREMENT TRACKER RECORDS
    # WITHOUT A UNIT COST SO THEY WON'T IMPACT THE STANDARD COST WHEN AVERAGED
    logging.info("START PULLING PROCUREMENT TRACKER FROM GDRIVE")

    # proc_gdrive = '/Volumes/GoogleDrive/Shared drives/Oxide Benchmark Shared/Benchmark Procurement/On Hand Inventory/Oxide Inv Receipts and Inv Tracker at Benchmark (Rochester).xlsx'
    proc_gdrive = './data/ox_bm_inv_shared.xlsx'
    proc_get = pd.read_excel(proc_gdrive,
                             sheet_name='Oxide Inventory Receipts',
                             header=0)
    proc_get['Manufacturer P/N'] = proc_get['Manufacturer P/N'].astype(
        str).str.strip()

    proc_get['Oxide Received Inventory @ Benchmark'] = proc_get['Oxide Received Inventory @ Benchmark'].fillna(
        0)
    proc_get['Emeryville & Other Oxide Inventory'] = proc_get['Emeryville & Other Oxide Inventory'].fillna(
        0)
    proc_get['Benchmark Owned Inventory'] = proc_get['Benchmark Owned Inventory'].fillna(
        0)
    proc_get['on_hand'] = proc_get.apply(lambda x: x['Oxide Received Inventory @ Benchmark'] +
                                        x['Benchmark Owned Inventory'] + x['Emeryville & Other Oxide Inventory'], axis=1)

    proc_total = proc_get.groupby(
        ['Manufacturer P/N']).agg('sum').reset_index()
    cols = ['Manufacturer P/N',
            # 'Oxide Received Inventory @ Benchmark',
            'on_hand',
            'Order Qty To Go (Calculated)',
            'Qty Ordered']
    proc = proc_total[cols]
    proc = proc.rename(columns={'Manufacturer P/N': 'proc_mpn',
                                'Qty Ordered': 'total_qty',
                                # 'Oxide Received Inventory @ Benchmark': 'on_hand' ,
                                'Order Qty To Go (Calculated)': 'open_orders'})

    logging.info("FINISH PULLING PROCUREMENT TRACKER FROM GDRIVE")

    # In[UNPACKING QUOTE AND LT INFO FROM DURO BOM]
    """
    UNPACKING QUOTE AND LT INFO FROM DURO BOM
    """
    logging.info("START UNPACKING QUOTE AND LT INFO FROM DURO BOM")
    # UNPACK NESTED DICT SOURCES.MANUFACTURERS AND RESET INDEX
    manf = ds.unpack(
        manf_cpn['sources.manufacturers']).reset_index(drop=False)
    # lt = ds.unpack(manf['lead_time'])
    # UNPACK DISTRIBUTORS COL AND RESET INDEX
    dst = ds.unpack(manf['distributors']).reset_index(drop=False)
    # UNPACK QUOTES COL, STRIP OUT ALL 1 PIECE QUOTES AND CHANGE TYPE TO INT
    qts = ds.unpack(dst['quotes'])
    qts = qts[qts['minQuantity'] > 1]
    qts['minQuantity'] = qts['minQuantity'].astype(int)
    # RENAME IND COL TO CPN, RESET AND KEEP INDEX, AND RENAME MANFIND
    manf = manf.reset_index(drop=False).rename(columns={'index': 'manf_ind',
                                                        'ind': 'cpn',
                                                        'name': 'manf_name',
                                                        'description': 'manf_desc'})

    # DST IND COL IS KEY BACK TO MANF INDEX COL
    # RENAME IND COL TO MANFIND, RESET AND KEEP INDEX AND RENAME DSTIND
    dst = dst.reset_index(drop=False).rename(columns={'index': 'dst_ind',
                                                      'ind': 'manf_ind',
                                                      'name': 'dst_name',
                                                      'description': 'dst_desc'})
    pkg = ds.unpack(dst['package'])
    dst = pd.concat([dst, pkg],
                    axis=1)
    # QTS IND COL IS KEY BACK TO DST INDEX COL
    # RENAME IND COL TO DSTIND, RESET INDEX
    qts = qts.reset_index(drop=False).rename(columns={'ind': 'dst_ind',
                                                      'minQuantity':'min_qty',
                                                      'unitPrice':'unit_price'})
    srcs = manf.merge(dst, 'left', 'manf_ind')
    srcs = srcs.merge(qts, 'left', 'dst_ind')
    # DROP NULL ROWS MISSING DST AND QTS INFO
    srcs = srcs[srcs['min_qty'].notnull()].reset_index(drop=True)
    lt = ds.unpack(srcs['leadTime_y'])
    srcs = pd.concat([srcs, lt],
                     axis=1)

    cols_to_move = ['cpn',
                    'manf_name',
                    'manf_desc',
                    'dst_name',
                    'dst_desc',
                    'type',
                    'min_qty',
                    'unit_price',
                    'units',
                    'value']

    srcs = srcs[cols_to_move]
    srcs = srcs.drop_duplicates()

    logging.info("FINISH UNPACKING QUOTE AND LT INFO FROM DURO BOM")

    # In[MERGING MPN AND PROC DATA INTO DF]
    """
    MERGING MPN AND PROC DATA INTO DF
    """

    logging.info("START MERGING MPN AND PROC DATA INTO DF")

    # MERGING MPN KEY AND PROCUREMENT DATA INTO ONE DF
    mpn['mpn'] = mpn['mpn'].str.lower()
    mpn = mpn.drop_duplicates()
    proc['proc_mpn'] = proc['proc_mpn'].astype(str).str.lower()
    mpn_proc = mpn.merge(proc, 'left', left_on='mpn', right_on='proc_mpn')
    mpn_proc['mpn'] = mpn_proc['mpn'].fillna('-')
    mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].fillna('-')
    mpn_proc = mpn_proc.fillna(0)
    # GROUP ENTRIES BY MPN AND SUM REQUIRED QUANTITIES
    # WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
    mpn_proc = mpn_proc.groupby(['cpn'], as_index=False).agg(
        lambda x: x.sum() if x.dtype == 'float64' else ', '.join(x))
    mpn_proc['mpn'] = mpn_proc['mpn'].str.upper()
    mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].str.upper()

    csv = histup + 'mpn_proc' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
    mpn_proc.to_excel(csv, index=False)

    logging.info("FINISH MERGING MPN AND PROC DATA INTO DF")

    # In[MERGE CPN, MPN, PROC AND MERGE TO BOM]
    """
    MERGE CPN, MPN, PROC AND MERGE TO BOM
    """

    logging.info("START MERGE CPN, MPN, PROC AND MERGE TO BOM")
    # DOESNT WORK WITHOUT SOME CPN TO MPN DECODER RING
    durobom_forecast_mpn_proc = durobom_forecast.merge(mpn_proc,
                                                       'left',
                                                       'cpn')
    csv = histup + 'durobom_forecast_mpn_proc' + \
        time.strftime("%Y-%m-%d-%H%M%S") + '.xlsx'
    durobom_forecast_mpn_proc.to_excel(csv, index=False)
    hash1 = pd.util.hash_pandas_object(durobom_forecast_mpn_proc).sum()
    logging.debug(hash1)
    logging.debug(str(hash1) + " = durobom_forecast_mpn_proc hash")

    logging.info("FINISH MERGE CPN, MPN, PROC AND MERGE TO BOM")

    # In[REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN]
    """
    REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN
    """

    logging.info(
        "START REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN")


    # ASSEMBLIES AND BOMS ARE REMOVED FROM DF
    # FILTERING OUT ASSEMBLIES - MOST WILL END UP BEING MAKE PARTS BUT WE WANT
    # TO TRACK CABLE ASSEMBLIES AND ANY BUY PARTS
    # ALL SUB ASSEMBLIES ARE 991 AND GREATER SO TURN CPN INTO AN INT AND FILTER 
    # OUT ANY BUY PARTS OVER 9910000000 AND DROP THE REST
    comps_forecast = durobom_forecast
    comps_forecast['cpn_int'] = comps_forecast['cpn'].str.replace('-','').astype(int)
    comps_forecast = comps_forecast.loc[(comps_forecast['procurement']=='Buy') |
                                        (~comps_forecast['category'].str.contains("Miscellaneous"))]
    comps_forecast = comps_forecast.loc[(comps_forecast['procurement']=='Buy') |
                                        (comps_forecast['cpn_int']<9910000000)]
    # ALSO WANT TO DROP PCBAS THAT AREN'T PURCHASED
    comps_forecast = comps_forecast.loc[(comps_forecast['procurement']=='Buy') |
                                        (~comps_forecast['cpn'].str.startswith('913-'))]
    # AND EBOM AND MBOMS
    comps_forecast = comps_forecast.loc[(comps_forecast['procurement']=='Buy') |
                                        (~comps_forecast['category'].str.contains("BOM"))]
    # AND ALL BOARDS THAT AREN'T PURCHASED
    comps_forecast = comps_forecast.loc[(comps_forecast['procurement']=='Buy') |
                                        (~comps_forecast['category'].str.contains("Board"))]
    # AND SPARES CPNS THAT AREN'T PURCHASED
    comps_forecast = comps_forecast.loc[(comps_forecast['procurement']=='Buy') |
                                        (~comps_forecast['name'].str.contains("SPARE"))]


    # GROUP BY CPN AND SUM REQUIRED QUANTITIES
    # WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
    comps_forecast = comps_forecast.groupby(['cpn'], as_index=False).agg(
        lambda x: x.sum() if x.dtype == 'float64' else ', '.join(x))
    comps_forecast = comps_forecast.drop(columns=['query_pn',
                                                  'name',
                                                  'category',
                                                  'lt_units'])

    # BETTER COMPARISON BECAUSE MPN AND PROC IS GROUPED
    # AND SUMMED TO A SINGLE CPN ENTRY BEFORE MERGE
    comps_forecast_mpn_proc = comps_forecast.merge(mpn_proc, 'left', 'cpn')
    comps_forecast_mpn_proc = comps_forecast_mpn_proc.merge(
        durobom_forecast[['cpn', 'name', 'category']].drop_duplicates())

    def colcomp(forecast, onhand):
        if onhand > forecast:
            return 'Yes'
        else:
            return 'No'

    # CREATING INDICATOR COLUMNS
    # EACH COLUMN = YES IF THERE IS ENOUGH MATERIAL ORDERED, = NO IF NOT
    # TO BE A YES FOR DVT, ORDER QTY > SUM(EVT, DVT) AND SO ON FOR PVT AND MP
    comps_forecast_mpn_proc['evt_ok'] = comps_forecast_mpn_proc.apply(
        lambda x: colcomp(x['evt1']+x['evt2'], x['total_qty']), axis=1)
    comps_forecast_mpn_proc['dvt_ok'] = comps_forecast_mpn_proc.apply(
        lambda x: colcomp(x['evt1']+x['evt2']+x['dvt'], x['total_qty']), axis=1)
    comps_forecast_mpn_proc['pvt_ok'] = comps_forecast_mpn_proc.apply(
        lambda x: colcomp(x['evt1']+x['evt2']+x['dvt']+x['pvt'], x['total_qty']), axis=1)
    comps_forecast_mpn_proc['lot1_ok'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(
        x['evt1']+x['evt2']+x['dvt']+x['pvt']+x['lot1'], x['total_qty']), axis=1)
    comps_forecast_mpn_proc['lot2_ok'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(
        x['evt1']+x['evt2']+x['dvt']+x['pvt']+x['lot1']+x['lot2'], x['total_qty']), axis=1)
    comps_forecast_mpn_proc['lot3_ok'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(
        x['evt1']+x['evt2']+x['dvt']+x['pvt']+x['lot1']+x['lot2']+x['lot3'], x['total_qty']), axis=1)
    comps_forecast_mpn_proc['lot4_ok'] = comps_forecast_mpn_proc.apply(lambda x: colcomp(
        x['evt1']+x['evt2']+x['dvt']+x['pvt']+x['lot1']+x['lot2']+x['lot3']+x['lot4'], x['total_qty']), axis=1)
    comps_forecast_mpn_proc = comps_forecast_mpn_proc.sort_values(by=['evt_ok',
                                                                      'dvt_ok',
                                                                      'pvt_ok',
                                                                      'lot1_ok',
                                                                      'lot2_ok',
                                                                      'lot3_ok',
                                                                      'lot4_ok'],
                                                                  ascending=True).reset_index(drop=True)

    # FINAL FORMATTING FOR LEGIBILITY
    cols_to_move = ['parent',
                    'cpn',
                    'name',
                    'category']

    cols = cols_to_move + \
        [col for col in comps_forecast_mpn_proc.columns if col not in cols_to_move]
    comps_forecast_mpn_proc = comps_forecast_mpn_proc[cols]

    # TO COMPUTE THE BUY QTY WE US APPLY AND SUBTRACT THE TOTAL PURCHASED FROM
    # THE FORECASTED TOTAL WHERE FORECAST IS GREATER THAN WHAT WE'VE PURCHASED
    comps_forecast_mpn_proc['buy_qty'] = comps_forecast_mpn_proc.apply(
        lambda x: x['forecast_total'] - x['total_qty'] if x['forecast_total'] > x['total_qty'] else 0, axis=1)

    csv = histup + 'comp_forecast_mpn_proc' + \
        time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
    comps_forecast_mpn_proc.to_excel(csv, index=False)

    # In[THIS SECTION BUILDS THE BUY QTY DF]
    # WE WANT TO PULL SOURCING DATA FROM DURO FOR QTYS ABOVE AND BELOW OUR 
    # IDENTIFIED BUY QTY
    buyqty = comps_forecast_mpn_proc[['cpn', 'name', 'buy_qty']]
    buyqty = buyqty.merge(srcs, 'left', 'cpn')
    # THE ORDER RATIO HELPS US IDENTIFY WHICH MOQS ARE NEAREST OUR
    # BUY QTY
    buyqty['ord_ratio'] = buyqty.apply(
        lambda x: x['buy_qty']/x['min_qty'], axis=1)
    buyqty = buyqty[buyqty['buy_qty'] != 0]
    # ONE PRICE WE WANT IS FOR ORDERS MORE THAN OUR NEED
    # MOQ > NEED IS SMALLEST ORDER RATIO GREATER THAN 1
    buyqty_high = buyqty[buyqty['ord_ratio'] < 1]
    buyqty_high = buyqty_high[['cpn', 'ord_ratio']].groupby(
        ['cpn'], as_index=False).agg(max)
    # THE OTHER PRICE WE WANT IS FOR ORDERS LESS THAN OUR NEED
    # MOQ < NEED IS LARGEST ORDER RATIO LESS THAN 1
    buyqty_low = buyqty[buyqty['ord_ratio'] > 1]
    buyqty_low = buyqty_low[['cpn', 'ord_ratio']].groupby(
        ['cpn'], as_index=False).agg(min)
    buys = [buyqty, buyqty_high, buyqty_low]
    # CREATE A KEY TO JOIN ON THE buyqty DF AND IDENTIFY WHICH MOQS WE WANT TO KEEP
    for df in buys:
        df['key'] = ''
        df['key'] = df.apply(lambda x: x['cpn'] + str(x['ord_ratio']), axis=1)

    # ADDING AN INDICATOR COLUMN TO SORT ON
    buyqty_high['high_low'] = 'MOQ>NEED'
    buyqty_low['high_low'] = 'NEED>MOQ'

    # MERGING IN KEYS FOR LOW AND HIGH
    buyqty_high = buyqty_high[['key', 'high_low']]
    buyqty_low = buyqty_low[['key', 'high_low']]
    buyqty_low = buyqty_low.append(buyqty_high)
    buyqty = buyqty.merge(buyqty_low, 'left', 'key')

    # REDUCING THE DF TO KEEP ONLY THE MOQS WE WANT
    buyqty = buyqty.loc[(buyqty['high_low'].notnull())]
    # buyqty = buyqty.sort_values(by=['cpn'], ascending=True)

    # CLEANING UP THE DF
    buyqtyind = buyqty[['cpn', 'key']].copy()
    buyqtyind['sourcing_info'] = 'Yes'
    buyqtyind = buyqtyind.drop(columns=['key']).drop_duplicates()
    comps_forecast_mpn_proc = comps_forecast_mpn_proc.merge(
        buyqtyind, 'left', 'cpn')
    comps_forecast_mpn_proc['sourcing_info'] = comps_forecast_mpn_proc['sourcing_info'].fillna(
        'No')

    hash1 = pd.util.hash_pandas_object(comps_forecast_mpn_proc).sum()
    logging.debug("comps_forecast_mpn_proc hash value = "+str(hash1))
    logging.debug(hash1)

    # In[HERE IS WHERE WE SPLIT THE COMPONENTS BY ELECTRICAL AND MECHANICAL]
    # WE START BY REFERENCING A DURO CATEGORY FILE SAVED LOCALLY
    # THIS FILE WILL NEED UPDATING IF DURO MAKES UPDATES ON OUR BEHALF
    elec = pd.read_csv("./DURO-CATEGORY-REFERENCES.csv")
    elec = elec[elec['Category'] ==
                "-- ELECTRICAL --"].drop(columns=['Category'])

    # THE NEXT 3 LINES ADD CATEGORIES TO THE ELECTRICAL DF THAT AREN'T
    # IN DURO - THIS GETS FANS AND HEATSINKS TO THE ELECTRICAL DF FOR TRACKING
    assys = ['Fan', 'Cable Assembly', 'Heatsink']
    assys = pd.DataFrame(assys, columns=['Value'])
    elec = elec.append(assys)

    elec_comps_forecast_mpn_proc = comps_forecast_mpn_proc[comps_forecast_mpn_proc['category'].isin(
        elec['Value'])]
    mech_comps_forecast_mpn_proc = comps_forecast_mpn_proc[~comps_forecast_mpn_proc['category'].isin(
        elec['Value'])]

    # NEW SECTION FOR COPYING NOTES OVER USING CTB TAB
    # PULL CTB NOTES, THEN CF NOTES, APPEND CF TO CTB AND DROP DUPS KEEP FIRST
    old_notes = './data/old_comp_forecast.xlsx'
    old_ctb_get = pd.read_excel(old_notes,
                             sheet_name='ClearToBuild',
                             header=0)
    old_cf_get = pd.read_excel(old_notes,
                             sheet_name='Elec.Component.Analysis',
                             header=0)
    old_notes = old_ctb_get[['cpn', 'Notes']].append(old_cf_get[['cpn', 'Notes']])
    old_notes = old_notes.drop_duplicates(subset=['cpn'], keep='first')#(keep='first')
    elec_comps_forecast_mpn_proc = elec_comps_forecast_mpn_proc.merge(old_notes, 'left', 'cpn')

    # THIS IS THE NEW CLEAR TO BUILD DF
    ctb = elec_comps_forecast_mpn_proc.loc[(elec_comps_forecast_mpn_proc['lot4_ok']=='No')]
    # In[CREATE AND NAME XLS MULTI-WORKSHEET FILE]
    # CREATE AND NAME XLS MULTI-WORKSHEET FILE
    uploads = './uploads/'
    csv = uploads + 'Component_Forecast_Analysis.xlsx'
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')

    # Write each dataframe to a different worksheet.
    ctb.to_excel(writer, sheet_name='ClearToBuild')
    elec_comps_forecast_mpn_proc.to_excel(
        writer, sheet_name='Elec.Component.Analysis')
    mech_comps_forecast_mpn_proc.to_excel(
        writer, sheet_name='Mech.Component.Analysis')
    buyqty.to_excel(writer, sheet_name='DuroSourcingOptions', index=False)

    # THIS SECTION CREATES THE DEMAND FORECAST AND PLANNING PN DFS
    # THIS HELPS RETAIN WHAT DEMAND FORECAST AND PLANNING PNS WERE USED TO
    # GENERATE THE REPORT
    pldmnd = forecast_grouped_pvt.join(
        pd.DataFrame.from_dict(bldphase, orient='index'))
    pldmnd = pldmnd.sort_values(by=[0]).reset_index(
        drop=False).drop(columns=[0])
    pldmnd.to_excel(writer, sheet_name='DemandForecast', index=False)
    plcpn = forecast_get[forecast_get['CPN Name'].str.contains(
        'PLANNING')].copy()
    plcpn = plcpn['CPN']
    plbom = durobom_forecast_mpn_proc[['query_pn',
                                       'cpn',
                                       'name',
                                       'category',
                                       'quantity',
                                       'level']].copy()
    plbom = plbom[plbom['level'] < 2]
    plbom = plbom[plbom['query_pn'].isin(plcpn)]
    plbom.to_excel(writer, sheet_name='Planning.BOM.PNs', index=False)
    # opnord = oxopn.main()
    # opnord.to_excel(writer, sheet_name='Open.Orders', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    logging.info(
        "FINISH REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN")
    nd = dt.datetime.now()
    logging.info(nd-st)
    return comps_forecast_mpn_proc, srcs, manf

# In[__name__ == __main__]
if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('./gitlogs/compforecast' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    logging.info("START compforecast.py __main__ HERE")

    desc_string = 'This utility builds the Ops Component Forecast Analsyis.' \
    ' It combines Duro BOM, GDrive Inventory, and GDrive Forecast info to ' \
    'calculate required component quantities for forecasted demand. It ' \
    'produces the Clear To Build report stored at GDrive > OpsAuto > Reports'
    epilog_string = ''

    parser = argparse.ArgumentParser(description=desc_string,
                                     epilog=epilog_string,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # run_type = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-a', '--api_key',
                        action='store',
                        dest='API',
                        help='api_key required to query Duro')

    args = parser.parse_args()


    # BEFORE BEGINNING BLOCK 1 > DELETE FILES IN ECO UPDATE GDRIVE FOLDER
    if args.API:
        print('run parameter is api = %s' % str(args.API))
        cfmp, sources, manf_names = main(args.API)
        
    else:
        print("NO CLI Input")
        with open("./creds.yml", 'r') as crstream:
            allcreds = yaml.safe_load(crstream)
        cfmp, sources, manf_names = main(allcreds['oxide_duro']['api_key'])
        
    logging.info("FINISH compforecast.py __main__ HERE")
