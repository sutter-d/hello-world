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

from src import duro_utils as duro
# import ds_utils as ds

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

bldphase = {'evt': 1,
            'dvt': 3,
            'pvt': 4,
            'lot1': 5,
            'lot2': 6,
            'lot3': 7,
            'lot4': 8,
            'lot5': 9,
            'lot6': 10,
            'lot7': 11,
            'lot8': 12}
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
    oxbom : DATAFRAME
        THIS FUNCTION RETURNS A DATAFRAME WITH EXTENDED BOM QUANTITIES.

    """
    logging.info("START FORECAST LOOP FOR QUERY PN %s" % oxbom.at[0, 'cpn'])
    # oxforecast = forecast.iloc[0:1,:]
    # oxbom = durobom
    for x in range(len(oxforecast['key'])):
        logging.debug(oxforecast.at[x, 'key'])
        oxbom[oxforecast.at[x, 'key']] = [
            oxforecast.at[x, 'volume'] * i for i in oxbom['ext_qty']]

    oxbom['evt'] = 0
    oxbom['evt'] = oxbom['evt'].astype(float)
    oxbom['dvt'] = 0
    oxbom['dvt'] = oxbom['dvt'].astype(float)

    oxbom['forecast_total'] = oxbom[prod_lots].sum(axis=1)

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
        [col for col in oxbom.columns if col not in cols_to_move]
    oxbom = oxbom[cols]
    oxbom['lead_time'] = oxbom['lead_time'].fillna(0)
    oxbom['lt_units'] = oxbom['lt_units'].fillna("0")
    logging.info("FINISH FORECAST LOOP FOR QUERY PN %s" % oxbom.at[0, 'cpn'])
    return oxbom

# In[MAIN FUNCTION]


def main(api_key):
    """

    Returns
    -------
    comps_forecast_mpn_inv : DATAFRAME
        COMPONENT FORECAST WITH DURO, THE PRODUCTION FORECAST, AND THE
        PROCUREMENT TRACKER AS SOURCES OF INFORMATION
    srcs : DATAFRAME
        SOURCING INFORMATION FROM OCTOPART FOR EASY REFERENCE

    manf : DATAFRAME
        MANUFACTURER NAME INFORMATION FROM OCTOPART FOR EASY REFERENCE

    """
    api_key = allcreds['oxide_duro']['api_key']  # FOR DEBUG
    st = dt.datetime.now()
    histup = './data/'

    # In[FORECAST DATA PULLED FROM GDRIVE]
    """
    FORECAST DATA PULLED FROM GDRIVE
    """

    logging.info("START FORECAST DATA PULLED FROM GDRIVE")

    mps_path = './data/mps.xlsx'
    mps = pd.read_excel(mps_path, sheet_name='Master Schedule', header=4)
    mps = duro.clean_mps(mps)
    mps = mps[mps['lot_num'] != 'planning fence']
    mps['qty'] = mps['qty'].astype(int)
    mps = mps.rename(columns={'lot_num': 'Build Phase'})
    mps_pns = mps['cpn'].drop_duplicates().reset_index(drop=True)
    for value in prod_lots:
        logging.debug(str(value) + ' prod lot')
        data = ['999-0000001', 0, 0, 0, value, 1]
        logging.debug(str(data) + ' data for production lot')
        mps = mps.append(pd.DataFrame([data], columns=mps.columns))

    mps_tbl = mps.groupby(['Build Phase', 'cpn']).agg(sum)
    mps_tbl = mps_tbl.pivot_table(index=["Build Phase"],
                                  columns=["cpn"],
                                  values="qty")
    mps_tbl = mps_tbl.fillna(0)
    mps_tbl = mps_tbl.drop(['999-0000001'], axis=1)

    logging.info("FINISH FORECAST DATA PULLED FROM GDRIVE")

    # In[USE FORECAST PNS TO PULL BOMS FROM DURO API]
    """
    USE FORECAST PNS TO PULL BOMS FROM DURO API
    """

    logging.info("START USE FORECAST PNS TO PULL BOMS FROM DURO API")

    durobom_forecast = []
    mpn = []
    for pns in mps_pns:
        logging.debug(pns)
        # duroid = pns
        mps_grouped_pn = mps_tbl[[pns]].reset_index(drop=False)
        # mps_grouped_pn = mps_grouped_pn.drop(columns=['CPN']).reset_index(drop=True)
        mps_grouped_pn.columns = ['key', 'volume']
        logging.debug(mps_grouped_pn)

        # ASSIGNING PN TO QUERY AND RUNNING FUNCTION TO BUILD DURO BOM
        durobom = duro.get_flatbom_all_s2s(pns, api_key)
        # SAVING DURO MPN INFO TO DF FOR LATER
        if len(mpn) == 0:
            mpn = durobom
        else:
            mpn = mpn.append(durobom)
        # RUNNING PRODUCTION SCHEDULE FUNCTION TO GET FULL BOM FORECAST
        if len(durobom_forecast) == 0:
            durobom_forecast = prodsched(mps_grouped_pn, durobom)
            logging.debug(durobom_forecast)
        else:
            durobom_forecast = durobom_forecast.append(
                prodsched(mps_grouped_pn, durobom))
            logging.debug(durobom_forecast)
    cols_to_move = ['query_pn',
                    'parent',
                    'cpn',
                    'name',
                    'pnladder']

    cols = cols_to_move + \
        [col for col in durobom_forecast.columns if col not in cols_to_move]
    durobom_forecast = durobom_forecast[cols]
    # mpn = durobom
    mpn = mpn.set_index(['cpn'])
    mpn = duro.unpack(mpn['sources.manufacturers'])
    mpn = pd.concat([mpn.reset_index(drop=False),
                    duro.unpack(mpn['mpn'])], axis=1)
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
    cols_to_move = ['query_pn',
                    'parent',
                    'cpn',
                    'name',
                    'category',
                    'level',
                    'pnladder']
    cols = cols_to_move + \
        [col for col in durobom_forecast.columns if col not in cols_to_move]
    durobom_forecast = durobom_forecast[cols]
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

    inv_gdrive = './data/ox_prod_inv.xlsx'
    open_po_get = pd.read_excel(inv_gdrive,
                                sheet_name='Open POs',
                                header=1)

    on_hand_get = pd.read_excel(inv_gdrive,
                                sheet_name='Inventory on hand',
                                header=1)

    open_po = open_po_get[['Part Number', 'Order', 'Seq', 'Quantity']].copy()
    open_po['open_orders'] = open_po.apply(
        lambda x: x['Seq'] + x['Quantity'], axis=1)
    open_po = open_po.groupby(by=['Part Number']).agg(sum)
    open_po = open_po.drop(columns=['Seq', 'Quantity'])
    # open_po = open_po.rename(columns={'Part Number':'cpn'})

    on_hand = on_hand_get[['Part', 'Description', 'Quantity']].copy()
    on_hand = on_hand.dropna()
    on_hand['Part'] = on_hand['Part'].map(lambda x: x.replace('OXC', ''))
    on_hand['cpn'] = on_hand['Part'].map(lambda x: x[:11])
    # on_hand = on_hand.groupby(by=['cpn']).agg(sum)
    # TEMPORARY UNTIL ENGINEERING INVENTORY IS MOVED TO PRODUCTION
    # REMOVE HERE DOWN
    on_hand = on_hand.groupby(by=['cpn'], as_index=False).agg(sum)
    on_hand = on_hand.rename(columns={'Quantity': 'on_hand'})

    eng_gdrive = './data/ox_eng_inv.xlsx'
    eng_oh_get = pd.read_excel(eng_gdrive,
                               sheet_name='Summary',
                               header=0)
    eng_oh = eng_oh_get.copy()
    eng_oh['cpn'] = eng_oh['cpn'].fillna('-')
    eng_oh = eng_oh[eng_oh['cpn'] != '-']
    eng_oh = eng_oh.groupby(by=['cpn'], as_index=False).agg(sum)
    eng_oh = eng_oh.rename(columns={'total_qty': 'on_hand'})

    on_hand = pd.concat((on_hand, eng_oh)).reset_index(drop=True)
    on_hand = on_hand.groupby(by=['cpn']).agg(sum)
    # REMOVE HERE UP

    inv = pd.concat((open_po, on_hand), axis=1)

    inv = inv.reset_index(drop=False)
    inv = inv.fillna(0)
    inv = inv.rename(columns={'Quantity': 'on_hand', 'index': 'cpn'})
    inv['total_qty'] = inv.apply(
        lambda x: x['open_orders'] + x['on_hand'], axis=1)

    logging.info("FINISH PULLING PROCUREMENT TRACKER FROM GDRIVE")

# =============================================================================
#     # In[UNPACKING QUOTE AND LT INFO FROM DURO BOM]
#     """
#     UNPACKING QUOTE AND LT INFO FROM DURO BOM
#     """
#     logging.info("START UNPACKING QUOTE AND LT INFO FROM DURO BOM")
#     # UNPACK NESTED DICT SOURCES.MANUFACTURERS AND RESET INDEX
#     manf = duro.unpack(
#         manf_cpn['sources.manufacturers']).reset_index(drop=False)
#     # lt = ds.unpack(manf['lead_time'])
#     # UNPACK DISTRIBUTORS COL AND RESET INDEX
#     dst = duro.unpack(manf['distributors']).reset_index(drop=False)
#     # UNPACK QUOTES COL, STRIP OUT ALL 1 PIECE QUOTES AND CHANGE TYPE TO INT
#     qts = duro.unpack(dst['quotes'])
#     qts = qts[qts['minQuantity'] > 1]
#     qts['minQuantity'] = qts['minQuantity'].astype(int)
#     # RENAME IND COL TO CPN, RESET AND KEEP INDEX, AND RENAME MANFIND
#     manf = manf.reset_index(drop=False).rename(columns={'index': 'manf_ind',
#                                                         'ind': 'cpn',
#                                                         'name': 'manf_name',
#                                                         'description': 'manf_desc'})
#
#     # DST IND COL IS KEY BACK TO MANF INDEX COL
#     # RENAME IND COL TO MANFIND, RESET AND KEEP INDEX AND RENAME DSTIND
#     dst = dst.reset_index(drop=False).rename(columns={'index': 'dst_ind',
#                                                       'ind': 'manf_ind',
#                                                       'name': 'dst_name',
#                                                       'description': 'dst_desc'})
#     pkg = duro.unpack(dst['package'])
#     dst = pd.concat([dst, pkg],
#                     axis=1)
#     # QTS IND COL IS KEY BACK TO DST INDEX COL
#     # RENAME IND COL TO DSTIND, RESET INDEX
#     qts = qts.reset_index(drop=False).rename(columns={'ind': 'dst_ind',
#                                                       'minQuantity': 'min_qty',
#                                                       'unitPrice': 'unit_price'})
#     srcs = manf.merge(dst, 'left', 'manf_ind')
#     srcs = srcs.merge(qts, 'left', 'dst_ind')
#     # DROP NULL ROWS MISSING DST AND QTS INFO
#     srcs = srcs[srcs['min_qty'].notnull()].reset_index(drop=True)
#     lt = duro.unpack(srcs['leadTime_y'])
#     srcs = pd.concat([srcs, lt],
#                       axis=1)
#
#     cols_to_move = ['cpn',
#                     'manf_name',
#                     'manf_desc',
#                     'dst_name',
#                     'dst_desc',
#                     'type',
#                     'min_qty',
#                     'unit_price',
#                     'units',
#                     'value']
#
#     srcs = srcs[cols_to_move]
#     srcs = srcs.drop_duplicates()
#
#     logging.info("FINISH UNPACKING QUOTE AND LT INFO FROM DURO BOM")
#
# =============================================================================
    # In[MERGING MPN AND PROC DATA INTO DF]
    """
    MERGING MPN AND PROC DATA INTO DF
    """

    logging.info("START MERGING MPN AND PROC DATA INTO DF")

    # MERGING MPN KEY AND PROCUREMENT DATA INTO ONE DF
    mpn['mpn'] = mpn['mpn'].str.lower()
    mpn = mpn.drop_duplicates()
    # inv['inv_mpn'] = inv['inv_mpn'].astype(str).str.lower()
    mpn_inv = inv.merge(mpn, 'left', 'cpn')
    mpn_inv['mpn'] = mpn_inv['mpn'].fillna('-')
    mpn_inv['manf_name'] = mpn_inv['manf_name'].fillna('-')
    mpn_inv['on_hand'] = mpn_inv['on_hand'].astype(float)
    mpn_inv = mpn_inv.fillna(0)
    # GROUP ENTRIES BY MPN AND SUM REQUIRED QUANTITIES
    # WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
    mpn_inv = mpn_inv.groupby(['cpn'], as_index=False).agg(
        lambda x: x.sum() if x.dtype == 'float64' else ', '.join(x))
    mpn_inv['mpn'] = mpn_inv['mpn'].str.upper()
    # mpn_inv['inv_mpn'] = mpn_inv['inv_mpn'].str.upper()

    csv = histup + 'mpn_inv' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
    mpn_inv.to_excel(csv, index=False)

    logging.info("FINISH MERGING MPN AND inv DATA INTO DF")

    # In[MERGE CPN, MPN, PROC AND MERGE TO BOM]
    """
    MERGE CPN, MPN, PROC AND MERGE TO BOM
    """

    logging.info("START MERGE CPN, MPN, PROC AND MERGE TO BOM")
    # DOESNT WORK WITHOUT SOME CPN TO MPN DECODER RING
    durobom_forecast_mpn_inv = durobom_forecast.merge(mpn_inv,
                                                      'left',
                                                      'cpn')
    csv = histup + 'durobom_forecast_mpn_inv' + \
        time.strftime("%Y-%m-%d-%H%M%S") + '.xlsx'
    durobom_forecast_mpn_inv.to_excel(csv, index=False)
    hash1 = pd.util.hash_pandas_object(durobom_forecast_mpn_inv).sum()
    logging.debug(hash1)
    logging.debug(str(hash1) + " = durobom_forecast_mpn_inv hash")

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
    comps_forecast['cpn_int'] = comps_forecast['cpn'].str.replace(
        '-', '').astype(int)

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
    comps_forecast_mpn_inv = comps_forecast.merge(mpn_inv, 'left', 'cpn')
    comps_forecast_mpn_inv = comps_forecast_mpn_inv.merge(
        durobom_forecast[['cpn', 'name', 'category']].drop_duplicates())

    comps_forecast_mpn_inv['mpn'] = comps_forecast_mpn_inv['mpn'].fillna('')
    # comps_forecast_mpn_inv['mpn'] = comps_forecast_mpn_inv.apply(
    #     lambda x: str(x['cpn']) + str(x['mpn']), axis=1)

    # def colcomp(forecast, onhand):
    #     if onhand > forecast:
    #         return 'Yes'
    #     else:
    #         return 'No'
    
    def yel_comp(forecast, onhand):
        """
        This is the fcn used to label an item as yellow in the CTB report.

        Parameters
        ----------
        forecast : TYPE Int
            DESCRIPTION. Forecast Total for specified lot
        onhand : TYPE Int
            DESCRIPTION. Total Inventory physically received and on hand

        Returns
        -------
        str
            DESCRIPTION. Yellow for parts where onhand < forecast

        """
        if onhand < forecast:
            return '2-Yellow'
        else:
            return '3-Ok'
        
    def red_comp(forecast, open_onhand):
        """
        This is the fcn used to label an item as red in the CTB report

        Parameters
        ----------
        forecast : TYPE Int
            DESCRIPTION. Forecast total for specified Lot
        open_onhand : TYPE Int
            DESCRIPTION. Total Inventory - both open orders and on hand

        Returns
        -------
        str
            DESCRIPTION. Red for parts where open+onhand < forecast

        """
        if open_onhand < forecast:
            return '1-Red'

    # CREATING INDICATOR COLUMNS
    # EACH COLUMN = YES IF THERE IS ENOUGH MATERIAL ORDERED, = NO IF NOT
    # TO BE A YES FOR DVT, ORDER QTY > SUM(EVT, DVT) AND SO ON FOR PVT AND MP
    comps_forecast_mpn_inv['evt_ok'] = comps_forecast_mpn_inv.apply(
        lambda x: red_comp(x['evt'], x['total_qty']), axis=1)
    comps_forecast_mpn_inv['dvt_ok'] = comps_forecast_mpn_inv.apply(
        lambda x: red_comp(x['evt']+x['dvt'], x['total_qty']), axis=1)
    comps_forecast_mpn_inv['pvt_ok'] = comps_forecast_mpn_inv.apply(
        lambda x: red_comp(x['evt']+x['dvt']+x['pvt'], x['total_qty']), axis=1)
    comps_forecast_mpn_inv['lot1_ok'] = comps_forecast_mpn_inv.apply(lambda x: red_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1'], x['total_qty']), axis=1)
    comps_forecast_mpn_inv['lot2_ok'] = comps_forecast_mpn_inv.apply(lambda x: red_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1']+x['lot2'], x['total_qty']), axis=1)
    comps_forecast_mpn_inv['lot3_ok'] = comps_forecast_mpn_inv.apply(lambda x: red_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1']+x['lot2']+x['lot3'], x['total_qty']), axis=1)
    comps_forecast_mpn_inv['lot4_ok'] = comps_forecast_mpn_inv.apply(lambda x: red_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1']+x['lot2']+x['lot3']+x['lot4'], x['total_qty']), axis=1)
    comps_forecast_mpn_inv = comps_forecast_mpn_inv.sort_values(by=['evt_ok',
                                                                    'dvt_ok',
                                                                    'pvt_ok',
                                                                    'lot1_ok',
                                                                    'lot2_ok',
                                                                    'lot3_ok',
                                                                    'lot4_ok'],
                                                                ascending=True).reset_index(drop=True)

    comps_forecast_mpn_inv['evt_ok'] = comps_forecast_mpn_inv.apply(
        lambda x: yel_comp(x['evt'], x['on_hand']), axis=1)
    comps_forecast_mpn_inv['dvt_ok'] = comps_forecast_mpn_inv.apply(
        lambda x: yel_comp(x['evt']+x['dvt'], x['on_hand']), axis=1)
    comps_forecast_mpn_inv['pvt_ok'] = comps_forecast_mpn_inv.apply(
        lambda x: yel_comp(x['evt']+x['dvt']+x['pvt'], x['on_hand']), axis=1)
    comps_forecast_mpn_inv['lot1_ok'] = comps_forecast_mpn_inv.apply(lambda x: yel_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1'], x['on_hand']), axis=1)
    comps_forecast_mpn_inv['lot2_ok'] = comps_forecast_mpn_inv.apply(lambda x: yel_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1']+x['lot2'], x['on_hand']), axis=1)
    comps_forecast_mpn_inv['lot3_ok'] = comps_forecast_mpn_inv.apply(lambda x: yel_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1']+x['lot2']+x['lot3'], x['on_hand']), axis=1)
    comps_forecast_mpn_inv['lot4_ok'] = comps_forecast_mpn_inv.apply(lambda x: yel_comp(
        x['evt']+x['dvt']+x['pvt']+x['lot1']+x['lot2']+x['lot3']+x['lot4'], x['on_hand']), axis=1)
    comps_forecast_mpn_inv = comps_forecast_mpn_inv.sort_values(by=['evt_ok',
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
        [col for col in comps_forecast_mpn_inv.columns if col not in cols_to_move]
    comps_forecast_mpn_inv = comps_forecast_mpn_inv[cols]

    # TO COMPUTE THE BUY QTY WE US APPLY AND SUBTRACT THE TOTAL PURCHASED FROM
    # THE FORECASTED TOTAL WHERE FORECAST IS GREATER THAN WHAT WE'VE PURCHASED
    comps_forecast_mpn_inv['buy_qty'] = comps_forecast_mpn_inv.apply(
        lambda x: x['forecast_total'] - x['total_qty'] if x['forecast_total'] > x['total_qty'] else 0, axis=1)

    csv = histup + 'comp_forecast_mpn_inv' + \
        time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
    comps_forecast_mpn_inv.to_excel(csv, index=False)

# =============================================================================
#     # In[THIS SECTION BUILDS THE BUY QTY DF]
#     # WE WANT TO PULL SOURCING DATA FROM DURO FOR QTYS ABOVE AND BELOW OUR
#     # IDENTIFIED BUY QTY
#     buyqty = comps_forecast_mpn_inv[['cpn', 'name', 'buy_qty']]
#     buyqty = buyqty.merge(srcs, 'left', 'cpn')
#     # THE ORDER RATIO HELPS US IDENTIFY WHICH MOQS ARE NEAREST OUR
#     # BUY QTY
#     buyqty['ord_ratio'] = buyqty.apply(
#         lambda x: x['buy_qty']/x['min_qty'], axis=1)
#     buyqty = buyqty[buyqty['buy_qty'] != 0]
#     # ONE PRICE WE WANT IS FOR ORDERS MORE THAN OUR NEED
#     # MOQ > NEED IS SMALLEST ORDER RATIO GREATER THAN 1
#     buyqty_high = buyqty[buyqty['ord_ratio'] < 1]
#     buyqty_high = buyqty_high[['cpn', 'ord_ratio']].groupby(
#         ['cpn'], as_index=False).agg(max)
#     # THE OTHER PRICE WE WANT IS FOR ORDERS LESS THAN OUR NEED
#     # MOQ < NEED IS LARGEST ORDER RATIO LESS THAN 1
#     buyqty_low = buyqty[buyqty['ord_ratio'] > 1]
#     buyqty_low = buyqty_low[['cpn', 'ord_ratio']].groupby(
#         ['cpn'], as_index=False).agg(min)
#     buys = [buyqty, buyqty_high, buyqty_low]
#     # CREATE A KEY TO JOIN ON THE buyqty DF AND IDENTIFY WHICH MOQS WE WANT TO KEEP
#     for df in buys:
#         df['key'] = ''
#         df['key'] = df.apply(lambda x: x['cpn'] + str(x['ord_ratio']), axis=1)
#
#     # ADDING AN INDICATOR COLUMN TO SORT ON
#     buyqty_high['high_low'] = 'MOQ>NEED'
#     buyqty_low['high_low'] = 'NEED>MOQ'
#
#     # MERGING IN KEYS FOR LOW AND HIGH
#     buyqty_high = buyqty_high[['key', 'high_low']]
#     buyqty_low = buyqty_low[['key', 'high_low']]
#     buyqty_low = buyqty_low.append(buyqty_high)
#     buyqty = buyqty.merge(buyqty_low, 'left', 'key')
#
#     # REDUCING THE DF TO KEEP ONLY THE MOQS WE WANT
#     buyqty = buyqty.loc[(buyqty['high_low'].notnull())]
#     # buyqty = buyqty.sort_values(by=['cpn'], ascending=True)
#
#     # CLEANING UP THE DF
#     buyqtyind = buyqty[['cpn', 'key']].copy()
#     buyqtyind['sourcing_info'] = 'Yes'
#     buyqtyind = buyqtyind.drop(columns=['key']).drop_duplicates()
#     comps_forecast_mpn_inv = comps_forecast_mpn_inv.merge(
#         buyqtyind, 'left', 'cpn')
#     comps_forecast_mpn_inv['sourcing_info'] = comps_forecast_mpn_inv['sourcing_info'].fillna(
#         'No')
#
#     hash1 = pd.util.hash_pandas_object(comps_forecast_mpn_inv).sum()
#     logging.debug("comps_forecast_mpn_inv hash value = "+str(hash1))
#     logging.debug(hash1)
# =============================================================================

    # In[HERE IS WHERE WE SPLIT THE COMPONENTS BY ELECTRICAL AND MECHANICAL]
    # WE START BY REFERENCING A DURO CATEGORY FILE SAVED LOCALLY
    # THIS FILE WILL NEED UPDATING IF DURO MAKES UPDATES ON OUR BEHALF
    elec = pd.read_csv("./DURO-CATEGORY-REFERENCES.csv")
    elec = elec[elec['Category'] ==
                "-- ELECTRICAL --"].drop(columns=['Category']).reset_index(drop=True)

    # PULLING IN PROCUREMENT DECISION FILE AND DROPPING EXISTING PROCURMENT COL
    proc = pd.read_excel("./data/procurement.xlsx",
                         sheet_name='Full Rack Flattened',
                         header=0)
    proc = proc[['CPN', 'Procurement']]
    proc = proc.rename(columns={'CPN': 'cpn', 'Procurement': 'procurement'})
    comps_forecast_mpn_inv = comps_forecast_mpn_inv.drop(
        ['procurement'], axis=1)
    comps_forecast_mpn_inv = comps_forecast_mpn_inv.merge(proc, 'left', 'cpn')

    comps_forecast_mpn_inv['type'] = comps_forecast_mpn_inv.apply(
        lambda x: '--electrical--' if elec['Value'].str.contains(x['category']).any()else 'mechanical', axis=1)

    cols_to_move = ['parent',
                    'cpn',
                    'name',
                    'type',
                    'category',
                    'procurement']

    cols = cols_to_move + \
        [col for col in comps_forecast_mpn_inv.columns if col not in cols_to_move]
    comps_forecast_mpn_inv = comps_forecast_mpn_inv[cols]

    # NEW SECTION FOR COPYING NOTES OVER USING CTB TAB
    # PULL CTB NOTES, THEN CF NOTES, APPEND CF TO CTB AND DROP DUPS KEEP FIRST
    old_notes = './data/old_ctb.xlsx'
    old_ctb_get = pd.read_excel(old_notes,
                                sheet_name='Clear.To.Build',
                                header=0)
    old_cf_get = pd.read_excel(old_notes,
                               sheet_name='Full.Comp.Forecast',
                               header=0)
    old_notes = old_ctb_get[['cpn', 'Notes']].append(
        old_cf_get[['cpn', 'Notes']])
    old_notes = old_notes.drop_duplicates(
        subset=['cpn'], keep='first')  # (keep='first')
    comps_forecast_mpn_inv = comps_forecast_mpn_inv.merge(
        old_notes, 'left', 'cpn')

    # THIS IS THE NEW CLEAR TO BUILD DF
    ctb = comps_forecast_mpn_inv.loc[(
        comps_forecast_mpn_inv['lot4_ok'] == 'No')]
    # In[CREATE AND NAME XLS MULTI-WORKSHEET FILE]
    # CREATE AND NAME XLS MULTI-WORKSHEET FILE
    uploads = './uploads/'
    csv = uploads + 'ClearToBuild_Analysis.xlsx'
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')

    ctb_cont = './data/ctb_contents.xlsx'
    ctb_get_cont = pd.read_excel(ctb_cont,
                                 sheet_name='Sheet1',
                                 header=0)

    # Write each dataframe to a different worksheet beginning with the CTB Contents
    ctb_get_cont.to_excel(writer, sheet_name='CTB.Contents', index=False)

    # Then the CTB analysis
    ctb.to_excel(writer, sheet_name='Clear.To.Build', index=False)

    # And the full component forecast
    comps_forecast_mpn_inv.to_excel(
        writer, sheet_name='Full.Comp.Forecast', index=False)

    # and the MP
    mps[mps['year'] != 0].to_excel(
        writer, sheet_name='Demand.Signal', index=False)

    # THIS SECTION CREATES THE PLANNING PN DFS
    # THIS HELPS RETAIN WHAT PLANNING PNS WERE USED TO
    # GENERATE THE REPORT
    plcpn = durobom_forecast[durobom_forecast['name'].str.contains(
        'PLANNING')].copy()
    plcpn = plcpn['cpn']
    plbom = durobom_forecast_mpn_inv[['query_pn',
                                      'cpn',
                                      'name',
                                      'category',
                                      'quantity',
                                      'level']].copy()
    plbom = plbom[plbom['level'] < 2]
    plbom = plbom[plbom['query_pn'].isin(plcpn)]
    plbom.to_excel(writer, sheet_name='Planning.BOMs', index=False)

    # THIS IS THE FLATTENED BOM TAB
    cols = ['index',
            'query_pn',
            'parent',
            'cpn',
            'name',
            'category',
            'level',
            'pnladder',
            'procurement',
            'quantity',
            'ext_qty']
    durobom = durobom_forecast[cols]
    durobom.to_excel(writer, sheet_name='Flattened.BOMs', index=False)

    # AND THE FLATTENED BOM x MPS TAB
    durobom_forecast.to_excel(
        writer, sheet_name='Flattened.BOMs.and.Forecast', index=False)

    # THE MAKE/BUY DECISION TAB
    proc.to_excel(writer, sheet_name='CPN.Make.Buy.Decision', index=False)

    # AND OUR INVENTORY POSITION
    inv.to_excel(writer, sheet_name='Current.Inventory', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    logging.info(
        "FINISH REMOVING ASSEMBLIES AND GROUPING DATA BY COMPONENT PN")
    nd = dt.datetime.now()
    logging.info(nd-st)
    return comps_forecast_mpn_inv


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
        cfmp = main(args.API)

    else:
        print("NO CLI Input")
        with open("./creds.yml", 'r') as crstream:
            allcreds = yaml.safe_load(crstream)
        cfmp = main(allcreds['oxide_duro']['api_key'])

    logging.info("FINISH compforecast.py __main__ HERE")
