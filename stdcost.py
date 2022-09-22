#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 11:37:15 2022

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

import ds_utils as ds
# import openorders as oxopn
import cpn_mpn_export as cme

"""
# =============================================================================
# # ORDER OF THIS SCRIPT-BUILD BOM TOP DOWN
# # WE'LL USE A COUPLE HELPER FCNS TO START
# # - cpn() from restapi.py
# # - cpnmpn() from cpn_mpn_export.py
# # ^THESE TWO FCNS ARE GOING TO PULL ALL CPNS FROM DURO AND USE THAT TO RETURN A CPN-MPN MAPPING
# # NEXT STEPS:
# # PULL INV DATA FROM GDRIVE
# # CLEAN INV DATA FOR MERGES AND TRANSFORMS
# # MERGE WITH CPN-MPN LIST
# # CLEAN FOR NAs AND EMPTIES
# # GROUPBY CPN TO SUM PO TOTALS AND QTY ORDERED
# # STD COST = SUM PO TOTALS / SUM QTY ORDERED
# # WRITE RESUTLS TO GDRIVE - OPS AUTO > REPORTS
# =============================================================================
""" 

def main():
    """
    

    Returns
    -------
    mpn_proc : dataframe
        This function returns calls the main() function from cpnmpnexport to get a cpn mpn mapping from duro
        and merges that with std cost data from the procurement tracker and returns that dataframe

    """
    
    """
    CALLING MAIN() FROM cpnmpnexport.py
    """
    # CALL AND STORE THE CPN FUNCTION FROM FROM RESTAPI
    # THE CPN FUNCTION 'Provides the raw Duro output for an all CPN query for use in other scripts'
    cpns = ds.cpn()
    # THIS FUNCTION TAKES A DATAFRAME WITH CPN AND SOURCES.MANUFACTURERS
    # OUTPUT FROM DURO AND RETURNS A DURO CPN TO MPN MAPPING.
    cpnbom_mpn = cme.cpnmpn(cpns)
    
    # In[PULLING PROCUREMENT TRACKER FROM GDRIVE]
    """
    PULLING PROCUREMENT TRACKER FROM GDRIVE
    """
    
    logging.debug("START PULLING PROCUREMENT TRACKER FROM GDRIVE")
    
    # GRAB THE ON HAND INVENTORY FILE FROM GDRIVE
    proc_gdrive = '/Volumes/GoogleDrive/Shared drives/Oxide Benchmark Shared/Benchmark Procurement/On Hand Inventory/Oxide Inv Receipts and Inv Tracker at Benchmark (Rochester).xlsx'
    proc_get = pd.read_excel(proc_gdrive, sheet_name='Oxide Inventory Receipts',  header = 0)
    proc_get['Manufacturer P/N'] = proc_get['Manufacturer P/N'].astype(str).str.strip()
    # CLEAN THE FILE FOR LATER MERGES AND TRANSFORMS
    proc_get['Oxide Received Inventory @ Benchmark'] = proc_get['Oxide Received Inventory @ Benchmark'].fillna(0)
    proc_get['Emeryville & Other Oxide Inventory'] = proc_get['Emeryville & Other Oxide Inventory'].fillna(0)
    proc_get['Benchmark Owned Inventory'] = proc_get['Benchmark Owned Inventory'].fillna(0)
    proc_get['on_hand'] = proc_get.apply(lambda x: x['Oxide Received Inventory @ Benchmark'] + x['Benchmark Owned Inventory'] + x['Emeryville & Other Oxide Inventory'], axis=1)

    # TRIM THE DF TO ONLY ROWS WITH A UNIT PRICE
    proc_uc = proc_get[~proc_get['Unit Price'].isnull()].reset_index(drop=True)
    proc_zerocost = proc_uc[proc_uc['Qty Ordered']==0]
    # DROP ANY ROWS WHERE QTY ORDERED IS ZERO
    proc_uc = proc_uc[proc_uc['Qty Ordered']!=0]
    # ANYWHERE THE PO TOTAL IS BLANK, MULTIPLY UNIT PRICE BY QTY AND OVERWRITE
    proc_uc['Total PO Price (Incl. Tax)'].loc[proc_uc['Total PO Price (Incl. Tax)'].isnull()] = proc_uc['Unit Price'] * proc_uc['Qty Ordered']
    
    # GROUP BY MPN AND SUM
    proc_total = proc_uc.groupby(['Manufacturer P/N']).agg('sum').reset_index()
    # DROP SOME COLUMNS AND RENAME
    cols = ['Manufacturer P/N',
            # 'Oxide Received Inventory @ Benchmark',
            'on_hand',
            'Order Qty To Go (Calculated)',
            'Qty Ordered',
            'Total PO Price (Incl. Tax)']
    proc = proc_total[cols].copy()
    proc = proc.rename(columns={'Manufacturer P/N':'proc_mpn',
                                'Qty Ordered': 'total_qty',
                                # 'Oxide Received Inventory @ Benchmark': 'on_hand' ,
                                'Order Qty To Go (Calculated)': 'open_orders',
                                'Total PO Price (Incl. Tax)': 'po_total'})
    # COMPUTE STD COST AS AN AVERAGE OF THE SUMMED POS AND TOTAL QTY
    proc['std_cost'] = proc['po_total'] / proc['total_qty']
    
    # In[COMBINE CPN MPN AND PROC DATA INTO SINGLE DATAFRAME]
    """
    COMBINE CPN MPN AND PROC DATA INTO SINGLE DATAFRAME
    """
    
    logging.debug("START COMBINE CPN MPN AND PROC DATA INTO SINGLE DATAFRAME")
    # NOW THAT WE HAVE COMPUTED STD COST AND STORED TO THE PROC DF, LET'S MERGE
    # IT WITH THE CPN-MPN DF WE PULLED FROM DURO
    # FIRST WE NEED TO PREP AND CLEAN EACH DF FOR THE MERGE
    cpnbom_mpn['mpn'] = cpnbom_mpn['mpn'].str.lower()
    proc['proc_mpn'] = proc['proc_mpn'].astype(str).str.lower()
    mpn_proc = cpnbom_mpn.merge(proc, 'left', left_on='mpn', right_on ='proc_mpn')
    # NOW FILL BLANKS AND NAS
    mpn_proc['mpn'] = mpn_proc['mpn'].fillna('-')
    mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].fillna('-')
    mpn_proc = mpn_proc.fillna(0)
    # NEXT GROUP ENTRIES BY MPN AND SUM EACH NUMBER COLUMN
    # WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
    mpn_proc = mpn_proc.groupby(['cpn'], as_index=False).agg(lambda x : x.sum() if x.dtype=='float64' else ', '.join(x))    
    mpn_proc['mpn'] = mpn_proc['mpn'].str.upper()
    mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].str.upper()
    
    logging.debug("FINISH COMBINE CPN MPN AND PROC DATA INTO SINGLE DATAFRAME")
    # In[RETURN MPN PROC]
    return mpn_proc

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/stdcost' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    mpn_proc = main()
    # WRITE STD COST DF TO THE OPS AUTO > REPORTS FOLDER
    reports = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Reports/'
    csv = reports + 'Oxide_Std_Cost_' + time.strftime("%Y-%m-%d-%H%M%S") + '.xlsx'
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')
    
    # Write each dataframe to a different worksheet.
    mpn_proc.to_excel(
        writer, sheet_name='ox_std_cost')
   
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()