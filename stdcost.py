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

import restapi as oxrest
# import openorders as oxopn
import cpn_mpn_export as cme


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
    cpns = oxrest.cpn()
    cpnbom_mpn = cme.cpnmpn(cpns)
    
    # In[PULLING PROCUREMENT TRACKER FROM GDRIVE]
    """
    PULLING PROCUREMENT TRACKER FROM GDRIVE
    """
    
    logging.debug("START PULLING PROCUREMENT TRACKER FROM GDRIVE")
    
    proc_gdrive = '/Volumes/GoogleDrive/Shared drives/Oxide Benchmark Shared/Benchmark Procurement/On Hand Inventory/Oxide Inv Receipts and Inv Tracker at Benchmark (Rochester).xlsx'
    proc_get = pd.read_excel(proc_gdrive, sheet_name='Oxide Inventory Receipts',  header = 0)
    proc_get['Manufacturer P/N'] = proc_get['Manufacturer P/N'].astype(str).str.strip()

    proc_get['Oxide Received Inventory @ Benchmark'] = proc_get['Oxide Received Inventory @ Benchmark'].fillna(0)
    proc_get['Emeryville & Other Oxide Inventory'] = proc_get['Emeryville & Other Oxide Inventory'].fillna(0)
    proc_get['Benchmark Owned Inventory'] = proc_get['Benchmark Owned Inventory'].fillna(0)
    proc_get['on_hand'] = proc_get.apply(lambda x: x['Oxide Received Inventory @ Benchmark'] + x['Benchmark Owned Inventory'] + x['Emeryville & Other Oxide Inventory'], axis=1)

    proc_uc = proc_get[~proc_get['Unit Price'].isnull()].reset_index(drop=True)
    proc_uc['Total PO Price (Incl. Tax)'].loc[proc_uc['Total PO Price (Incl. Tax)'].isnull()] = proc_uc['Unit Price'] * proc_uc['Qty Ordered']
    
    proc_total = proc_uc.groupby(['Manufacturer P/N']).agg('sum').reset_index()
    cols = ['Manufacturer P/N',
            # 'Oxide Received Inventory @ Benchmark',
            'on_hand',
            'Order Qty To Go (Calculated)',
            'Qty Ordered',
            'Unit Price',
            'Total PO Price (Incl. Tax)']
    proc = proc_total[cols].copy()
    proc = proc.rename(columns={'Manufacturer P/N':'proc_mpn',
                                'Qty Ordered': 'total_qty',
                                # 'Oxide Received Inventory @ Benchmark': 'on_hand' ,
                                'Order Qty To Go (Calculated)': 'open_orders',
                                'Total PO Price (Incl. Tax)': 'po_total',
                                'Unit Price': 'unit_price'})
    
    proc['std_cost'] = proc['po_total'] / proc['total_qty']
    
    # In[COMBINE CPN MPN AND PROC DATA INTO SINGLE DATAFRAME]
    """
    COMBINE CPN MPN AND PROC DATA INTO SINGLE DATAFRAME
    """
    
    logging.debug("START COMBINE CPN MPN AND PROC DATA INTO SINGLE DATAFRAME")
    
    cpnbom_mpn['mpn'] = cpnbom_mpn['mpn'].str.lower()
    proc['proc_mpn'] = proc['proc_mpn'].astype(str).str.lower()
    mpn_proc = cpnbom_mpn.merge(proc, 'left', left_on='mpn', right_on ='proc_mpn')
    mpn_proc['mpn'] = mpn_proc['mpn'].fillna('-')
    mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].fillna('-')
    mpn_proc = mpn_proc.fillna(0)
    # GROUP ENTRIES BY MPN AND SUM REQUIRED QUANTITIES WHILE FLATTENING STRINGS TO A SINGLE ENTRY PER CPN
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
    # logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/stdcost' + time.strftime("%Y-%m-%d") + '.log'),
    #                     level=loglvl,
    #                     format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    file_handler = logging.FileHandler('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/stdcost' + time.strftime("%Y-%m-%d") + '.log')
    # file_handler = logging.FileHandler('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/stdcost.log')
    file_handler.setFormatter(formatter)
    # file_handler.setLevel(loglvl)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # logger.addHandler(stream_handler)
    mpn_proc = main()