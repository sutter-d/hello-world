#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 11:37:15 2022

@author: danielsutter
"""

import logging
import math
import time
import yaml
import os
import inspect
# import datetime as dt
# import base64
# from requests import HTTPError
# import json

import pandas as pd
# from pyairtable import Api, Base, Table
import requests

import ds_utils as ds


def cpnmpn(oxcpns):
    """


    Parameters
    ----------
    oxcpns : DATAFRAME
        DATAFRAME OUTPUT FROM CPN().

    Returns
    -------
    cpnbom_mpn : DATAFRAME
        THIS FUNCTION TAKES A DATAFRAME WITH CPN AND SOURCES.MANUFACTURERS
        OUTPUT FROM DURO AND RETURNS A DURO CPN TO MPN MAPPING.

    """
    # In[UNPACK MPN FROM CPN DATA]
    # This unpacks the MPN data from the nested list column
    # 'sources.manufacturers' and ties it back to the CPN
    # The 1 CPN to many MPN relationship is preserved on the 3rd line
    cpnbom_mpn = oxcpns.set_index(['cpn'])
    cpnbom_mpn = ds.unpack(cpnbom_mpn['sources.manufacturers'])
    cpnbom_mpn = pd.concat([cpnbom_mpn.reset_index(drop=False),
                            ds.unpack(cpnbom_mpn['mpn'])],
                           axis = 1)
    cpnbom_mpn = cpnbom_mpn[['ind', 'key']].rename(columns = {'ind': 'cpn', 'key': 'mpn'}).drop_duplicates()
    return cpnbom_mpn

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/cpn_mpn_export_' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    logging.info("START cpn_mpn_export.py __main__ HERE")
    cpns = ds.cpn()
    cpn_mpns = cpnmpn(cpns)

    # Save results to your local desktop
    dsktp = '~/Desktop/'
    csv = dsktp + 'cpn_mpn_export' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')
    cpn_mpns.to_excel(writer, sheet_name='CPN_MPN_EXPORT', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    logging.info("FINISH cpn_mpn_export.py __main__ HERE")
