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

import restapi as oxrest


# requests.packages.urllib3.disable_warnings()
# with open("./config.yml", 'r') as stream:
#     opsconfigs = yaml.safe_load(stream)
# logconfigs = opsconfigs['logging_configs']
# loglvl = logconfigs['level']
# logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/cpnmpn' + time.strftime("%Y-%m-%d") + '.log'),
#                     level=loglvl,
#                     format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

def cpn():
    """

    Returns
    -------
    cpnbom : dataframe
        THIS FUNCTION PULLS ALL CPN DATA FROM DURO AND RETURNS A DF.

    """
    # Opening creds.yml and assigning Duro creds
    # dirname = os.path.dirname(__file__)
    # filename = os.path.join(dirname, 'creds.yml')
    # print(dirname)
    # print(filename)
    # with open(filename, 'r') as stream:
    with open("./creds.yml", 'r') as stream:
        allcreds = yaml.safe_load(stream)
    durocreds = allcreds['oxide_duro']
    api_key = durocreds['api_key']

    # In[START BUILDING GET QUERY]
    logging.info('START BUILDING GET QUERY')
    # creates headers for the GET query
    api_call_headers = {'x-api-key': api_key}

    # Duro components URL with CPN set to wildcat,
    # results perPage set to 100,
    # and page ready to iterate
    comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
    page = 1
    qry = comp_api_url + str(page)

    # GET REQUEST
    api_call_response = requests.get(qry, headers = api_call_headers, verify=False)

    # This returns the record count ['resultCount'] and sets it to a variable
    tot = api_call_response.json()['resultCount']

    # Send 1 page of results to log for debug
    logging.debug(api_call_response.json())

    # Page 1 of Results
    cpnbom = pd.DataFrame(api_call_response.json())
    cpnbom = oxrest.unpack(cpnbom['components'])

    # Using the record count and page size, we get the number of pages we need to query
    rnge = list(range(2,(math.ceil(tot/100)+1)))
    for x in rnge:
        logging.debug("Printing page {}".format(x))
        comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
        page = x
        logging.debug('Query: page {} results'.format(page))
        qry = comp_api_url + str(page)
        api_call_response = requests.get(qry, headers = api_call_headers, verify=False)
        temp = pd.DataFrame(api_call_response.json())
        temp = oxrest.unpack(temp['components'])
        cpnbom = cpnbom.append(temp)
    return cpnbom

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
    cpnbom_mpn = oxrest.unpack(cpnbom_mpn['sources.manufacturers'])
    cpnbom_mpn = pd.concat([cpnbom_mpn.reset_index(drop=False),
                            oxrest.unpack(cpnbom_mpn['mpn'])],
                           axis = 1)
    cpnbom_mpn = cpnbom_mpn[['ind', 'key']].rename(columns = {'ind': 'cpn', 'key': 'mpn'}).drop_duplicates()
    return cpnbom_mpn

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    # dirname = os.path.dirname(__file__)
    # filename = os.path.join(dirname, 'config.yml')
    # print(dirname)
    # print(filename)
    # with open(filename, 'r') as stream:
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/cpnmpn' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    logging.info("START cpnmpnexport.py __main__ HERE")
    cpns = cpn()
    cpn_mpns = cpnmpn(cpns)

    # Save results to your local desktop
    dsktp = '~/Desktop/'
    csv = dsktp + 'cpnbom' + time.strftime("%Y%m%d-%H%M%S") + '.xlsx'
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')
    cpn_mpns.to_excel(writer, sheet_name='CPN_BOM_EXPORT', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    logging.info("FINISH cpnmpnexport.py __main__ HERE")
