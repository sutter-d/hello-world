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
import sys

# import json
import yaml
import pandas as pd
# from pyairtable import Table
import requests
from pyfiglet import Figlet

import restapi as oxrest
# import openorders as oxopn

requests.packages.urllib3.disable_warnings()
with open("./config.yml", 'r') as co_stream:
    opsconfigs = yaml.safe_load(co_stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/cpn_qty_print2desktop_' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
fascii = Figlet(font='slant')

# Opening creds.yml and assigning Duro creds
with open("./creds.yml", 'r') as cr_stream:
    allcreds = yaml.safe_load(cr_stream)
durocreds = allcreds['oxide_duro']
api_key = durocreds['api_key']

# In[]
logging.debug('START BUILDING GET QUERY')
# creates headers for the GET query
if len(sys.argv) > 1:
    duropn = sys.argv[1]
    print("CLI Input = " + sys.argv[1])
else:
    print("NO CLI Input")
# duropn = '999-0000014'
if duropn[0:3] == '999':
    comp_api_url = 'https://public-api.duro.app/v1/products/'
else:
    comp_api_url = 'https://public-api.duro.app/v1/components/'
# api_key = oxcreds['api_key'] # 16152961423738m50/MAHXgvcvj4RMROq2g==

# creates headers for the GET query
api_call_headers = {'x-api-key': api_key}
api_call_params = {'cpn': duropn}


# GET REQUEST
if (requests.get(comp_api_url, headers=api_call_headers, params=api_call_params, verify=False).status_code == 200):
    api_call_response = requests.get(
        comp_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    print("Query Successfull")

    # Send 1 page of results to log for debug
    logging.debug(api_call_response.json())

else:
    print("Query Status 200")

# [UNPACKING FOR CHILD COMPONENTS]
# Ripped straight from restapi.py

# Page 1 of Results
duro = pd.DataFrame(api_call_response.json())
# cpnbom = oxrest.unpack(cpnbom['components'])

duro['parent'] = duropn
duro = duro.set_index('parent')
x = 0
duro
if duropn[0:3] == '999':
    durop = oxrest.unpack(duro['products'])
else:
    durop = oxrest.unpack(duro['components'])

durop['parent'] = durop['cpn']
durop['level'] = x

# [DURO API GET FOR LEVEL 1 COMPONENTS]
rent = durop.at[0, 'parent']
# In[]
# INSERTING IF ELSE HERE FOR MAKE/BUY DECISION
comp_api_url = 'https://public-api.duro.app/v1/components/'

if durop['procurement'].item() != 'Buy':
    duropc = oxrest.unpack(durop['children'])
    duropc = duropc.reset_index(drop=True)

    duropcc = pd.DataFrame()

    x = 1

    for i in range(len(duropc['component'])):
        print("Querying for child PN " + str(i) +
              " ID " + duropc.at[i, 'component'])
        api_call_headers = {'x-api-key': api_key}
        # api_call_params = {'': i}
        # QUERY THE API FOR DETAILS ASSOCIATED WITH CHILD COMPONENT ID PROVIDED IN PREVIOUS QUERY
        # api_call_response = requests.get(comp_api_url+str(i), headers = api_call_headers, verify=False)
        api_call_response = requests.get(
            comp_api_url+str(duropc.at[i, 'component']), headers=api_call_headers, verify=False)
        temp = pd.json_normalize(api_call_response.json())
        # temp = pd.DataFrame(api_call_response)
        # temp = oxrest.unpack(temp['products'])
        duropcc = duropcc.append(temp)
    duropcc['parent'] = rent
    duropcc['level'] = x
    duropcc = duropcc.reset_index(drop=True)
    duropc = pd.concat([duropc[['quantity']], duropcc],
                       axis=1).sort_values(by=['cpn'])
    durorent = pd.concat([durop, duropc], axis=0).reset_index(drop=True)
else:
    durorent = durop

durorent['pnladder'] = [durorent.at[i, 'parent'] + " " +
                        durorent.at[i, 'cpn'] for i in range(len(durorent['cpn']))]

cpnqty = durorent[['cpn', 'quantity']]

# Save results to your local desktop
dsktp = '~/Desktop/'
csv = dsktp + 'cpnqty.' + duropn + "." + \
    time.strftime("%Y%m%d-%H%M%S") + '.csv'
cpnqty.to_csv(csv, index=False)
print(fascii.renderText("ops cli "+str(duropn)))

logging.debug("FINISH WRITING TO DESKTOP")
