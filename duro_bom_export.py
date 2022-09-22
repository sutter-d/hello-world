#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 16 11:54:00 2022

@author: danielsutter
"""

import logging
# import math
import time
import datetime as dt
import sys
# import base64
import io

# import json
import yaml
import pandas as pd
# from pyairtable import Table
import requests

import ds_utils as ds
# import openorders as oxopn
import cpn_mpn_export as oxmpn

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

requests.packages.urllib3.disable_warnings()
with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('./gitlogs/compforecast' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

if len(sys.argv) > 1:
    duropn = sys.argv[1]
    print("CLI Input Provided")
else:
    duropn = "999-0000014"
    print("No PN Input")

with open("./creds.yml", 'r') as stream:
    allcreds = yaml.safe_load(stream)
durocreds = allcreds['oxide_duro']
durocreds = durocreds['api_key']
print("Duro Keys from local")

durobom = ds.s2sbuildbom_all(duropn, durocreds)

cols = ['parent',
        'cpn',
        'level',
        'quantity',
        'name',
        'category',
        'procurement']

durobom = durobom[cols]


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

cols = ['Manufacturer P/N',
        'Manufacturer']

proc = proc_get[cols]
proc = proc.rename(columns={'Manufacturer P/N': 'proc_mpn',
                            'Manufacturer': 'manf'})

# In[]
"""
MERGING MPN AND PROC DATA INTO DF
"""

logging.info("START MERGING MPN AND PROC DATA INTO DF")

cpn = ds.cpn()
mpn = oxmpn.cpnmpn(cpn)
mpn['mpn'] = mpn['mpn'].str.lower()
proc['proc_mpn'] = proc['proc_mpn'].astype(str).str.lower()
mpn_proc = mpn.merge(proc, 'left', left_on='mpn', right_on='proc_mpn')
mpn_proc['mpn'] = mpn_proc['mpn'].fillna('-')
mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].fillna('-')
mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].astype(str)
mpn_proc['manf'] = mpn_proc['manf'].fillna('-')

mpn_proc['manf'] = mpn_proc['manf'].astype(str)

mpn_proc = mpn_proc.groupby(['cpn'], as_index=False).agg(lambda x: ', '.join(x))
# In[]
mpn_proc['mpn'] = mpn_proc['mpn'].str.upper()
mpn_proc['proc_mpn'] = mpn_proc['proc_mpn'].str.upper()

durobom = durobom.merge(mpn_proc, 'left', 'cpn')
durobom['level'] = durobom['level'].astype(int)
durobom['indent'] = "" 
for x in range(len(durobom['name'])):
    durobom.at[x, 'indent'] = durobom.at[x, 'name'].rjust(durobom.at[x, 'level']*2+len(durobom.at[x, 'name']))

reports = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Reports/'
desktop = '~/Desktop/'
data = './data/'
uploads = './uploads/'
csv = uploads + 'duro_bom_export.xlsx'

durobom.to_excel(csv, index=True)

SCOPES = ['https://www.googleapis.com/auth/drive']

creds = None

SERVICE_ACCOUNT_FILE = './oxops-gcp-project-service.json'

creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

logging.info("built creds object: " + str(creds))

service = build('drive', 'v3', credentials=creds)
fh = io.BytesIO()

# folder_id='1qdCL8sJMU1TBwbqnW7dT7h8Q3o3EbgRF' # OpsAuto > OldBoms
folder_id='1ZvsvOTCPuQTsDthVXvy0vgn4f0D9kNPN' # OpsAuto > HistUpdates

file_metadata = {'name': duropn + "_" + time.strftime("%Y-%m-%d-%H%M%S") + '.xlsx',
                 'parents': [folder_id]}
media = MediaFileUpload('./uploads/duro_bom_export.xlsx')
file = service.files().create(body=file_metadata,
                                    media_body=media,
                                    supportsAllDrives=True,
                                    fields='id').execute()
print('File ID: %s' % file.get('id'))