#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 13 15:13:31 2022

@author: danielsutter
"""

import os.path
import time
import io
import shutil
import logging
import requests
import yaml

import pandas as pd
import restapi as oxrest

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

requests.packages.urllib3.disable_warnings()
with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('./gitlogs/gdrive_post_s2s_' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

SCOPES = ['https://www.googleapis.com/auth/drive']

creds = None

SERVICE_ACCOUNT_FILE = './oxops-gcp-project-service.json'

creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

logging.info("built creds object: " + str(creds))

service = build('drive', 'v3', credentials=creds)
fh = io.BytesIO()

folder_id='1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON'

file_metadata = {'name': 'Component_Forecast_Analysis_' + time.strftime("%Y-%m-%d") + '.xlsx',
                 'parents': [folder_id]}
media = MediaFileUpload('./data/Component_Forecast_Analysis.xlsx')
file = service.files().create(body=file_metadata,
                                    media_body=media,
                                    supportsAllDrives=True,
                                    fields='id').execute()
print('File ID: %s' % file.get('id'))

