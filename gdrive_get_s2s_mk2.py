#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 17:01:03 2022

@author: danielsutter
"""

import os.path
import time
import io
import shutil
import json
import requests
import yaml
import logging
import sys

import pandas as pd
import restapi as oxrest

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

def main(ghsecret):
    logging.info("starting to build googleapi service object")
    
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    creds = None
    with open("./oxops-gcp-project-service1.json", 'w') as json_file:
        json.dump(ghsecret, json_file)
    
    SERVICE_ACCOUNT_FILE = "./oxops-gcp-project-service1.json"
    
    creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    logging.debug("built creds object: " + str(creds))
    
    # In[]
    service = build('drive', 'v3', credentials=creds)
    logging.debug("built service object: "+str(service))
    fh = io.BytesIO()
    
    file_id = ['1FCbWNjMVxbwumpXy84MMtTi4wu-I7qd_', #BM AND OXIDE INVENTORY FILE IN SHARED DOC FOLDER
               '12fgP8PwRlmmsySJVCt3RjEmo8ZsG_igG'] #OXIDE PRODUCTION FORECAST FILE
    logging.debug('making request for inv file')
    request = service.files().get_media(fileId=file_id[0])
    
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%" % int(status.progress() * 100))
    
    # The file has been downloaded into RAM, now save it in a file
    logging.debug("download complete, saving file from RAM to location")
    fh.seek(0)
    logging.debug(fh)
    with open('./data/ox_bm_inv_shared.xlsx', 'wb') as f:
        shutil.copyfileobj(fh, f)
    
    # In[]
    
    request = service.files().get_media(fileId=file_id[1])
    
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%" % int(status.progress() * 100))
    
    # The file has been downloaded into RAM, now save it in a file
    fh.seek(0)
    with open('./data/prod_forecast.xlsx', 'wb') as f:
        shutil.copyfileobj(fh, f)
    
    # In[]
    shared_drive_id = '0AKcpdSVwv34AUk9PVA'
    folder_id='1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON'
    qry = "'{}' in parents and (trashed=false)".format(folder_id)
    file = service.files().list(corpora = 'drive',
                                q=qry,
                                orderBy='createdTime desc',
                                driveId = shared_drive_id,
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                pageToken=None).execute()
    
    df = pd.DataFrame(file)
    df = oxrest.unpack(df['files'])
    df = df[df['name'].str.startswith('Component_Forecast')]
    df = df.loc[0,'id']
    
    request = service.files().get_media(fileId=df)
    
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%" % int(status.progress() * 100))
    
    # The file has been downloaded into RAM, now save it in a file
    fh.seek(0)
    with open('./data/old_comp_forecast.xlsx', 'wb') as f:
        shutil.copyfileobj(fh, f)

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
        logconfigs = opsconfigs['logging_configs']
        loglvl = logconfigs['level']
        logging.basicConfig(filename=('./gitlogs/gdrive_get_s2s_' + time.strftime("%Y-%m-%d") + '.log'),
                            level=loglvl,
                            format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
        print(sys.argv[1])
        # main(sys.argv[1])
