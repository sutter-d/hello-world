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

import pandas as pd
import ds_utils as ds

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# requests.packages.urllib3.disable_warnings()
# with open("./config.yml", 'r') as stream:
#     opsconfigs = yaml.safe_load(stream)
# logconfigs = opsconfigs['logging_configs']
# loglvl = logconfigs['level']
# logging.basicConfig(filename=('./gitlogs/gdrive_get_s2s_' + time.strftime("%Y-%m-%d") + '.log'),
#                     level=loglvl,
#                     format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

# In[get_file fcn]


def get_file(oxfile_id, oxfile_name):
    """
    This function pulls a file from GDrive and stores it to
    the local ./data folder

    Parameters
    ----------
    oxfile_id : TYPE String
        DESCRIPTION. GDrive file id for file to download
    oxfile_name : TYPE String
        DESCRIPTION. Text for file name for file file in ./data folder

    Returns
    -------
    None.

    """
    file_id = oxfile_id
    logging.debug("starting to build googleapi service object")

    SCOPES = ['https://www.googleapis.com/auth/drive']

    creds = None

    SERVICE_ACCOUNT_FILE = './oxops-gcp-project-service.json'

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    logging.debug("built creds object: " + str(creds))

    # In[BUILDING GOOGLE DRIVE SERVICE API OBJECT]
    # We'll assign our s2s creds and use this object to pull files from Gdrive
    # Added cache_discovery=False to silence a file_cache error the build function
    # was throwing
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    logging.debug("built service object: "+str(service))
    fh = io.BytesIO()

    # This is the file pull request
    logging.debug('making request for inv file')
    request = service.files().get_media(fileId=file_id)

    # Storing the file to memory and providing download progress
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        logging.debug("Download %d%%" % int(status.progress() * 100))

    # The file has been downloaded into RAM, now save it in a file
    logging.debug("download complete, saving %s from RAM to location" % file_id)
    fh.seek(0)
    logging.debug(fh)
    with open('./data/' + str(oxfile_name) + '.xlsx', 'wb') as f:
        shutil.copyfileobj(fh, f)
    logging.debug("download save to location complete")


# In[get_list fcn]
def get_list(oxshared_id, oxfolder_id):
    """
    This function pulls a list of filenames and file_ids from the
    specified GDrive folder

    Parameters
    ----------
    oxshared_id : TYPE String
        DESCRIPTION. This is the GDrive shared drive ID - different from
        the folder ID
    oxfolder_id : TYPE
        DESCRIPTION. This is the GDrive folder ID

    Returns
    -------
    df : TYPE DataFrame
        DESCRIPTION. List of all the files stored in the specified folder

    """
    logging.debug("starting to build googleapi service object")

    SCOPES = ['https://www.googleapis.com/auth/drive']

    creds = None

    SERVICE_ACCOUNT_FILE = './oxops-gcp-project-service.json'

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    logging.debug("built creds object: " + str(creds))

    # In[BUILDING GOOGLE DRIVE SERVICE API OBJECT]
    # We'll assign our s2s creds and use this object to pull files from Gdrive
    # Added cache_discovery=False to silence a file_cache error the build function
    # was throwing
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    logging.debug("built service object: "+str(service))

    logging.debug(
        "Setting DriveID and Folder ID for recent compforecast file pull")
    shared_drive_id = oxshared_id
    folder_id = oxfolder_id
    qry = "'{}' in parents and (trashed=false)".format(folder_id)
    file = service.files().list(corpora='drive',
                                q=qry,
                                orderBy='createdTime desc',
                                driveId=shared_drive_id,
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                pageToken=None).execute()

    logging.debug("Saving file list DF to var and cleaning")

    df = pd.DataFrame(file)
    df = ds.unpack(df['files'])
    return df

# In[Main Block]

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('./gitlogs/gdrive_get_s2s_' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    # Here we are pulling static GDrive files used in analysis
    # We pull each file and save it to the ./data folder for use
    data = [['1FCbWNjMVxbwumpXy84MMtTi4wu-I7qd_', 'ox_bm_inv_shared'],  # BM AND OXIDE INVENTORY FILE IN SHARED DOC FOLDER
            ['1UXyOtpZ9OEL3SmTq-Ak0pUUlVz1Q6mS2', 'prod_forecast'],  # OXIDE PRODUCTION FORECAST FILE
            ['1DQJneaNwyosIyFsnJzKhr3y0D4X2gLW_', 'ctb_contents'],  #LATEST CTB SUMMARY FILE FOR FIST PAGE OF XLSX
            ['1bEEtdW9rBK2KI6HRsVyhxnleZ3wkiJVQ', 'ox_eng_inv'],  # GRABBING LATEST ENG INV FROM Ops > OpsAuto > Attachments
            ['1UXyOtpZ9OEL3SmTq-Ak0pUUlVz1Q6mS2', 'mps'],   # GRABBING LATEST MPS FROM Ops > Forecast/Master Schedule
            ['1PYgmEmpzg5X54wnSUCl6mY_LZpmhZ4VK', 'procurement'],  # GRABBING LATEST PROCUREMENT DECISION FROM Ops > OpsAuto
            ['1MNrEGRl-cZnbBsOGHCHcgUs_jVPJCHnG', 'item_master'] # GRABBING LATEST ITEM MASTER
            ]

    file_id = pd.DataFrame(data=data, columns=['id', 'name'])
    print("pulling static GDrive files")
    logging.info("pulling static GDrive files")
    for x in range(len(file_id['id'])):
        logging.info(file_id.iloc[x,:])
        get_file(file_id.at[x, 'id'], file_id.at[x, 'name'])


    # Here we are pulling the meta data for the ops auto > reports folder
    # We need to find the latest version of the comp forecast file to
    # Copy over the notes and comments

    print("pulling dated GDrive files")
    logging.info("pulling dated GDrive files")
    drive_id = '0AKcpdSVwv34AUk9PVA' #oxide shared drive id - reqd for meta data pull
    flder_id = '1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON' #ops auto > reports folder id

    df = get_list(drive_id, flder_id)
    df = df[df['name'].str.startswith(
        'ClearToBuild_')].reset_index(drop=True)
    df = df.loc[0, 'id']
    logging.debug("File ID: " + df)

    get_file(df, 'old_ctb')

    # Here we are pulling the meta data for the ops auto > attachments folder
    # We need to find the latest version of the production inventory file to
    # Copy over the notes and comments

    flder_id = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco' #ops auto > attachments folder id

    df = get_list(drive_id, flder_id)
    df = df[df['name'].str.startswith(
        'Oxide IOOASL')].reset_index(drop=True)
    df = df.loc[0, 'id']
    logging.debug("File ID: " + df)

    get_file(df, 'ox_prod_inv')

    # Here we are pulling the meta data for the ops auto > attachments folder
    # We need to find the latest version of the MRP file to
    # Copy over the notes and comments

    flder_id = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco' #ops auto > attachments folder id

    df = get_list(drive_id, flder_id)
    df = df[df['name'].str.startswith(
        'mrp_export')].reset_index(drop=True)
    df = df.loc[0, 'id']
    logging.debug("File ID: " + df)

    get_file(df, 'mrp_export')