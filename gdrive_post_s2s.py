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
import argparse
import ds_utils as ds

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

def main(oxfolder='1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON', isodate = True):
    """
    This functions will post files to a GDrive folder. The default is
    the OpsAuto > Reports folder but you can specify other destinations

    Parameters
    ----------
    oxfolder : TYPE, optional
        DESCRIPTION. The upload destination GDrive folder id.
        The default is '1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON' (OpsAuto > Reports).
    isodate : TYPE, optional
        DESCRIPTION. Boolean input. If True, files will be uploaded with the 
        isodate (YYYYMMDD) concatenated to the end of spreadsheets. If False,
        files will be uploaded with only the original filename.
        The default is True.

    Returns
    -------
    None.

    """
    SCOPES = ['https://www.googleapis.com/auth/drive']

    creds = None
    
    SERVICE_ACCOUNT_FILE = './oxops-gcp-project-service.json'
    
    creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    logging.debug("built creds object: " + str(creds))
    
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    # fh = io.BytesIO()
    logging.info("UPLOADING ALL FILES FROM ./uploads/")
    
    #UPLOADING ALL DOCS FROM THE ./uploads/ FOLDER TO GDRIVE OPSAUTO > REPORTS FOLDER
    # if oxfolder:
    #     folder_id = oxfolder
    #     logging.debug("Folder Id: {folder_id} entered as input")
    # else:
    #     folder_id='1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON'
    #     logging.debug("Default Folder Id: {folder_id} entered as input")
    folder_id = oxfolder
    logging.info("Folder Id: %s entered as input for destination folder" % folder_id)

    
    directory = './uploads/'
    
    for filename in os.listdir(directory):
        if filename.endswith(".py")==False:
            logging.debug(os.path.join(directory, filename))
            if isodate is True:
                logging.debug(filename.replace('.xlsx', ''))
                file_metadata = {'name': filename.replace('.xlsx', '') + time.strftime("%Y-%m-%d") + '.xlsx',
                                 'parents': [folder_id]}
            else:
                file_metadata = {'name': filename, 
                                 'parents': [folder_id]}
            media = MediaFileUpload(os.path.join(directory, filename))
            file = service.files().create(body=file_metadata,
                                                media_body=media,
                                                supportsAllDrives=True,
                                                fields='id').execute()
            print('File ID: %s' % file.get('id'))
    
    logging.info("UPLOADING ALL LOGS FROM ./gitlogs/")
    #UPLOADING ALL LOGS FROM THE GITLOGS FOLDER TO GDRIVE OPSAUTO > LOGS FOLDER
    folder_id='1BOLxZBfpBt0zPuM1FUt06D60AVTZnxLH'
    logging.info("Folder Id: %s used for gitlogs" % folder_id)
    directory = './gitlogs/'
    for filename in os.listdir(directory):
        if filename.endswith(".py")==False:
            logging.debug(os.path.join(directory, filename))
            logging.debug(filename.replace('.xlsx', ''))
            file_metadata = {'name': filename,
                              'parents': [folder_id]}
            media = MediaFileUpload(os.path.join(directory, filename))
            file = service.files().create(body=file_metadata,
                                                media_body=media,
                                                supportsAllDrives=True,
                                                fields='id').execute()
            print('File ID: %s' % file.get('id'))


if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('./gitlogs/gdrive_post_s2s_' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    desc_string = 'This functions will post files to a GDrive folder. The' \
        ' default is the OpsAuto > Reports folder but you can specify' \
        ' other destinations'
    epilog_string = "Folder default '1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON' " \
        "(OpsAuto > Reports) and will default to adding ISO Dates to xlsx files."

    parser = argparse.ArgumentParser(description=desc_string,
                                     epilog=epilog_string,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # run_type = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-f', '--folder_id',
                        action='store',
                        dest='FLDR',
                        default = 'OpsAuto>Reports',
                        help='destination GDrive folder_id')

    parser.add_argument('-i', '--isodate',
                        action='store',
                        dest='ISO',
                        default = 'ISO=True',
                        help='True = add isodate to filename, False = keep filename')


    args = parser.parse_args()
    if args.FLDR == 'OpsAuto>Reports' and args.ISO == 'ISO=True':
        main()
    if args.FLDR != 'OpsAuto>Reports' and args.ISO == 'ISO=True':
        main(oxfolder=args.FLDR)
    if args.FLDR == 'OpsAuto>Reports' and args.ISO != 'ISO=True':
        main(isodate=args.ISO)
    if args.FLDR != 'OpsAuto>Reports' and args.ISO != 'ISO=True':
        main(oxfolder=args.FLDR, isodate=args.ISO)
