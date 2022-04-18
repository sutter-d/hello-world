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
import pandas as pd
import restapi as oxrest

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive']

creds = None
# The file gdrive_token.json stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists('gdrive_token.json'):
    creds = Credentials.from_authorized_user_file('gdrive_token.json', SCOPES)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('gdrive_token.json', 'w') as token:
        token.write(creds.to_json())

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

