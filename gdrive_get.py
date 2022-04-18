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
import pandas as pd
import restapi as oxrest

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

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


file_id = ['1FCbWNjMVxbwumpXy84MMtTi4wu-I7qd_',
           '12fgP8PwRlmmsySJVCt3RjEmo8ZsG_igG']

request = service.files().get_media(fileId=file_id[0])

downloader = MediaIoBaseDownload(fh, request)
done = False
while done is False:
    status, done = downloader.next_chunk()
    print("Download %d%%" % int(status.progress() * 100))

# The file has been downloaded into RAM, now save it in a file
fh.seek(0)
with open('./data/ox_bm_inv_shared.xlsx', 'wb') as f:
    shutil.copyfileobj(fh, f)

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


