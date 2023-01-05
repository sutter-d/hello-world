#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 12:11:00 2022

@author: danielsutter
"""

import os.path
import time
import io
import shutil
import mimetypes
import base64
from email.mime.text import MIMEText
import pandas as pd
import requests
import yaml
import logging

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import ds_utils as ds
import gdrive_post_s2s as post


#example
def get_list(service, user_id, qry):
    """Get list of gmail messages matching query parameters.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      qry: qry parameters for limiting list of returned email ids.

    Returns:
      lst = list of emails ids.
    """
    try:
        message = (service.users().messages().list(
            userId=user_id, q=qry).execute())
        # print('Message Id: %s' % message['id'])
        print('Message Returned')
        return message
    except HttpError as error:
        print('An error occurred: %s' % error)

def get_attachmentId(service, user_id, mess_id):
    """Get attachment details and metadata.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      mess_id: Message ID.
      att_id: Attachment ID

    Returns:
      Attachment.
    """
    try:
        attachment = (service.users().messages().get(
            userId=user_id, id=mess_id).execute())
        # print('Message Id: %s' % message['id'])
        print('Attachment ID Returned')
        return attachment
    except HttpError as error:
        print('An error occurred: %s' % error)

def get_attachment(service, user_id, mess_id, att_id):
    """Get attachment raw data.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      mess_id: Message ID.
      att_id: Attachment ID

    Returns:
      Attachment.
    """
    try:
        attachment = (service.users().messages().attachments().get(
            userId=user_id, messageId=mess_id, id=att_id).execute())
        # print('Message Id: %s' % message['id'])
        print('Attachment Downloaded')
        return attachment
    except HttpError as error:
        print('An error occurred: %s' % error)

def add_label(service, user_id, mess_id, bdy):
    """Get attachment raw data.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      mess_id: Message ID.
      att_id: Attachment ID

    Returns:
      Attachment.
    """
    try:
        label = (service.users().messages().modify(
            userId=user_id, id=mess_id, body=bdy).execute())
        # print('Message Id: %s' % message['id'])
        print('Message label attached')
        return label
    except HttpError as error:
        print('An error occurred: %s' % error)



if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('./gitlogs/gdrive_attachments_s2s_' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    print("starting to build googleapi service object")
    SCOPES = ['https://mail.google.com/']
    creds = None
    SERVICE_ACCOUNT_FILE = './oxops-gcp-project-service.json'
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        # this could be removed and may be redundant with line 90 below
        subject='oxide_ops_automation@oxidecomputer.com'
    )

    print("built creds object: " + str(creds))

    gmail_service = build('gmail', 'v1', credentials=creds)

    # qry = "from:Erin.Fort@bench.com"
    qry = "has:attachment from:Erin.Fort@bench.com AND NOT label:dwnld"

    lst = get_list(gmail_service, 'oxide_ops_automation@oxidecomputer.com', qry)
    
    if lst['resultSizeEstimate']==0:
        print('No New Emails Returned')
    else:
        # mId = '183e5fa2f9496ee5'
        mId = pd.DataFrame(lst['messages'])
        mId = mId[['id']]
        for x in range(len(mId['id'])):
            print('messageId is %s' % str(mId.at[x,'id']))
            # mId = mId.at[x,'id']
    
            attach_id = get_attachmentId(gmail_service, 'oxide_ops_automation@oxidecomputer.com', mId.at[x,'id'])
    
            aId = attach_id['payload']
            aId = aId['parts']
            aId = pd.DataFrame(aId)
            aId = aId[aId['mimeType']=='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
            if aId.size > 0:
                aId = aId.reset_index(drop=True)
                filename = aId.at[0,'filename']
                print('Spreadsheet %s Attached' % filename)
                aId = aId['body']
                aId = ds.unpack(aId)
                aId = aId.at[0, 'attachmentId']
                
                attach_raw = get_attachment(gmail_service, 'oxide_ops_automation@oxidecomputer.com', mId, aId)
                data = attach_raw['data']
                file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                with open('./uploads/' + str(filename) + "_", 'wb') as f:
                    f.write(file_data)
                print('Spreadsheet %s Downloaded Locally' % filename)
            else:
                print('No Spreadsheet Attachments')
    
            msg_bdy = {"addLabelIds":["Label_4249575710768922908"]}
            print('Adding label to message_id = %s' % mId.at[x,'id'])
            lbls = add_label(gmail_service, 'oxide_ops_automation@oxidecomputer.com', mId.at[x,'id'], msg_bdy)
    
        gdrive_folder = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco'
        post.main(gdrive_folder)
        print('Files uploaded to GDrive folder_id = %s' % gdrive_folder)
    

