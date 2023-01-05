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


# example
def get_list(service, user_id, qry, nextPgToken = 'None'):
    """Get list of gmail messages matching query parameters.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      qry: qry parameters for limiting list of returned email ids.

    Returns:
      message = list of emails ids.
    """
    try:
        if nextPgToken == 'None':
            message = (service.users().messages().list(
                userId=user_id, q=qry).execute())
        else:
            message = (service.users().messages().list(
                userId=user_id, q=qry, pageToken = nextPgToken).execute())
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
      attachment_Id - dict of attachment metadata.
    """
    try:
        attachment_Id = (service.users().messages().get(
            userId=user_id, id=mess_id).execute())
        print('Attachment ID Returned')
        return attachment_Id
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
      attachment - dict of raw data from specified attachment.
    """
    try:
        attachment = (service.users().messages().attachments().get(
            userId=user_id, messageId=mess_id, id=att_id).execute())
        print('Attachment Downloaded')
        return attachment
    except HttpError as error:
        print('An error occurred: %s' % error)


def add_label(service, user_id, mess_id, bdy):
    """Add label to gmail message.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      mess_id: Message ID.
      bdy: body in json format with label to be added

    Returns:
      label dataframe.
    """
    try:
        label = (service.users().messages().modify(
            userId=user_id, id=mess_id, body=bdy).execute())
        print('Message label attached')
        return label
    except HttpError as error:
        print('An error occurred: %s' % error)

xlsx_formats = ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'application/xlsx',
                'application/vnd.ms-excel',
                'application/vnd.ms-excel.sheet.macroenabled.12',
                'application/octet-stream'
                ]

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

    # WRITING QUERY USING GMAIL SEARCH SYNTAX - EXAMPLES BELOW
    # CREATING DICT TO RUN MULTIPLE QUERIES
    # qry = "from:Erin.Fort@bench.com"
    # qry = "has:attachment after:2022/10/17" # AND NOT label:dwnld"
    # qry = "has:attachment from:OnlineSupportUS@avnet.com AND NOT label:dwnld"
    qry = "has:attachment replyto:no-reply@arrow.com AND NOT label:dwnld"
    qry = ["has:attachment from:Erin.Fort@bench.com AND NOT label:dwnld",
           "has:attachment from:OnlineSupportUS@avnet.com AND NOT label:dwnld",
           "has:attachment from:Paula.Thor@mouser.com AND NOT label:dwnld",
           "has:attachment from:Darla.Mason@bench.com AND NOT label:dwnld",
           "has:attachment replyto:no-reply@arrow.com AND NOT label:dwnld"]

    # FOR LOOP FOR EACH QUERY IN QRY DICT
    for qry in qry:
        print("query is %s" % qry)
        # ds.clear_dir('./uploads') #NOT NEEDED FOR S2S AUTOMATION
        # THIS WILL RETURN A LIST OF MESSAGES BASED ON THE QUERY
        lst = get_list(
            gmail_service, 'oxide_ops_automation@oxidecomputer.com', qry)
    
    
        # SINCE WE'RE PLANNING ON RUNNING THIS AT A SET INTERVAL
        # , NEW EMAILS MAY NOT HAVE BEEN RECEIVED. 
        # IF NO NEW EMAILS (lst['resultSizeEstimate'] == 0), RETURN 'NO NEW EMAILS'
        if lst['resultSizeEstimate'] == 0:
            print('No New Emails Returned')
        # IF THE QUERY RETURNS NEW EMAILS
        else:
            mId = lst['messages'].copy()
            # SINCE THERE MAY BE MULTIPLE PAGES OF RESULTS, WE NEED
            # TO TEST FOR THE 'nextPageToken' KEY AND
            # LOOP ON THE QUERY UNTIL WE HAVE ALL RESULTS
            while 'nextPageToken' in lst:
                print('nextPageToken is %s' % lst['nextPageToken'])
                lst = get_list(gmail_service,
                               'oxide_ops_automation@oxidecomputer.com',
                               qry,
                               lst['nextPageToken'])
                mId = mId + lst['messages']
            else:
                print('No more pages')
            # mId is the messageId from the message
            # mId = '183e5fa2f9496ee5'
            mId = pd.DataFrame(mId)
            mId = mId[['id']]
            # HERE WE'LL LOOP OVER THE MESSAGE IDS AND CHECK IF THEY HAVE A
            # SPREADSHEET ATTACHMENT THAT WE CAN UPLOAD
            for x in range(len(mId['id'])):
                print('messageId is %s' % str(mId.at[x, 'id']))
                # mId = mId.at[x,'id']
    
                # THIS WILL PULL THE MESSAGE METADATA AND WE NEED TO COMB THROUGH
                # FOR THE ATTACHMENT ID
                attach_id = get_attachmentId(
                    gmail_service, 'oxide_ops_automation@oxidecomputer.com', mId.at[x, 'id'])
    
                # aId is the attachmentId from the message
                aId = attach_id['payload']
                aId = aId['parts']
                aId = pd.DataFrame(aId)
                # print(aId['mimeType'])
                aId = aId[aId['mimeType'].isin(xlsx_formats)]
    
                # IF THERE ARE NO SPREADSHEET ATTACHMENTS, aId.size == 0
                # AND WE CAN SKIP TO THE NEXT mId
                if aId.size > 0:
                    aId = aId.reset_index(drop=True)
                    filename = aId.at[0, 'filename']
                    print('Spreadsheet %s Attached' % filename)
                    aId = aId['body']
                    aId = ds.unpack(aId)
                    aId = aId.at[0, 'attachmentId']
    
                    attach_raw = get_attachment(
                        gmail_service, 'oxide_ops_automation@oxidecomputer.com', mId, aId)
                    data = attach_raw['data']
                    file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                    with open('./uploads/' + str(filename), 'wb') as f:
                        f.write(file_data)
                    print('Spreadsheet %s Downloaded Locally' % filename)
                else:
                    print('No Spreadsheet Attachments')
    
                # HERE WE'RE ADDING THE 'dwnld' LABEL TO THE MESSAGES WE PULLED
                # THE QUERY IS SET TO IGNORE THEM ON THE NEXT RUN
                msg_bdy = {"addLabelIds": ["Label_4249575710768922908"]}
                print('Adding label to message_id = %s' % mId.at[x, 'id'])
                lbls = add_label(
                    gmail_service, 'oxide_ops_automation@oxidecomputer.com', mId.at[x, 'id'], msg_bdy)
    
            # NOT USED FOR VENV S2S VERSION
            # gdrive_folder = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco'
            # post.main(gdrive_folder, isodate=False)
            # print('Files uploaded to GDrive folder_id = %s' % gdrive_folder)
            # ds.clear_dir("./uploads")
