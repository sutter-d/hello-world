#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 11:23:39 2022

@author: danielsutter
"""

import os.path
import time
import io
import shutil
import pandas as pd
import restapi as oxrest
import mimetypes
import base64

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# creds = None
# # The file token.json stores the user's access and refresh tokens, and is
# # created automatically when the authorization flow completes for the first
# # time.
# if os.path.exists('gmail_token.json'):
#     creds = Credentials.from_authorized_user_file('gmail_token.json', SCOPES)
# # If there are no (valid) credentials available, let the user log in.
# if not creds or not creds.valid:
#     if creds and creds.expired and creds.refresh_token:
#         creds.refresh(Request())
#     else:
#         flow = InstalledAppFlow.from_client_secrets_file(
#             'credentials.json', SCOPES)
#         creds = flow.run_local_server(port=0)
#     # Save the credentials for the next run
#     with open('gmail_token.json', 'w') as token:
#         token.write(creds.to_json())

# gmail_service = build('gmail', 'v1', credentials=creds)

# In[]


def create_message(sender, to, subject, message_text):
    """Create a message for an email.

    Args:
      sender: Email address of the sender.
      to: Email address of the receiver.
      subject: The subject of the email message.
      message_text: The text of the email message.

    Returns:
      An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    # message.attach(MIMEText(message_text, 'html'))
    return {'raw': base64.urlsafe_b64encode(message.as_bytes())}


# msg_bdy = "Team  - there's a new Comp Forecast file available - please follow this link:\
#     https://drive.google.com/drive/folders/1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON"
# msg = create_message('oxide_ops_automation@oxidecomputer.com',
#                      'daniel@oxidecomputer.com',
#                      time.strftime("%Y-%m-%d") + " New Comp Forecast file",
#                      msg_bdy)

# msg['raw'] = msg['raw'].decode()

def send_message(service, user_id, message):
    """Send an email message.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.

    Returns:
      Sent Message.
    """
    try:
        message = (service.users().messages().send(
            userId=user_id, body=message).execute())
        print('Message Id: %s' % message['id'])
        return message
    except HttpError as error:
        print('An error occurred: %s' % error)


if __name__ == '__main__':
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('gmail_token.json'):
        creds = Credentials.from_authorized_user_file(
            'gmail_token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('gmail_token.json', 'w') as token:
            token.write(creds.to_json())

    gmail_service = build('gmail', 'v1', credentials=creds)

    msg_bdy = "Team  - there's a new Comp Forecast file available - please follow this link: https://drive.google.com/drive/folders/1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON"

    msg_sub = time.strftime("%Y-%m-%d") + " New Comp Forecast file"

    msg = create_message('oxide_ops_automation@oxidecomputer.com',
                         'procurement@oxidecomputer.com',
                         msg_sub,
                         msg_bdy)

    msg['raw'] = msg['raw'].decode()

    rslt = send_message(
        gmail_service, 'oxide_ops_automation@oxidecomputer.com', msg)
