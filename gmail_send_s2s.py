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

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.text import MIMEText


# In[Create a message for an email fcn]
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

# In[Send an email message fcn]
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

# In[__name__ == __main__]

if __name__ == '__main__':
    print("starting to build googleapi service object")
    SCOPES = ['https://mail.google.com/']
    creds = None
    SERVICE_ACCOUNT_FILE = './oxops-gcp-project-service.json'
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject='oxide_ops_automation@oxidecomputer.com' #this could be removed and may be redundant with line 90 below
        )

    print("built creds object: " + str(creds))

    gmail_service = build('gmail', 'v1', credentials=creds)

    # BELOW HERE IS THE OLD EMAIL GENERATOR
    # UNCOMMENT WHEN BUILD OBJECT RETURNS A SUCCESSFUL QUERY
    msg_bdy = "Team  - there's a new Comp Forecast file available - please follow this link: https://drive.google.com/drive/folders/1cifHUNhTLyeuDWtdhhmbJ176sL0O9uON"
    msg_sub = time.strftime("%Y-%m-%d") + " New Comp Forecast file"
    msg = create_message('oxide_ops_automation@oxidecomputer.com',
                          'procurement@oxidecomputer.com',
                          msg_sub,
                          msg_bdy)

    msg['raw'] = msg['raw'].decode()

    rslt = send_message(
        gmail_service, 'oxide_ops_automation@oxidecomputer.com', msg)
    print("send gmail message")
