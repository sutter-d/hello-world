# -*- coding: utf-8 -*-
"""
Spyder Editor
Author: Sutter
"""
from __future__ import print_function
import logging
import math

import requests
from requests import HTTPError
import yaml
import json
import pandas as pd
import shutil
from pyairtable import Api, Table

import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
# import yaml
# import time

# logging.basicConfig(filename=('./restapi.log'), filemode='w', level=logging.DEBUG,
#                     format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

# In[unpack]
# Helper Function for all the nested dicts


def unpack(oxjs):
    df = pd.DataFrame()
    js = oxjs.dropna()
    if all(isinstance(i, list) for i in js):
        for index, value in js.items():
            temp = pd.DataFrame(value)
            temp['ind'] = index
            df = df.append(temp)
        df = df.reset_index(drop=True).set_index(['ind'])
    else:
        df = list(filter(None, js))
        df = pd.json_normalize(df)
    logging.debug("Ran unpack fcn successfully")
    return df

# In[clear_dir]
# Helper function for clearing a folder

def clear_dir(folder):
    for filename in os.listdir(folder):
        if filename.endswith(".py") is False:
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as exc:
                print('Failed to delete %s. Reason: %s' % (file_path, exc))

# In[dktokenrefresh]


def dktokenrefresh(oxcreds):
    creds = oxcreds

    token_url = creds['token_url']
    hist_api_url = creds['hist_api_url']

    dk_id = creds['client_id']
    dk_secret = creds['client_secret']
    r_token = creds['refresh_token']
    a_token = creds['access_token']
    cust_id = creds['cust_id']

    # creates headers for the GET query
    api_call_headers = {'Authorization': 'Bearer ' + a_token,
                        'X-DIGIKEY-Client-Id': dk_id,
                        'CustomerID': str(cust_id),
                        'OpenOnly': 'true'}
    # create a blank order details dataframe

   # IF statement checking to see if the query runs successfully and, if not, to try refreshing the access token using the refresh token
    if (requests.get(hist_api_url, headers=api_call_headers, verify=False).status_code != 200):
        data = {'grant_type': 'refresh_token', 'client_id': dk_id,
                'client_secret': dk_secret, 'refresh_token': r_token}
        logging.debug("Using Refresh token")
        refresh_token_response = requests.post(token_url, data=data)
        logging.debug("response")
        logging.debug(refresh_token_response.headers)
        logging.debug('body: ' + refresh_token_response.text)
        # Need to overwrite old tokens AND write new tokens back to config file
        tokens = json.loads(refresh_token_response.text)
        creds['access_token'] = tokens['access_token']
        creds['refresh_token'] = tokens['refresh_token']
    logging.info("TokenRefresh ran successfully")
    return creds

# In[dkordhistapi]


def dkordhistapi(oxcreds, oxopncls):
    # def dkordhistapi(oxcreds):
    creds = oxcreds
    opncls = oxopncls

    hist_api_url = creds['hist_api_url']

    dk_id = creds['client_id']
    a_token = creds['access_token']
    cust_id = creds['cust_id']

    # creates headers for the GET query
    # api_call_headers = {'Authorization': 'Bearer ' + a_token, 'X-DIGIKEY-Client-Id': dk_id, 'CustomerID': str(cust_id), 'OpenOnly': str(opncls)}
    api_call_headers = {'Authorization': 'Bearer ' +
                        a_token, 'X-DIGIKEY-Client-Id': dk_id}
    #
    api_call_params = {'CustomerID': str(
        cust_id),  'StartDate': '2020-01-01', 'OpenOnly': str(opncls)}
    # create a blank order details dataframe
    orderdetails = pd.DataFrame()
    if (requests.get(hist_api_url,
                     headers=api_call_headers,
                     params=api_call_params,
                     verify=False).status_code != 401):
        api_call_response = requests.get(hist_api_url,
                                         headers=api_call_headers,
                                         params=api_call_params,
                                         verify=False)
        orderdetails = pd.json_normalize(api_call_response.json())
    logging.info("dkordhistapi ran successfully")
    return orderdetails


# In[dkordstatapi]
# UPDATE - using this loop we can pull multiple records based on order number and append to a dataframe for export
# There is an IF statement to confirm

def dkordstatapi(oxcreds, dkordernumbers):
    creds = oxcreds

    stat_api_url = creds['stat_api_url']

    dk_id = creds['client_id']
    a_token = creds['access_token']

    # creates headers for the GET query
    api_call_headers = {'Authorization': 'Bearer ' +
                        a_token, 'X-DIGIKEY-Client-Id': dk_id}
    # create a blank order details dataframe
    orderdetails = pd.DataFrame()
    ordernumbers = dkordernumbers

    # IF statement - API will run if Access Token is Valid
    if (requests.get(stat_api_url+'1',
                     headers=api_call_headers,
                     verify=False).status_code != 401):
        # for loop based on ordernumbers file
        for x in ordernumbers:
            api_call_response = requests.get(stat_api_url+str(x),
                                             headers=api_call_headers,
                                             verify=False)
            temp = pd.json_normalize(api_call_response.json())
            orderdetails = orderdetails.append(temp)
            logging.debug(str(x))
    logging.info("dkordstatapi ran successfully")
    return orderdetails

# In[msrordhistapi]

# FIRST PASS AT MOUSER API
# This function takes creds.yml and pulls all 0xide order history from mouser and returns a dataframe


def msrordhistapi(oxcreds):
    creds = oxcreds
    hist_api_url = creds['hist_api_url']
    api_call_params = {'apiKey': creds['api_key'], 'dateFilter': 'ALL'}
    orderdetails = pd.DataFrame()

    api_call_response = requests.get(
        hist_api_url, params=api_call_params, verify=False)

    if api_call_response.status_code == 200:
        logging.debug('Success! ' + str(api_call_response.status_code))
    elif api_call_response.status_code != 200:
        logging.debug(
            'Failed GET ' + str(api_call_response.status_code) + api_call_response.text)

    temp = api_call_response.json()
    temp = temp['OrderHistoryItems']
    orderdetails = pd.json_normalize(temp)
    logging.info("msrodrhistapi ran successfully")
    return orderdetails

# In[msrordstatapi]
# This functions takes creds.yml and the order history from msrordhistapi (full or trimmed)
# , pulls the extended order details from mouser, and returns a dataframe


def msrordstatapi(oxcreds, msrordernumbers):
    creds = oxcreds
    # ordernmbrs = msrordernumbers[msrordernumbers['OrderStatusDisplay']!='COMPLETE']
    ordernmbrs = msrordernumbers['WebOrderNumber']

    stat_api_url = creds['stat_api_url']
    api_call_params = {'apiKey': creds['api_key']}
    orderdetails = pd.DataFrame()
    for x in ordernmbrs:
        api_call_response = requests.get(
            stat_api_url + str(x), params=api_call_params, verify=False)
        if api_call_response.status_code == 200:
            logging.debug('Success! ' + str(api_call_response.status_code))
            temp = pd.json_normalize(api_call_response.json())
            orderdetails = orderdetails.append(temp)
        elif api_call_response.status_code != 200:
            logging.debug(
                'Failed GET ' + str(api_call_response.status_code) + api_call_response.text)
        logging.debug(str(x))
    logging.info("msrordstatapi ran successfully")
    return orderdetails

# In[MOUSER PARSING FUNCTION]
# THIS FUNCTION TAKES MOUSER GMAIL BODY HTML FOR PARSING AND RETURNS A DATAFRAME


def msrparse(oxshp):
    oxdtls = []

    for i in range(len(oxshp['html'])):
        # ASSIGNING GMAIL MESSAGE HTML FROM BASE64 DECODE TO VAR FOR PARSING
        emhtml = oxshp.loc[i, 'html']
        # FIND FIRST POSITION OF HYPERLINKED TEXT
        aref = emhtml.find("a href=")
        # INDEX HTML TEXT FORWARD TO POSITION
        emhtml = emhtml[aref+8:]
        while aref != -1:
            # IF THE HYPERLINK ISN'T FOR A PN, LOOK FOR THE NEXT HYPERLINK
            if (emhtml[:36] != 'https://www.mouser.com/ProductDetail'):
                aref = emhtml.find("a href=")
                logging.debug(aref)
                if (aref != -1):
                    emhtml = emhtml[aref+8:]
            # IF THE HYPERLINK IS FOR A PN BUILD A DICT OF THREADID, PN, QTY AND SHIP DATE
            else:
                td = {'ind': i}
                td['threadId'] = oxshp.loc[i, 'threadId']
                pn = emhtml.find(">")
                emhtml = emhtml[pn+1:]
                pn = emhtml[:emhtml.find("<")]
                td['pn'] = pn
                emhtml = emhtml[emhtml.find("<"):]
                logging.debug(pn)
                remqty = emhtml.find("<td")
                emhtml = emhtml[remqty:]
                remqty = emhtml.find(">")
                emhtml = emhtml[remqty+1:]
                remqty = emhtml[:emhtml.find("<")]
                logging.debug(remqty)
                td['remqty'] = remqty
                shipqty = emhtml.find("<td")
                emhtml = emhtml[shipqty:]
                shipqty = emhtml.find(">")
                emhtml = emhtml[shipqty+1:]
                shipqty = emhtml[:emhtml.find("<")]
                logging.debug(shipqty)
                td['shipqty'] = shipqty
                if (oxshp.loc[i, 'type'] == 'OrderStatus'):
                    shipdate = emhtml.find("<td")
                    emhtml = emhtml[shipdate:]
                    shipdate = emhtml.find(">")
                    emhtml = emhtml[shipdate+1:]
                    shipdate = emhtml.find("<")
                    shipdate = emhtml[:shipdate]
                    td['estshipdate'] = shipdate
                    logging.debug(shipdate)
                logging.debug(td)
                # APPEND THE PARSING RESULTS TO THE RESULTS FROM THE PREVIOUS EMAIL
                oxdtls.append(td)
                aref = emhtml.find("a href=")
                if (aref != -1):
                    emhtml = emhtml[aref+8:]
    oxparse = pd.DataFrame(oxdtls)
    # MAKE SHIP DATE READABLE
    oxparse['estshipdate'] = oxparse['estshipdate'].replace(
        '&nbsp;', ' ', regex=True)
    return oxparse

# In[MOUSER SNIPPET PARSEING FUNCTION]
# THIS FUNCTION TAKES A GMAIL HEADER DF AND PARSES THE SNIPPET COLUMN TO RETURN THE EMAIL TYPE


def msrsnippet(oxhdr):
    oxsnip = []
    # DEFINE TYPE OF EMAIL FROM SNIPPET TEXT
    for x in range(len(oxhdr['snippet'])):
        if (oxhdr.loc[x, 'snippet'].startswith("Order Status")):
            td = {'type': 'OrderStatus'}
        elif (oxhdr.loc[x, 'snippet'].startswith("Order Confirmation")):
            td = {'type': 'OrderConf'}
        elif (oxhdr.loc[x, 'snippet'].startswith("Shipment")):
            td = {'type': 'ShipNote'}
            td['shipdate'] = oxhdr.loc[x, 'snippet'][oxhdr.loc[x, 'snippet'].find(
                "Ship Date : ")+12:oxhdr.loc[x, 'snippet'].find(" Estimated Arrival Date")]
            td['edd'] = oxhdr.loc[x, 'snippet'][oxhdr.loc[x,
                                                          'snippet'].find(" Estimated Arrival Date")+26:]
        else:
            td = {'type': 'Junk'}
        # ASSIGN PO (SAME AS SO) TO VAR AND STORE IN DICT
        so = oxhdr.loc[x, 'snippet'].find("Purchase Order Number : ")
        logging.debug(so)
        so = oxhdr.loc[x, 'snippet'][so+24: so+32]
        logging.debug(so)
        td['so'] = so
        # APPEND RESULTS TO LIST OF DICTS OF OTHER EMAILS
        oxsnip.append(td)

    oxsnip = pd.DataFrame(oxsnip)
    return oxsnip

# In[GMAIL GET QUERY FOR MOUSER RECORDS]


def gmailget():
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)

    # # Call the Gmail API

    api_call_response = service.users().messages().list(
        userId='me', q='from:procurement@oxidecomputer.com').execute()

    gmraw = pd.DataFrame(api_call_response)

    # msrraw = gmailget()
    gmraw = unpack(gmraw['messages'])

    gmids = gmraw['id'].copy()
    gmbody = pd.DataFrame()

    for x in gmids:
        api_call_get = service.users().messages().get(userId='me', id=x).execute()
        gmraw = pd.json_normalize(api_call_get)
        gmbody = gmbody.append(gmraw)
        logging.debug(x)

    return gmbody

# In[AIRTABLE GET API WORKSPACE]


def atblget(oxcreds):
    creds = oxcreds
    api_key = creds['api_key']
    base_id = creds['base_id']
    table_name = creds['table_name']
    atbl_tbl = Table(api_key, base_id, table_name)

    atbl_get = pd.DataFrame()
    # TRY/EXCEPT LOOP FOR THE AIRTABLE API QUERY
    try:
        api_call_response = atbl_tbl.all()
        logging.debug("oxrest GET query success")
    except HTTPError as exc:
        logging.debug("oxrest GET query failed")
        logging.debug(exc.response.status_code)

    # STORING GET QUERY JSON RESULTS TO DATAFRAME
    atbl_get = pd.json_normalize(api_call_response)
    return atbl_get

# In[AIRTABLE CREATE RECORDS API WORKSPACE]


def atblcreate(oxcreds, oxcreate):
    creds = oxcreds
    api_key = creds['api_key']
    base_id = creds['base_id']
    table_name = creds['table_name']
    atbl_tbl = Table(api_key, base_id, table_name)
    ox_create = pd.DataFrame()
    api_call_response = []
    # TRY/EXCEPT LOOP FOR THE AIRTABLE API QUERY
    try:
        api_call_response = atbl_tbl.batch_create(oxcreate)
        logging.debug("oxrest batch_create post query success")
    except HTTPError as exc:
        logging.debug(exc.response.status_code)
        logging.debug(exc.response.text)
        logging.debug("oxrest batch_create post query fail")
    # STORING CREATE QUERY JSON RESULTS TO DATAFRAME
    ox_create = pd.json_normalize(api_call_response)
    return ox_create


# In[ARROW ORDER STATUS API]

def arrowordstat(oxcreds, oxordnums):
    creds = oxcreds
    stat_api_url = creds['stat_api_url']
    stat_api_uname = creds['stat_api_uname']
    stat_api_secret = creds['stat_api_secret']

    arrow_order_num = oxordnums
    arrow_order_stat = pd.DataFrame()

    for x in arrow_order_num:
        logging.debug(x)
        # SOME AIRTABLE RECORDS HAVE NO WEB-SO AT START OF STRING BUT REQUIRED FOR LOOKUP
        if x[0:6] != 'WEB-SO':
            x = 'WEB-SO'+str(x)
            logging.debug(x)
        logging.debug('Assigning API vars')
        api_call_params = {'username': stat_api_uname,
                           'password': stat_api_secret,
                           'orderNo': str(x)}
        logging.debug('Running API Query')
        api_call_response = requests.post(
            stat_api_url, params=api_call_params, verify=False)
        logging.debug('Ran API Query')
        # IF THE LOOKUP DOESNT RETURN RESULTS, IT DOESN'T RETURN AT 400 HTTPERROR
        # THIS IF STATEMENT CHECKS THE 200 RESULTS FOR BAD CONTENT AND APPENDS GOOD CONTENT
        if 'order.not.found' not in api_call_response.text:
            logging.debug('Success! ' + str(api_call_response.text))
            # logging.debug('Success! ' + str(api_call_response.text))
            arrow_order_stat = arrow_order_stat.append(
                pd.json_normalize(api_call_response.json()))
        else:
            logging.debug(
                'Failed GET ' + str(api_call_response.status_code) + api_call_response.text)
            # logging.debug('Failed GET ' + str(api_call_response.status_code) + api_call_response.text)
    return arrow_order_stat

# In[AVNET GET API]


def avnetget():
    WCToken = '41706324%2CP1W3a72gkaLgRGMwDBtfzeMNaSolfq3z6dLZ1%2BMyAV3E1SmAxdI%2BZ5a3TfQt7xZMapW3t0fuY55FEVQ5V9R0PqLQreoUqOARpp94LNut9ZxLFsNx61QRsJqil2DUAxxX73YljYZVbDk1Fq0gtcWz6twsXUICVs%2BtUJwa9kjaEnfxLc2A%2BgA9c57pGcRtFa5rx5qlNB%2BkSrFDzSfw7u0HgMXwauaT%2B0Ylvq1frZwapkNZY8yhEFZB9SPALmMICjBM'
    WCTrusted = '41706324%2C0uGi8ww24sDgI7mdnW1IkFzsJR%2BvUe7MoG0CDmPIAIM%3D'
    SubKey = 'ffa7a8d3ca124a7182d6e7f5152e5ded'
    # api_url = 'https://apigw.avnet.com/external/getDEXFetchProducts?STORE_ID=715839035&amp;searchTerm=3074457345626574374&amp;searchType=CATENTRYID&amp;infoLevel=COMPLETE'
    api_url = 'https://apigw.avnet.com/external/getDEXFetchProducts?'

    # creates headers for the GET query
    api_call_headers = {'WCToken': WCToken,
                        'WCTrustedToken': WCTrusted, 'Ocp-Apim-Subscription-Key': SubKey}
    #
    api_call_params = {'STORE_ID': '715839035', 'searchTerm': '3074457345626574374',
                       'searchType': 'CATENTRYID', 'infoLevel': 'COMPLETE'}
    # create a blank order details dataframe
    orderdetails = pd.DataFrame()
    api_call_response = requests.get(
        api_url, headers=api_call_headers, params=api_call_params, verify=False)
    logging.debug(api_call_response.text)
    orderdetails = pd.json_normalize(api_call_response.json())
    # if (requests.get(api_url, headers=api_call_headers, verify=False).status_code == 401):
    #     api_call_response = requests.get(api_url, headers=api_call_headers, verify=False)
    return orderdetails


# In[BUILD BOM FUNCTION]
"""
BUILD BOM FUNCTION
"""


def buildbom(oxpn, oxcreds):
    # [DURO RACK API GET]
    # oxpn = '991-0000024'
    # oxpn = '999-0000014'  # oxpnE RACK PN
    # sandbox_api_url = 'https://public-api.staging-gcp.durolabs.xyz/v1/products/'
    prod_api_url = 'https://public-api.duro.app/v1/products/'
    comp_api_url = 'https://public-api.duro.app/v1/components/'
    api_key = oxcreds['api_key']  # 16152961423738m50/MAHXgvcvj4RMROq2g==

    # creates headers for the GET query
    api_call_headers = {'x-api-key': api_key}
    api_call_params = {'cpn': oxpn}

    # GET REQUEST
    # IF PN BEGINS WITH 999 THEN PROD ELSE COMPONENT
    if oxpn[0:3] == '999':
        api_call_response = requests.get(
            prod_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    else:
        api_call_response = requests.get(
            comp_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    duro = pd.DataFrame(api_call_response.json())

    # [UNPACKING FOR CHILD COMPONENTS]

    duro['parent'] = oxpn
    duro = duro.set_index('parent')
    x = 0
    duro
    if oxpn[0:3] == '999':
        durop = unpack(duro['products'])
    else:
        durop = unpack(duro['components'])
    durop['parent'] = durop['cpn']
    durop['level'] = x

    # [DURO API GET FOR LEVEL 1 COMPONENTS]
    rent = durop.at[0, 'parent']

    cpns = cpn().drop(columns=['value'])

    # INSERTING IF ELSE HERE FOR MAKE/BUY DECISION
    if durop['procurement'].item() != 'Buy':
        duropc = unpack(durop['children'])
        duropc = duropc.reset_index(drop=True)

        duropcc = pd.DataFrame()

        duropcc = cpns[cpns['_id'].isin(duropc['component'])]
        x = 1
        duropcc = duropcc.assign(parent=rent)
        duropcc = duropcc.assign(level=x)
        duropcc = duropcc.reset_index(drop=True)

        duropc = duropcc.merge(duropc[['component', 'quantity', 'refDes']], 'left',
                               left_on='_id', right_on='component').drop(columns=['component'])
        durorent = pd.concat([durop, duropc], axis=0).reset_index(drop=True)
    else:
        durorent = durop

    durorent['pnladder'] = [durorent.at[i, 'parent'] + " " +
                            durorent.at[i, 'cpn'] for i in range(len(durorent['cpn']))]

    # [FORMATTING L7 DF]
    lvls = [2, 3, 4, 5, 6, 7]
    # lvls=[2,3,4]
    lvlflag = True
    if len(durorent['cpn']) == 1:
        lvlflag = False
    for xx in lvls:
        # xx=6 #DEBUG ONLY
        logging.debug("level " + str(xx))
        if lvlflag:
            # FORMATTING LOWER LVL DF]
            durop = durorent
            cols_to_move = ['parent',
                            'cpn',
                            'level',
                            'quantity',
                            'name','refDes']
            cols = cols_to_move + \
                [col for col in durop.columns if col not in cols_to_move]
            durop = durop[cols]

            # ix=0
            duropc = pd.DataFrame()
            # UNPACKING LWR LVL CHILD COMPONENTS
            for ix in range(len(durop['cpn'])):
                # ix+=1 #DEBUG ONLY
                # IF PROCUREMENT IS SET TO BUY THEN SKIP AND DON'T PULL CHILD PARTS
                if (durop.at[ix, 'procurement'] == 'Buy'):
                    # ix=ix+1
                    logging.debug(str(ix) + ' 1st')
                # CURRENTLY PULLING XX LEVEL COMPONENTS SO IF LEVEL OF CURRENT ROW IS XX-1 (OR A PARENT)
                # THEN UNPACK THE CHILDREN COL TO A TEMP DF, TAG THE CPN TO THE PARENT, AND APPEND TO THE DURO PARENT CHILD DF
                elif (durop.at[ix, 'level'] == xx-1):
                    temp = pd.DataFrame((durop.at[ix, 'children']))
                    temp['parent'] = durop.at[ix, 'cpn']
                    duropc = duropc.append(temp)
                    # ix=ix+1
                    logging.debug(str(ix) + ' 2nd')
                else:
                    # ix=ix+1
                    logging.debug(str(ix) + ' 3rd')

            # DURO API GET FOR LWR LVL COMPONENTS
            # REMOVE DUPLICATES - IF A PN IS LISTED IN TWO PLACES ON THE BOM, THE ABOVE LOOP WILL ADD
            # ADD IT TO THE DUROPC DF 2X AND THAT WILL CREATE DUPLICATES IN THE BOM
            duropc = duropc.drop_duplicates(keep='first')

            if (duropc.size > 0):
                logging.debug("children = yes " + str(duropc.size))
                cols = ['parent'] + \
                    [col for col in duropc.columns if col != 'parent']
                duropc = duropc[cols]
                duropc = duropc.reset_index(drop=True)

                duropcc = pd.DataFrame()
                duropcc = cpns[cpns['_id'].isin(duropc['component'])]

                duropcc = duropcc.reset_index(drop=True)

                durochild = duropcc.merge(duropc[['parent', 'component', 'quantity', 'refDes']],
                                          'left', left_on='_id', right_on='component').drop(columns=['component'])
                durochild = durochild.assign(level=xx)
                cols = cols_to_move + \
                    [col for col in durochild.columns if col not in cols_to_move]
                durochild = durochild[cols]
                cols = cols_to_move + \
                    [col for col in durorent.columns if col not in cols_to_move]
                durorent = durorent[cols]

                durobom = pd.DataFrame(columns=cols)
                i = 0
                for i in range(len(durorent['cpn'])):
                    # for i in range(300):
                    durobom = durobom.append(durorent.loc[i])
                    if (durorent.at[i, 'level'] == xx-1):
                        temp = durochild[durochild['parent']
                                         == durorent.at[i, 'cpn']]
                        durobom = durobom.append(temp)
                    # logging.debug(i)
                    # logging.debug(durorent.at[i,'cpn'])
                    # i+=1
                durorent = durobom.reset_index(drop=True).copy()
                durorent['pnladder'] = [durorent.at[i, 'parent'] + " " +
                                        durorent.at[i, 'cpn'] for i in range(len(durorent['cpn']))]
            else:
                logging.debug("no children")
                lvlflag = False

    durorent.at[0, 'quantity'] = 1
    cols_to_move = ['parent',
                    'cpn',
                    'level',
                    'quantity',
                    'pnladder',
                    'name','refDes']
    cols = cols_to_move + \
        [col for col in durop.columns if col not in cols_to_move]
    durorent = durorent[cols]

    # [COMPUTING SINGLE RACK EXTENDED BOM QTY]
    duroext = durorent.copy()
    # lvls= [0,1]
    lvls = [0]
    prnt = [{'parent': duroext.at[i, 'cpn'], 'qty':duroext.at[i, 'quantity']}
            for i in range(len(duroext['cpn'])) if duroext.at[i, 'level'] in lvls]

    prnt = pd.DataFrame(prnt)
    # durorent['ext_qty'] = [durorent.at[i,'quantity']*1 for i in range(len(durorent['cpn'])) if durorent.at[i,'cpn'] in prnt]

    # x=1
    # pn = prnt.at[x, 'parent']
    # qty = prnt.at[x, 'qty']

    for y in range(len(prnt['parent'])):
        pn = prnt.at[y, 'parent']
        qty = prnt.at[y, 'qty']
        for i in range(len(duroext['cpn'])):
            # logging.debug(i)
            if (duroext.at[i, 'cpn'] == pn):
                logging.debug("pn match" + duroext.at[i, 'cpn'] + " " + pn)
                duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']
            if (duroext.at[i, 'parent'] == pn):
                logging.debug("parent match" + duroext.at[i, 'parent'] + " " + pn)
                duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']*qty

    # i = i+1
    # NEED TO ISOLATE LIST OF LEVEL 2 PARTS AND BLOW THAT THROUGH REST OF BOM, THEN 3, THEN 4 ETC
    cols_to_move = ['parent',
                    'cpn',
                    'level',
                    'quantity',
                    'ext_qty',
                    'pnladder',
                    'name','refDes']
    cols = cols_to_move + \
        [col for col in durop.columns if col not in cols_to_move]
    duroext = duroext[cols]

    # lvls = [1,2,3,4]
    lvls = duroext['level'].drop_duplicates().tolist()
    lvls = lvls[1:]
    lvls = lvls[:-1]
    logging.debug(lvls)
    logging.debug(lvls)
    for z in lvls:
        logging.debug(str(z) + " = level of bom explosion attempted")
        chld = [{'parent': duroext.at[i, 'cpn'], 'qty':duroext.at[i, 'ext_qty']}
                for i in range(len(duroext['cpn'])) if duroext.at[i, 'level'] == z]
        chld = pd.DataFrame(chld)

        for y in range(len(chld['parent'])):
            pn = chld.at[y, 'parent']
            qty = chld.at[y, 'qty']
            for i in range(len(duroext['cpn'])):
                # logging.debug(i)
                if (duroext.at[i, 'cpn'] == pn):
                    logging.debug("pn match" + duroext.at[i, 'cpn'] + " " + pn)
                    # duroext.at[i, 'ext_qty'] = duroext.at[i,'quantity']
                if (duroext.at[i, 'parent'] == pn):
                    logging.debug("parent match" + duroext.at[i, 'parent'] + " " + pn)
                    duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']*qty
        logging.debug(str(z) + " = level of bom explosion completed")

    duroext['query_pn'] = oxpn
    duroext = duroext.rename(columns={
                             'sources.primarySource.leadTime.value': 'lead_time', 'sources.primarySource.leadTime.units': 'lt_units'})

    cols_to_move = ['query_pn',
                    'parent',
                    'cpn',
                    'name',
                    'category',
                    'level',
                    'procurement',  # ADDED 12/16/21
                    'quantity',
                    'ext_qty']
    duroshort = duroext[cols_to_move]
    hash1 = pd.util.hash_pandas_object(duroshort).sum()
    logging.debug(hash1)
    # CREATE FUNCTION FOR BUILDING BOM THAT CAN BE RERUN
    return duroext

# In[CPN ID EXPORT FROM DURO]


def cpn():
    """
    Provides the raw Duro output for an all CPN query for use in other scripts

    Returns
    -------
    cpnbom : dataframe
        raw Duro output from an all CPN query.

    """
    # Opening creds.yml and assigning Duro creds
    with open("./creds.yml", 'r') as stream:
        allcreds = yaml.safe_load(stream)
    durocreds = allcreds['oxide_duro']
    api_key = durocreds['api_key']

    logging.debug('START BUILDING GET QUERY')
    # creates headers for the GET query
    api_call_headers = {'x-api-key': api_key}

    # Duro components URL with CPN set to wildcat,
    # results perPage set to 100,
    # and page ready to iterate
    comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
    page = 1
    qry = comp_api_url + str(page)

    # GET REQUEST
    api_call_response = requests.get(
        qry, headers=api_call_headers, verify=False)

    # This returns the record count ['resultCount'] and sets it to a variable
    tot = api_call_response.json()['resultCount']

    # Send 1 page of results to log for debug
    logging.debug(api_call_response.json())

    # Page 1 of Results
    cpnbom = pd.DataFrame(api_call_response.json())
    cpnbom = unpack(cpnbom['components'])

    # Using the record count and page size, we get the number of pages we need to query
    rnge = list(range(2, (math.ceil(tot/100)+1)))
    for x in rnge:
        # logging.debug(x)
        comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
        page = x
        qry = comp_api_url + str(page)
        api_call_response = requests.get(
            qry, headers=api_call_headers, verify=False)
        temp = pd.DataFrame(api_call_response.json())
        temp = unpack(temp['components'])
        cpnbom = cpnbom.append(temp)
    return cpnbom

# In[S2S CPN ID EXPORT FROM DURO]


def s2scpn(duro_api_key):
    """
    Provides the raw Duro output for an all CPN query for use in other scripts

    Returns
    -------
    cpnbom : dataframe
        raw Duro output from an all CPN query.

    """
    api_key = duro_api_key

    logging.debug('START BUILDING GET QUERY')
    # creates headers for the GET query
    api_call_headers = {'x-api-key': api_key}

    # Duro components URL with CPN set to wildcat,
    # results perPage set to 100,
    # and page ready to iterate
    comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
    page = 1
    qry = comp_api_url + str(page)

    # GET REQUEST
    api_call_response = requests.get(
        qry, headers=api_call_headers, verify=False)

    # This returns the record count ['resultCount'] and sets it to a variable
    tot = api_call_response.json()['resultCount']

    # Send 1 page of results to log for debug
    logging.debug(api_call_response.json())

    # Page 1 of Results
    cpnbom = pd.DataFrame(api_call_response.json())
    cpnbom = unpack(cpnbom['components'])

    # Using the record count and page size, we get the number of pages we need to query
    rnge = list(range(2, (math.ceil(tot/100)+1)))
    for x in rnge:
        # logging.debug(x)
        comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
        page = x
        qry = comp_api_url + str(page)
        api_call_response = requests.get(
            qry, headers=api_call_headers, verify=False)
        temp = pd.DataFrame(api_call_response.json())
        temp = unpack(temp['components'])
        cpnbom = cpnbom.append(temp)
    return cpnbom

# In[CPN and MPN api export]
def cpnmpn(oxcpns):
    """
    THIS FUNCTION TAKES A DATAFRAME WITH CPN AND SOURCES.MANUFACTURERS
        OUTPUT FROM DURO AND RETURNS A DURO CPN TO MPN MAPPING

    Parameters
    ----------
    oxcpns : DATAFRAME
        DATAFRAME OUTPUT FROM CPN().

    Returns
    -------
    cpnbom_mpn : DATAFRAME
        DURO CPN TO MPN MAPPING.

    """
    # In[UNPACK MPN FROM CPN DATA]
    # This unpacks the MPN data from the nested list column
    # 'sources.manufacturers' and ties it back to the CPN
    # The 1 CPN to many MPN relationship is preserved on the 3rd line
    cpnbom_mpn = oxcpns.set_index(['cpn'])
    cpnbom_mpn = unpack(cpnbom_mpn['sources.manufacturers'])
    cpnbom_mpn = pd.concat([cpnbom_mpn.reset_index(drop=False),
                            unpack(cpnbom_mpn['mpn'])],
                           axis = 1)
    cpnbom_mpn = cpnbom_mpn[['ind', 'key']].rename(columns = {'ind': 'cpn', 'key': 'mpn'}).drop_duplicates()
    return cpnbom_mpn

# In[S2S BUILD BOM FUNCTION]
"""
BUILD S2S BOM FUNCTION
"""


def s2sbuildbom(oxpn, oxcreds):
    """
    This fcn takes a Duro product (999) or component (XXX-XXXXXXX) CPN and returns \
        a flattened BOM. This version WILL stop at any component labeled -buy- and \
        not drill any further. API Credentials are designed to be generated by \
        github automation
        

    Parameters
    ----------
    oxpn : TYPE String
        DESCRIPTION. Duro Product or Component CPN
    oxcreds : TYPE String
        DESCRIPTION. Duro API Credentials

    Returns
    -------
    duroext : TYPE DataFrame
        DESCRIPTION. Flattened BOM 

    """

    # [DURO RACK API GET]
    # oxpn = '991-0000024'
    # oxpn = '999-0000014'  # oxpnE RACK PN
    # sandbox_api_url = 'https://public-api.staging-gcp.durolabs.xyz/v1/products/'
    prod_api_url = 'https://public-api.duro.app/v1/products/'
    comp_api_url = 'https://public-api.duro.app/v1/components/'
    api_key = oxcreds  # 16152961423738m50/MAHXgvcvj4RMROq2g==

    # creates headers for the GET query
    api_call_headers = {'x-api-key': api_key}
    api_call_params = {'cpn': oxpn}

    # GET REQUEST
    # IF PN BEGINS WITH 999 THEN PROD ELSE COMPONENT
    if oxpn[0:3] == '999':
        api_call_response = requests.get(
            prod_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    else:
        api_call_response = requests.get(
            comp_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    duro = pd.DataFrame(api_call_response.json())

    # [UNPACKING FOR CHILD COMPONENTS]

    duro['parent'] = oxpn
    duro = duro.set_index('parent')
    x = 0
    duro
    if oxpn[0:3] == '999':
        durop = unpack(duro['products'])
    else:
        durop = unpack(duro['components'])
    durop['parent'] = durop['cpn']
    durop['level'] = x

    # [DURO API GET FOR LEVEL 1 COMPONENTS]
    rent = durop.at[0, 'parent']

    cpns = s2scpn(oxcreds).drop(columns=['value'])

    # INSERTING IF ELSE HERE FOR MAKE/BUY DECISION
    if durop['procurement'].item() != 'Buy':
        duropc = unpack(durop['children'])
        duropc = duropc.reset_index(drop=True)

        duropcc = pd.DataFrame()

        duropcc = cpns[cpns['_id'].isin(duropc['component'])]
        x = 1
        duropcc = duropcc.assign(parent=rent)
        duropcc = duropcc.assign(level=x)
        duropcc = duropcc.reset_index(drop=True)

        duropc = duropcc.merge(duropc[['component', 'quantity', 'refDes']], 'left',
                               left_on='_id', right_on='component').drop(columns=['component'])
        durorent = pd.concat([durop, duropc], axis=0).reset_index(drop=True)
    else:
        durorent = durop

    print('pnladder 1')
    durorent['pnladder'] = [durorent.at[i, 'parent'] + " " +
                            durorent.at[i, 'cpn'] for i in range(len(durorent['cpn']))]
    # durorent['pnladder'] = durorent.apply(lambda x: x['pnladder'] + " " + x['cpn'], axis=1)

    # [FORMATTING L7 DF]
    lvls = [2, 3, 4, 5, 6, 7]
    # lvls=[2, 3, 4]
    lvlflag = True
    if len(durorent['cpn']) == 1:
        lvlflag = False
    for xx in lvls:
        # xx=2 #DEBUG ONLY
        logging.debug("level " + str(xx))
        if lvlflag:
            # FORMATTING LOWER LVL DF]
            durop = durorent
            cols_to_move = ['parent',
                            'cpn',
                            'level',
                            # 'pnladder',
                            'quantity',
                            'name',
                            'refDes']
            cols = cols_to_move + \
                [col for col in durop.columns if col not in cols_to_move]
            durop = durop[cols]

            # ix=0
            duropc = pd.DataFrame()
            # UNPACKING LWR LVL CHILD COMPONENTS
            for ix in range(len(durop['cpn'])):
                # ix+=1 #DEBUG ONLY
                # IF PROCUREMENT IS SET TO BUY THEN SKIP AND DON'T PULL CHILD PARTS
                if (durop.at[ix, 'procurement'] == 'Buy'):
                    # ix=ix+1
                    logging.debug(str(ix) + ' 1st')
                # CURRENTLY PULLING XX LEVEL COMPONENTS SO IF LEVEL OF CURRENT ROW IS XX-1 (OR A PARENT)
                # THEN UNPACK THE CHILDREN COL TO A TEMP DF, TAG THE CPN TO THE PARENT, AND APPEND TO THE DURO PARENT CHILD DF
                elif (durop.at[ix, 'level'] == xx-1):
                    temp = pd.DataFrame((durop.at[ix, 'children']))
                    temp['parent'] = durop.at[ix, 'cpn']
                    duropc = duropc.append(temp)
                    # ix=ix+1
                    logging.debug(str(ix) + ' 2nd')
                else:
                    # ix=ix+1
                    logging.debug(str(ix) + ' 3rd')

            # DURO API GET FOR LWR LVL COMPONENTS
            # REMOVE DUPLICATES - IF A PN IS LISTED IN TWO PLACES ON THE BOM, THE ABOVE LOOP WILL
            # ADD IT TO THE DUROPC DF 2X AND THAT WILL CREATE DUPLICATES IN THE BOM
            duropc = duropc.drop_duplicates(keep='first')

            if (duropc.size > 0):
                logging.debug("children = yes " + str(duropc.size))
                cols = ['parent'] + \
                    [col for col in duropc.columns if col != 'parent']
                duropc = duropc[cols]
                duropc = duropc.reset_index(drop=True)

                duropcc = pd.DataFrame()
                duropcc = cpns[cpns['_id'].isin(duropc['component'])]

                duropcc = duropcc.reset_index(drop=True)

                durochild = duropcc.merge(duropc[['parent', 'component', 'quantity', 'refDes']],
                                          'left', left_on='_id', right_on='component').drop(columns=['component'])
                durochild = durochild.assign(level=xx)
                cols = cols_to_move + \
                    [col for col in durochild.columns if col not in cols_to_move]
                durochild = durochild[cols]
                cols = cols_to_move + \
                    [col for col in durorent.columns if col not in cols_to_move]
                durorent = durorent[cols]

                durobom = pd.DataFrame(columns=cols)
                i = 0
                for i in range(len(durorent['cpn'])):
                    # for i in range(300):
                    durobom = durobom.append(durorent.loc[i])
                    if (durorent.at[i, 'level'] == xx-1):
                        temp = durochild[durochild['parent']
                                         == durorent.at[i, 'cpn']].copy()
                        temp['pnladder'] = durorent.at[i,'pnladder']
                        durobom = durobom.append(temp)
                    # logging.debug(i)
                    # logging.debug(durorent.at[i,'cpn'])
                    # i+=1
                durorent = durobom.reset_index(drop=True).copy()
                print('pnladder %s' % xx)
                # durorent['pnladder'] = [durorent.at[i, 'parent'] + " " +
                #                         durorent.at[i, 'cpn'] for i in range(len(durorent['cpn']))]
                durorent['pnladder'] = durorent['pnladder'].fillna('')
                # durorent['pnladder'] = durorent.apply(lambda x: x['pnladder'] + " " + x['parent'], axis=1)
                durorent.loc[(durorent['level']==xx),'pnladder'] = durorent.apply(lambda x: x['pnladder'] + " " + x['cpn'], axis=1)
            else:
                logging.debug("no children")
                lvlflag = False

    durorent.at[0, 'quantity'] = 1
    cols_to_move = ['parent',
                    'cpn',
                    'level',
                    'quantity',
                    'pnladder',
                    'name',
                    'refDes']
    cols = cols_to_move + \
        [col for col in durop.columns if col not in cols_to_move]
    durorent = durorent[cols]

    # [COMPUTING SINGLE RACK EXTENDED BOM QTY]
    duroext = durorent.copy()
    # lvls= [0,1]
    lvls = [0]
    prnt = [{'parent': duroext.at[i, 'cpn'], 'qty':duroext.at[i, 'quantity']}
            for i in range(len(duroext['cpn'])) if duroext.at[i, 'level'] in lvls]

    prnt = pd.DataFrame(prnt)
    # durorent['ext_qty'] = [durorent.at[i,'quantity']*1 for i in range(len(durorent['cpn'])) if durorent.at[i,'cpn'] in prnt]

    # x=1
    # pn = prnt.at[x, 'parent']
    # qty = prnt.at[x, 'qty']

    for y in range(len(prnt['parent'])):
        pn = prnt.at[y, 'parent']
        qty = prnt.at[y, 'qty']
        for i in range(len(duroext['cpn'])):
            # logging.debug(i)
            if (duroext.at[i, 'cpn'] == pn):
                logging.debug("pn match" + duroext.at[i, 'cpn'] + " " + pn)
                duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']
            if (duroext.at[i, 'parent'] == pn):
                logging.debug("parent match" + duroext.at[i, 'parent'] + " " + pn)
                duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']*qty

    # i = i+1
    # NEED TO ISOLATE LIST OF LEVEL 2 PARTS AND BLOW THAT THROUGH REST OF BOM, THEN 3, THEN 4 ETC
    cols_to_move = ['parent',
                    'cpn',
                    'level',
                    'quantity',
                    'ext_qty',
                    'pnladder',
                    'name',
                    'refDes']
    cols = cols_to_move + \
        [col for col in durop.columns if col not in cols_to_move]
    duroext = duroext[cols]

    # lvls = [1,2,3,4]
    lvls = duroext['level'].drop_duplicates().tolist()
    lvls = lvls[1:]
    lvls = lvls[:-1]
    logging.debug(lvls)
    logging.debug(lvls)
    for z in lvls:
        logging.debug(str(z) + " = level of bom explosion attempted")
        # chld = [{'parent': duroext.at[i, 'cpn'], 'qty':duroext.at[i, 'ext_qty']}
        #         for i in range(len(duroext['cpn'])) if duroext.at[i, 'level'] == z]
        chld = [{'parent': duroext.at[i, 'pnladder'], 'qty':duroext.at[i, 'ext_qty']}
        for i in range(len(duroext['pnladder'])) if duroext.at[i, 'level'] == z]
        chld = pd.DataFrame(chld)

        for y in range(len(chld['parent'])):
            pn = chld.at[y, 'parent']
            qty = chld.at[y, 'qty']
            for i in range(len(duroext['cpn'])):
                logging.debug(str(i) + " " + str(duroext.at[i,'pnladder'][:-12]))
                # if (duroext.at[i, 'cpn'] == pn):
                if (duroext.at[i, 'pnladder'] == pn):
                    logging.debug("pn match" + duroext.at[i, 'cpn'] + " " + pn)
                    # duroext.at[i, 'ext_qty'] = duroext.at[i,'quantity']
                if (duroext.at[i, 'pnladder'][:-12] == pn):
                    logging.debug("parent match" + duroext.at[i, 'parent'] + " " + pn)
                    duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']*qty
        logging.debug(str(z) + " = level of bom explosion completed")

    duroext['query_pn'] = oxpn
    duroext = duroext.rename(columns={
                             'sources.primarySource.leadTime.value': 'lead_time', 'sources.primarySource.leadTime.units': 'lt_units'})

    cols_to_move = ['query_pn',
                    'parent',
                    'cpn',
                    'name',
                    'category',
                    'level',
                    'procurement',  # ADDED 12/16/21
                    'quantity',
                    'ext_qty',
                    'refDes']
    duroshort = duroext[cols_to_move]
    cols = cols_to_move + \
        [col for col in duroext.columns if col not in cols_to_move]
    duroext = duroext[cols]
    hash1 = pd.util.hash_pandas_object(duroshort).sum()
    logging.debug(hash1)
    # CREATE FUNCTION FOR BUILDING BOM THAT CAN BE RERUN
    return duroext


# In[]

def s2sbuildbom_all(oxpn, oxcreds):
    """
    This fcn takes a Duro product (999) or component (XXX-XXXXXXX) CPN and returns \
        a flattened BOM. This version will not stop at any component labeled -buy-. \
        API Credentials are designed to be generated by github automation

    Parameters
    ----------
    oxpn : TYPE String
        DESCRIPTION. Duro Product or Component CPN
    oxcreds : TYPE String
        DESCRIPTION. Duro API Credentials

    Returns
    -------
    duroext : TYPE DataFrame
        DESCRIPTION. Flattened BOM 

    """
    # [DURO RACK API GET]
    # oxpn = '991-0000024'
    # oxpn = '999-0000014'  # oxpnE RACK PN
    # sandbox_api_url = 'https://public-api.staging-gcp.durolabs.xyz/v1/products/'
    prod_api_url = 'https://public-api.duro.app/v1/products/'
    comp_api_url = 'https://public-api.duro.app/v1/components/'
    api_key = oxcreds  # 16152961423738m50/MAHXgvcvj4RMROq2g==

    # creates headers for the GET query
    api_call_headers = {'x-api-key': api_key}
    api_call_params = {'cpn': oxpn}

    # GET REQUEST
    # IF PN BEGINS WITH 999 THEN PROD ELSE COMPONENT
    if oxpn[0:3] == '999':
        api_call_response = requests.get(
            prod_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    else:
        api_call_response = requests.get(
            comp_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    duro = pd.DataFrame(api_call_response.json())

    # [UNPACKING FOR CHILD COMPONENTS]

    duro['parent'] = oxpn
    duro = duro.set_index('parent')
    x = 0
    duro
    if oxpn[0:3] == '999':
        durop = unpack(duro['products'])
    else:
        durop = unpack(duro['components'])
    durop['parent'] = durop['cpn']
    durop['level'] = x

    # [DURO API GET FOR LEVEL 1 COMPONENTS]
    rent = durop.at[0, 'parent']

    cpns = s2scpn(oxcreds).drop(columns=['value'])

    # INSERTING IF ELSE HERE FOR MAKE/BUY DECISION
    # if durop['procurement'].item() != 'Buy': #REMOVED IF ELSE
    duropc = unpack(durop['children'])
    duropc = duropc.reset_index(drop=True)

    duropcc = pd.DataFrame()

    duropcc = cpns[cpns['_id'].isin(duropc['component'])]
    x = 1
    duropcc = duropcc.assign(parent=rent)
    duropcc = duropcc.assign(level=x)
    duropcc = duropcc.reset_index(drop=True)

    duropc = duropcc.merge(duropc[['component', 'quantity', 'refDes']], 'left',
                           left_on='_id', right_on='component').drop(columns=['component'])
    durorent = pd.concat([durop, duropc], axis=0).reset_index(drop=True)
    # else:
    #     durorent = durop

    print('pnladder 1')
    durorent['pnladder'] = [durorent.at[i, 'parent'] + " " +
                            durorent.at[i, 'cpn'] for i in range(len(durorent['cpn']))]
    # durorent['pnladder'] = durorent.apply(lambda x: x['pnladder'] + " " + x['cpn'], axis=1)

    # [FORMATTING L7 DF]
    lvls = [2, 3, 4, 5, 6, 7]
    # lvls=[2,3,4]
    lvlflag = True
    if len(durorent['cpn']) == 1:
        lvlflag = False
    for xx in lvls:
        # xx=6 #DEBUG ONLY
        logging.debug("level " + str(xx))
        if lvlflag:
            # FORMATTING LOWER LVL DF]
            durop = durorent
            cols_to_move = ['parent',
                            'cpn',
                            'level',
                            # 'pnladder',
                            'quantity',
                            'name',
                            'refDes']
            cols = cols_to_move + \
                [col for col in durop.columns if col not in cols_to_move]
            durop = durop[cols]

            # ix=0
            duropc = pd.DataFrame()
            # UNPACKING LWR LVL CHILD COMPONENTS
            for ix in range(len(durop['cpn'])):
                # ix+=1 #DEBUG ONLY
                # IF PROCUREMENT IS SET TO BUY THEN SKIP AND DON'T PULL CHILD PARTS
                # if (durop.at[ix, 'procurement'] == 'Buy'):
                #     # ix=ix+1
                #     logging.debug(str(ix) + ' 1st')
                # CURRENTLY PULLING XX LEVEL COMPONENTS SO IF LEVEL OF CURRENT ROW IS XX-1 (OR A PARENT)
                # THEN UNPACK THE CHILDREN COL TO A TEMP DF, TAG THE CPN TO THE PARENT, AND APPEND TO THE DURO PARENT CHILD DF
                if (durop.at[ix, 'level'] == xx-1):
                    temp = pd.DataFrame((durop.at[ix, 'children']))
                    temp['parent'] = durop.at[ix, 'cpn']
                    duropc = duropc.append(temp)
                    # ix=ix+1
                    logging.debug(str(ix) + ' 2nd')
                else:
                    # ix=ix+1
                    logging.debug(str(ix) + ' 3rd')

            # DURO API GET FOR LWR LVL COMPONENTS
            # REMOVE DUPLICATES - IF A PN IS LISTED IN TWO PLACES ON THE BOM, THE ABOVE LOOP WILL ADD
            # ADD IT TO THE DUROPC DF 2X AND THAT WILL CREATE DUPLICATES IN THE BOM
            duropc = duropc.drop_duplicates(keep='first')

            if (duropc.size > 0):
                logging.debug("children = yes " + str(duropc.size))
                cols = ['parent'] + \
                    [col for col in duropc.columns if col != 'parent']
                duropc = duropc[cols]
                duropc = duropc.reset_index(drop=True)

                duropcc = pd.DataFrame()
                duropcc = cpns[cpns['_id'].isin(duropc['component'])]

                duropcc = duropcc.reset_index(drop=True)

                durochild = duropcc.merge(duropc[['parent', 'component', 'quantity', 'refDes']],
                                          'left', left_on='_id', right_on='component').drop(columns=['component'])
                durochild = durochild.assign(level=xx)
                cols = cols_to_move + \
                    [col for col in durochild.columns if col not in cols_to_move]
                durochild = durochild[cols]
                cols = cols_to_move + \
                    [col for col in durorent.columns if col not in cols_to_move]
                durorent = durorent[cols]

                durobom = pd.DataFrame(columns=cols)
                i = 0
                for i in range(len(durorent['cpn'])):
                    # for i in range(300):
                    durobom = durobom.append(durorent.loc[i])
                    if (durorent.at[i, 'level'] == xx-1):
                        temp = durochild[durochild['parent']
                                         == durorent.at[i, 'cpn']].copy()
                        temp['pnladder'] = durorent.at[i,'pnladder']
                        durobom = durobom.append(temp)
                    # logging.debug(i)
                    # logging.debug(durorent.at[i,'cpn'])
                    # i+=1
                durorent = durobom.reset_index(drop=True).copy()
                print('pnladder %s' % xx)
                # durorent['pnladder'] = [durorent.at[i, 'parent'] + " " +
                #                         durorent.at[i, 'cpn'] for i in range(len(durorent['cpn']))]
                durorent['pnladder'] = durorent['pnladder'].fillna('')
                # durorent['pnladder'] = durorent.apply(lambda x: x['pnladder'] + " " + x['parent'], axis=1)
                durorent.loc[(durorent['level']==xx),'pnladder'] = durorent.apply(lambda x: x['pnladder'] + " " + x['cpn'], axis=1)
            else:
                logging.debug("no children")
                lvlflag = False

    durorent.at[0, 'quantity'] = 1
    cols_to_move = ['parent',
                    'cpn',
                    'level',
                    'quantity',
                    'pnladder',
                    'name',
                    'refDes']
    cols = cols_to_move + \
        [col for col in durop.columns if col not in cols_to_move]
    durorent = durorent[cols]

    # [COMPUTING SINGLE RACK EXTENDED BOM QTY]
    duroext = durorent.copy()
    # lvls= [0,1]
    lvls = [0]
    prnt = [{'parent': duroext.at[i, 'cpn'], 'qty':duroext.at[i, 'quantity']}
            for i in range(len(duroext['cpn'])) if duroext.at[i, 'level'] in lvls]

    prnt = pd.DataFrame(prnt)
    # durorent['ext_qty'] = [durorent.at[i,'quantity']*1 for i in range(len(durorent['cpn'])) if durorent.at[i,'cpn'] in prnt]

    # x=1
    # pn = prnt.at[x, 'parent']
    # qty = prnt.at[x, 'qty']

    for y in range(len(prnt['parent'])):
        pn = prnt.at[y, 'parent']
        qty = prnt.at[y, 'qty']
        for i in range(len(duroext['cpn'])):
            # logging.debug(i)
            if (duroext.at[i, 'cpn'] == pn):
                logging.debug("pn match" + duroext.at[i, 'cpn'] + " " + pn)
                duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']
            if (duroext.at[i, 'parent'] == pn):
                logging.debug("parent match" + duroext.at[i, 'parent'] + " " + pn)
                duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']*qty

    # i = i+1
    # NEED TO ISOLATE LIST OF LEVEL 2 PARTS AND BLOW THAT THROUGH REST OF BOM, THEN 3, THEN 4 ETC
    cols_to_move = ['parent',
                    'cpn',
                    'level',
                    'quantity',
                    'ext_qty',
                    'pnladder',
                    'name',
                    'refDes']
    cols = cols_to_move + \
        [col for col in durop.columns if col not in cols_to_move]
    duroext = duroext[cols]

    # lvls = [1,2,3,4]
    lvls = duroext['level'].drop_duplicates().tolist()
    lvls = lvls[1:]
    lvls = lvls[:-1]
    logging.debug(lvls)
    logging.debug(lvls)
    for z in lvls:
        logging.debug(str(z) + " = level of bom explosion attempted")
        # chld = [{'parent': duroext.at[i, 'cpn'], 'qty':duroext.at[i, 'ext_qty']}
        #         for i in range(len(duroext['cpn'])) if duroext.at[i, 'level'] == z]
        chld = [{'parent': duroext.at[i, 'pnladder'], 'qty':duroext.at[i, 'ext_qty']}
        for i in range(len(duroext['pnladder'])) if duroext.at[i, 'level'] == z]
        chld = pd.DataFrame(chld)

        for y in range(len(chld['parent'])):
            pn = chld.at[y, 'parent']
            qty = chld.at[y, 'qty']
            for i in range(len(duroext['cpn'])):
                logging.debug(str(i) + " " + str(duroext.at[i,'pnladder'][:-12]))
                # if (duroext.at[i, 'cpn'] == pn):
                if (duroext.at[i, 'pnladder'] == pn):
                   logging.debug("pn match" + duroext.at[i, 'cpn'] + " " + pn)
                    # duroext.at[i, 'ext_qty'] = duroext.at[i,'quantity']
                if (duroext.at[i, 'pnladder'][:-12] == pn):
                    logging.debug("parent match" + duroext.at[i, 'parent'] + " " + pn)
                    duroext.at[i, 'ext_qty'] = duroext.at[i, 'quantity']*qty
        logging.debug(str(z) + " = level of bom explosion completed")

    duroext['query_pn'] = oxpn
    duroext = duroext.rename(columns={
                             'sources.primarySource.leadTime.value': 'lead_time', 'sources.primarySource.leadTime.units': 'lt_units'})

    cols_to_move = ['query_pn',
                    'parent',
                    'cpn',
                    'name',
                    'category',
                    'level',
                    'procurement',  # ADDED 12/16/21
                    'quantity',
                    'ext_qty',
                    'refDes']
    duroshort = duroext[cols_to_move]
    cols = cols_to_move + \
        [col for col in duroext.columns if col not in cols_to_move]
    duroext = duroext[cols]
    hash1 = pd.util.hash_pandas_object(duroshort).sum()
    logging.debug(hash1)
    # CREATE FUNCTION FOR BUILDING BOM THAT CAN BE RERUN
    return duroext

# In[]
def clean_mps(oxmps):
    """
    This function will take the MPS from the Ops > Forecast/Master Schedule
    GDrive and clean and pivot for use in Prod Scheduling as an Order list by
    PN, Qty, and Lot

    Parameters
    ----------
    oxmps : TYPE Pandas DataFrame
        DESCRIPTION. Dataframe of MPS pulled FROM Ops > Forecast/Master Schedule

    Returns
    -------
    oxmps : TYPE Pandas DataFrame
        DESCRIPTION. Pivotted and cleaned, tidy MPS for use in Prod Scheduling

    """
    # oxmps = oxmps.iloc[0:9, 2:108]
    oxmps = oxmps.iloc[0:5, 2:160]
    oxmps = oxmps.iloc[2:5, :]
    oxmps = oxmps.drop(columns=['ISO Week']).rename(columns={'Unnamed: 2': 'cpn'})
    oxmps = oxmps.fillna(0)
    oxmps = pd.melt(oxmps, id_vars='cpn')
    oxmps = oxmps.rename(columns={'variable': 'wk', 'value': 'qty'})
    oxmps['wk'] = oxmps['wk'].apply(lambda x: str(x))
    oxmps['year'] = oxmps.apply(lambda x: 2023 if '.1' in x['wk'] else (2024 if '.2' in x['wk'] else 2022), axis=1)
    oxmps['wk'] = oxmps['wk'].apply(lambda x: x.replace('.1', ''))
    oxmps['wk'] = oxmps['wk'].apply(lambda x: x.replace('.2', ''))
    oxmps['wk'] = oxmps['wk'].apply(lambda x: int(x))
    oxmps = oxmps[oxmps['qty'] != 0]
    cols = ['cpn',
            'year',
            'wk',
            'qty']
    oxmps = oxmps[cols]
    
    oxmps['lot_num'] = ''
    oxmps['lot_num'] = oxmps.apply(
        lambda x: 'lot1' if x['year'] == 2023 else ('lot5' if x['year'] == 2024 else 'dvt'), axis=1)
    oxmps.loc[(oxmps['year'] == 2022) & (oxmps['wk'] < 40), 'lot_num'] = 'evt'
    oxmps.loc[(oxmps['year'] == 2023) & (oxmps['wk'] < 9), 'lot_num'] = 'pvt'
    oxmps.loc[(oxmps['year'] == 2023) & (oxmps['wk'] > 13), 'lot_num'] = 'lot2'
    oxmps.loc[(oxmps['year'] == 2023) & (oxmps['wk'] > 26), 'lot_num'] = 'lot3'
    oxmps.loc[(oxmps['year'] == 2023) & (oxmps['wk'] > 39), 'lot_num'] = 'lot4'

    oxmps.loc[(oxmps['year'] == 2024) & (oxmps['wk'] > 13), 'lot_num'] = 'lot6'
    oxmps.loc[(oxmps['year'] == 2024) & (oxmps['wk'] > 26), 'lot_num'] = 'lot7'
    oxmps.loc[(oxmps['year'] == 2024) & (oxmps['wk'] > 39), 'lot_num'] = 'lot8'

    oxmps['lot_ord'] = oxmps[oxmps['lot_num'] != 'planning fence'].groupby(
        ['lot_num']).cumcount()+1
    oxmps['lot_ord'] = oxmps['lot_ord'].fillna(0)
    oxmps = oxmps.reset_index(drop=True)
    return oxmps