#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 14:11:29 2022

@author: danielsutter
"""
from __future__ import print_function
import logging
import math
import time

import requests
from requests import HTTPError
import yaml
import json
import datetime as dt

import pandas as pd
from pyairtable import Api, Table
import ds_utils as ds

# import os.path
# from googleapiclient.discovery import build
# from google_auth_oauthlib.flow import InstalledAppFlow
# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials

requests.packages.urllib3.disable_warnings()

# Opening creds.yml and assigning Duro creds
with open("./creds.yml", 'r') as stream:
    allcreds = yaml.safe_load(stream)
durocreds = allcreds['oxide_duro']
api_key = durocreds['api_key'] # 16152961423738m50/MAHXgvcvj4RMROq2g==

# Setting the logger up
with open("./config.yml", 'r') as stream:
    oxconfig = yaml.safe_load(stream)
loglvl = oxconfig['logging_configs']['level']
logging.basicConfig(filename=('./gitlogs/bomrevqry_' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

st = dt.datetime.now()
# In[CPN QUERY FOR LATEST MODIFIED == FALSE CPNS]

# =============================================================================
# logging.debug('START BUILDING GET QUERY')
# # creates headers for the GET query
# api_call_headers = {'x-api-key': api_key}
# api_call_params = {'cpn': '*',
#                    'perPage': 100,
#                    'page': 1}
#                    # 'revision': '6238fc7680b6e10008fb1576'}
#                    # 'status': 'DESIGN'}
# 
# # Duro components URL with CPN set to wildcat,
# # results perPage set to 100,
# # and page ready to iterate
# comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
# # comp_api_url = 'https://public-api.duro.app/v1/components/'
# # comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&modified=False&page='
# page = 1
# qry = comp_api_url + str(page)
# 
# # GET REQUEST
# api_call_response = requests.get(
#     qry, 
#     headers=api_call_headers, 
#     # params=api_call_params,
#     verify=False)
# 
# # This returns the record count ['resultCount'] and sets it to a variable
# tot = api_call_response.json()['resultCount']
# 
# # Send 1 page of results to log for debug
# logging.debug(api_call_response.json())
# 
# # Page 1 of Results
# cpnbom = pd.DataFrame(api_call_response.json())
# cpnbom = ds.unpack(cpnbom['components'])
# 
# # Using the record count and page size, we get the number of pages we need to query
# rnge = list(range(2, (math.ceil(tot/100)+1)))
# for x in rnge:
#     logging.debug(x)
#     # comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&modified=False&page='
#     comp_api_url = 'https://public-api.duro.app/v1/components/?cpn:*&perPage=100&page='
#     page = x
#     qry = comp_api_url + str(page)
#     api_call_response = requests.get(
#         qry, headers=api_call_headers, verify=False)
#     temp = pd.DataFrame(api_call_response.json())
#     temp = ds.unpack(temp['components'])
#     cpnbom = cpnbom.append(temp)
# 
# cpns = cpnbom
# =============================================================================

cpns = ds.cpn()

# In[CREDS FILE UPLOAD AND API PARAMS]

prod_api_url = 'https://public-api.duro.app/v1/product/revision/'
comp_api_url = 'https://public-api.duro.app/v1/component/revision/'

oxpn = '999-0000014'

# creates headers for the GET query
api_call_headers = {'x-api-key': api_key}
api_call_params = {'cpn': oxpn}



# In[API CALL RESPONSE AND UNPACK]
# ['6238fc7680b6e10008fb1576', '62ba0cdc77e47100096fc554', '62e95be84f3e8500096285a6', '62fbcf2a4a5173000864a419', '62fbd88cb017550009084a64', '62fd3ccc816887000857abd6']
# GET REQUEST
# IF PN BEGINS WITH 999 THEN PROD ELSE COMPONENT
if oxpn[0:3] == '999':
    api_call_response = requests.get(
        prod_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    duro = pd.DataFrame(api_call_response.json())
    rev = ds.unpack(duro['productRevisions'])
else:
    api_call_response = requests.get(
        comp_api_url, headers=api_call_headers, params=api_call_params, verify=False)
    duro = pd.DataFrame(api_call_response.json())
    rev = ds.unpack(duro['componentRevisions'])

# In[PCBA MODIFIED = FALSE]
# THIS IS OUR TOP LEVEL ASSEMBLY (AT LEAST AS FAR AS THE QUERY IS CONCERNED)
# FIRST WE SORT BY CREATED DATE
tla = rev.sort_values(by=['created'], ascending=False)
# THEN WE GROUP BY MODIFIED AND TAKE THE FIRST INSTANCE OF EACH
# SINCE ITS BOOLEAN IT'LL BE 1 OR 2 RESULTS
tla = tla.groupby('modified').first().reset_index()
# WE NEED THE MODIFIED == FALSE RESULT
tla = tla[tla['modified'] == False].reset_index(drop=True)
# WE WANT TO RECORD PARENT TO CHILD RELATIONSHIP SO THAT START HERE
tla['parent'] = oxpn
tla['level'] = 0
tla['quantity'] = 1

# UNPACKING THE CHILDREN ARRAY INTO A SUB ASSEMBLY DF
tla_subs = ds.unpack(tla['children'])

# RENAMING SOME FIELDS IN THIS DF TO SHOW ITS THE CHILD COMPONENT
tla_subs = tla_subs.rename(columns={'component':'chld_component', 'assemblyRevision':'chld_aR'})

# SORTING THE SUBS DF BY CHILD COMPONENT
tla_subs = tla_subs.sort_values(by=['chld_component'], axis=0)

# THIS MERGE GIVES US THE CPN FOR EACH COMPONENT ID
tla_subs_cpns = tla_subs.merge(cpns, 'left', left_on='chld_component', right_on='_id')

# ASSIGN THE BOM LEVEL AS 1 AND SET THE PARENT PN = THE TLA
tla_subs_cpns['level'] = 1
tla_subs_cpns['parent'] = oxpn

# ORGANIZE THE DF TO BE A LITTLE MORE LEGIBLE
cols_to_move = ['parent',
                'cpn',
                'level',
                'quantity',
                'name',
                'refDes',
                'chld_component',
                'chld_aR',
                'category',
                'revision',
                'children']
cols = cols_to_move + \
    [col for col in tla_subs_cpns.columns if col not in cols_to_move]

# SORT BY CPN DESCENDING SO ASSEMBLIES ARE ON THE TOP
tla_subs_cpns = tla_subs_cpns[cols].sort_values(by=['cpn'],
                                               ascending=False)
tla_subs_cpns = tla_subs_cpns.reset_index(drop=True)

# In[Loop on Child Components with Rev GET]
# st = dt.datetime.now()

# =============================================================================
# # THIS WORKS - THIS JOIN GIVES US THE TLA AND SUBS IN ONE DF
# # REDUNDANT BUT HELPS WITH DEBUGGING
# tla_ext = tla.append(tla_subs_cpns)
# 
# cols_to_move = ['parent',
#                 'cpn',
#                 'level',
#                 'quantity',
#                 'name',
#                 'refDes',
#                 'chld_component',
#                 'chld_aR',
#                 'category',
#                 'revision',
#                 'children']
# cols = cols_to_move + \
#     [col for col in tla_ext.columns if col not in cols_to_move]
# tla_ext = tla_ext[cols]
# tla_ext = tla_ext.reset_index(drop=True)
# tla_ext = tla_ext.sort_values(by=['cpn'], ascending=False)
# 
# 
# tla = tla_ext[tla_ext['cpn']==oxpn].reset_index(drop=True).copy()
# 
# subs = tla_ext[tla_ext['cpn']!=oxpn].reset_index(drop=True).copy()
# 
# # y = 4 #DEBUG ONLY
# for y in range(len(subs['cpn'])):
#     # oxpn = '910-0000019'
#     oxpn = subs.at[y, 'cpn']
#     logging.info('loop %s PN ' %y + subs.at[y, 'cpn'])
#     print('loop %s PN ' %y + subs.at[y, 'cpn'])
#     # creates headers for the GET query
#     api_call_headers = {'x-api-key': api_key}
#     api_call_params = {'cpn': subs.at[y, 'cpn']}
# 
#     
#     if oxpn[0:3] == '999':
#         api_call_response = requests.get(
#             prod_api_url, headers=api_call_headers, params=api_call_params, verify=False)
#         duro = pd.DataFrame(api_call_response.json())
#         rev = ds.unpack(duro['productRevisions'])
#     else:
#         api_call_response = requests.get(
#             comp_api_url, headers=api_call_headers, params=api_call_params, verify=False)
#         duro = pd.DataFrame(api_call_response.json())
#         rev = ds.unpack(duro['componentRevisions'])
# 
#     subs_revs = rev.sort_values(by=['created'], ascending=False)
#     logging.debug(tla_subs_cpns.at[y,'chld_aR'])
#     print(tla_subs_cpns.at[y,'chld_aR'])
#     logging.debug(subs_revs['_id'])
#     subs_revs = subs_revs[subs_revs['_id']==tla_subs_cpns.at[y,'chld_aR']]
#     subs_subs = ds.unpack(subs_revs['children'])
#     logging.debug('child_child size = ' + str(subs_subs.size))
#     print('child_child size = ' + str(subs_subs.size))
#     if subs_subs.size != 0:
#         subs_subs = subs_subs.rename(columns={'component':'chld_component', 'assemblyRevision':'chld_aR'})
#         subs_subs = subs_subs.sort_values(by=['chld_component'], axis=0)
#         subs_subs_cpns = subs_subs.merge(cpns, 'left', left_on='chld_component', right_on='_id')
#         subs_subs_cpns['level'] = 2
#         subs_subs_cpns['parent'] = subs.at[y, 'cpn']
#         
#         cols_to_move = ['parent',
#                         'cpn',
#                         'level',
#                         'quantity',
#                         'name',
#                         'refDes',
#                         'chld_component',
#                         'chld_aR',
#                         'category',
#                         'revision',
#                         'children']
#         cols = cols_to_move + \
#             [col for col in subs_subs_cpns.columns if col not in cols_to_move]
#         subs_subs_cpns = subs_subs_cpns[cols]
#         
#         tla = tla.append(subs.loc[y]).reset_index(drop=True)
#         tla = tla.append(subs_subs_cpns).reset_index(drop=True)
#     else:
#         tla = tla.append(subs.loc[y]).reset_index(drop=True)
# =============================================================================

# THIS JOIN GIVES US THE TLA AND SUBS IN ONE DF
# REDUNDANT BUT HELPS WITH DEBUGGING
tla_ext = tla.append(tla_subs_cpns)

cols_to_move = ['parent',
                'cpn',
                'level',
                'quantity',
                'name',
                'refDes',
                'chld_component',
                'chld_aR',
                'category',
                'revision',
                'children']
cols = cols_to_move + \
    [col for col in tla_ext.columns if col not in cols_to_move]
tla_ext = tla_ext[cols]
tla_ext = tla_ext.reset_index(drop=True)
tla_ext = tla_ext.sort_values(by=['cpn'], ascending=False)
tla_lvl1 = tla_ext

# tla = tla_ext[tla_ext['cpn']==oxpn].reset_index(drop=True).copy()

# subs = tla_ext[tla_ext['cpn']!=oxpn].reset_index(drop=True).copy()

# In[]
# [FORMATTING L7 DF]
lvls = [1, 2, 3, 4, 5, 6, 7]
# lvls = 1
more_subs = tla_ext[tla_ext['level']==1]
more_subs = more_subs['children']
# # lvls=[2,3,4]
lvlflag = True
tla = pd.DataFrame()
# if len(durorent['cpn']) == 1:
if len(more_subs) == 0:
    lvlflag = False
for xx in lvls:
    # xx=3 #DEBUG ONLY
    logging.debug("level " + str(xx))
    if lvlflag:
        # y=3 #DEBUG ONLY
        # xx=1 #DEBUG ONLY
        for y in range(len(tla_ext['cpn'])):
            # oxpn = '910-0000019'
            oxpn = tla_ext.at[y, 'cpn']
            logging.info('loop %s PN ' %y + tla_ext.at[y, 'cpn'])
            print('loop %s PN ' %y + tla_ext.at[y, 'cpn'])
        
            if tla_ext.at[y, 'level'] < xx:
                tla = tla.append(tla_ext.loc[y])#.reset_index(drop=True)
            else:
                yn_chlds = tla_ext.at[y,'children']
                logging.info('Child PN Array length = ' + str(len(yn_chlds)))
                print('Child PN Array length = ' + str(len(yn_chlds)))
                if len(yn_chlds) != 0:    
                    # creates headers for the GET query
                    api_call_headers = {'x-api-key': api_key}
                    api_call_params = {'cpn': tla_ext.at[y, 'cpn']}
                
                    
                    if oxpn[0:3] == '999':
                        api_call_response = requests.get(
                            prod_api_url, headers=api_call_headers, params=api_call_params, verify=False)
                        duro = pd.DataFrame(api_call_response.json())
                        rev = ds.unpack(duro['productRevisions'])
                    else:
                        api_call_response = requests.get(
                            comp_api_url, headers=api_call_headers, params=api_call_params, verify=False)
                        duro = pd.DataFrame(api_call_response.json())
                        rev = ds.unpack(duro['componentRevisions'])
                
                    subs_revs = rev.sort_values(by=['created'], ascending=False)
                    logging.debug(tla_ext.at[y,'chld_aR'])
                    print(tla_ext.at[y,'chld_aR'])
                    logging.debug(subs_revs['_id'])
                    print(subs_revs['_id'])
                    subs_revs = subs_revs[subs_revs['_id']==tla_ext.at[y,'chld_aR']]
                    subs_subs = ds.unpack(subs_revs['children'])
                    logging.debug('child_child size = ' + str(subs_subs.size))
                    print('child_child size = ' + str(subs_subs.size))
                    subs_subs = subs_subs.rename(columns={'component':'chld_component', 'assemblyRevision':'chld_aR'})
                    subs_subs = subs_subs.sort_values(by=['chld_component'], axis=0)
                    subs_subs_cpns = subs_subs.merge(cpns, 'left', left_on='chld_component', right_on='_id')
                    subs_subs_cpns['level'] = xx+1
                    subs_subs_cpns['parent'] = tla_ext.at[y, 'cpn']
                    
                    cols_to_move = ['parent',
                                    'cpn',
                                    'level',
                                    'quantity',
                                    'name',
                                    'refDes',
                                    'chld_component',
                                    'chld_aR',
                                    'category',
                                    'revision',
                                    'children']
                    cols = cols_to_move + \
                        [col for col in subs_subs_cpns.columns if col not in cols_to_move]
                    subs_subs_cpns = subs_subs_cpns[cols]
                    
                    tla = tla.append(tla_ext.loc[y])#.reset_index(drop=True)
                    tla = tla.append(subs_subs_cpns)#.reset_index(drop=True)
                else:
                    tla = tla.append(tla_ext.loc[y])#.reset_index(drop=True)
                cols_to_move = ['parent',
                'cpn',
                'level',
                'quantity',
                'name',
                'refDes',
                'chld_component',
                'chld_aR',
                'category',
                'revision',
                'children']
                cols = cols_to_move + \
                    [col for col in tla.columns if col not in cols_to_move]
                tla = tla[cols]

        # RESET THE INDEX AFTER THE UNPACK CHILD COMPONENT LOOP
        tla = tla.reset_index(drop=True)
        # FIND ANY MORE CHILD COMPS, IF NONE, SET LVLFLAG = TRUE
        more_subs = tla[tla['level']==xx+1]
        more_subs = ds.unpack(more_subs['children'])
        if len(more_subs['component']) == 0:
            lvlflag = False
        
        tla_ext = tla
        tla = pd.DataFrame()

# cols_to_move = ['parent',
#                 'cpn',
#                 'level',
#                 'quantity',
#                 'name',
#                 'refDes',
#                 'chld_component',
#                 'chld_aR',
#                 'category',
#                 'revision',
#                 'children']
# cols = cols_to_move + \
#     [col for col in tla.columns if col not in cols_to_move]
# tla = tla[cols]
nd = dt.datetime.now()
print(nd-st)