#!usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:59:01 2021

@author: danielsutter
"""

import logging
# import math
import time
# import datetime as dt
# import base64

# import json
import yaml
import pandas as pd
# from pyairtable import Table
import requests

import restapi as oxrest
# import openorders as oxopn

requests.packages.urllib3.disable_warnings()
with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/cpn_distr_lts_' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')


with open("./creds.yml", 'r') as stream:
    allcreds = yaml.safe_load(stream)
durocreds = allcreds['oxide_duro']

proc_update = False

"""
# =============================================================================
# # ORDER OF THIS SCRIPT-BUILD BOM TOP DOWN
# # 1. QUERY DURO API FOR RACK PARENT PRODUCT DETAILS
# # 2. UNPACK GET QUERY DETAILS AND PULL CHILD COMPONENTS
# # REQUERY API FOR CHILD COMPONENT DETAILS (REPEAT 1 AND 2)
# # APPEND RESULTS TO DATAFRAME AND BUILD BOM FILE
# # MULTIPLY ASSEMBLY QUANTITIES TOP DOWN TO GET TOTAL BOM QTY PER PN
# # UNPACK PRODUCTION SCHEDULE FROM DURO OR QUERY FROM AIRTABLE
# # MULTIPLY FORECAST BY TOTAL BOM QTY (EXTENDED QTY) TO GET TOTAL FORECASTED DEMAND
# =============================================================================
"""


# In[PULL DURO EXISTING BOM]
"""
PULL DURO EXISTING BOM
"""
# oldbom = oxrest.buildbom(duroid, durocreds)
oldbom = oxrest.cpn()
manf = oldbom[['cpn', 'sources.manufacturers']].set_index(['cpn'])
# UNPACK NESTED DICT SOURCES.MANUFACTURERS AND RESET INDEX
manf = oxrest.unpack(manf['sources.manufacturers']).reset_index(drop=False)
# lt = oxrest.unpack(manf['leadTime'])
# UNPACK DISTRIBUTORS COL AND RESET INDEX
dst = oxrest.unpack(manf['distributors']).reset_index(drop=False)
# UNPACK QUOTES COL, STRIP OUT ALL 1 PIECE QUOTES AND CHANGE TYPE TO INT
qts = oxrest.unpack(dst['quotes'])
qts = qts[qts['minQuantity']>1]
qts['minQuantity'] = qts['minQuantity'].astype(int)

manf = manf.reset_index(drop=False).rename(columns={'index': 'manf_ind',
                                                    'ind': 'cpn',
                                                    'name': 'manf_name',
                                                    'description': 'manf_desc'})

# DST IND COL IS KEY BACK TO MANF INDEX COL
# RENAME IND COL TO MANFIND, RESET AND KEEP INDEX AND RENAME DSTIND
dst = dst.reset_index(drop=False).rename(columns={'index': 'dst_ind',
                                                  'ind': 'manf_ind',
                                                  'name': 'dst_name',
                                                  'description': 'dst_desc'})
pkg = oxrest.unpack(dst['package'])
dst = pd.concat([dst, pkg],
                axis=1)
# QTS IND COL IS KEY BACK TO DST INDEX COL
# RENAME IND COL TO DSTIND, RESET INDEX
qts = qts.reset_index(drop=False).rename(columns={'ind': 'dst_ind',
                                                  'minQuantity':'min_qty',
                                                  'unitPrice':'unit_price'})
srcs = manf.merge(dst, 'left', 'manf_ind')
srcs = srcs.merge(qts, 'left', 'dst_ind')


# DROP NULL ROWS MISSING DST AND QTS INFO
srcs = srcs[srcs['min_qty'].notnull()].reset_index(drop=True)
lt = oxrest.unpack(srcs['leadTime_y'])
srcs = pd.concat([srcs, lt],
                 axis=1)

cols_to_move = ['cpn',
                'manf_name',
                'manf_desc',
                'dst_name',
                'dst_desc',
                'type',
                'min_qty',
                'unit_price',
                'units',
                'value']

srcs = srcs[cols_to_move]
