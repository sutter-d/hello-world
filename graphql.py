#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 17:02:13 2022

@author: danielsutter
"""

import logging
import time
import datetime as dt
import sys

import yaml
import pandas as pd
import requests
import argparse
import json as json

import ds_utils as ds
import sgqlc

requests.packages.urllib3.disable_warnings()

with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('./gitlogs/compforecast' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

with open("./creds.yml", 'r') as stream:
    opscreds = yaml.safe_load(stream)

# In[]
# api_url = 'https://core-api.staging-gcp.durolabs.xyz/'
api_url = 'https://core-api.staging-gcp.durolabs.xyz/graphql'
api_key = opscreds['oxide_duro']['graphql_key']

qry = """query {
            componentRevision{
                cpn
            }
        }"""

# qry = """ query {
#   components(orderBy: [{created:asc}]) {
#     {
#         id
#         name
#         created
#         lastModified
#     }
#   }
# }"""

qry = """query {
  componentById(id: "631a6432e064510009b2c5c7") {
    id
    alias
    category
    name
    mode
    cpn {
      prefix
      counter
      displayValue
    }
    revision
    revisions {
      id
      name
      # etc...
    }
    # etc...
 }
}"""

qry = json.dumps('component { id name cpn}')

qry = """
query {
  components(orderBy: [{created:asc}]) {
    connection(
      first: 10
    ) {
      totalCount
      edges {
        cursor
        node {
          id
          name
          created
          lastModified
        }
      }
    }
  }
}"""



# creates headers for the GET query
api_call_headers = {'apiToken': api_key}
api_call_params = {'query': qry}

api_call_response = requests.get(api_url, headers = api_call_headers, params = api_call_params, verify=False)
print(api_call_response.text)

# json_data = json.loads(api_call_response.text)

# In[]

# qry = """query {
#             results{
#                 cpn
#                 description
#                 children
#             }
#         }"""

# qry = """ query {
#   components(orderBy: [{created:asc}]) {
#     {
#         id
#         name
#         created
#         lastModified
#     }
#   }
# }"""

json_data = json.loads(api_call_response.text)


duro = pd.DataFrame(json_data)
duro = ds.unpack(duro)

