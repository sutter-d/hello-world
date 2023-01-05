#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 09:57:57 2022

@author: danielsutter
"""
import yaml
from sgqlc.endpoint.http import HTTPEndpoint
from sgqlc.types import Type, Field, list_of
from sgqlc.types.relay import Connection, connection_args
from sgqlc.operation import Operation

with open("./creds.yml", 'r') as stream:
    opscreds = yaml.safe_load(stream)

url = 'https://core-api.staging-gcp.durolabs.xyz/graphql'
api_key = opscreds['oxide_duro']['graphql_key']
headers = {'apiToken': api_key}

qry ="""
query {
    components(orderBy: [{created:asc}]) {
        connection(first: 10) {
            edges {
                node {
                    id
                    name
                    created
                    lastModified
                    cpn {
                        id
                        displayValue
                        }
                }
            }
        }
    }
}"""
# variables = {'varName': 'value'} #NOT USING VARIABLES RIGHT NOW

endpoint = HTTPEndpoint(url, headers)
data = endpoint(qry)

# In[SGQLC SYNTAX]

# =============================================================================
# # Declare types matching GitHub GraphQL schema:
# class Component(Type):
#     id = int
#     name = str
#     created = str
#     lastModified = str
# 
# # class Cpn(Type):
# #     id = int
# #     displayValue = str
# 
# class ComponentConnection(Connection):  # Connection provides page_info!
#     edge = list_of(ComponentEdges)
# 
# class ComponentEdge(Type):
#     node = 
# 
# class Components(Type):
#      component = Field(Connection, args=connection_args())
# 
# class Query(Type):  # GraphQL's root
#     components = Field(Components, args={'orderBy': str, 'created': str})
# =============================================================================
