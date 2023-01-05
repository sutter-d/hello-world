#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 13:30:28 2022

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

import ds_utils as ds
import gdrive_get_s2s as get
# Here we are pulling the meta data for the ops auto > reports folder
# We need to find the latest version of the comp forecast file to
# Copy over the notes and comments

drive_id = '0AKcpdSVwv34AUk9PVA' #oxide shared drive id - reqd for meta data pull
flder_id = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco' #ops auto > reports folder id

df = get.get_list(drive_id, flder_id)
df = df[df['name'].str.startswith(
    'Oxide ')].reset_index(drop=True)
df = df.loc[0, 'id']
logging.debug("File ID: " + df)

get.get_file(df, 'ox_prod_inv')

# In[]
# CAN'T USE stdcost.py BECAUSE THAT FILE FILTERS OUT PROCUREMENT TRACKER RECORDS
# WITHOUT A UNIT COST SO THEY WON'T IMPACT THE STANDARD COST WHEN AVERAGED
logging.info("START PULLING PROCUREMENT TRACKER FROM GDRIVE")

inv_gdrive = './data/ox_prod_inv.xlsx'
open_po_get = pd.read_excel(inv_gdrive,
                         sheet_name='Open PO',
                         header=1)

on_hand_get = pd.read_excel(inv_gdrive,
                         sheet_name='Inventory on hand',
                         header=1)

open_po = open_po_get[['Part Number', 'Order', 'Seq', 'Quantity']].copy()
open_po['open_orders'] = open_po.apply(lambda x: x['Seq'] + x['Quantity'], axis=1)
open_po = open_po.groupby(by=['Part Number']).agg(sum)
open_po = open_po.drop(columns=['Seq', 'Quantity'])
# open_po = open_po.rename(columns={'Part Number':'cpn'})

on_hand = on_hand_get[['Part', 'Description', 'Quantity']].copy()
on_hand['Part'] = on_hand['Part'].map(lambda x: x.replace('OXC', ''))
on_hand['cpn'] = on_hand['Part'].map(lambda x: x[:11])
# on_hand = on_hand.groupby(by=['cpn']).agg(sum)
# TEMPORARY UNTIL ENGINEERING INVENTORY IS MOVED TO PRODUCTION
# REMOVE HERE DOWN 
on_hand = on_hand.groupby(by=['cpn'], as_index=False).agg(sum)
on_hand = on_hand.rename(columns={'Quantity':'on_hand'})

eng_gdrive = './data/ox_eng_inv.xlsx'
eng_oh_get = pd.read_excel(eng_gdrive,
                           sheet_name='Summary',
                           header=0)
eng_oh = eng_oh_get.copy()
eng_oh['cpn'] = eng_oh['cpn'].fillna('-')
eng_oh = eng_oh[eng_oh['cpn']!='-']
eng_oh = eng_oh.groupby(by=['cpn'], as_index=False).agg(sum)
eng_oh = eng_oh.rename(columns={'total_qty':'on_hand'})

on_hand = pd.concat((on_hand, eng_oh)).reset_index(drop=True)
on_hand = on_hand.groupby(by=['cpn']).agg(sum)
# REMOVE HERE UP

inv = pd.concat((open_po, on_hand), axis=1)

inv = inv.reset_index(drop=False)
inv = inv.fillna(0)

inv['total_qty'] = inv.apply(lambda x: x['open_orders'] + x['on_hand'], axis=1)
