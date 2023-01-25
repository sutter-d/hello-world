#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 20 14:52:18 2022

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
import datetime as dt

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import ds_utils as ds
import gdrive_get_s2s as get
import gdrive_post_s2s as post

requests.packages.urllib3.disable_warnings()
with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('./gitlogs/duro_mrp' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

# In[]


def mrp_build(oxrack, oxlts, oxordr):
    """
    THIS BUILDS THE MRP DATAFRAME USING THE PRODUCT STRUCTURE FROM DURO, 
    LEADTIME DATA MAINTAINED ON GDRIVE, AND MPS DATA MAINTAINED ON GDRIVE

    Parameters
    ----------
    oxrack : TYPE dataframe
        DESCRIPTION. This is a rack export from Duro built using ds.s2sbuildbom()
    oxlts : TYPE dataframe
        DESCRIPTION. This is the lead time info for the Rack, TLAs, and some
        subs and PCBAs depending on info available. The file is stored on GDrive
        with an id of 1D0PcawQYmnFd5F8BcNRK9fILWji78iOD at Ops > OpsAuto
    oxordr : TYPE dataframe
        DESCRIPTION. This is a manipulation of the 'MPS' tab of the Master
        Schedule file stored on GDrive at Ops > Forecast / Master Schedule >
        with an id of 1UXyOtpZ9OEL3SmTq-Ak0pUUlVz1Q6mS2

    Returns
    -------
    oxmrp : TYPE dataframe
        DESCRIPTION. This is a thin dataframe with cpn, due date and year, nbd
        and year and required qty for use in the mrp table

    """
    # [COMBINING RACK AND LT DFs AND FINDING NEED BY DATE]
    oxordr = oxordr.reset_index(drop=True)
    # oxmrp = oxrack.merge(oxlts[['cpn', 'lt']], 'left', 'cpn')
    oxmrp = oxrack.merge(oxlts, 'left', 'cpn')
    oxmrp['lt'] = oxmrp['lt'].fillna(0)
    oxmrp['due_date'] = None
    oxmrp['due_year'] = None
    oxmrp['need_by_date'] = None

    oxmrp['nbd_year'] = None
    oxmrp['ord_qty'] = oxmrp.apply(
        lambda x: oxordr.at[0, 'qty'] * x['ext_qty'], axis=1)

    oxmrp['due_date'] = oxmrp.apply(
        lambda x: oxordr.at[0, 'wk'] if x['cpn'] == oxordr.at[0, 'cpn'] else None, axis=1)
    oxmrp['need_by_date'] = oxmrp.apply(
        lambda x: x['due_date']-x['lt'] if x['due_date'] != None else None, axis=1)
    oxmrp['due_year'] = oxordr.at[0, 'year']
    oxmrp['nbd_year'] = oxordr.at[0, 'year']

    par_nbd = oxmrp.copy()

    # bom_lvls = [1,2,3,4]
    bom_lvls = oxmrp['level']
    bom_lvls = bom_lvls.drop_duplicates().reset_index(drop=True)
    bom_lvls = bom_lvls.loc[1:].reset_index(drop=True)
    for lvls in bom_lvls:
        logging.debug(lvls)
        for x in range(len(oxmrp['cpn'])):
            if oxmrp.at[x, 'level'] == lvls:
                logging.debug(oxmrp.at[x, 'cpn'])
                tmp = par_nbd[par_nbd['cpn'] ==
                              oxmrp.at[x, 'parent']].reset_index(drop=True)
                logging.debug(tmp)
                logging.debug(tmp.at[0, 'need_by_date'])
                oxmrp.at[x, 'due_date'] = tmp.at[0, 'need_by_date']
                oxmrp.at[x, 'due_year'] = tmp.at[0, 'nbd_year']
                logging.debug(oxmrp.at[x, 'due_date'])
                logging.debug('yes')

        oxmrp['need_by_date'] = oxmrp.apply(
            lambda x: x['due_date']-x['lt'], axis=1)
        par_nbd = oxmrp.copy()
    # [CORRECTING FOR DATES ROLLING INTO A PREVIOUS CALENDAR YEAR]
    for x in range(len(oxmrp['cpn'])):
        logging.debug(oxmrp.at[x, 'cpn'])
        logging.debug(oxmrp.loc[x, :])
        if oxmrp.at[x, 'need_by_date'] < 0:
            tmp = oxmrp.at[x, 'need_by_date']
            tmp = tmp + 52
            oxmrp.at[x, 'need_by_date'] = tmp
            tmp = oxmrp.at[x, 'nbd_year']
            tmp = tmp - 1
            oxmrp.at[x, 'nbd_year'] = tmp
        if oxmrp.at[x, 'due_date'] < 0:
            tmp = oxmrp.at[x, 'due_date']
            tmp = tmp + 52
            oxmrp.at[x, 'due_date'] = tmp
            tmp = oxmrp.at[x, 'due_year']
            tmp = tmp - 1
            oxmrp.at[x, 'due_year'] = tmp
        logging.debug(oxmrp.loc[x, :])
    # [CONDENSING THE COLS TO SAVE FOR oxmrp]
    oxmrp['order'] = str(oxordr.at[0, 'lot_num']) + \
        "-" + str(oxordr.at[0, 'lot_ord'])
    cols_to_move = ['order',
                    'query_pn',
                    'pnladder',
                    'parent',
                    'cpn',
                    'name',
                    'category',
                    'level',
                    'lt',
                    'due_date',
                    'due_year',
                    'need_by_date',
                    'nbd_year',
                    'ord_qty']
    oxmrp = oxmrp[cols_to_move]

    return oxmrp


# In[GRABBING LT FILE FOR RACK AND ALL TLA, SUBS, AND PCBAs]
# lt_id = '1D0PcawQYmnFd5F8BcNRK9fILWji78iOD'
# lt_name = 'Rack.LTs'
# lt_path = './data/Rack.LTs.xlsx'
# get.get_file(lt_id, lt_name)
#
# lts = pd.read_excel(lt_path, sheet_name='Sheet1', header=0)
# lts = lts[~lts['lt'].isna()]
# lts = lts.groupby("cpn", as_index=False).first().reset_index(drop=True)

# PULLING IN PROCUREMENT DECISION FILE AND DROPPING EXISTING PROCUREMENT COL
item_master = pd.read_excel("./data/item_master.xlsx",
                     sheet_name='CPN Item Master',
                     header=0)
item_master = item_master[['CPN', 'Lead Time (Weeks)']]
item_master = item_master.rename(columns={'CPN': 'cpn', 'Lead Time (Weeks)': 'lt'})

lts = item_master[~item_master['lt'].isna()]
lts = lts.groupby("cpn", as_index=False).first().reset_index(drop=True)
logging.info("Lead Time df ready")

# GRABBING LATEST MPS FROM Ops > Forecast/Master Schedule
mps_id = '1UXyOtpZ9OEL3SmTq-Ak0pUUlVz1Q6mS2'
mps_name = 'mps'
mps_path = './data/mps.xlsx'

get.get_file(mps_id, mps_name)

# In[CLEAN TO GET MPS TAB INTO USABLE TIDY FORMAT]

mps = pd.read_excel(mps_path, sheet_name='Master Schedule', header=4)

def clean_mps(oxmps):
    """
    This function will take the MPS from the Ops > Forecast/Master Schedule
    GDrive and clean and pivot for use in Prod Scheduling as an Order list by
    PN, Qty, and Lot.
    ***NOTE*** SET HEADER TO ROW 4

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
    oxmps = oxmps.iloc[0:9, 2:160]
    oxmps = oxmps.iloc[2:9, :]
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
    # oxmps.loc[oxmps['qty'].str.startswith('^') == True, 'lot_num'] = 'planning fence'
    # fnc = oxmps.loc[oxmps['qty'].str.startswith('^') == True].copy()
    # oxmps = oxmps.loc[oxmps['qty'].str.startswith('^') == False]
    oxmps['lot_ord'] = oxmps[oxmps['lot_num'] != 'planning fence'].groupby(
        ['lot_num']).cumcount()+1
    oxmps['lot_ord'] = oxmps['lot_ord'].fillna(0)
    oxmps = oxmps.reset_index(drop=True)
    return oxmps

mps = clean_mps(mps)
iso_dt = dt.date.isocalendar(dt.datetime.now())
mps = mps[mps['year'] >= iso_dt[0]]
mps = mps[mps['wk'] >= iso_dt[1]]
logging.info("MPS df ready")

# In[BUILDING ISOWEEK AND YEAR DATAFRAME FOR MRP CALCS]
tmp = pd.DataFrame(index=range(53), columns=range(1))
tmp = tmp.reset_index(drop=False).drop(columns=[0])
isowk = tmp.copy()
isowk['year'] = 2022
isowk = isowk.rename(columns={'index': 'wk'})
tmp['year'] = 2023
tmp = tmp.rename(columns={'index': 'wk'})
isowk = isowk.append(tmp)
tmp['year'] = 2024
tmp = tmp.rename(columns={'index': 'wk'})
isowk = isowk.append(tmp)
isowk = isowk.reset_index(drop=True)
isowk = isowk[isowk['wk'] != 0]

isowk['Monday'] = isowk.apply(lambda x: str(
    dt.date.fromisocalendar(x['year'], x['wk'], 1)), axis=1)
isowk['Friday'] = isowk.apply(lambda x: str(
    dt.date.fromisocalendar(x['year'], x['wk'], 5)), axis=1)
logging.info("ISO df ready")

# In[BUILDING RACK BOM]

# oxrack = rack
# oxlts = lts
# oxordr = ordr_short.loc[rows:rows+1,:]

# SPECIFYING WHICH ORDER TO PULL
ordr = mps[mps['lot_num'] != 'planning fence'].copy()
ordr = ordr.reset_index(drop=True)

with open("./creds.yml", 'r') as streamd:
    opscreds = yaml.safe_load(streamd)

cpns = ordr[['cpn']].copy()
cpns = cpns.drop_duplicates().reset_index(drop=True)

mrp = pd.DataFrame()

for pns in range(len(cpns['cpn'])):
    logging.debug(pns)
    logging.debug(cpns.at[pns, 'cpn'])
    rack = ds.s2sbuildbom_all(cpns.at[pns, 'cpn'],
                          opscreds['oxide_duro']['api_key'])

    rack = rack.loc[:, 'query_pn':'status']
    logging.debug(rack.head(1))
    logging.info("Rack df ready")

    ordr_short = ordr[ordr['cpn'] == cpns.at[pns, 'cpn']].copy().reset_index(drop=True)
    # ordr_short = ordr[ordr['cpn'] == '999-0000014'].copy().reset_index(drop=True)
    logging.info("Below is the set of orders for rack cpn  %s" %
                 cpns.at[pns, 'cpn'])
    logging.info(ordr_short)

    for rows in range(len(ordr_short['cpn'])):
        logging.debug("Order info for row %s" % rows)
        logging.debug(ordr_short.loc[rows:rows+1, :])
        tmp = mrp_build(rack, lts, ordr_short.loc[rows:rows+1, :])
        logging.debug(tmp.head(1))
        logging.info("Appending results to MRP df")
        mrp = mrp.append(tmp)
        logging.debug(mrp.tail(1))

mrp = mrp.reset_index(drop=True)

uploads = './uploads/'
# csv = uploads + 'mrp_raw.xlsx'
# mrp.to_excel(csv, index=False)
# gdrive_folder = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco'
# post.main(gdrive_folder, isodate=False)
# print('Files uploaded to GDrive folder_id = %s' % gdrive_folder)
# ds.clear_dir("./uploads")


# In[]

meta = mrp[['pnladder', 'parent', 'cpn', 'name', 'category']].copy()
meta = meta.drop_duplicates()
meta = meta.set_index(['pnladder', 'parent', 'cpn'])
meta = meta.rename(columns={'name':('','name'), 'category':('','category')})

# In[]

cols_to_move = ['pnladder',
                'parent',
                'cpn',
                'due_date',
                'due_year',
                # 'need_by_date',
                # 'nbd_year',
                'ord_qty']
dd_mrp = mrp[cols_to_move].copy()

dd_mrp = dd_mrp.rename(columns={'due_year':'year', 'due_date':'wk'})
dd_mrp = dd_mrp.groupby(by=['pnladder',
                            'parent',
                            'cpn',
                            'year',
                            'wk',]).sum()


dd_mrp = dd_mrp.reset_index(drop=False)

dd_tbl = dd_mrp.pivot_table(
    index = ["pnladder", "parent", "cpn"],
    columns = ["year", "wk"],
    values = "ord_qty")

dd_tbl['type'] = 'finish_date'


# In[]
cols_to_move = ['pnladder',
                'parent',
                'cpn',
                # 'due_date',
                # 'due_year',
                'need_by_date',
                'nbd_year',
                'ord_qty']
nbd_mrp = mrp[cols_to_move].copy()

nbd_mrp = nbd_mrp.rename(columns={'nbd_year':'year', 'need_by_date':'wk'})
nbd_mrp = nbd_mrp.groupby(by=['pnladder',
                            'parent',
                            'cpn',
                            'year',
                            'wk',]).sum()

nbd_mrp = nbd_mrp.reset_index(drop=False)

nbd_tbl = nbd_mrp.pivot_table(
    index = ["pnladder", "parent", "cpn"],
    columns = ["year", "wk"],
    values = "ord_qty")

nbd_tbl['type'] = 'start_date'


# In[GRABBING LATEST INV FILE FROM BENCHMARK]

drive_id = '0AKcpdSVwv34AUk9PVA' #oxide shared drive id - reqd for meta data pull
flder_id = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco' #ops auto > attachments folder id

df = get.get_list(drive_id, flder_id)
df = df[df['name'].str.startswith(
    'Oxide ')].reset_index(drop=True)
df = df.loc[0, 'id']
logging.debug("File ID: " + df)

get.get_file(df, 'bm_inv')

# In[]

inv_gdrive = './data/bm_inv.xlsx'
inv_get = pd.read_excel(
    inv_gdrive, sheet_name='Inventory on Hand', header=0)

inv_cleaned = inv_get.copy()
inv_cleaned['BEI Part'] = inv_cleaned['BEI Part'].map(lambda x: x.replace('OXC', ''))
inv_cleaned['cpn'] = inv_cleaned['BEI Part'].map(lambda x: x[:11])

cols_to_move = ['cpn']
cols = cols_to_move + \
    [col for col in inv_cleaned.columns if col not in cols_to_move]
inv_cleaned = inv_cleaned[cols]

inv_oh = inv_cleaned.groupby(by=['cpn']).sum().reset_index(drop=False)

inv_oh = inv_oh[['cpn', 'Quantity']]
inv_oh = inv_oh.rename(columns={'Quantity':'qty'})
iso_dt = dt.date.isocalendar(dt.datetime.now())

inv_oh['wk'] = iso_dt[1]
inv_oh['year'] = iso_dt[0]

cols_to_move = ['pnladder',
                'parent',
                'cpn']
inv_mrp = mrp[cols_to_move].copy()

inv_mrp = inv_mrp.merge(inv_oh, 'left', 'cpn')
inv_mrp = inv_mrp[~inv_mrp['qty'].isnull()]

inv_mrp = inv_mrp.groupby(by=['pnladder',
                            'parent',
                            'cpn',
                            'year',
                            'wk']).sum()

inv_mrp = inv_mrp.reset_index(drop=False)

inv_tbl = inv_mrp.pivot_table(
    index = ["pnladder", "parent", "cpn"],
    columns = ["year", "wk"],
    values = "qty")

av_tbl = inv_tbl.copy()
inv_tbl['type'] = 'OnHand'
av_tbl['type'] = 'Available'
inv_tbl = inv_tbl.append(av_tbl)

# In[]

tbl = dd_tbl.append(nbd_tbl)
tbl = tbl.append(inv_tbl)

tbl = tbl.reset_index(drop=False)

cols_to_move = ['pnladder',
                'parent',
                'cpn',
                'type']
tbl = tbl.set_index(cols_to_move)
tbl = tbl.sort_index(level=['pnladder', 'type']).fillna(0)
tbl = tbl.astype(int)

# tbl_calc = tbl[tbl['year']>2021 ]

# In[]
tbl = tbl.reset_index(drop=False)
uploads = './uploads/'
csv = uploads + 'mrp_export.xlsx'
writer = pd.ExcelWriter(csv, engine='xlsxwriter')
mps.to_excel(writer, sheet_name = 'mps', index=False)
tbl.to_excel(writer, sheet_name = 'mrp_table')
mrp.to_excel(writer, sheet_name="mrp_raw", index_label="idx")
lts.to_excel(writer, sheet_name = 'lead_times', index=False)
writer.save()
gdrive_folder = '1LIN-_wuwnJWnE0xKhvNbFwk2lBtkHfco' #ops auto > attachments folder id
post.main(gdrive_folder, isodate=False)
print('Files uploaded to GDrive folder_id = %s' % gdrive_folder)
ds.clear_dir("./uploads")
