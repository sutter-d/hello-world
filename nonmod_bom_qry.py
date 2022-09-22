#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 14:11:29 2022

@author: danielsutter
"""
from __future__ import print_function
import logging
# import math
import time
import sys
import datetime as dt

import yaml
# import json
import requests
import pandas as pd
import argparse
# from pyairtable import Api, Table
import ds_utils as ds
import gdrive_post_s2s as post


requests.packages.urllib3.disable_warnings()


def main(oxpn):
    """


    Parameters
    ----------
    oxpn : TYPE - Duro CPN string
        DESCRIPTION - the Oxide CPN non-modified BOM you require

    Returns
    -------
    tla_ext : TYPE - Pandas DF
        DESCRIPTION - the full non-modified BOM in the form of a pandas DF

    """
    # Opening creds.yml and assigning Duro creds
    with open("./creds.yml", 'r') as strm:
        allcreds = yaml.safe_load(strm)
    durocreds = allcreds['oxide_duro']
    api_key = durocreds['api_key']  # 16152961423738m50/MAHXgvcvj4RMROq2g==

    # In[CPN QUERY]

    cpns = ds.cpn()

    # In[CREDS FILE UPLOAD AND API PARAMS]

    prod_api_url = 'https://public-api.duro.app/v1/product/revision/'
    comp_api_url = 'https://public-api.duro.app/v1/component/revision/'

    # oxpn = '999-0000014'

    # creates headers for the GET query
    api_call_headers = {'x-api-key': api_key}
    api_call_params = {'cpn': oxpn}

    # In[API CALL RESPONSE AND UNPACK]
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
    tla_subs = tla_subs.rename(
        columns={'component': 'chld_component', 'assemblyRevision': 'chld_aR'})

    # SORTING THE SUBS DF BY CHILD COMPONENT
    tla_subs = tla_subs.sort_values(by=['chld_component'], axis=0)

    # THIS MERGE GIVES US THE CPN FOR EACH COMPONENT ID
    tla_subs_cpns = tla_subs.merge(
        cpns, 'left', left_on='chld_component', right_on='_id')

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
    # tla_lvl1 = tla_ext #DEBUG ONLY

    # In[]
    # [FORMATTING L7 DF]
    lvls = [1, 2, 3, 4, 5, 6, 7]
    # lvls = 1 #DEBUG ONLY

    # SETTING THE LEVEL COL TO 1 FOR TLA CHILD COMPS
    more_subs = tla_ext[tla_ext['level'] == 1]
    more_subs = more_subs['children']

    # THIS lvlflag IS SET TO FALSE LATER TO INDICATE WHEN ALL CHILD COMPS
    # HAVE BEEN PULLED
    lvlflag = True
    tla = pd.DataFrame()

    # FIRST CONFIRM THAT MORE SUBS RETURNS A DF
    if more_subs.size == 0:
        lvlflag = False

    # THIS FOR LOOP RUNS 7 LEVELS DEEP, OUR BOM IS CURRENTLY 5
    for xx in lvls:
        # xx=3 #DEBUG ONLY
        logging.debug("level %s" % xx)
        if lvlflag:
            # y=3 #DEBUG ONLY
            # xx=1 #DEBUG ONLY
            for y in range(len(tla_ext['cpn'])):
                # oxpn = '910-0000019' #DEBUG ONLY
                # SET OXPN EQUAL TO THE CURRENT ROWS CPN
                oxpn = tla_ext.at[y, 'cpn']
                logging.info('loop %s PN ' % y + tla_ext.at[y, 'cpn'])
                print('loop %s PN ' % y + tla_ext.at[y, 'cpn'])

                # IF ITS A PARENT, SKIP OVER AND APPEND TO TLA DF
                if tla_ext.at[y, 'level'] < xx:
                    tla = tla.append(tla_ext.loc[y])  # .reset_index(drop=True)
                # ELSE ASSIGN NESTED DICT OF CHILD COMPS TO yn_chlds
                else:
                    yn_chlds = tla_ext.at[y, 'children']
                    logging.info('Child PN Array length =  %s' %
                                 str(len(yn_chlds)))
                    print('Child PN Array length = %s' % str(len(yn_chlds)))
                    # IF yn_chlds CONTAINS CHILD COMPONENTS, THEN QRY FOR THE
                    # CHILD COMPS AND APPEND BOTH ROW INDEX Y AND CHILD COMPS
                    # TO TLA DF
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

                        # QRY RETURNS CHILD COMPS BY REVISION
                        subs_revs = rev.sort_values(
                            by=['created'], ascending=False)
                        logging.debug(tla_ext.at[y, 'chld_aR'])
                        print(tla_ext.at[y, 'chld_aR'])
                        logging.debug(subs_revs['_id'])
                        print(subs_revs['_id'])

                        # HERE'S THE KEY PART OF THE LOOP
                        # THE CHILD COMP NESTED DICT CONTAINS THE ASSEMBLY
                        # REVISION FOR EACH CHILD COMP - RENAMED chld_aR above
                        # THE COMP REVISIONS QRY ABOVE PROVIDES COMP DATA BY
                        # REV STORED IN THE _id COL
                        # HERE WE MATCH SUBS REVS _id TO PARENT chld_aR
                        subs_revs = subs_revs[subs_revs['_id']
                                              == tla_ext.at[y, 'chld_aR']]
                        subs_subs = ds.unpack(subs_revs['children'])
                        logging.debug('child_child size = ' +
                                      str(subs_subs.size))
                        print('child_child size = ' + str(subs_subs.size))
                        subs_subs = subs_subs.rename(
                            columns={'component': 'chld_component', 'assemblyRevision': 'chld_aR'})
                        subs_subs = subs_subs.sort_values(
                            by=['chld_component'], axis=0)
                        subs_subs_cpns = subs_subs.merge(
                            cpns, 'left', left_on='chld_component', right_on='_id')
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

                        # .reset_index(drop=True)
                        tla = tla.append(tla_ext.loc[y])
                        # .reset_index(drop=True)
                        tla = tla.append(subs_subs_cpns)
                    # IF yn_chlds IS EMPTY, THEN APPEND JUST ROW INDEX Y TO DF
                    else:
                        # .reset_index(drop=True)
                        tla = tla.append(tla_ext.loc[y])
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
            more_subs = tla[tla['level'] == xx+1]
            more_subs = ds.unpack(more_subs['children'])
            if more_subs.size == 0:
                lvlflag = False

            tla_ext = tla
            tla = pd.DataFrame()
    return tla_ext


def mpns(tla_bom):
    """


    Parameters
    ----------
    tla_bom : TYPE - pandas DF
        DESCRIPTION - a full non-modified BOM in the form of a pandas DF
        generated by the main() fcn.

    Returns
    -------
    srcs : TYPE - pandas DF
        DESCRIPTION - In case we need to provide Benchmark with all possible
        mpns for each of our CPNs and match AGILE formatting.

    """

    srcs = tla_bom

    # UNPACK DISTRIBUTORS COL AND RESET INDEX
    manf = ds.unpack(srcs['sources.manufacturers']).reset_index(drop=False)

    # UNPACK MPN COL
    mpn = ds.unpack(manf['mpn'])  # .reset_index(drop=False)
    manf = pd.concat([manf, mpn], axis=1)

    # KEEP IND, NAME, KEY AND RENAME KEY TO MPN AND NAME TO MANF_NAME
    manf = manf[['ind', 'name', 'key']]
    manf = manf.rename(columns={'key': 'mpn', 'name': 'manf_name'})

    # RESET INDEX, AND USE OLD INDEX FOR MERGE
    srcs = srcs.reset_index(drop=False)
    srcs = srcs.merge(manf, 'left', left_on='index', right_on='ind')

    cols_to_move = ['index',
                    'parent',
                    'cpn',
                    'level',
                    'quantity',
                    'name',
                    'refDes',
                    'chld_component',
                    'chld_aR',
                    'category',
                    'revision',
                    'children',
                    'manf_name',
                    'mpn']
    # cols = cols_to_move + \
    #     [col for col in srcs.columns if col not in cols_to_move]
    srcs = srcs[cols_to_move]
    return srcs


if __name__ == '__main__':
    # Setting the logger up
    with open("./config.yml", 'r') as stream:
        oxconfig = yaml.safe_load(stream)
    loglvl = oxconfig['logging_configs']['level']
    logging.basicConfig(filename=('./gitlogs/bomrevqry_' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    desc_string = 'This utility will query Duro for the latest approved ' \
    'CPN rev where the modified flag is FALSE. Files will be pushed to ' \
    'GDrive and stored at OpsAuto > Duro > ECO_Process > ECO update'
    epilog_string = 'NOTE: Any non-component Assembly can be pulled using ' \
    'this query'

    parser = argparse.ArgumentParser(description=desc_string,
                                     epilog=epilog_string,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # run_type = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-c', '--cpn',
                        action='store',
                        dest='CPN',
                        default='999-0000014',
                        help='download the Duro non-modified Bill Of Material')

    args = parser.parse_args()

    print('run parameters are cpn = %s' % str(args.CPN))

    st = dt.datetime.now()
    # print(sys.argv[1])

    nonmod_bom = main(args.CPN)
    nonmod_bom_mpns = mpns(nonmod_bom)
    # clear ./data and ./uploads folders before saving tla to for upload
    ds.clear_dir('./data')
    ds.clear_dir('./uploads')

    # convert the df to an excel file
    csv = './uploads/%s_nonmod_bom_.xlsx' % str(args.CPN)
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')
    nonmod_bom.to_excel(writer, sheet_name='Sheet1', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    # convert the df to an excel file
    csv = './uploads/%s_nonmod_bom_mpns_.xlsx'  % str(args.CPN)
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')
    nonmod_bom_mpns.to_excel(writer, sheet_name='Sheet1', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    # Post the excel file to GDrive
    # OpsAuto > Duro > ECO_Process > ECO update
    folder_id = '1o1xNRFF5iA49F7JDWniowhaCFeudWe0A'
    post.main(folder_id)

    nd = dt.datetime.now()
    print(nd-st)
