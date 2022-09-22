#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 15:56:34 2021

@author: danielsutter
"""

# import requests
# from requests import HTTPError
# import json
# import pandas as pd
# import yaml
# import logging
# import time
# import restapi as oxrest
# from pyairtable import Api, Base, Table
# import base64
# import datetime as dt
# import glob
# import os

import os
import glob
import logging
# import math
import time
import datetime as dt
# import base64

# import json
import yaml
import pandas as pd
from pyairtable import Table, Api
import requests
from requests import HTTPError

import ds_utils as ds

# requests.packages.urllib3.disable_warnings()
# with open("./config.yml", 'r') as stream:
#     opsconfigs = yaml.safe_load(stream)
# logconfigs = opsconfigs['logging_configs']
# loglvl = logconfigs['level']
# logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/openorders' + time.strftime("%Y-%m-%d") + '.log'),
#                     level=loglvl,
#                     format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

# st = dt.datetime.now()

# # def main(): #TO RUN THIS FROM THE CMD LINE, UNCOMMENT AND INDENT ENTIRE SCRIPT EXCEPT LAST IF STATEMENT
# histup = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/HistUpdates/'
# att = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Attachments/'

"""
# =============================================================================
# # ORDER OF THIS SCRIPT
# # DIGIKEY ETL
# # MOUSER ETL
# # ARROW ETL
# # AVNET ETL
# # COMBINATION ETL
# # AIRTABLE - PULL CREDS AND PULL RECORDS
# # COMPARE AIRTABLE RESULTS TO COMBINED ETL
# # UPDATE EXISTING RECORDS IN AT OPEN ORDERS
# # CREATE NEW RECORDS IN AT OPEN ORDERS
# # DROP CLOSED RECORDS FROM AT OPEN ORDERS
# # CREATE NEW RECORDS IN CLOSED ORDERS AT 
# =============================================================================
"""

# In[DK ETL]
"""
DK ETL
"""


def main():
    st = dt.datetime.now()

    histup = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/HistUpdates/'
    att = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Attachments/'

    # pulling local creds.yml
    with open("./creds.yml", 'r') as stream:
        allcreds = yaml.safe_load(stream)

    dkcreds = allcreds['oxide_digikey']

    # FIRST Refresh the Access Token
    dkcreds = ds.dktokenrefresh(dkcreds)

    # SECOND Update creds.yml
    allcreds['oxide_digikey'] = dkcreds
    with open("./creds.yml", 'w') as outfile:
        yaml.dump(allcreds, outfile, default_flow_style=False)

    # THIRD Run OrderHist API Query
    dkopncls = 'true'  # 'true' = OPEN RECORDS; 'false' = ALL RECORDS
    dkordernumbers = ds.dkordhistapi(dkcreds, dkopncls)
    logging.info("dkordhistapi fcn ran successfully")
    csv = histup + 'rawdkordernumbers' + \
        time.strftime("%Y%m%d-%H%M%S") + '.csv'
    dkordernumbers.to_csv(csv, index=False)
    dkordernumbers = dkordernumbers['SalesorderId']

    # FOURTH Run OrderStatus API Query
    dkorderdetails = ds.dkordstatapi(dkcreds, dkordernumbers)
    logging.info("dkordstatapi fcn ran successfully")
    csv = histup + 'rawdkorderdetails' + \
        time.strftime("%Y%m%d-%H%M%S") + '.csv'
    dkorderdetails.to_csv(csv, index=False)
    dkorderdetails = dkorderdetails.reset_index(drop=True)

    # EXTRACTING Nested SHIPPING DETAILS DICT and merging with SalesorderId
    # Without a PO number, SalesorderId is the only unique identifier for the dataset
    # If that's a dealbreaker I'll contact DK and see why it's not populating
    shipdetails = dkorderdetails[['SalesorderId',
                                  'ShippingDetails']].copy()
    shipdetails = shipdetails.set_index(
        ['SalesorderId']).apply(pd.Series.explode).reset_index()
    shipdetails = pd.concat([shipdetails,
                             ds.unpack(shipdetails['ShippingDetails'])],
                            axis=1)
    shipdetails = shipdetails[shipdetails['CanceledOrVoided'] != True]
    for i in shipdetails.index:
        shipdetails.at[i, 'key'] = str(
            shipdetails.at[i, 'SalesorderId']) + str(shipdetails.at[i, 'InvoiceId'])

    logging.info("shipping details dataframe created")

    # EXTRACTING SALESORDER LINEITEMS and merging with SalesorderId
    lineitems = dkorderdetails[['SalesorderId',
                                'LineItems']].copy()
    lineitems = lineitems.set_index(['SalesorderId']).apply(
        pd.Series.explode).reset_index()
    lineitems = pd.concat([lineitems,
                           ds.unpack(lineitems['LineItems'])],
                          axis=1)
    for i in lineitems.index:
        lineitems.at[i, 'key'] = str(
            lineitems.at[i, 'SalesorderId']) + str(lineitems.at[i, 'InvoiceId'])

    # GROUP BY TO SUM QTY SHIPPED PER PN
    lisum = lineitems.groupby(
        ['SalesorderId', 'DigiKeyPartNumber']).sum().reset_index(drop=False)
    lisum = lisum[['SalesorderId', 'DigiKeyPartNumber', 'QuantityShipped']]
    for i in lisum.index:
        lisum.at[i, 'key2'] = str(lisum.at[i, 'SalesorderId']) + \
            str(lisum.at[i, 'DigiKeyPartNumber'])
    for i in lineitems.index:
        lineitems.at[i, 'key2'] = str(
            lineitems.at[i, 'SalesorderId']) + str(lineitems.at[i, 'DigiKeyPartNumber'])
    lineitems = lineitems.drop(columns=['QuantityShipped'])
    lisum = lisum[['key2', 'QuantityShipped']]
    lineitems = lineitems.merge(lisum, 'left', 'key2')
    lineitems = lineitems.drop(columns=['key2'])

    logging.info("line item details dataframe created")

    # EXTRACTING BACKORDERED OR SHORT SHITPPED ORDERs and their EST SHIP DATES
    backorder = lineitems[lineitems['InvoiceId'] == 0].copy()
    backorder = backorder[['key',
                           'Schedule']]
    backorder = backorder.set_index(['key']).apply(
        pd.Series.explode).reset_index()
    backorder = backorder.dropna(subset=['Schedule'])
    backorder = pd.concat([backorder.reset_index(drop=True),
                           ds.unpack(backorder['Schedule'])],
                          axis=1)
    backorder = backorder.drop(columns=['Schedule'])

    # MERGING EST SHIP DATES BACK INTO SALESORDERID LINEITEMS
    lineitems = lineitems.merge(backorder,
                                'left',
                                'key')
    lineitems = lineitems.merge(shipdetails.drop(columns=['SalesorderId', 'InvoiceId']),
                                'left',
                                'key')

    logging.info(
        "backorder line items and shipping estimates merged with sales order line items")

    lineitems['DeliveryDate'] = lineitems['DeliveryDate'].fillna("")
    lidel = lineitems[lineitems['DeliveryDate'] != ""].copy()
    lidel['Filter'] = lidel['DeliveryDate'].str[:10]
    lidel = lidel.reset_index(drop=True)
    for i in range(len(lidel['Filter'])):
        logging.debug(lidel.at[i,'Filter'])
        lidel.at[i, 'Filter'] = dt.datetime.strptime(
            lidel.at[i, 'Filter'], '%Y-%m-%d')
    lidel = lidel[lidel['Filter'] > dt.datetime.today()].reset_index(drop=True)
    lidel['ScheduledDate'] = lidel['DeliveryDate']
    lineitems = lineitems[lineitems['DeliveryDate']
                          == ""].reset_index(drop=True)
    lineitems = pd.concat([lidel, lineitems], axis=0).reset_index(drop=True)

    # SETTING LIST OF COLUMN NAMES TO REDUCE DF TO MATCH FINAL AIRTABLE UPLOAD
    cols = ['SalesorderId',
            'DigiKeyPartNumber',
            'Manufacturer',
            'Quantity',
            'QuantityShipped',
            # 'InvoiceId',
            'ScheduledQuantity',
            'ScheduledDate',
            'Carrier',
            'CarrierPackageId',
            'TrackingUrl']
    dk = lineitems[cols].copy()
    dk['Vendor'] = 'DigiKey'
    dk['ScheduledQuantity'] = dk['ScheduledQuantity'].fillna(0)
    # dk = dk[dk['ScheduledQuantity'] != '0']
    dk = dk.rename(columns={'DigiKeyPartNumber': 'PartNumber'})
    # PARSING SCHEDULED DATE FOR FIRST 10 CHARS SO STRING READS YYYY-DD-MM
    dk['ScheduledDate'] = dk['ScheduledDate'].str[:10]

    # In[MOUSER GDRIVE ETL]
    """
    MOUSER GDRIVE ETL
    """

    # * means all if need specific format then *.csv
    list_of_files = glob.glob(att + "*")

    for name in glob.glob(att + "/*"):
        logging.debug(name)
        if 'OpenOrderRpt' in name:
            logging.debug(name)
            list_of_files = [name]

    latest_av = max(list_of_files, key=os.path.getctime)
    msr_gdrive = pd.read_excel(latest_av, sheet_name="Sheet1", header=9)
    msr_openorders = msr_gdrive[~msr_gdrive['ESTIMATED SHIPMENT DATE'].str.contains(
        'SHIPPED')]

    msr_openorders = msr_openorders.rename(columns={'QUANTITY ORDERED': 'Quantity',
                                                    'QUANTITY REMAINING': 'ScheduledQuantity',
                                                    'ESTIMATED SHIPMENT DATE': 'ScheduledDate',
                                                    'CUSTOMER PURCHASE ORDER': 'SalesorderId',
                                                    'MANUFACTURER PART NUMBER': 'PartNumber',
                                                    'MANUFACTURER': 'Manufacturer',
                                                    'TRACKING LINK': 'TrackingUrl'})

    msr_openorders = msr_openorders.fillna('0')
    msr_openorders['QuantityShipped'] = 0

    msr_openorderscols = ['Carrier',
                          'CarrierPackageId']
    for c in msr_openorderscols:
        msr_openorders[c] = '0'

    cols = ['SalesorderId',
            'PartNumber',
            'Manufacturer',
            'Quantity',
            'QuantityShipped',
            # 'InvoiceId',
            'ScheduledQuantity',
            'ScheduledDate',
            'Carrier',
            'CarrierPackageId',
            'TrackingUrl']
    msr = msr_openorders[cols].copy()

    msr['Vendor'] = 'Mouser'

    # In[AVNET ETL]
    """
    AVNET ETL
    """
    list_of_files = glob.glob(
        att + "*")  # * means all if need specific format then *.csv

    for name in glob.glob(att + "/*"):
        logging.debug(name)
        if 'Orders_' in name:
            logging.debug(name)
            list_of_files = [name]

    latest_av = max(list_of_files, key=os.path.getctime)
    avnet_gdrive = pd.read_excel(latest_av, sheet_name="Report")
    avnet_wip = avnet_gdrive[avnet_gdrive['Order Status']
                             != "Shipped"].reset_index(drop=True)
    avnet_wip['Vendor'] = 'avnet'
    avnet_wip['QuantityShipped'] = 0
    avnet_wip['Remaining Qty'] = avnet_wip['Remaining Qty'].fillna(0)
    avnet_wip['ScheduledQuantity'] = avnet_wip['Order Qty']
    avnet_wip['Quantity'] = avnet_wip.apply(
        lambda x: x['Remaining Qty']+x['Order Qty'], axis=1)
    avnet_wip['TrackingUrl'] = '0'
    avnet_wip['Carrier'] = '0'
    avnet_wip['CarrierPackageId'] = '0'
    avnet_wip = avnet_wip.rename(columns={'Sales Order #': 'SalesorderId',
                                          'Mfr Part': 'PartNumber',
                                          'Promised Date': 'ScheduledDate'})
    cols = ['SalesorderId',
            'PartNumber',
            'Manufacturer',
            'Quantity',
            'QuantityShipped',
            # 'InvoiceId',
            'ScheduledQuantity',
            'ScheduledDate',
            'Carrier',
            'CarrierPackageId',
            'TrackingUrl',
            'Vendor']
    avnet = avnet_wip[cols]
    # In[COMBINE ETLS INTO ONE DATAFRAME FOR AIRTABLE UPDATE]
    """
    COMBINE ETLS INTO ONE DATAFRAME FOR AIRTABLE UPDATE
    """

    # CONCATENATING ALL API DATAFRAMES INTO ONE ETL DATAFRAME FOR AIRTABLE UPDATE
    etl = pd.concat([dk, msr])
    # COMMENT OUT NEXT 2 LINES TO RETURN TO PREVIOUS WORKING STATE WITH DK AND MOUSER
    etl = pd.concat([etl, avnet])
    # etl = pd.concat([etl, arrow])

    etl['SalesorderId'] = etl['SalesorderId'].astype(str)

    cols = ['Vendor',
            'SalesorderId',
            'PartNumber',
            'Manufacturer',
            'Quantity',
            'QuantityShipped',
            # 'InvoiceId',
            'ScheduledQuantity',
            'ScheduledDate',
            'Carrier',
            'CarrierPackageId',
            'TrackingUrl']
    etl = etl[cols]

    etl['UpdateTime'] = str(dt.datetime.now())
    etl = etl.rename(columns={'ScheduledDate': 'ShipDate'})
    etl = etl.reset_index(drop=True)
    nd = dt.datetime.now()
    logging.info("Script Run Time: {}".format(nd-st))
    return etl


if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    with open("./config.yml", 'r') as stream:
        opsconfigs = yaml.safe_load(stream)
    logconfigs = opsconfigs['logging_configs']
    loglvl = logconfigs['level']
    logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/openorders' + time.strftime("%Y-%m-%d") + '.log'),
                        level=loglvl,
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    logging.info("START openorders.py __main__ HERE")
    opnord = main()
    reports = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Reports/'
    csv = reports + 'Open_Orders_' + \
        time.strftime("%Y-%m-%d-%H%M%S") + '.xlsx'
    writer = pd.ExcelWriter(csv, engine='xlsxwriter')

    # Write each dataframe to a different worksheet.
    opnord.to_excel(
        writer, sheet_name='Open_Orders', index=False)
    writer.save()

    logging.info("FINISH openorders.py __main__ HERE")
