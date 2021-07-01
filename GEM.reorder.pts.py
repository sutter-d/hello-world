# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 10:44:16 2019

@author: sutter.d
"""



# =============================================================================
# import datetime as dt
# import glob
# import os
# =============================================================================

import pandas as pd
import time
import pyodbc
from dsfcns import etl


# In[]

dr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/Hist.Updates/"
edr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/"
invdr = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Inventory/"

data = [['Ordered', 'Allocated', 3],
        ['A1 - Unit Not Ordered (Needs Review)', 'Allocated', 3],
        ['Not Ordered', 'Allocated', 3],
        ['BR Approved', 'Allocated', 3],
        ['Draft', 'Allocated', 3],
        ['Alteration Requested', 'Allocated', 3],
        ['Ordered - Alteration Pending ', 'Allocated', 3],
        ['BR Submitted', 'Allocated', 3],
        ['At MAV', 'Allocated', 3],
        ['Prepare For Pick Up', 'Staging', 4],
        ['Ready to Ship', 'Staging', 4],
        ['Shipped', 'Shipped', 5],
        ['Pick Up Confirmed', 'Shipped', 5],
        ['VAT Processed', 'Shipped', 5],
        ['Customs', 'Customs', 6],
        ['Delivered', 'Delivered', 7]]

us = pd.DataFrame(data,
                  columns=['Unit Status',
                           'Status',
                           'ID'])

def gemstatus(x):

    wipraw = x

    wiprawcols = wipraw.columns
    wiprawcols = wiprawcols.str.replace("\n", "")
    wipraw.columns = wiprawcols

    wip = wipraw[['Plant',
                  'Line',
                  'P&G Part #',
                  'Supplier',
                  'PO Total Price (USD)',
                  'Unit Status']]
    wip = wip.sort_values(by=['Plant', 'Line'], ascending=False)
    wip = wip[wip['Unit Status'] != 'Cancelled']
    wip = wip[wip['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    wip = wip[['Plant',
               'Line',
               'PO Total Price (USD)',
               'Unit Status']]

    wipshort = wip.groupby(['Plant', 'Line', 'Unit Status']).agg('sum')
    wipshort.reset_index(level=['Plant', 'Line', 'Unit Status'], inplace=True)
    wipshort = pd.merge(wipshort, us, how='left', on='Unit Status')

    wipstatus = wipshort.drop(columns=['Plant', 'Line', 'Unit Status']).groupby(['Status', 'ID']).agg('sum')
    wipstatus.reset_index(level=['Status', 'ID'], inplace=True)

    ls = wipshort[['Status', 'Plant', 'Line']].sort_values(by=['Status'], ascending = False)
    ls = ls.drop_duplicates(keep='first')
    ls.reset_index(drop=True, inplace=True)
    ls['count'] = 1
    ls = ls.groupby('Status').agg('sum')
    ls.reset_index(inplace=True)

    if len(wipstatus.index)>0:    
        wipstatus = pd.merge(wipstatus,
                             ls,
                             how='left',
                             on='Status').sort_values(by=['ID'], ascending = True)
        wipstatus.reset_index(drop = True, inplace=True)
        wipstatus = wipstatus[['Status',
                               'ID',
                               'PO Total Price (USD)',
                               'count']]

    gdns = x

    gdns = gdns[gdns['Unit Status'] != 'Cancelled']
    gdns = gdns[gdns['Plant'] == 'Global Plant DO NOT SHIP-GBS']

    gdns = gdns[['Plant', 'PO Total Price (USD)']]

    gdns = gdns.groupby('Plant').agg('sum')
    gdns.reset_index(inplace=True)

    gdns['Status'] = 'Inventory'
    gdns['ID'] = 2
    # gdns['count'] = '-'
    gdns['count'] = 0

    gdns = gdns[['Status',
                 'ID',
                 'PO Total Price (USD)',
                 'count']]

    gemsum = gdns.append(wipstatus)

    return gemsum

def wipstatus(x):

    wipraw = x

    wiprawcols = wipraw.columns
    wiprawcols = wiprawcols.str.replace("\n", "")
    wipraw.columns = wiprawcols

    wip = wipraw[['Plant',
                  'Line',
                  'P&G Part #',
                  'Supplier',
                  'PO Total Price (USD)',
                  'Unit Status']]
    wip = wip.sort_values(by=['Plant', 'Line'], ascending=False)
    wip = wip[wip['Unit Status'] != 'Cancelled']
    wip = wip[wip['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    wip = wip[['Plant',
               'Line',
               'PO Total Price (USD)',
               'Unit Status']]

    wipshort = wip.groupby(['Plant', 'Line', 'Unit Status']).agg('sum')
    wipshort.reset_index(level=['Plant', 'Line', 'Unit Status'], inplace=True)
    wipshort = pd.merge(wipshort, us, how='left', on='Unit Status')

    wipstatus = wipshort.drop(columns=['Plant',
                                       'Line',
                                       'Unit Status']).groupby(['Status', 'ID']).agg('sum')
    wipstatus.reset_index(level=['Status', 'ID'], inplace=True)

    return wipshort


def gemxcharge(x):

    wipraw = x

    wiprawcols = wipraw.columns
    wiprawcols = wiprawcols.str.replace("\n", "")
    wipraw.columns = wiprawcols

    wip = wipraw[['Plant',
                  'Line',
                  'P&G Part #',
                  'Supplier',
                  'PO Total Price (USD)',
                  'Unit Status',
                  'Xcharge Date']]
    wip = wip.sort_values(by=['Plant', 'Line'], ascending=False)
    wip = wip[wip['Unit Status'] != 'Cancelled']
    wip = wip[wip['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    wip = wip[wip['Xcharge Date'].isnull()]
    wip = wip[['Plant', 'Line', 'PO Total Price (USD)', 'Unit Status']]

    wipshort = wip.groupby(['Plant', 'Line', 'Unit Status']).agg('sum')
    wipshort.reset_index(level=['Plant', 'Line', 'Unit Status'], inplace=True)
    wipshort = pd.merge(wipshort, us, how='left', on='Unit Status')

    wipstatus = wipshort.drop(columns=['Plant',
                                       'Line',
                                       'Unit Status']).groupby(['Status', 'ID']).agg('sum')
    wipstatus.reset_index(level=['Status', 'ID'], inplace=True)

    ls = wipshort[['Status',
                   'Plant', 'Line']].sort_values(by=['Status'], ascending=False)
    ls = ls.drop_duplicates(keep='first')
    ls.reset_index(drop=True, inplace=True)
    ls['count'] = 1
    ls = ls.groupby('Status').agg('sum')
    ls.reset_index(inplace=True)

    if len(wipstatus.index)>0:    
        wipstatus = pd.merge(wipstatus,
                             ls,
                             how='left',
                             on='Status').sort_values(by=['ID'], ascending = True)
        wipstatus.reset_index(drop=True, inplace=True)
        wipstatus = wipstatus[['Status',
                               'ID',
                               'PO Total Price (USD)',
                               'count']]


    gdns = x

    gdns = gdns[gdns['Unit Status'] != 'Cancelled']
    gdns = gdns[gdns['Plant'] == 'Global Plant DO NOT SHIP-GBS']

    gdns = gdns[['Plant', 'PO Total Price (USD)']]

    gdns = gdns.groupby('Plant').agg('sum')
    gdns.reset_index(inplace=True)

    gdns['Status'] = 'Inventory'
    gdns['ID'] = 2
    gdns['count'] = '-'

    gdns=gdns[['Status',
               'ID',
               'PO Total Price (USD)',
               'count']]

    gemsum = gdns.append(wipstatus)

    return gemsum

# In[]

#list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.Vz/*') # * means all if need specific format then *.csv
#latest_file = max(list_of_files, key=os.path.getctime)
#print(latest_file)
#y = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)

server = 'GADC-GEMSQL001\GEMSP101' 
database = 'GEMInterface' 
username = 'GEMInterface_User' 
password = 'GEMUser2016' 

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# In[]

# VERIZON CAAS QUERY
cursor.execute("exec sp_GetESMData 2426") 
esm2426 = cursor.fetchall()

y = etl(esm2426)
ogcaasvz = y

csv = edr + "CaaS.Vz/2426ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaasvz.to_excel(csv, index=False)

caasvz = gemstatus(y)
wipcaasvz = wipstatus(y)
caasvzxcharge = gemxcharge(y)
wipcaasvz['program'] = 'caas.vz'

# BUILD QUERY
cursor.execute("exec sp_GetESMData 2425") 
esm2425 = cursor.fetchall()

y = etl(esm2425)
ogbuild = y

csv = edr + "Build/2425ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogbuild.to_excel(csv, index=False)

build = gemstatus(y)
wipbuild = wipstatus(y)
buildxcharge = gemxcharge(y)
wipbuild['program'] = 'build'

# SWAP QUERY
cursor.execute("exec sp_GetESMData 2427") 
esm2427 = cursor.fetchall()

y = etl(esm2427)
ogswap = y

csv = edr + "SWAP/2427ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogswap.to_excel(csv, index=False)

swap = gemstatus(y)
wipswap = wipstatus(y)
swapxcharge = gemxcharge(y)
wipswap['program'] = 'swap'

# WWT CAAS QUERY
cursor.execute("exec sp_GetESMData 2428") 
esm2428 = cursor.fetchall()

y = etl(esm2428)
ogcaaswwt = y

csv = edr + "CaaS.WWT/2428ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaaswwt.to_excel(csv, index=False)

cursor.execute("exec sp_GetESMData 2602") 
esm2602 = cursor.fetchall()

y = etl(esm2602)
newcaaswwt = y

csv = edr + "CaaS.WWT.2/2602ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
newcaaswwt.to_excel(csv, index=False)

y = ogcaaswwt.append(newcaaswwt)

caaswwt = gemstatus(y)
wipcaaswwt = wipstatus(y)
caaswwtxcharge = gemxcharge(y)
wipcaaswwt['program'] = 'caas.wwt'

# CBTS CAAS QUERY
cursor.execute("exec sp_GetESMData 2601") 
esm2601 = cursor.fetchall()

y = etl(esm2601)
ogcaascbts = y

csv = edr + "CaaS.CBTS/2601ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaascbts.to_excel(csv, index=False)

caascbts = gemstatus(y)
wipcaascbts = wipstatus(y)
caascbtsxcharge = gemxcharge(y)
wipcaascbts['program'] = 'caas.cbts'

# CORE QUERY
cursor.execute("exec sp_GetESMData 2510") 
esm2510 = cursor.fetchall()

y = etl(esm2510)
ogcore = y

csv = edr + "CORE/2510ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcore.to_excel(csv, index=False)

core = gemstatus(y)
wipcore = wipstatus(y)
corexcharge = gemxcharge(y)
wipcore['program'] = 'core'

# In[]

temp = caasvz.append(caascbts)
temp = temp.groupby(['Status', 'ID']).agg('sum').reset_index()
caasvz = temp

temp = caasvzxcharge.append(caascbtsxcharge)
temp.loc[temp['count']=='-', 'count'] = 0
temp = temp.groupby(['Status', 'ID']).agg('sum').reset_index()
caasvzxcharge = temp

temp = wipcaasvz.append(wipcaascbts)
#temp = temp.groupby(['Status', 'ID']).agg('sum').reset_index()
wipcaasvz = temp

# In[]
# =============================================================================
# 
# data = [['Inventory', 2],
#         ['Allocated', 3],
#         ['Staging', 4],
#         ['Shipped', 5],
#         ['Customs', 6],
#         ['Delivered', 7]]
# 
# gem = pd.DataFrame(data,
#                   columns=['Status',
#                            'ID'])
# 
# 
# #gem = caaswwt.rename(index=str, columns={'PO Total Price (USD)': 'CaaS.Vz', 'count': 'CaaS.Vz Count'})
# gem = pd.merge(gem, 
#                caaswwt[['Status',
#                         'PO Total Price (USD)',
#                         'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.WWT','count': 'CaaS.WWT Count'}),
#                how='left',
#                on='Status')
# 
# gem = pd.merge(gem,
#                caasvz[['Status',
#                         'PO Total Price (USD)',
#                         'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.Vz','count': 'CaaS.Vz Count'}),
#                how='left',
#                on='Status')
# #ADDING CBTS HERE
# gem = pd.merge(gem,
#                caascbts[['Status',
#                         'PO Total Price (USD)',
#                         'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.CBTS','count': 'CaaS.CBTS Count'}),
#                how='left',
#                on='Status')
# #END CBTS HERE
# gem = pd.merge(gem,
#                build[['Status',
#                       'PO Total Price (USD)',
#                       'count']].rename(index=str, columns={'PO Total Price (USD)': 'Build', 'count': 'Build Count'}),
#                how='left',
#                on='Status')
# gem = pd.merge(gem,
#                swap[['Status',
#                      'PO Total Price (USD)',
#                      'count']].rename(index=str, columns={'PO Total Price (USD)': 'SWAP', 'count': 'SWAP Count'}),
#                how='left',
#                on='Status')
# 
# gem = pd.merge(gem,
#                core[['Status',
#                      'PO Total Price (USD)',
#                      'count']].rename(index=str, columns={'PO Total Price (USD)': 'Core', 'count': 'Core Count'}),
#                how='left',
#                on='Status')
# gem = gem.fillna(0)
# gem['Total'] = gem['CaaS.Vz'] + gem['CaaS.WWT'] + gem['CaaS.CBTS'] + gem['Build'] + gem['SWAP'] + gem['Core']
# 
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.update.csv"
# gem.to_csv(csv, index=False)
# csv = dr + "GEM.update." + time.strftime("%Y%m%d-%H%M%S") + ".csv"
# gem.to_csv(csv, index=False)
# 
# gemwip = wipcaaswwt.append(wipcaasvz)
# #
# gemwip = gemwip.append(wipcaascbts)
# #
# gemwip = gemwip.append(wipbuild)
# gemwip = gemwip.append(wipswap)
# gemwip = gemwip.append(wipcore)
# 
# cols = ['program',
#         'Plant',
#         'Line',
#         'PO Total Price (USD)',
#         'Unit Status',
#         'Status',
#         'ID']
# 
# gemwip = gemwip[cols]
# 
# csv = dr + "GEMwip.update" + time.strftime("%Y%m%d-%H%M%S") + ".csv"
# gemwip.to_csv(csv, index=False)
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEMwip.update.csv"
# gemwip.to_csv(csv, index=False)
# 
# gemxcharge = pd.DataFrame(data,
#                   columns=['Status',
#                            'ID'])
# gemxcharge = pd.merge(gemxcharge, 
#                       caasvzxcharge[['Status',
#                                      'PO Total Price (USD)',
#                                      'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.Vz', 'count': 'CaaS.Vz Count'}),
#                       how='left',
#                       on='Status')
# gemxcharge = pd.merge(gemxcharge,
#                       caaswwtxcharge[['Status',
#                                       'PO Total Price (USD)',
#                                       'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.WWT', 'count': 'CaaS.WWT Count'}),
#                       how='left',
#                       on='Status')
# #ADD CBTS XCHARGE
# gemxcharge = pd.merge(gemxcharge,
#                       caascbtsxcharge[['Status',
#                                       'PO Total Price (USD)',
#                                       'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.CBTS', 'count': 'CaaS.CBTS Count'}),
#                       how='left',
#                       on='Status')
# #END CBTS XCHARGE
# gemxcharge = pd.merge(gemxcharge,
#                       buildxcharge[['Status', 'PO Total Price (USD)', 'count']].rename(index=str, columns={'PO Total Price (USD)': 'Build', 'count': 'Build Count'}),
#                       how='left',
#                       on='Status')
# gemxcharge = pd.merge(gemxcharge,
#                       swapxcharge[['Status', 'PO Total Price (USD)', 'count']].rename(index=str, columns={'PO Total Price (USD)': 'SWAP', 'count': 'SWAP Count'}),
#                       how='left',
#                       on='Status')
# gemxcharge = pd.merge(gemxcharge,
#                       corexcharge[['Status', 'PO Total Price (USD)', 'count']].rename(index=str, columns={'PO Total Price (USD)': 'Core', 'count': 'Core Count'}),
#                       how='left',
#                       on='Status')
# gemxcharge = gemxcharge.fillna(0)
# gemxcharge['Total'] = gemxcharge['CaaS.Vz'] + gemxcharge['CaaS.WWT'] + gemxcharge['CaaS.CBTS'] + gemxcharge['Build'] + gemxcharge['SWAP'] + gemxcharge['Core']
# 
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.xcharge.update.csv"
# gemxcharge.to_csv(csv, index=False)
# csv = dr + "GEM.xcharge.update." + time.strftime("%Y%m%d-%H%M%S") + ".csv"
# gemxcharge.to_csv(csv, index=False)
# =============================================================================


# In[]
# =============================================================================
# FORECAST DELIVERIES
# =============================================================================

dmnd = ogcaasvz.append(ogcaaswwt)
dmnd = dmnd.append(newcaaswwt)
#ADD CBTS
dmnd = dmnd.append(ogcaascbts)
#END CBTS
dmnd = dmnd.append(ogcore)
dmnd = dmnd.append(ogbuild)
dmnd = dmnd.append(ogswap)

dmnd = dmnd[dmnd['Plant'] != 'Global Plant DO NOT SHIP-GBS']

list_of_vz = ['Verizon', 'VERIZON']

wwtdmnd = dmnd[dmnd['Supplier'] == 'World Wide Technology'].copy()

vzdmnd = dmnd[dmnd['Supplier'].str.contains('|'.join(list_of_vz))].copy()

cbtsdmnd = dmnd[dmnd['Supplier'] == 'CINCINNATI BELL TECHNOLOGY SOLUTIONS INC'].copy()

vzdmnd['Destination ROS Date'] = pd.to_datetime(vzdmnd['Destination ROS Date'])
vzdmnd = vzdmnd.groupby([vzdmnd['Destination ROS Date'].dt.strftime('%B %Y'), vzdmnd['P&G Part #']])['Qty'].sum().sort_values()
vzdmnd = vzdmnd.reset_index()
vzdmnd = vzdmnd.rename(index=str,
                               columns={'Destination ROS Date': 'ROS Date'})

wwtdmnd['Destination ROS Date'] = pd.to_datetime(wwtdmnd['Destination ROS Date'])
wwtdmnd = wwtdmnd.groupby([wwtdmnd['Destination ROS Date'].dt.strftime('%B %Y'), wwtdmnd['P&G Part #']])['Qty'].sum().sort_values()
wwtdmnd = wwtdmnd.reset_index()
wwtdmnd = wwtdmnd.rename(index=str,
                                 columns={'Destination ROS Date': 'ROS Date'})

cbtsdmnd['Destination ROS Date'] = pd.to_datetime(cbtsdmnd['Destination ROS Date'])
cbtsdmnd = cbtsdmnd.groupby([cbtsdmnd['Destination ROS Date'].dt.strftime('%B %Y'), cbtsdmnd['P&G Part #']])['Qty'].sum().sort_values()
cbtsdmnd = cbtsdmnd.reset_index()
cbtsdmnd = cbtsdmnd.rename(index=str,
                               columns={'Destination ROS Date': 'ROS Date'})


data = [['October 2019', 1],
        ['November 2019', 2],
        ['December 2019', 3],
        ['January 2020', 4],
        ['February 2020', 5],
        ['March 2020', 6],
        ['April 2020', 7],
        ['May 2020', 8],
        ['June 2020', 9],
        ['July 2020', 10],
        ['August 2020', 11],
        ['September 2020', 12],
        ['October 2020', 13],
        ['November 2020', 14],
        ['December 2020', 15],
        ['January 2021', 16]]

mths = pd.DataFrame(data,
                    columns=['ROS Date',
                             'ROSDate#'])

vzdmnd = pd.merge(mths, vzdmnd, 'left', 'ROS Date')
vzdmnd['Vendor'] = 'Vz'
wwtdmnd = pd.merge(mths, wwtdmnd, 'left', 'ROS Date')
wwtdmnd['Vendor'] = 'WWT'
cbtsdmnd = pd.merge(mths, cbtsdmnd, 'left', 'ROS Date')
cbtsdmnd['Vendor'] = 'CBTS'
dmnd = wwtdmnd.append(vzdmnd)
dmnd = dmnd.append(cbtsdmnd)

qrtdmnd = dmnd.copy()

qrtdmnd = qrtdmnd[qrtdmnd['ROSDate#']>8]
qrtdmnd = qrtdmnd[qrtdmnd['ROSDate#']<12]

qrtdmnd = qrtdmnd.groupby(['Vendor', 'P&G Part #'])['Qty'].sum().sort_values()
qrtdmnd = qrtdmnd.reset_index()
qrtdmnd = qrtdmnd.rename(index=str, columns={'Qty': 'Qrtr Dmnd'})

yrdmnd = dmnd.copy()

yrdmnd = yrdmnd[yrdmnd['ROSDate#']>3]
yrdmnd = yrdmnd[yrdmnd['ROSDate#']<12]

yrdmnd = yrdmnd.groupby(['Vendor', 'P&G Part #'])['Qty'].sum().sort_values()
yrdmnd = yrdmnd.reset_index()
yrdmnd = yrdmnd.rename(index=str, columns={'Qty': 'Yr Dmnd'})

wwtinv = pd.read_excel(invdr + 'CaaS.WWT.Inventory.xlsx')

wwtinv = pd.merge(wwtinv, qrtdmnd[qrtdmnd['Vendor']=='WWT'], 'left', 'P&G Part #').drop(columns=['Vendor'])
wwtinv = pd.merge(wwtinv, yrdmnd[yrdmnd['Vendor']=='WWT'], 'left', 'P&G Part #').drop(columns=['Vendor'])
wwtinv['QSS'] = wwtinv['Qrtr Dmnd']
wwtinv['QDDLT'] = wwtinv['Qrtr Dmnd']/3
wwtinv['QReOrder.Pt'] = wwtinv['QSS'] + wwtinv['QDDLT']

wwtinv['YSS'] = wwtinv['Yr Dmnd']/3
wwtinv['YDDLT'] = wwtinv['Yr Dmnd']/9
wwtinv['YReOrder.Pt'] = wwtinv['YSS'] + wwtinv['YDDLT']


wwtinv['QReOrder'] = 'more info'
wwtinv.loc[wwtinv['QReOrder.Pt'] > wwtinv['GDNS Qty'], ['QReOrder']] = 'yes'
wwtinv.loc[wwtinv['QReOrder.Pt'] < wwtinv['GDNS Qty'], ['QReOrder']] = 'no'

wwtinv['YReOrder'] = 'more info'
wwtinv.loc[wwtinv['YReOrder.Pt'] > wwtinv['GDNS Qty'], ['YReOrder']] = 'yes'
wwtinv.loc[wwtinv['YReOrder.Pt'] < wwtinv['GDNS Qty'], ['YReOrder']] = 'no'


wwtreordr = wwtinv[wwtinv['QReOrder'] == 'yes']
wwtreordr = wwtreordr.append(wwtinv[wwtinv['YReOrder'] == 'yes'])
wwtreordr = wwtreordr.drop_duplicates()

wwtreordr.to_excel("C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/Replenishment/WWT.Replen.Order.xlsx", index=False)

#dmnd['Delivery Total'] = dmnd.fillna(0)['WWT PO Total Price (USD)'] + dmnd.fillna(0)['Vz PO Total Price (USD)'] + dmnd.fillna(0)['CBTS PO Total Price (USD)']
#csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.dmnd.csv"
#dmnd.to_csv(csv, index=False)

