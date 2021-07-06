# -*- coding: utf-8 -*-
#"""
#Created on Wed Nov 20 10:44:16 2019
#
#@author: sutter.d
#"""



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

# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.Vz/*') # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# print(latest_file)
# y = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)


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

ogcaasvz = etl(esm2426)
ogcaasvz = ogcaasvz[~ogcaasvz['Supplier'].str.contains('World Wide Technology')]
ogcaasvz['ProjID'] = "2426"

csv = edr + "CaaS.Vz/2426ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaasvz.to_excel(csv, index=False)

# new Vz project IDs

cursor.execute("exec sp_GetESMData 2641")
esm2641 = cursor.fetchall()

if esm2641:
    newcaasvz = etl(esm2641)
    csv = edr + "CaaS.Vz/2641ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
    newcaasvz['ProjID'] = "2641"
    newcaasvz.to_excel(csv, index=False)
    ogcaasvz = ogcaasvz.append(newcaasvz)

cursor.execute("exec sp_GetESMData 2642")
esm2642 = cursor.fetchall()


if esm2642:
    newcaasvz = etl(esm2642)
    csv = edr + "CaaS.Vz/2642ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
    newcaasvz['ProjID'] = "2642"
    newcaasvz.to_excel(csv, index=False)
    ogcaasvz = ogcaasvz.append(newcaasvz)

cursor.execute("exec sp_GetESMData 2643")
esm2643 = cursor.fetchall()

if esm2643:
    newcaasvz = etl(esm2643)
    csv = edr + "CaaS.Vz/2643ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
    newcaasvz['ProjID'] = "2643"
    newcaasvz.to_excel(csv, index=False)
    ogcaasvz = ogcaasvz.append(newcaasvz)

caasvz = gemstatus(ogcaasvz)
wipcaasvz = wipstatus(ogcaasvz)
caasvzxcharge = gemxcharge(ogcaasvz)
wipcaasvz['program'] = 'caas.vz'

# BUILD QUERY
cursor.execute("exec sp_GetESMData 2425")
esm2425 = cursor.fetchall()

ogbuild = etl(esm2425)
ogbuild['ProjID'] = "2425"
csv = edr + "Build/2425ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogbuild.to_excel(csv, index=False)

build = gemstatus(ogbuild)
wipbuild = wipstatus(ogbuild)
buildxcharge = gemxcharge(ogbuild)
wipbuild['program'] = 'build'

# SWAP QUERY
cursor.execute("exec sp_GetESMData 2427")
esm2427 = cursor.fetchall()

ogswap = etl(esm2427)
ogswap['ProjID'] = "2427"
csv = edr + "SWAP/2427ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogswap.to_excel(csv, index=False)

swap = gemstatus(ogswap)
wipswap = wipstatus(ogswap)
swapxcharge = gemxcharge(ogswap)
wipswap['program'] = 'swap'

# WWT CAAS QUERY
cursor.execute("exec sp_GetESMData 2428")
esm2428 = cursor.fetchall()

ogcaaswwt = etl(esm2428)
ogcaaswwt['ProjID'] = "2428"

cursor.execute("exec sp_GetESMData 2426")
esm2426 = cursor.fetchall()
esm2426 = etl(esm2426)
esm2426['ProjID'] = "2426"
esm2426 = esm2426[esm2426['Supplier'].str.contains('World Wide Technology')]

ogcaaswwt = ogcaaswwt.append(esm2426)

csv = edr + "CaaS.WWT/2428ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaaswwt.to_excel(csv, index=False)

cursor.execute("exec sp_GetESMData 2602")
esm2602 = cursor.fetchall()

newcaaswwt = etl(esm2602)
newcaaswwt['ProjID'] = "2602"
csv = edr + "CaaS.WWT.2/2602ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
newcaaswwt.to_excel(csv, index=False)

ogcaaswwt = ogcaaswwt.append(newcaaswwt)

cursor.execute("exec sp_GetESMData 2794")
esm2794 = cursor.fetchall()

newcaaswwt = etl(esm2794)
newcaaswwt['ProjID'] = "2794"
csv = edr + "CaaS.WWT.2/2794ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
newcaaswwt.to_excel(csv, index=False)

ogcaaswwt = ogcaaswwt.append(newcaaswwt)

cursor.execute("exec sp_GetESMData 2795")
esm2795 = cursor.fetchall()

newcaaswwt = etl(esm2795)
newcaaswwt['ProjID'] = "2795"
csv = edr + "CaaS.WWT.2/2795ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
newcaaswwt.to_excel(csv, index=False)

ogcaaswwt = ogcaaswwt.append(newcaaswwt)

caaswwt = gemstatus(ogcaaswwt)
wipcaaswwt = wipstatus(ogcaaswwt)
caaswwtxcharge = gemxcharge(ogcaaswwt)
wipcaaswwt['program'] = 'caas.wwt'

# CBTS CAAS QUERY
cursor.execute("exec sp_GetESMData 2601")
esm2601 = cursor.fetchall()

ogcaascbts = etl(esm2601)
ogcaascbts['ProjID'] = "2601"
csv = edr + "CaaS.CBTS/2601ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaascbts.to_excel(csv, index=False)

caascbts = gemstatus(ogcaascbts)
wipcaascbts = wipstatus(ogcaascbts)
caascbtsxcharge = gemxcharge(ogcaascbts)
wipcaascbts['program'] = 'caas.cbts'

# CORE QUERY
cursor.execute("exec sp_GetESMData 2510")
esm2510 = cursor.fetchall()

ogcore = etl(esm2510)
ogcore['ProjID'] = "2510"
csv = edr + "CORE/2510ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcore.to_excel(csv, index=False)

core = gemstatus(ogcore)
wipcore = wipstatus(ogcore)
corexcharge = gemxcharge(ogcore)
wipcore['program'] = 'core'

# ADD PROGRAM TAG TO EACH ESM
ogcore['program'] = 'core'
ogcaascbts['program'] = 'caas.cbts'
ogcaaswwt['program'] = 'caas.wwt'
ogswap['program'] = 'swap'
ogbuild['program'] = 'build'
ogcaasvz['program'] = 'caas.vz'

ogcaasvz['vendor'] = 'vz'
ogcaaswwt['vendor'] = 'wwt'
ogcaascbts['vendor'] = 'cbts'
ogcaascmplt = ogcaasvz.append(ogcaaswwt)
ogcaascmplt = ogcaascmplt.append(ogcaascbts)

# =============================================================================
# ogcaascmplt = ogcaascmplt[ogcaascmplt['Plant']!='Global Plant DO NOT SHIP-GBS']
# ogcaascmplt = ogcaascmplt[ogcaascmplt['CapExp']!='EXP']
# ogcaascmplt = ogcaascmplt[ogcaascmplt['Unit Status']=='Delivered']
# ogcaascmplt['OnSiteDate'] = pd.to_datetime(ogcaascmplt['OnSiteDate'])
# ogcaascmplt['ShipDate'] = pd.to_datetime(ogcaascmplt['ShipDate'])
# ogcaascmplt['QuotedTimeStamp'] = pd.to_datetime(ogcaascmplt['QuotedTimeStamp'])
# ogcaascmplt = ogcaascmplt.sort_values('Invoice1A')
# cshort = ogcaascmplt[['Plant', 'Line', 'Supplier', 'Invoice1A', 'QuotedTimeStamp', 'ShipDate', 'OnSiteDate']]
# cshort['key'] = cshort['Plant'] + cshort['Line'] + cshort['Invoice1A']
# =============================================================================

csv = edr + "CombCaaSESM.update.csv"
ogcaascmplt.to_csv(csv, index=False)

csv = edr + "CombITSESM.update.csv"
its = ogcaascmplt.append(ogbuild)
its = its.append(ogcore)
its = its.append(ogswap)
its = its[ogcaascmplt.columns.tolist()]
its['shortpo'] = its['PONumber'].str[-10:]
its.to_csv(csv, index=False)
its.to_csv("C:/Users/sutter.d/Procter and Gamble/Project Governance - GEM (ESM) File/CombCaaSESM.update.csv", index=False)

ogcaaswwt['shortpo'] = ogcaaswwt['PONumber'].str[-10:]
csv = edr + "CaaS.WWT/WWT.Comb.ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaaswwt.to_excel(csv, index=False)

ogcaasvz['po'] = ogcaasvz['PONumber'].str[-10:]
csv = edr + "CaaS.Vz/Vz.Comb.ESM.update." + time.strftime("%Y%m%d-%H%M%S") + ".xlsx"
ogcaasvz.to_excel(csv, index=False)

# In[]

#temp = caasvz.append(caascbts)
#temp = temp.groupby(['Status', 'ID']).agg('sum').reset_index()
#caasvz = temp
#
#temp = caasvzxcharge.append(caascbtsxcharge)
#temp.loc[temp['count']=='-', 'count'] = 0
#temp = temp.groupby(['Status', 'ID']).agg('sum').reset_index()
#caasvzxcharge = temp
#
#temp = wipcaasvz.append(wipcaascbts)
##temp = temp.groupby(['Status', 'ID']).agg('sum').reset_index()
#wipcaasvz = temp

# In[]

data = [['Inventory', 2],
        ['Allocated', 3],
        ['Staging', 4],
        ['Shipped', 5],
        ['Customs', 6],
        ['Delivered', 7]]

gem = pd.DataFrame(data,
                  columns=['Status',
                           'ID'])


#gem = caaswwt.rename(index=str, columns={'PO Total Price (USD)': 'CaaS.Vz', 'count': 'CaaS.Vz Count'})
gem = pd.merge(gem, 
               caaswwt[['Status',
                        'PO Total Price (USD)',
                        'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.WWT','count': 'CaaS.WWT Count'}),
               how='left',
               on='Status')

gem = pd.merge(gem,
               caasvz[['Status',
                        'PO Total Price (USD)',
                        'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.Vz','count': 'CaaS.Vz Count'}),
               how='left',
               on='Status')
#ADDING CBTS HERE
gem = pd.merge(gem,
               caascbts[['Status',
                        'PO Total Price (USD)',
                        'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.CBTS','count': 'CaaS.CBTS Count'}),
               how='left',
               on='Status')
#END CBTS HERE
gem = pd.merge(gem,
               build[['Status',
                      'PO Total Price (USD)',
                      'count']].rename(index=str, columns={'PO Total Price (USD)': 'Build', 'count': 'Build Count'}),
               how='left',
               on='Status')
gem = pd.merge(gem,
               swap[['Status',
                     'PO Total Price (USD)',
                     'count']].rename(index=str, columns={'PO Total Price (USD)': 'SWAP', 'count': 'SWAP Count'}),
               how='left',
               on='Status')

gem = pd.merge(gem,
               core[['Status',
                     'PO Total Price (USD)',
                     'count']].rename(index=str, columns={'PO Total Price (USD)': 'Core', 'count': 'Core Count'}),
               how='left',
               on='Status')
gem = gem.fillna(0)
gem['Total'] = gem['CaaS.Vz'] + gem['CaaS.WWT'] + gem['CaaS.CBTS'] + gem['Build'] + gem['SWAP'] + gem['Core']

csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.update.csv"
gem.to_csv(csv, index=False)
csv = dr + "GEM.update." + time.strftime("%Y%m%d-%H%M%S") + ".csv"
gem.to_csv(csv, index=False)

gemwip = wipcaaswwt.append(wipcaasvz)
#
gemwip = gemwip.append(wipcaascbts)
#
gemwip = gemwip.append(wipbuild)
gemwip = gemwip.append(wipswap)
gemwip = gemwip.append(wipcore)

cols = ['program',
        'Plant',
        'Line',
        'PO Total Price (USD)',
        'Unit Status',
        'Status',
        'ID']

gemwip = gemwip[cols]

csv = dr + "GEMwip.update" + time.strftime("%Y%m%d-%H%M%S") + ".csv"
gemwip.to_csv(csv, index=False)
csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEMwip.update.csv"
gemwip.to_csv(csv, index=False)

gemxcharge = pd.DataFrame(data,
                  columns=['Status',
                           'ID'])
gemxcharge = pd.merge(gemxcharge, 
                      caasvzxcharge[['Status',
                                     'PO Total Price (USD)',
                                     'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.Vz', 'count': 'CaaS.Vz Count'}),
                      how='left',
                      on='Status')
gemxcharge = pd.merge(gemxcharge,
                      caaswwtxcharge[['Status',
                                      'PO Total Price (USD)',
                                      'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.WWT', 'count': 'CaaS.WWT Count'}),
                      how='left',
                      on='Status')
#ADD CBTS XCHARGE
gemxcharge = pd.merge(gemxcharge,
                      caascbtsxcharge[['Status',
                                      'PO Total Price (USD)',
                                      'count']].rename(index=str, columns={'PO Total Price (USD)': 'CaaS.CBTS', 'count': 'CaaS.CBTS Count'}),
                      how='left',
                      on='Status')
#END CBTS XCHARGE
gemxcharge = pd.merge(gemxcharge,
                      buildxcharge[['Status', 'PO Total Price (USD)', 'count']].rename(index=str, columns={'PO Total Price (USD)': 'Build', 'count': 'Build Count'}),
                      how='left',
                      on='Status')
gemxcharge = pd.merge(gemxcharge,
                      swapxcharge[['Status', 'PO Total Price (USD)', 'count']].rename(index=str, columns={'PO Total Price (USD)': 'SWAP', 'count': 'SWAP Count'}),
                      how='left',
                      on='Status')
gemxcharge = pd.merge(gemxcharge,
                      corexcharge[['Status', 'PO Total Price (USD)', 'count']].rename(index=str, columns={'PO Total Price (USD)': 'Core', 'count': 'Core Count'}),
                      how='left',
                      on='Status')
gemxcharge = gemxcharge.fillna(0)
gemxcharge['Total'] = gemxcharge['CaaS.Vz'] + gemxcharge['CaaS.WWT'] + gemxcharge['CaaS.CBTS'] + gemxcharge['Build'] + gemxcharge['SWAP'] + gemxcharge['Core']

csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.xcharge.update.csv"
gemxcharge.to_csv(csv, index=False)
csv = dr + "GEM.xcharge.update." + time.strftime("%Y%m%d-%H%M%S") + ".csv"
gemxcharge.to_csv(csv, index=False)

# In[]
# =============================================================================
# FORECAST DELIVERIES
# =============================================================================

#forecast = ogcaasvz.append(ogcaaswwt)
##forecast = forecast.append(newcaaswwt)
##ADD CBTS
#forecast = forecast.append(ogcaascbts)
##END CBTS
#forecast = forecast.append(ogcore)
#forecast = forecast.append(ogbuild)
#forecast = forecast.append(ogswap)

forecast = its
forecast = forecast[forecast['Plant'] != 'Global Plant DO NOT SHIP-GBS']

list_of_vz = ['Verizon', 'VERIZON', 'Westcon', 'NCR', 'LLC']

wwtforecast = forecast[forecast['program'] == 'caas.wwt'].copy()
vzforecast = forecast[forecast['program'] == 'caas.vz'].copy()
cbtsforecast = forecast[forecast['program'] == 'caas.cbts'].copy()

wwtforecast['Destination ROS Date'] = pd.to_datetime(wwtforecast['Destination ROS Date'])
wwtforecast = wwtforecast.groupby(wwtforecast['Destination ROS Date'].dt.strftime('%B %Y'))['PO Total Price (USD)'].sum().sort_values()
wwtforecast = wwtforecast.reset_index()
wwtforecast = wwtforecast.rename(index=str,
                                 columns={'PO Total Price (USD)': 'WWT PO Total Price (USD)', 'Destination ROS Date': 'ROS Date'})

vzforecast['Destination ROS Date'] = pd.to_datetime(vzforecast['Destination ROS Date'])
vzforecast = vzforecast.groupby(vzforecast['Destination ROS Date'].dt.strftime('%B %Y'))['PO Total Price (USD)'].sum().sort_values()
vzforecast = vzforecast.reset_index()
vzforecast = vzforecast.rename(index=str,
                               columns={'PO Total Price (USD)': 'Vz PO Total Price (USD)', 'Destination ROS Date': 'ROS Date'})

cbtsforecast['Destination ROS Date'] = pd.to_datetime(cbtsforecast['Destination ROS Date'])
cbtsforecast = cbtsforecast.groupby(cbtsforecast['Destination ROS Date'].dt.strftime('%B %Y'))['PO Total Price (USD)'].sum().sort_values()
cbtsforecast = cbtsforecast.reset_index()
cbtsforecast = cbtsforecast.rename(index=str,
                               columns={'PO Total Price (USD)': 'CBTS PO Total Price (USD)', 'Destination ROS Date': 'ROS Date'})


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
        ['January 2021', 16],
        ['February 2021', 17],
        ['March 2021', 18],
        ['April 2021', 19],
        ['May 2021', 20],
        ['June 2021', 21],
        ['July 2021', 22],
        ['August 2021', 23],
        ['September 2021', 24],
        ['October 2021', 25],]

mths = pd.DataFrame(data,
                    columns=['ROS Date',
                             'ROSDate#'])

forecast = pd.merge(mths, vzforecast, 'left', 'ROS Date')
forecast = pd.merge(forecast, wwtforecast, 'left', 'ROS Date')
forecast = pd.merge(forecast, cbtsforecast, 'left', 'ROS Date')
forecast['Delivery Total'] = forecast.fillna(0)['WWT PO Total Price (USD)'] + forecast.fillna(0)['Vz PO Total Price (USD)'] + forecast.fillna(0)['CBTS PO Total Price (USD)']
csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.forecast.csv"
forecast.to_csv(csv, index=False)

