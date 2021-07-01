# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 15:10:21 2020

@author: sutter.d
"""


# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 10:44:16 2019

@author: sutter.d
"""


import pandas as pd
# import datetime as dt
import time
import glob
import os

data = [['Ordered', 'Staging', 3],
        ['A1 - Unit Not Ordered (Needs Review)', 'Staging', 3],
        ['Not Ordered', 'Staging', 3],
        ['BR Approved', 'Staging', 3],
        ['Draft', 'Staging', 3],
        ['Alteration Requested', 'Staging', 3],
        ['BR Submitted', 'Staging', 3],
        ['At MAV', 'Staging', 3],
        ['Prepare For Pick Up', 'Prepare for Pick Up', 4],
        ['Shipped', 'Shipped', 5],
        ['Pick Up Confirmed', 'Shipped', 5],
        ['VAT Processed', 'Shipped', 5],
        ['Customs', 'Customs', 6],
        ['Delivered', 'Delivered', 7]]

us = pd.DataFrame(data,
                  columns=['Unit Status',
                           'Status',
                           'ID'])

# =============================================================================
# def gemstatus(x):
# 
#     wipraw = x
# 
#     wiprawcols = wipraw.columns
#     wiprawcols = wiprawcols.str.replace("\n", "")
#     wipraw.columns = wiprawcols
# 
#     wip = wipraw[['Plant', 'Line', 'P&G Part #', 'Supplier', 'PO Total Price (USD)', 'Unit Status']]
#     wip = wip.sort_values(by=['Plant', 'Line'], ascending=False)
#     wip = wip[wip['Unit Status'] != 'Cancelled']
#     wip = wip[wip['Plant'] != 'Global Plant DO NOT SHIP-GBS']
#     wip = wip[['Plant', 'Line', 'PO Total Price (USD)', 'Unit Status']]
# 
#     wipshort = wip.groupby(['Plant', 'Line', 'Unit Status']).agg('sum')
#     wipshort.reset_index(level=['Plant', 'Line', 'Unit Status'], inplace=True)
#     wipshort = pd.merge(wipshort, us, how='left', on='Unit Status')
# 
#     wipstatus = wipshort.drop(columns=['Plant', 'Line', 'Unit Status']).groupby(['Status', 'ID']).agg('sum')
#     wipstatus.reset_index(level=['Status', 'ID'], inplace=True)
# 
#     ls = wipshort[['Status', 'Plant', 'Line']].sort_values(by=['Status'], ascending = False)
#     ls = ls.drop_duplicates(keep='first')
#     ls.reset_index(drop=True, inplace=True)
#     ls['count'] = 1
#     ls = ls.groupby('Status').agg('sum')
#     ls.reset_index(inplace=True)
# 
#     if len(wipstatus.index)>0:    
#         wipstatus = pd.merge(wipstatus,
#                              ls,
#                              how='left',
#                              on='Status').sort_values(by=['ID'], ascending = True)
#         wipstatus.reset_index(drop = True, inplace=True)
#         wipstatus = wipstatus[['Status',
#                                'ID',
#                                'PO Total Price (USD)',
#                                'count']]
# 
#     gdns = x
# 
#     gdns = gdns[gdns['Unit Status'] != 'Cancelled']
#     gdns = gdns[gdns['Plant'] == 'Global Plant DO NOT SHIP-GBS']
# 
#     gdns = gdns[['Plant', 'PO Total Price (USD)']]
# 
#     gdns = gdns.groupby('Plant').agg('sum')
#     gdns.reset_index(inplace=True)
# 
#     gdns['Status'] = 'Inventory'
#     gdns['ID'] = 2
#     # gdns['count'] = '-'
#     gdns['count'] = 0
# 
#     gdns = gdns[['Status',
#                  'ID',
#                  'PO Total Price (USD)',
#                  'count']]
# 
#     gemsum = gdns.append(wipstatus)
# 
#     return gemsum
# =============================================================================

# =============================================================================
# def gemxcharge(x):
# 
#     wipraw = x
# 
#     wiprawcols = wipraw.columns
#     wiprawcols = wiprawcols.str.replace("\n","")
#     wipraw.columns = wiprawcols
# 
#     wip = wipraw[['Plant',
#                   'Line',
#                   'P&G Part #',
#                   'Supplier',
#                   'PO Total Price (USD)',
#                   'Unit Status',
#                   'Xcharge Date']]
#     wip = wip.sort_values(by=['Plant', 'Line'], ascending=False)
#     wip = wip[wip['Unit Status'] != 'Cancelled']
#     wip = wip[wip['Plant'] != 'Global Plant DO NOT SHIP-GBS']
#     wip = wip[wip['Xcharge Date'].isnull()]
#     wip = wip[['Plant', 'Line', 'PO Total Price (USD)', 'Unit Status']]
# 
#     wipshort = wip.groupby(['Plant', 'Line', 'Unit Status']).agg('sum')
#     wipshort.reset_index(level=['Plant', 'Line', 'Unit Status'], inplace=True)
#     wipshort = pd.merge(wipshort, us, how='left', on='Unit Status')
# 
#     wipstatus = wipshort.drop(columns=['Plant',
#                                        'Line',
#                                        'Unit Status']).groupby(['Status', 'ID']).agg('sum')
#     wipstatus.reset_index(level=['Status', 'ID'], inplace=True)
# 
#     ls = wipshort[['Status',
#                    'Plant', 'Line']].sort_values(by=['Status'], ascending=False)
#     ls = ls.drop_duplicates(keep='first')
#     ls.reset_index(drop=True, inplace=True)
#     ls['count'] = 1
#     ls = ls.groupby('Status').agg('sum')
#     ls.reset_index(inplace=True)
# 
#     if len(wipstatus.index)>0:    
#         wipstatus = pd.merge(wipstatus,
#                              ls,
#                              how='left',
#                              on='Status').sort_values(by=['ID'], ascending = True)
#         wipstatus.reset_index(drop=True, inplace=True)
#         wipstatus = wipstatus[['Status',
#                                'ID',
#                                'PO Total Price (USD)',
#                                'count']]
# 
# 
#     gdns = x
# 
#     gdns = gdns[gdns['Unit Status'] != 'Cancelled']
#     gdns = gdns[gdns['Plant'] == 'Global Plant DO NOT SHIP-GBS']
# 
#     gdns = gdns[['Plant', 'PO Total Price (USD)']]
# 
#     gdns = gdns.groupby('Plant').agg('sum')
#     gdns.reset_index(inplace=True)
# 
#     gdns['Status'] = 'Inventory'
#     gdns['ID'] = 2
#     gdns['count'] = '-'
# 
#     gdns=gdns[['Status',
#                'ID',
#                'PO Total Price (USD)',
#                'count']]
# 
#     gemsum = gdns.append(wipstatus)
# 
#     return gemsum
# =============================================================================


def historicals(x):

    x = y
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
                  'Qty',
                  'Report']].copy()
    indexes_to_drop = []
    for index, row in wip.iterrows():
        if row['Report'] == '2426':
            if row['Supplier'] != 'World Wide Technology':
                indexes_to_drop.append(index)
        elif row['Report'] == '2428':
            if row['Supplier'] == 'World Wide Technology':
                pass
            elif row['Supplier'] == 'Natural Nydegger Transport Co.':
                pass
            else:
                indexes_to_drop.append(index)

    wipkeep = wip[~wip.index.isin(indexes_to_drop)]

    wipdrop = wip[wip.index.isin(indexes_to_drop)]

    wipkeep = wipkeep.sort_values(by=['Supplier',
                                      'P&G Part #'], ascending=False)
    wipkeep = wipkeep[~wipkeep['Unit Status'].isin(['Cancelled',
                                                    'A1 - Unit Not Ordered (Needs Review)',
                                                    'Cancelled - Before Ordered'])]
    wipkeep = wipkeep[wipkeep['Plant'] != 'Global Plant DO NOT SHIP-GBS']
    wipkeep = wipkeep[['Supplier',
                       'P&G Part #',
                       'Plant',
                       'Line',
                       'Qty',
                       'PO Total Price (USD)',
                       'Unit Status']]

    wipshort = wipkeep.groupby(['Supplier',
                                'P&G Part #',
                                'Plant']).agg('sum')
    wipshort.reset_index(level=['Supplier',
                                'P&G Part #',
                                'Plant'], inplace=True)

    wipshort = wipshort[wipshort['Supplier'] == 'World Wide Technology']
    wipshort = wipshort.sort_values(by=['Supplier',
                                        'P&G Part #'], ascending=False)

    return wipshort



list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.Vz/*') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)
print(latest_file)
y = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)

y['Report'] = '2426'
ogcaasvz = y
wipcaasvz = historicals(y)

list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/CaaS.WWT/*') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)
print(latest_file)
y = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)

y['Report'] = '2428'
ogcaaswwt = y
wipcaaswwt = historicals(y)

hist = wipcaasvz.append(wipcaaswwt)

histshort = hist.groupby(['Supplier', 'P&G Part #']).agg('sum')
histshort.reset_index(level=['Supplier', 'P&G Part #'], inplace=True)

sitelist = hist['Plant']
sitelist = sitelist.drop_duplicates(keep='first')
sitecount = sitelist.count()   

histshort['SiteCount'] = sitecount
histshort['QtyMean'] = histshort['Qty'] / histshort['SiteCount']
histshort['CostMean'] = histshort['PO Total Price (USD)'] / histshort['SiteCount']
histshort.loc['Total'] = histshort.sum()









































# =============================================================================
# list_of_files = glob.glob('C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/Core/*') # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# print(latest_file)
# y = pd.read_excel(latest_file, sheet_name='Level3-Details', header=6)
# 
# ogcore = y
# core = gemstatus(y)
# wipcore = wipstatus(y)
# corexcharge = gemxcharge(y)
# wipcore['program'] = 'core'
# 
# data = [['Inventory', 2],
#         ['Staging', 3],
#         ['Prepare for Pick Up', 4],
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
# gem['Total'] = gem['CaaS.Vz'] + gem['CaaS.WWT'] + gem['Build'] + gem['SWAP'] + gem['Core']
# 
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.update.csv"
# gem.to_csv(csv, index=False)
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/Hist.Updates/GEM.update." + time.strftime("%Y%m%d-%H%M%S") + ".csv"
# gem.to_csv(csv, index=False)
# 
# gemwip = wipcaaswwt.append(wipcaasvz)
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
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/Hist.Updates/GEMwip.update" + time.strftime("%Y%m%d-%H%M%S") + ".csv"
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
# gemxcharge['Total'] = gemxcharge['CaaS.Vz'] + gemxcharge['CaaS.WWT'] + gemxcharge['Build'] + gemxcharge['SWAP'] + gemxcharge['Core']
# 
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.xcharge.update.csv"
# gemxcharge.to_csv(csv, index=False)
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/Hist.Updates/GEM.xcharge.update." + time.strftime("%Y%m%d-%H%M%S") + ".csv"
# gemxcharge.to_csv(csv, index=False)
# 
# 
# 
# # =============================================================================
# # FORECAST DELIVERIES
# # =============================================================================
# 
# forecast = ogcaasvz.append(ogcaaswwt)
# forecast = forecast.append(ogcore)
# forecast = forecast.append(ogbuild)
# forecast = forecast.append(ogswap)
# 
# forecast = forecast[forecast['Plant'] != 'Global Plant DO NOT SHIP-GBS']
# 
# list_of_vz = ['Verizon', 'VERIZON']
# 
# wwtforecast = forecast[forecast['Supplier'] == 'World Wide Technology'].copy()
# 
# vzforecast = forecast[forecast['Supplier'].str.contains('|'.join(list_of_vz))].copy()
# 
# #vzforecast = vzforecast.groupby(vzforecast['Ship Date'].dt.strftime('%B %Y'))['PO Total Price (USD)'].sum().sort_values()
# #vzforecast = vzforecast.reset_index()
# #vzforecast = vzforecast.rename(index=str,
# #                               columns={'PO Total Price (USD)': 'Vz PO Total Price (USD)'})
# #
# #wwtforecast = wwtforecast.groupby(wwtforecast['Ship Date'].dt.strftime('%B %Y'))['PO Total Price (USD)'].sum().sort_values()
# #wwtforecast = wwtforecast.reset_index()
# #wwtforecast = wwtforecast.rename(index=str,
# #                                 columns={'PO Total Price (USD)': 'WWT PO Total Price (USD)'})
# 
# vzforecast = vzforecast.groupby(vzforecast['Destination ROS Date'].dt.strftime('%B %Y'))['PO Total Price (USD)'].sum().sort_values()
# vzforecast = vzforecast.reset_index()
# vzforecast = vzforecast.rename(index=str,
#                                columns={'PO Total Price (USD)': 'Vz PO Total Price (USD)', 'Destination ROS Date': 'ROS Date'})
# 
# wwtforecast = wwtforecast.groupby(wwtforecast['Destination ROS Date'].dt.strftime('%B %Y'))['PO Total Price (USD)'].sum().sort_values()
# wwtforecast = wwtforecast.reset_index()
# wwtforecast = wwtforecast.rename(index=str,
#                                  columns={'PO Total Price (USD)': 'WWT PO Total Price (USD)', 'Destination ROS Date': 'ROS Date'})
# 
# 
# 
# data = [['October 2019', 1],
#         ['November 2019', 2],
#         ['December 2019', 3],
#         ['January 2020', 4],
#         ['February 2020', 5],
#         ['March 2020', 6],
#         ['April 2020', 7],
#         ['May 2020', 8],
#         ['June 2020', 9],
#         ['July 2020', 10],
#         ['August 2020', 11],
#         ['September 2020', 12],
#         ['October 2020', 13],
#         ['November 2020', 14]]
# 
# mths = pd.DataFrame(data,
#                     columns=['ROS Date',
#                              'ROSDate#'])
# 
# forecast = pd.merge(mths, vzforecast, 'left', 'ROS Date')
# forecast = pd.merge(forecast, wwtforecast, 'left', 'ROS Date')
# forecast['Delivery Total'] = forecast.fillna(0)['WWT PO Total Price (USD)'] + forecast.fillna(0)['Vz PO Total Price (USD)']
# csv = "C:/Users/sutter.d/Procter and Gamble/ITS Hardware Inventory - Documents/General/ESMs/GEM.forecast.csv"
# forecast.to_csv(csv, index=False)
# =============================================================================

