#!usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:59:01 2021

@author: danielsutter
"""

import logging
# import math
import time
import yaml
# import datetime as dt
# import base64
# import json

import pandas as pd
# from pyairtable import Api, Base, Table
import requests

# import restapi as oxrest
import git_compforecast_s2s
import stdcost

requests.packages.urllib3.disable_warnings()
with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/logs/test/abc_compforecast' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
"""
# =============================================================================
# # THIS SCRIPT MAKES USE OF FCNs FROM THE FOLLOWING SCRIPTS
# # 1. COMPFORECAST.PY - THIS SCRIPT PULLS THE PROD SCHEDULE
# # # AND BOMs AND CREATES THE COMPONENT FORECAST
# # 2. STDCOST.PY - THIS SCRIPT PULL THE PROCUREMENT TRACKER
# # # AND CALCLUATES A STANDARD COST FOR EACH CPN (AGGREGATED MPNs)
# # 3. RUN COMPFORECAST.PY AND STORE COMP_FORECAST_MPN_PROC, SRCS, MANF TO DFs
# # 4. RUN STDCOST.PY AND SAVE TO MPN_PROC
# # 5. JOIN THE COMFORECAST AND STD COST
# # 6. OPEN THE CATEGORY CSV STORED IN TREADSTONE AND JOIN
# # 7. RUN A CUMULATIVE SUM COMMAND AND JOIN
# # 8. PULL MANF NAMES AND JOIN
# # 9. FORMAT AND EXPORT TO GDRIVE OPSAUTO/REPORTS
# =============================================================================
"""
# In[START RUNNING COMPONENT FORECAST MAIN() FUNCTION]
logging.info("START RUNNING COMPONENT FORECAST MAIN() FUNCTION")

comps_forecast_mpn_proc, srcs, manf = git_compforecast_s2s.main()

logging.info("FINISH RUNNING COMPONENT FORECAST MAIN() FUNCTION")

# In[START ABC CLASSIFICATION]

stdcost = stdcost.main()

# TRIM COMP FORECAST AND REMOVE PROCUREMENT DATA FOCUSED ON TOTALS AND NOT STANDARD COST
abc = comps_forecast_mpn_proc.loc[:,'cpn':'forecast_total']

# MERGE WITH STDCOST DATA
abc_stdcost = abc.merge(stdcost, 'left', 'cpn')

# USING STD COST DATA, CALCULATE INVENTORY VALUE
abc_stdcost['mpn'] = abc_stdcost['mpn'].fillna('---')
abc_stdcost['proc_mpn'] = abc_stdcost['proc_mpn'].fillna('---')
abc_stdcost = abc_stdcost.fillna(0)
abc_stdcost['inv_value'] = abc_stdcost['total_qty'] * abc_stdcost['std_cost']
abc_stdcost = abc_stdcost.sort_values(by=['inv_value'], ascending=False)

# USE SRCS (OCTOPART SOURCING DATA) TO GENERATE AND AVERAGE LT PER CPN
abcsrcs = srcs[srcs['value']!=0]
abcsrcs = abcsrcs.groupby(['cpn'], as_index=False).mean()
abc_stdcost_lt = abc_stdcost.merge(abcsrcs[['cpn','value']], 'left', 'cpn')

# PULL DURO CATEGORY DATA FROM TREADSTONE LOCAL FOLDER AND JOIN
cat = pd.read_csv("./DURO-CATEGORY-REFERENCES.csv")
abc_stdcost_lt_cat = abc_stdcost_lt.merge(cat, 'left', left_on='category', right_on='Value')

# CALCULATE INV SUM AND INVENTORY CUMULATIVE SUM
sm = abc_stdcost_lt_cat['inv_value'].sum()
cs = abc_stdcost_lt_cat['inv_value'].cumsum()
cs = pd.DataFrame(cs)
cs = cs.rename(columns={'inv_value':'cum_sum'})

# JOIN CUMULATIVE SUM AND CREATE ABC INV INDICATOR
abc_stdcost_lt_cat_cumsum = pd.concat([abc_stdcost_lt_cat, cs], axis=1)
abc_stdcost_lt_cat_cumsum['abc_inv'] = 1
# CUMULATIVE TOP 80% SET TO 9, 80% TO 90% SET TO 4, 90% AND ABOVE STAY AT 1
abc_stdcost_lt_cat_cumsum['abc_inv'].loc[(abc_stdcost_lt_cat_cumsum['cum_sum']<(sm*.9))]=4
abc_stdcost_lt_cat_cumsum['abc_inv'].loc[(abc_stdcost_lt_cat_cumsum['cum_sum']<(sm*.8))]=9

# CREATE ABC LT INDICATOR
abc_stdcost_lt_cat_cumsum['value'] = abc_stdcost_lt_cat_cumsum['value'].fillna(0)
abc_stdcost_lt_cat_cumsum['abc_lt'] = 1
# LT>26 WEEKS SET TO 9, LT>13 WEEKS SET TO 4, LT<13 WEEKS STAY AT 1
abc_stdcost_lt_cat_cumsum['abc_lt'].loc[(abc_stdcost_lt_cat_cumsum['value']>(13*7))]=4
abc_stdcost_lt_cat_cumsum['abc_lt'].loc[(abc_stdcost_lt_cat_cumsum['value']>(26*7))]=9
abc_stdcost_lt_cat_cumsum['RPN'] = abc_stdcost_lt_cat_cumsum['abc_inv']*abc_stdcost_lt_cat_cumsum['abc_lt']
abc_stdcost_lt_cat_cumsum = abc_stdcost_lt_cat_cumsum.rename(columns={'value':'lt_value',
                                                                'Category':'big_cat'})
abc_stdcost_lt_cat_cumsum = abc_stdcost_lt_cat_cumsum.sort_values(by=['RPN'],ascending=False)
# TRIM MANF FOR MANF NAME AND CPN AND JOIN
tmanf = manf[['cpn', 'manf_name']].drop_duplicates()
tmanf = tmanf.groupby(['cpn']).agg(lambda x : x.sum() if x.dtype=='float64' else ', '.join(x))
abc_stdcost_lt_cat_cumsum = abc_stdcost_lt_cat_cumsum.merge(tmanf, 'left', 'cpn').drop(columns=['Value'])

cols_to_move = ['cpn',
                'manf_name',
                'name',
                'big_cat',
                'category',
                'std_cost',
                'inv_value',
                'lt_value',
                'cum_sum',
                'abc_inv',
                'abc_lt',
                'RPN']

cols = cols_to_move + [col for col in abc.columns if col not in cols_to_move]
abc_stdcost_lt_cat_cumsum = abc_stdcost_lt_cat_cumsum[cols]
hash1 = pd.util.hash_pandas_object(abc).sum()
logging.debug("abc hash value = "+str(hash1))
print(hash1)
# In[]
reports = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/Reports/'
histup = '/Volumes/GoogleDrive/Shared drives/Docs/Operations/OpsAutomation/HistUpdates/'
csv = histup + 'ABC_Analysis_' + \
    time.strftime("%Y-%m-%d-%H%M%S") + '.xlsx'

abc_stdcost_lt_cat_cumsum.to_excel(csv, index=False)
