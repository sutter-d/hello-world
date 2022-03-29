#!usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:59:01 2021

@author: danielsutter
"""

import logging
# import math
import time
# import datetime as dt
# import base64

# import json
import os
import yaml
# import pandas as pd
# from pyairtable import Table
# import requests
# import webbrowser
import shutil

# import restapi as oxrest
# import openorders as oxopn

with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('/Volumes/GoogleDrive/My Drive/logs/local_weekly_backup_' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
logging.info("START local_weekly_backup.py __main__ HERE")

# path to source directory
src_dir = '/Users/danielsutter/Documents/oxide/scripts'
 
# path to destination directory
dest_dir = '/Volumes/GoogleDrive/My Drive/scripts/backup' + time.strftime("%Y-%m-%d")
 
# getting all the files in the source directory
files = os.listdir(src_dir)
 
shutil.copytree(src_dir, dest_dir)

logging.info("FINISH local_weekly_backup.py __main__ HERE")
