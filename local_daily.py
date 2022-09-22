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
# import os
import yaml
# import pandas as pd
# from pyairtable import Table
# import requests
import webbrowser
# import shutil

# import restapi as oxrest
# import openorders as oxopn

with open("./config.yml", 'r') as stream:
    opsconfigs = yaml.safe_load(stream)
logconfigs = opsconfigs['logging_configs']
loglvl = logconfigs['level']
logging.basicConfig(filename=('/Volumes/GoogleDrive/My Drive/logs/local_daily_' + time.strftime("%Y-%m-%d") + '.log'),
                    level=loglvl,
                    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
logging.info("START dailyrun.py __main__ HERE")

cont = webbrowser.get('Safari')
# cont.open_new('http://google.com')
# cont.open_new('https://reserveohio.com/OhioCampWeb/Facilities/SearchViewUnitAvailabity.aspx')
# cont.open_new('https://www.cars.com/shopping/results/?dealer_id=&keyword=&list_price_max=&list_price_min=&makes[]=volvo&maximum_distance=500&mileage_max=&models[]=volvo-s60_recharge_plug_in_hybrid&models[]=volvo-xc60_recharge_plug_in_hybrid&page_size=20&sort=distance&stock_type=new&year_max=&year_min=&zip=45208')
# cont.open_new('https://www.cars.com/shopping/results/?dealer_id=&keyword=&list_price_max=&list_price_min=&makes[]=jeep&maximum_distance=500&mileage_max=&models[]=jeep-grand_cherokee_4xe&page_size=20&sort=distance&stock_type=new&year_max=&year_min=&zip=45208')
cont.open_new('https://www.amazon.com/dp/B08HS45N13/?tag=thewire06-20&linkCode=xm2&ascsubtag=AwEAAAAAAAAAAiI3&th=1') #Humidifier
# cont.open_new('https://www.amazon.com/Sony-WH-1000XM4-Canceling-Headphones-phone-call/dp/B0863TXGM3/ref=sr_1_7?crid=3NUSO5Y3U8T28&keywords=wh-1000xm3&qid=1652801495&s=electronics&sprefix=WH-1000XM%2Celectronics%2C73&sr=1-7&th=1') #NOISE CANCELLING HEADPHONES
# cont.open_new('https://www.amazon.com/Beats-Studio3-Wireless-Over-Ear-Headphones/dp/B08528YFM2/ref=sr_1_5?keywords=Beats%2BAudio&qid=1652804612&sr=8-5&th=1') #BEATS STUDIO3
logging.info("webpages opened")

logging.info("FINISH dailyrun.py __main__ HERE")
