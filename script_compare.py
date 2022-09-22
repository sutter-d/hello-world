#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 16 13:32:17 2022

@author: danielsutter
"""

from difflib import Differ
  
with open('gdrive_get.py') as file_1, open('gdrive_get_s2s.py') as file_2:
    differ = Differ()
  
    for line in differ.compare(file_1.readlines(), file_2.readlines()):
        print(line)