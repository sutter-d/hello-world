#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 11:23:42 2022

@author: danielsutter
"""

import json
import sys

print("printing from cred_gen s2s_creds fcn")
# print(sys.argv[1])
json_decoded = json.loads(sys.argv[1])
print("saved secret to obj")
print(json_decoded)
with open("./oxops-gcp-project-service.json", 'w') as json_file:
    json.dump(json_decoded, json_file)
print("saved obj to folder")
