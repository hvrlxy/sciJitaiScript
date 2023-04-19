import json
import logging
import os
import datetime
import warnings
import pandas as pd


# read the xlsx file, first sheet into a pandas dataframe
# get the first sheet
df = pd.read_excel('/Users/hale/Desktop/sciJitaiScript/docs/SCI JITAI Messages.xlsx', sheet_name='Fortune Cookie Messages')

# get the first column
df = df['Fortune Cookie']
#turn into a list
df = df.tolist()
# remove comma in each item
df = [item.replace(',', '') for item in df]
#print the list with string wrap in double quotes
print(json.dumps(df, indent=4))
