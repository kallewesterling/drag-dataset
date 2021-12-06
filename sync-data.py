# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
from pathlib import Path
import json

try:
    json_str_existing = json.loads(Path('df_clean.json').read_text())
    pass # print(len(json_str_existing), 'records existing')
except FileNotFoundError:
    json_str_existing = []
    print('No data exists - starting from scratch.')
# -

skip_data = [
    'Source',
    'Imported from former archive',
    'Search (fulton)',
    'Search (newspapers.com)',
    'EIMA_Search'
]

urls = {
    "live": "https://docs.google.com/spreadsheets/d/e/2PACX-1vT0E0Y7txIa2pfBuusA1cd8X5OVhQ_D0qZC8D40KhTU3xB7McsPR2kuB7GH6ncmNT3nfjEYGbscOPp0/pub?gid=2042982575&single=true&output=csv"
}

df = pd.read_csv(urls['live'])
df.fillna('', inplace=True)

# +
# Create clean copy

df_clean = df.drop(skip_data, axis=1)
df_clean = df_clean.drop(df_clean[df_clean['Exclude from visualization']==True].index)
# -

# Make JSON
json_str = df_clean.to_json(orient="records")

# +
# Replace wonky characters (TODO: Fix this more elegantly)

replace_scheme = {
    '\\u2014': '',
    '\\u2019': '\'',
    '\\u00a0': ' ',
    '\\u2013': '–',
    '\\u00e9': 'é',
    '\\u201c': '“',
    '\\u201d': '”',
    '\\u00bd': '½'
}

for search, replace in replace_scheme.items():
    json_str = json_str.replace(search, replace)

# +
# Write if new

if str(json.loads(json_str)) == str(json_str_existing):
    pass # print('No updated data.')
else:
    Path('df_clean.json').write_text(json_str)
    print('Updated data written.')
# -


