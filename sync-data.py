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
# Imports

import json
import pandas as pd
from pathlib import Path

# +
# Variables

full_dataset_file = Path('data/full.json') # Name for where to store the full dataset

replace_scheme = { # Replacement strings for utf-encoded data (To be fixed in future versions)
    '\\u2014': '',
    '\\u2019': '\'',
    '\\u00a0': ' ',
    '\\u2013': '–',
    '\\u00e9': 'é',
    '\\u201c': '“',
    '\\u201d': '”',
    '\\u00bd': '½',
    '\\u2014': '—'
}

skip_data = [ # Columns to clear out in clean dataset
    'Source',
    'Imported from former archive',
    'Search (fulton)',
    'Search (newspapers.com)',
    'EIMA_Search'
]

urls = { # URLs - right now, we only use the `live` one
    "live": "https://docs.google.com/spreadsheets/d/e/2PACX-1vT0E0Y7txIa2pfBuusA1cd8X5OVhQ_D0qZC8D40KhTU3xB7McsPR2kuB7GH6ncmNT3nfjEYGbscOPp0/pub?gid=2042982575&single=true&output=csv"
}

pairings = [ # Here are the pairings that will be generated. Add to it if you want to create other datafiles.
    ('Normalized City', 'Normalized performer'),
    ('Normalized performer', 'Normalized City'),
    ('Year', 'Normalized City'),
    ('Unsure whether drag artist', 'Normalized performer'),
    ('Normalized performer', 'Normalized Venue'),
    ('Normalized Venue', 'Normalized performer'),
    ('Source clean', 'Normalized performer'),
    ('Normalized performer', 'Newspaper_ID'),
    ('Normalized performer', 'EIMA_ID'),
    ('Normalized performer', 'Comment on node: performer'),
    ('Normalized Venue', 'Comment on node: venue'),
    ('Normalized City', 'Comment on node: city'),
    ('Normalized Revue Name', 'Comment on edge: revue'),
    ('Newspaper', 'Normalized performer'),
    ('Normalized Revue Name', 'Normalized performer'),
    ('Has image', 'Normalized performer')
]

# +
# Set up functions, directories, etc

if not full_dataset_file.parent.exists():
    full_dataset_file.parent.mkdir(parents=True)

def fix_cat(cat):
    cat = cat.lower()
    for search, replace in {
        '\n': ' ',
        '/': '-',
        ':': ' ',
        ' ': '_'
    }.items():
        cat = cat.replace(search, replace)
    return cat

def get_year(row):
    try:
        return pd.to_datetime(row.Date).year
    except:
        return None

def save_result(cat, result, type):
    ''' type = "values" / "pairing" '''
    
    cat = fix_cat(cat)

    # Replace wonky characters (TODO: Fix this more elegantly)
    json_str = json.dumps(result)
    
    for search, replace in replace_scheme.items():
        json_str = json_str.replace(search, replace)
    
    prohibited = json_str.find('\\u')
    if prohibited > 0:
        print('Warning:', prohibited)
        print(json_str[prohibited-30:prohibited+30])

    # print(f'writing {cat}')
    Path(f'data/{type}/{cat}.json').write_text(json_str)
    
    return True
    
if not Path('data/values').exists():
    Path('data/values').mkdir(parents=True)
    
if not Path('data/pairings').exists():
    Path('data/pairings').mkdir(parents=True)

# +
# Check for existing data

try:
    json_str_existing = json.loads(full_dataset_file.read_text())
    pass # print(len(json_str_existing), 'records existing')
except FileNotFoundError:
    json_str_existing = []
    print('No data exists - starting from scratch.')
# -



# +
# PART I. MAIN DATASET

# +
# Read in data

df = pd.read_csv(urls['live'])

# +
# Fix `df`

df['Alleged age'] = df['Alleged age'].fillna(0)
df['Assumed birth year'] = df['Assumed birth year'].fillna(0)
df['EIMA_ID'] = df['EIMA_ID'].fillna(0)
df = df.astype({
    "Alleged age": int,
    "Assumed birth year": int,
    "EIMA_ID": int,
})
df.fillna('', inplace=True)

# +
# Create clean copy of `df` without columns in `skip_data` and that has `Exclude from viz` checked

df_clean = df.drop(skip_data, axis=1)
df_clean = df_clean.drop(df_clean[df_clean['Exclude from visualization']==True].index)

# +
# Set up `Year` column

df_clean['Year'] = df_clean.apply(lambda row: get_year(row), axis=1)
df_clean['Year'] = df_clean['Year'].fillna(0)
df_clean = df_clean.astype({"Year": int})
df_clean['Year'] = df_clean['Year'].replace(0, '')

# +
# Make json string

json_str = df_clean.to_json(orient="records")

# +
# Replace wonky characters (TODO: Fix this more elegantly)

for search, replace in replace_scheme.items():
    json_str = json_str.replace(search, replace)

# +
# Write out new data file if it does not match the existing one

if str(json.loads(json_str)) == str(json_str_existing):
    pass # print('No updated data.')
else:
    full_dataset_file.write_text(json_str)
    print('Updated data written.')
# -



# +
# PART II. VALUES DATASET

# +
# Replace all the null values

df_clean = df_clean.replace('–', '')
df_clean = df_clean.replace('—', '')

# +
# Set up and empty results dict

results = {}

# +
# Loop through all the columns to generate the `value_counts` for them

for column in df_clean.columns:
    d = {str(k): v for k, v in df_clean[column].value_counts().iteritems()}
    d = dict(sorted(d.items()))
    results[column] = d

# +
# Loop through all the results, fix their json-formatted data, and save the individual files

for cat, result in results.items():
    save_result(cat, result, 'values')
# -



# +
# PART III. PAIRINGS DATASET

# +
# Set up pairings `results` variable

results = {f'{x}-{y}': {} for x, y in pairings}

# +
# Loop through all pairings to generate the keys and values for all of the desired "filters"

for k, v in pairings:
    values = list(zip(list(df_clean[k]), list(df_clean[v])))
    d = {str(x[0]): [] for x in values}
    for x in values:
        cat = str(x[0])
        d[cat].append(x[1])
        d[cat] = sorted(list(set(x for x in d[cat] if x)))
    d = dict(sorted(d.items()))
    results[f'{k}-{v}'] = d

# +
# Loop through all the results, fix their json-formatted data, and save the individual files

for cat, result in results.items():
    save_result(cat, result, 'pairings')
# -


