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

full_dataset_file = Path('data/full.json')


replace_scheme = {
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

if not full_dataset_file.parent.exists():
    full_dataset_file.parent.mkdir(parents=True)

try:
    json_str_existing = json.loads(full_dataset_file.read_text())
    pass # print(len(json_str_existing), 'records existing')
except FileNotFoundError:
    json_str_existing = []
    print('No data exists - starting from scratch.')
    
    
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
# Create clean copy

df_clean = df.drop(skip_data, axis=1)
df_clean = df_clean.drop(df_clean[df_clean['Exclude from visualization']==True].index)
# -

# Make JSON
json_str = df_clean.to_json(orient="records")

# +
# Replace wonky characters (TODO: Fix this more elegantly)

for search, replace in replace_scheme.items():
    json_str = json_str.replace(search, replace)

# +
# Write if new

if str(json.loads(json_str)) == str(json_str_existing):
    pass # print('No updated data.')
else:
    full_dataset_file.write_text(json_str)
    print('Updated data written.')

# +
# Below is optional: get specific datasets, where data is matched across the rows, etc.

# +
# Set up directories

if not Path('data/values').exists():
    Path('data/values').mkdir(parents=True)
    
if not Path('data/pairings').exists():
    Path('data/pairings').mkdir(parents=True)


# +
# Set up `Year` column

def get_year(row):
    try:
        return pd.to_datetime(row.Date).year
    except:
        return None

df_clean['Year'] = df_clean.apply(lambda row: get_year(row), axis=1)
df_clean['Year'] = df_clean['Year'].fillna(0)
df_clean = df_clean.astype({"Year": int})
df_clean['Year'] = df_clean['Year'].replace(0, '')

# +
# Replace all the null values

df_clean = df_clean.replace('–', '')
df_clean = df_clean.replace('—', '')

# +
in_out = [
    ('Normalized City', 'Normalized performer'),
    ('Normalized performer', 'Normalized City'),
    ('Year', 'Normalized City'),
    ('Unsure whether drag artist', 'Normalized performer'),
]

results = {f'{x}-{y}': {} for x, y in in_out}

for ix, vals in enumerate(in_out):
    k, v = vals
    values = list(zip(list(df_clean[k]), list(df_clean[v])))
    d = {str(x[0]): [] for x in values}
    for x in values:
        cat = str(x[0])
        d[cat].append(x[1])
        d[cat] = sorted(list(set(x for x in d[cat] if x)))
    d = dict(sorted(d.items()))
    results[f'{k}-{v}'] = d
# -

for cat, result in results.items():
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
    Path(f'data/pairings/{cat}.json').write_text(json_str)

# +
results = {}

for column in df_clean.columns:
    # print(column, list(set(x for x in df_clean[column] if x))[:10])
    d = {str(k): v for k, v in df_clean[column].value_counts().iteritems()}
    d = dict(sorted(d.items()))
    results[column] = d
# -

for cat, result in results.items():
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
    Path(f'data/values/{cat}.json').write_text(json_str)






