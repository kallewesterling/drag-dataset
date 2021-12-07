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
import yaml
import pandas as pd
from pathlib import Path
from geopy.geocoders import Nominatim

# +
# Load settings

with open("settings.yml", "r") as stream:
    try:
        settings = yaml.safe_load(stream)
        full_dataset_file = Path(settings['data-directory'] + '/' + settings['full-dataset'])
        skip_data = settings['skip-columns']
        pairings = [(x[0], x[1]) for x in settings['pairings']]
        
        settings['pairings-directory'] = Path(settings['data-directory'] + '/' + settings['pairings-directory'] + '/')
        settings['values-directory'] = Path(settings['data-directory'] + '/' + settings['values-directory'] + '/')
    except yaml.YAMLError as exc:
        print(exc)

print('######## Settings loaded: ################')
print()
for key in settings:
    if type(settings[key]) == str:
        print(f"{key}: {settings[key]}")
        print()
    elif type(settings[key]) == list:
        print(f"{key}:")
        [print(f'- {x}') for x in settings[key]]
        print()
    else:
        print(f"{key}: {settings[key]}")
print('#########################################')

# +
# Variables

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

files_written = []

geo_cache = Path('geo-cache.json')
geolocator = Nominatim(user_agent='drag-dissertation')


# +
# Set up functions, directories, etc

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
    
    return Path(f'data/{type}/{cat}.json')

    
def get_geodata(city):
    if geo_cache.exists():
        geo_data = json.loads(geo_cache.read_text())
    else:
        geo_data = {}
    
    if not city in geo_data:
        print(f'geocoding {city}')
        d = geolocator.geocode(city)
        if not d:
            print(f'ERROR: Could not geocode {city}')
            return {}
        geo_data[city] = {'box': d.raw.get('boundingbox'), 'lat': d.raw.get('lat'), 'lon': d.raw.get('lon')}
        geo_cache.write_text(json.dumps(geo_data))
        
    return geo_data[city]


if not full_dataset_file.parent.exists():
    full_dataset_file.parent.mkdir(parents=True)
    
if not settings['values-directory'].exists():
    settings['values-directory'].mkdir(parents=True)
    
if not settings['pairings-directory'].exists():
    settings['pairings-directory'].mkdir(parents=True)

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

df = pd.read_csv(settings['urls']['live'])
print('Dataframe loaded.')

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

print('Dataframe fixed.')

# +
# Geo data

cities = [x for x in df.City if not x=='—']
cities.extend([x for x in df['Normalized City'] if not x=='—'])
cities = list(set([x.replace('?', '') for x in cities]))
cities = {city: get_geodata(city) for city in cities if city and city != 'Kursaal, Geneva'}

print('Dataframe fixed (geodata).')


# +
def get_geo(row, type):
    if 'norm-' in type:
        city = row['Normalized City']
    else:
        city = row.City
    
    city = city.replace('?', '')
    
    if city == '—' or city == '' or city == 'Kursaal, Geneva':
        return None
    
    type = type.replace('norm-', '')
    
    return get_geodata(city).get(type)

df['lat'] = df.apply(lambda row: get_geo(row, 'lat'), axis=1)
df['lon'] = df.apply(lambda row: get_geo(row, 'lon'), axis=1)
df['box'] = df.apply(lambda row: get_geo(row, 'box'), axis=1)
df['norm-lat'] = df.apply(lambda row: get_geo(row, 'norm-lat'), axis=1)
df['norm-lon'] = df.apply(lambda row: get_geo(row, 'norm-lon'), axis=1)
df['norm-box'] = df.apply(lambda row: get_geo(row, 'norm-box'), axis=1)

# +
# Create clean copy of `df` without columns in `skip_data` and that has `Exclude from viz` checked

df_clean = df.drop(skip_data, axis=1)
df_clean = df_clean.drop(df_clean[df_clean['Exclude from visualization']==True].index)

print('Clean copy of Dataframe created.')

# +
# Set up `Year` column

df_clean['Year'] = df_clean.apply(lambda row: get_year(row), axis=1)
df_clean['Year'] = df_clean['Year'].fillna(0)
df_clean = df_clean.astype({"Year": int})
df_clean['Year'] = df_clean['Year'].replace(0, '')

print('Year column created.')

# +
# Make json string

json_str = df_clean.to_json(orient="records")

print('Full dataset JSON generated.')

# +
# Replace wonky characters (TODO: Fix this more elegantly)

for search, replace in replace_scheme.items():
    json_str = json_str.replace(search, replace)
    
print('Full dataset JSON fixed.')

# +
# Write out new data file if it does not match the existing one

if str(json.loads(json_str)) == str(json_str_existing):
    print('No updated data.')
else:
    full_dataset_file.write_text(json_str)
    print('Updated data written.')

# +
# Add written filepath to `files_written`

files_written.append(str(full_dataset_file.absolute()))
# -



# +
# PART II. VALUES DATASET

# +
# Replace all the null values

df_clean = df_clean.replace('–', '')
df_clean = df_clean.replace('—', '')

print('Cleaned dataset fixed.')

# +
# Set up and empty results dict

results = {}

# +
# Loop through all the columns to generate the `value_counts` for them

for column in df_clean.columns:
    d = {str(k): v for k, v in df_clean[column].value_counts().iteritems()}
    d = dict(sorted(d.items()))
    results[column] = d

print('Values generated from cleaned dataset.')

# +
# Loop through all the results, fix their json-formatted data, and save the individual files

for cat, result in results.items():
    fp = save_result(cat, result, 'values')
    
    # Add written filepath to `files_written`
    files_written.append(str(fp.absolute()))
    
print('All values files saved.')
# -



# +
# PART III. PAIRINGS DATASET

# +
# Set up pairings `results` variable

results = {f'{x}-{y}': {} for x, y in pairings}

print('Pairings dictionary created.')

# +
# Loop through all pairings to generate the keys and values for all of the desired "filters"

print('Creating pairings from cleaned dataset...')

for k, v in pairings:
    print(f'   ... {k} - {v}')
    values = list(zip(list(df_clean[k]), list(df_clean[v])))
    d = {str(x[0]): [] for x in values}
    for x in values:
        cat = str(x[0])
        d[cat].append(x[1])
        d[cat] = sorted(list(set(x for x in d[cat] if x)))
    d = dict(sorted(d.items()))
    results[f'{k}-{v}'] = d
    
print('Done.')

# +
# Loop through all the results, fix their json-formatted data, and save the individual files

for cat, result in results.items():
    fp = save_result(cat, result, 'pairings')

    # Add written filepath to `files_written`
    files_written.append(str(fp.absolute()))

print('All pairings files saved.')
# -



print()
print('*************')
print()
print('Files written:')
print()
for file in files_written:
    print('-' + file)
print()
print('*************')




