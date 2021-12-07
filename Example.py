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
# For this example we use the built-in `requests` and `json` packages
# and `pandas` version 1.1.3 (see https://github.com/kallewesterling/drag-dataset/blob/main/requirements.txt)

import requests, json
import pandas as pd

# +
# Use requests to load the data

r = requests.get('https://github.com/kallewesterling/drag-dataset/raw/main/data/full.json')

# +
# Ensure that the loading went well

if not r.status_code == 200:
    raise RuntimeError(f'Encountered wrong status code ({r.status_code}), make sure all the addresses are correct.')

# +
# `json` package to read the resulting text

json_data = json.loads(r.text)

# +
# the entire dataset can then easily be loaded into a pandas DataFrame

df = pd.DataFrame(json_data)

# +
# Have a look at the DataFrame

df.head()
# -



# +
# If you want to have a look at a particular pairing of data, here is an example.
# We use the pairing of normalized performer names and normalized city names below
# but you can look in the `/pairings/` directory for more data files.

# +
# Use requests to load the data

r = requests.get('https://github.com/kallewesterling/drag-dataset/raw/main/data/pairings/normalized_performer-normalized_city.json')

# +
# Ensure that the loading went well

if not r.status_code == 200:
    raise RuntimeError(f'Encountered wrong status code ({r.status_code}), make sure all the addresses are correct.')

# +
# `json` package to read the resulting text

json_data = json.loads(r.text)

# +
# Loop through the data to illustrate how it can be used

for performer, city in json_data.items():
    if not performer:
        print(f'{len(city)} cities registered with no named performers.')
        continue
    print(f'{performer} appeared in {len(city)} cities across the dataset. For instance:', '; '.join(city[:3]))

# +
# Pandas can be used to plot
# -

json_data


