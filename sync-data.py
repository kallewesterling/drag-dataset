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

import pandas as pd
from geopy.geocoders import Nominatim
from utils import *

# -

T = Timer()

# +
files_written = []

geo_cache = Path("geo-cache.json")
geolocator = Nominatim(user_agent="drag-dissertation")


# +
# Check for existing data

try:
    json_str_existing = json.loads(full_dataset_file.read_text())
    pass  # log(len(json_str_existing), 'records existing')
except FileNotFoundError:
    json_str_existing = []
    log("No data exists - starting from scratch.", padding_bottom=True)


# +
# PART I. MAIN DATASET

# +
# Read in data

df = pd.read_csv(settings["urls"]["live"])
log("Dataframe loaded.", padding_bottom=True)

# +
# Fix `df`

df["Alleged age"] = df["Alleged age"].fillna(0)
df["Assumed birth year"] = df["Assumed birth year"].fillna(0)
df["EIMA_ID"] = df["EIMA_ID"].fillna(0)
df = df.astype(
    {
        "Alleged age": int,
        "Assumed birth year": int,
        "EIMA_ID": int,
    }
)
df.fillna("", inplace=True)

log("Dataframe fixed.", padding_bottom=True)


# +
# Geo data


def get_geodata(city):
    if city == "Bethlehem, PA":
        city = "Bethlehem, PA, USA"

    if geo_cache.exists():
        geo_data = json.loads(geo_cache.read_text())
    else:
        geo_data = {}

    if not city in geo_data:
        log(f"geocoding {city}")

        d = geolocator.geocode(city)
        if not d:
            log(f"ERROR: Could not geocode {city}")
            return {}
        geo_data[city] = {
            "box": d.raw.get("boundingbox"),
            "lat": d.raw.get("lat"),
            "lon": d.raw.get("lon"),
        }
        geo_cache.write_text(json.dumps(geo_data))

    return geo_data[city]


cities = [x for x in df.City if not x == "—"]
cities.extend([x for x in df["Normalized City"] if not x == "—"])
cities = list(set([x.replace("?", "") for x in cities]))
cities = {
    city: get_geodata(city) for city in cities if city and city != "Kursaal, Geneva"
}

log("Data generated (geodata).", padding_bottom=True)


# +
def get_geo(row, type):
    if "norm-" in type:
        city = row["Normalized City"]
    else:
        city = row.City

    city = city.replace("?", "")

    if city == "—" or city == "" or city == "Kursaal, Geneva":
        return None

    type = type.replace("norm-", "")

    return get_geodata(city).get(type)


df["lat"] = df.apply(lambda row: get_geo(row, "lat"), axis=1)
df["lon"] = df.apply(lambda row: get_geo(row, "lon"), axis=1)
df["box"] = df.apply(lambda row: get_geo(row, "box"), axis=1)
df["norm-lat"] = df.apply(lambda row: get_geo(row, "norm-lat"), axis=1)
df["norm-lon"] = df.apply(lambda row: get_geo(row, "norm-lon"), axis=1)
df["norm-box"] = df.apply(lambda row: get_geo(row, "norm-box"), axis=1)

# +
# Create clean copy of `df` without columns in `skip_data` and that has `Exclude from viz` checked

df_clean = df.drop(skip_data, axis=1)
df_clean = df_clean.drop(df_clean[df_clean["Exclude from visualization"] == True].index)

log("Clean copy of Dataframe created.", padding_bottom=True)
# -


# +
# Set up `Year` column


def get_year(row):
    try:
        return pd.to_datetime(row.Date).year
    except:
        return None


df_clean["Year"] = df_clean.apply(lambda row: get_year(row), axis=1)
df_clean["Year"] = df_clean["Year"].fillna(0)
df_clean = df_clean.astype({"Year": int})
df_clean["Year"] = df_clean["Year"].replace(0, "")

log("Year column created.", padding_bottom=True)

# +
# Make json string

json_str = df_clean.to_json(orient="records")

log("Full dataset JSON generated.", padding_bottom=True)

# +
# Replace wonky characters (TODO: Fix this more elegantly)

for search, replace in replace_scheme.items():
    json_str = json_str.replace(search, replace)

log("Full dataset JSON fixed.", padding_bottom=True)

# +
# Write out new data file if it does not match the existing one

if str(json.loads(json_str)) == str(json_str_existing):
    log("No updated data.", padding_bottom=True)
else:
    full_dataset_file.write_text(json_str)
    log("Updated data written.", padding_bottom=True)

# +
# Add written filepath to `files_written`

files_written.append(str(full_dataset_file.absolute()))


# +
# PART II. VALUES DATASET

# +
# Replace all the null values

df_clean = df_clean.replace("–", "")
df_clean = df_clean.replace("—", "")

log("Cleaned dataset fixed.", padding_bottom=True)

# +
# Set up and empty results dict

results = {}

# +
# Loop through all the columns to generate the `value_counts` for them

for column in df_clean.columns:
    d = {str(k): v for k, v in df_clean[column].value_counts().iteritems()}
    d = dict(sorted(d.items()))
    results[column] = d

log("Values generated from cleaned dataset.", padding_bottom=True)

# +
# Loop through all the results, fix their json-formatted data, and save the individual files

for cat, result in results.items():
    fp = save_result(cat, result, "values")

    # Add written filepath to `files_written`
    files_written.append(str(fp.absolute()))
# +
# Save all pairings

fp = save_result("full", results, "values")

# Add written filepath to `files_written`
files_written.append(str(fp.absolute()))
# -

log("All values files saved.", padding_bottom=True)


# +
# PART III. PAIRINGS DATASET

# +
# Set up pairings `results` variable

results = {f"{x}-{y}": {} for x, y in pairings}

log("Pairings dictionary created.", padding_bottom=True)

# +
# Loop through all pairings to generate the keys and values for all of the desired "filters"

log("Creating pairings from cleaned dataset...")
t = Timer()

for k, v in pairings:
    log(f"   ... {k} - {v}")
    values = list(zip(list(df_clean[k]), list(df_clean[v])))
    d = {str(x[0]): [] for x in values}
    for x in values:
        cat = str(x[0])
        d[cat].append(x[1])
        d[cat] = sorted(list(set(x for x in d[cat] if x)))
    d = dict(sorted(d.items()))
    results[f"{k}-{v}"] = d

log(f"Done. ({t.now}s)", padding_bottom=True)

# +
# Loop through all the results, fix their json-formatted data, and save the individual files

for cat, result in results.items():
    fp = save_result(cat, result, "pairings")

    # Add written filepath to `files_written`
    files_written.append(str(fp.absolute()))
# +
# Save all pairings

fp = save_result("full", results, "pairings")

# Add written filepath to `files_written`
files_written.append(str(fp.absolute()))

log("All pairings files saved.", padding_bottom=True)


# +
# PART IV. Network data

import community as community_louvain
import networkx as nx
from utils.network import *
from utils import *  # double up - not necessary

# +
# Set up clean DataFrame for purposes of network data

df = get_clean_network_data(
    min_date=datetime.datetime(year=1930, month=1, day=1),
    max_date=datetime.datetime(year=1940, month=12, day=31),
    verbose=False,
)

# +
# Make json string

json_str = df.to_json(orient="records")

log("Full network dataset JSON generated.", padding_bottom=True)

# +
# Write out new data file - TODO: if it does not match the existing one

fp = save_result("full", json_str, "network")

# Add written filepath to `files_written`
files_written.append(str(fp.absolute()))

log("Updated full network data written.", padding_bottom=True)


# +
# Group the data together

group_data_dict = get_group_data(df)
# -

group_data_json = json.dumps(group_data_dict)

# +
fp = save_result("group-data", group_data_json, "network")

# Add written filepath to `files_written`
files_written.append(str(fp.absolute()))

log("Updated group data written.", padding_bottom=True)


# +
# # ?????

log(f"Creating grouped networks...")
networks = {}

for venue, data in group_data_dict.items():
    for grouped_by, data2 in data.items():

        # If network does not exist, add them
        if not grouped_by in networks:
            networks[grouped_by] = nx.Graph()
            networks[grouped_by].generated = datetime.datetime.now()

        for date_group_id, data3 in data2.items():
            if not len(data3["performers"]) > 1:
                continue

            performers = data3["performers"]
            dates = data3["dates"]
            revues = data3["revues"]
            cities = data3["cities"]
            for performer in performers:
                for target in [x for x in performers if not x == performer]:
                    edge = (performer, target)
                    if not edge in networks[grouped_by].edges:
                        networks[grouped_by].add_edges_from([edge], coLocated={})
                    if not venue in networks[grouped_by].edges[edge]["coLocated"]:
                        networks[grouped_by].edges[edge]["coLocated"][venue] = []
                    if (
                        not dates
                        in networks[grouped_by].edges[edge]["coLocated"][venue]
                    ):
                        networks[grouped_by].edges[edge]["coLocated"][venue].append(
                            dates
                        )

                    if not "revues" in networks[grouped_by].edges[edge]:
                        networks[grouped_by].edges[edge]["revues"] = []
                    if not revues in networks[grouped_by].edges[edge]["revues"]:
                        networks[grouped_by].edges[edge]["revues"].extend(revues)
                        networks[grouped_by].edges[edge]["revues"] = list(
                            set(networks[grouped_by].edges[edge]["revues"])
                        )

                    if not "cities" in networks[grouped_by].edges[edge]:
                        networks[grouped_by].edges[edge]["cities"] = []
                    if not cities in networks[grouped_by].edges[edge]["cities"]:
                        networks[grouped_by].edges[edge]["cities"].extend(cities)
                        networks[grouped_by].edges[edge]["cities"] = list(
                            set(networks[grouped_by].edges[edge]["cities"])
                        )

log(
    f"Grouped network data created (total of {len(networks.keys())} networks)",
    padding_bottom=True,
)
# -


df

# +
# Set up copied filtered network views with no unnamed performers
# If settings['save-unnamed-networks'] is set to False, then delete all other networks

import copy


def drop_unnamed(n):
    return not "unnamed" in n.lower()


log(f"Setting up filtered networks...")

_networks = {}

for key in networks:
    _networks[key] = copy.deepcopy(networks[key])
    _networks[f"{key}-no-unnamed-performers"] = nx.subgraph_view(
        _networks[key], filter_node=drop_unnamed
    )
    _networks[f"{key}-no-unnamed-performers"].generated = datetime.datetime.now()

networks = _networks

if settings.get("save-unnamed-networks") == False:
    _networks = {}

    for key in networks:
        if "no-unnamed" in key:
            _networks[key] = copy.deepcopy(networks[key])

    networks = _networks
    log("Deleted networks with unnamed performers.")

log(f"Filtered networks set up.", padding_bottom=True)


# +
# Add `weights` attribute for edges

for key in networks:
    for edge in list(networks[key].edges):
        networks[key].edges[edge]["weights"] = {}
        for co_located, date_groups in networks[key].edges[edge]["coLocated"].items():
            networks[key].edges[edge]["weights"]["dateGroups"] = len(date_groups)
        networks[key].edges[edge]["weights"]["venues"] = len(
            networks[key].edges[edge]["coLocated"]
        )

log(f"Added weights attributes for all networks' edges.", padding_bottom=True)


# +
# Generating metadata for connected nodes in each network


def get_unique_networks(connected_nodes_per_node):
    def get_connected_nodes_per_node(G):
        return {
            node: sorted(nx.bfs_tree(G, node, reverse=False).nodes) for node in G.nodes
        }

    if isinstance(connected_nodes_per_node, dict):
        pass
    elif isinstance(connected_nodes_per_node, nx.classes.graph.Graph):
        connected_nodes_per_node = get_connected_nodes_per_node(
            connected_nodes_per_node
        )
    else:
        raise RuntimeError(
            "connected_nodes_per_node provided must be either a dictionary of nodes connected together or a networkx Graph object."
        )

    unique_networks = []
    for network in list(connected_nodes_per_node.values()):
        if not network in unique_networks:
            unique_networks.append(network)
    return unique_networks


log(f"Adding unique connected nodes for each network...")
t = Timer()

for key in networks:
    log(f"    {key}...")
    unique_networks = get_unique_networks(networks[key])

    for network_id, unique_network in enumerate(unique_networks, start=1):
        for performer in unique_network:
            networks[key].nodes[performer]["connected"] = {
                "network": {
                    "nodes": [x for x in unique_network if not x == performer],
                    "network_id": network_id,
                }
            }

log(f"Done. ({t.now}s)", padding_bottom=True)


# +
# Generate community algorithm data


def merge_community_dicts(*args):
    _ = {}
    for dictionary in args:
        for performer, data in dictionary.items():
            if not performer in _:
                _[performer] = {}
            for key, value in data.items():
                if not key in _[performer]:
                    if isinstance(value, dict):
                        _[performer][key] = {}
                    else:
                        raise NotImplemented("Nope")
                for key2, value2 in value.items():
                    if not key2 in _[performer][key]:
                        _[performer][key][key2] = value2
                    else:
                        raise NotImplemented("This should not happen")

    return _


log(f"Generating community data for each network...")
t = Timer()

for key in networks:
    log(f"    {key}...")

    louvain = community_louvain.best_partition(networks[key])
    louvain = {
        performer: {"modularities": {"Louvain": community_number}}
        for performer, community_number in louvain.items()
    }

    c = nx.community.greedy_modularity_communities(networks[key])
    clauset_newman_moore = {
        performer: {"modularities": {"Clauset-Newman-Moore": community_number}}
        for community_number, list_of_performers in enumerate(c, start=1)
        for performer in list_of_performers
    }

    community_dicts = merge_community_dicts(louvain, clauset_newman_moore)

    nx.set_node_attributes(networks[key], community_dicts)

    for performer in networks[key].nodes:
        networks[key].nodes[performer]["centralities"] = {}

    for performer, degree in nx.degree_centrality(networks[key]).items():
        networks[key].nodes[performer]["centralities"][
            "degree_centrality_100x"
        ] = round(degree * 100, 6)

    for performer, degree in nx.betweenness_centrality(
        networks[key], k=len(networks[key].nodes)
    ).items():
        networks[key].nodes[performer]["centralities"][
            "betweenness_centrality_100x"
        ] = round(degree * 100, 6)

    for performer, degree in nx.eigenvector_centrality(
        networks[key], max_iter=1000, weight="weight"
    ).items():
        networks[key].nodes[performer]["centralities"][
            "eigenvector_centrality_100x"
        ] = round(degree * 100, 6)

    for performer, degree in nx.closeness_centrality(networks[key]).items():
        networks[key].nodes[performer]["centralities"][
            "closeness_centrality_100x"
        ] = round(degree * 100, 6)

log(f"Done. ({t.now}s)", padding_bottom=True)


# +
# Generate degree information


def get_degrees(G, node):
    indegree = sum([1 for edge in G.edges if edge[0] == node])
    outdegree = sum([1 for edge in G.edges if edge[1] == node])
    degree = indegree + outdegree

    return {"indegree": indegree, "outdegree": outdegree, "degree": degree}


log(f"Generating degree inforation for each network...")
t = Timer()

for key in networks:
    log(f"    {key}...")

    degrees = {
        node: {"degrees": get_degrees(networks[key], node)}
        for node in networks[key].nodes
    }
    nx.set_node_attributes(networks[key], degrees)

log(f"Done. ({t.now}s)", padding_bottom=True)


# +
# Generate other meta information necessary for visualization

import unicodedata


def slugify(value, allow_unicode=False, verbose=False):
    init_value = str(value)
    value = init_value
    value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    value = re.sub(r"^(\d+)", r"n\1", value)
    value = re.sub(r"[-\s]+", "_", value).strip("-_")
    if verbose:
        clear_output(wait=True)
        log(f"Making slug from {init_value}: {value}", verbose=verbose)
    return value


log(f"Finalizing meta data for each network...")
t = Timer()

for key in networks:
    log(f"    {key}...")

    for node in networks[key].nodes:
        networks[key].nodes[node]["node_id"] = slugify(node)
        networks[key].nodes[node]["category"] = "performer"
        networks[key].nodes[node]["display"] = node

    for edge in networks[key].edges:
        networks[key].edges[edge]["edge_id"] = slugify(f"{edge[0]}-{edge[1]}")
        networks[key].edges[edge]["comments"] = []
        networks[key].edges[edge]["general_comments"] = []

        networks[key].edges[edge]["found"] = []
        for _, dates in networks[key].edges[edge]["coLocated"].items():
            for datelist in dates:
                for date in datelist:
                    if not date in networks[key].edges[edge]["found"]:
                        networks[key].edges[edge]["found"].append(date)

        networks[key].edges[edge]["comments"] = {
            "venues": {},
            "cities": {},
            "revues": {},
        }

    networks[key].finished = datetime.datetime.now()

log(f"Done. ({t.now}s)", padding_bottom=True)


# +
for key in networks:
    file_name = f"live-co-occurrence-{key}"

    data = nx.node_link_data(networks[key])
    data["createdDate"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    diff = datetime.datetime.now() - networks[key].generated
    data["timeToCreate"] = {
        "minutes": diff.seconds // 60,
        "seconds": diff.seconds % 60,
        "totalInSeconds": diff.seconds,
    }
    data["days"] = re.findall(r"\d+", key)[0]

    fp = save_result(file_name, data, f"network/live")

    # Add written filepath to `files_written`
    files_written.append(str(fp.absolute()))

log("Network data files written.", padding_bottom=True)


# +
log(f"Generating ego network for each node in the 14-day separated dataset...")
t = Timer()

ego_networks = {
    node: nx.ego.ego_graph(
        networks["grouped-by-14-days-no-unnamed-performers"], node, 1
    )
    for node in networks["grouped-by-14-days-no-unnamed-performers"].nodes()
}
ego_networks = {k: nx.node_link_data(v)["links"] for k, v in ego_networks.items()}

log(f"Done. ({t.now}s)", padding_bottom=True)

# +
fp = save_result("ego-networks-14-days-no-unnamed", ego_networks, "network/live")

# Add written filepath to `files_written`
files_written.append(str(fp.absolute()))

log(f"Saved ego network datafile.")
# -


log("*************", padding_y=True)
log(f"Seconds to execute: {T.now}", padding_bottom=True)
log("Files written:", padding_bottom=True)
for file in files_written:
    log("- " + file)
log("*************", padding_y=True)
