from pathlib import Path
import json
import yaml
import datetime


debug = False

replace_scheme = (
    {  # Replacement strings for utf-encoded data (To be fixed in future versions)
        "\\u2014": "",
        "\\u2019": "'",
        "\\u00a0": " ",
        "\\u2013": "–",
        "\\u00e9": "é",
        "\\u201c": "“",
        "\\u201d": "”",
        "\\u00bd": "½",
        "\\u2014": "—",
    }
)


def log(msg="", *args, **kwargs):
    if kwargs.get("verbose") == False:
        return

    if kwargs.get("padding_top") or kwargs.get("padding_y"):
        print()
    print(msg)
    if kwargs.get("padding_bottom") or kwargs.get("padding_y"):
        print()


# Load settings

with open("settings.yml", "r") as stream:
    try:
        settings = yaml.safe_load(stream)
        full_dataset_file = Path(
            settings["data-directory"] + "/" + settings["full-dataset"]
        )
        skip_data = settings["skip-columns"]
        pairings = [(x[0], x[1]) for x in settings["pairings"]]

        settings["pairings-directory"] = Path(
            settings["data-directory"] + "/" + settings["pairings-directory"] + "/"
        )
        settings["values-directory"] = Path(
            settings["data-directory"] + "/" + settings["values-directory"] + "/"
        )

        settings["save-unnamed-networks"] = False
    except yaml.YAMLError as e:
        raise RuntimeError(e)

if debug:
    log("######## Settings loaded: ################", padding_y=True)
for key in settings:
    if type(settings[key]) == list:
        if debug:
            log(f"{key}:")
            [log(f"- {x}") for x in settings[key]]
            log()
    else:
        if debug:
            log(f"{key}: {settings[key]}", padding_bottom=True)
if debug:
    log("#########################################", padding_y=True)


def save_result(cat, result, kind, pretty=False):
    '''kind = "values" / "pairing"'''

    def fix_cat(cat):
        cat = cat.lower()
        for search, replace in {"\n": " ", "/": "-", ":": " ", " ": "_"}.items():
            cat = cat.replace(search, replace)
        return cat

    cat = fix_cat(cat)

    if type(result) == dict:
        json_str = json.dumps(result)
    else:
        json_str = result

    _data = json.loads(json_str)
    if pretty:
        json_str = json.dumps(_data, sort_keys=True, indent=2)
    else:
        json_str = json.dumps(_data, separators=(",", ":"))

    # Replace wonky characters (TODO: Fix this more elegantly)
    for search, replace in replace_scheme.items():
        json_str = json_str.replace(search, replace)

    prohibited = json_str.find("\\u")
    if prohibited > 0:
        print("Warning:", prohibited)
        print(json_str[prohibited - 30 : prohibited + 30])

    # print(f'writing {cat}')

    if not Path(f"data/{kind}/{cat}.json").parent.exists():
        Path(f"data/{kind}/{cat}.json").parent.mkdir(parents=True)

    Path(f"data/{kind}/{cat}.json").write_text(json_str)

    return Path(f"data/{kind}/{cat}.json")


# Ensure main directories exist

if not full_dataset_file.parent.exists():
    full_dataset_file.parent.mkdir(parents=True)

if not settings["values-directory"].exists():
    settings["values-directory"].mkdir(parents=True)

if not settings["pairings-directory"].exists():
    settings["pairings-directory"].mkdir(parents=True)

if not Path("data/network").exists():
    Path("data/network").mkdir(parents=True)


class Timer:
    def __init__(self):
        self.s = datetime.datetime.now()

    @property
    def now(self):
        return (datetime.datetime.now() - self.s).seconds

    @property
    def full_start_date(self):
        return self.s.strftime("%Y-%m-%d %H:%M:%S")
