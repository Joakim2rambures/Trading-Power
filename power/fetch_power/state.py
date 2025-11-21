#state.py
# %%
import json
from pathlib import Path
import pandas as pd

"""""
this code below is to know what is the current state of the algorithm 
It's used by the workflows to understand the current state of teh algorithm
"""""

def load_hwm_map(path: str | Path) -> dict[str, pd.Timestamp]:
    """
    Load per-filter high watermarks from JSON.
    Returns a dict: { filter_id_str: Timestamp(UTC), ... }.
    If file does not exist, returns {}.
    Also handles old single-HWM format for easy migration.
    """
    file_path = Path(path)
    if not file_path.exists():
        return {}

    with open(file_path, "r") as f:
        data = json.load(f)

    # New format: { "4169": "...", "4170": "...", ... }
    hwm_map = {  
        str(key) : pd.Timestamp(ts_str, tz="UTC") for key, ts_str in data.items()
               }

    return hwm_map


def save_hwm_map(path: str | Path, hwm_map: dict[str, pd.Timestamp]) -> None:
    """
    Save per-filter high watermarks as JSON: { filter_id_str: ISO8601_UTC, ... }.
    """
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    serializable = {
        str(fid): pd.Timestamp(ts).tz_convert("UTC").isoformat() for fid, ts in hwm_map.items()
    }

    with open(file_path, "w") as f:
        json.dump(serializable, f)


def load_hwm(path: str | Path):
    file_path = Path(path)
    if not file_path.exists():
        return None
    with open(file_path, "r") as file_read: # that opens the file, 'r' precises it to read only. 
        json_load = json.load(file_read) #this loads the json file and converts it to a python object 
    return pd.Timestamp(json_load["last_timestamp"], tz="UTC")

"""""
load_hwm read json files. reads the last timestamp that was saved 
"""""

def save_hwm(path: str | Path, time_series):
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as open_file:
        json.dump(
            {"last_timestamp": pd.Timestamp(time_series).tz_convert("UTC").isoformat()},
            open_file)
         
# json.dump() turns the python object into a json file 
#converts the data to the timezone you want (in our case UTC, can also be US, Asian...)
#isoforamt() is used to return the iso format 

"""
save_hwm grabs a python object and turns it into a json file. It saves the date in high_watermark json to know what was 
the last date saved
we don't use it to save parquet files
"""

def ensure_utc(time_series):
    time_series = pd.Timestamp(time_series)
    if time_series.tzinfo is None:
        return time_series.tz_localize("UTC")
    else:
        return time_series.tz_convert("UTC")
    
# function above make sure that the python obejct you grab is utc aware
'''
We convert to UTC before floor_to_quarter so that:
all time math (quarters, high-watermark, overlaps) is done in one consistent timezone,
it’s independent of where the code runs (laptop in London vs server in Frankfurt vs container in UTC),
we avoid nasty DST / local-time weirdness.
''' 

def floor_to_quarter(time_series):
    time_series = ensure_utc(time_series)

    m = (time_series.minute // 15) * 15 # standardises the minutes 
    return time_series.replace(minute=m, second=0, microsecond=0)

'''
floor to quarter standardises the minutes : such that if the time point is not realeased 
at at a multiple of 15 (e.g its realted at 4:44, then we get the integer divison for 44/15 = 2 )
'''

def last_full_quarter(now_utc=None):
    if now_utc is None:
        # direct UTC-aware timestamp
        now_utc = pd.Timestamp.now(tz="UTC")
    else:
        now_utc = ensure_utc(now_utc)

    # drop the current in-progress quarter-hour
    return floor_to_quarter(now_utc) - pd.Timedelta(minutes=15)
# %%
