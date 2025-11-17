# %%
import json
from pathlib import Path
import pandas as pd

def load_hwm(path: str | Path):
    p = Path(path)
    if not p.exists():
        return None
    with open(p, "r") as f: # that opens the file, 'r' precises it to read only. 
        j = json.load(f) #this loads the json file and converts it to a python object 
    return pd.Timestamp(j["last_timestamp"], tz="UTC")

def save_hwm(path: str | Path, ts):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(
            {"last_timestamp": pd.Timestamp(ts).tz_convert("UTC").isoformat()},
            f,
        ) 
# json.dump() turns the python object into a json file 
#converts the data to the timezone you want (in our case UTC, can also be US, Asian...)
#isoforamt() is used to return the iso format 

"""
save_hwm grabs a python object and turns it into a json file 
"""

def ensure_utc(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    else:
        return ts.tz_convert("UTC")
    
# function above make sure that the python obejct you grab is utc aware
'''
We convert to UTC before floor_to_quarter so that:
all time math (quarters, high-watermark, overlaps) is done in one consistent timezone,
it’s independent of where the code runs (laptop in London vs server in Frankfurt vs container in UTC),
we avoid nasty DST / local-time weirdness.
''' 

def floor_to_quarter(ts):
    ts = ensure_utc(ts)

    m = (ts.minute // 15) * 15 # standardises the minutes 
    return ts.replace(minute=m, second=0, microsecond=0)

'''
floor to quarter standardises the minutes : such that if the time point is not realeased at at a multiple of 15 (e.g its realted at 4:44, then we get the integer divison for 44/15 = 2 )
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
