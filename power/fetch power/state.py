# %%
import json, fsspec
from datetime import datetime, timezone, timedelta
import pandas as pd

def load_hwm(url: str):
    fs, _, paths = fsspec.get_fs_token_paths(url)
    path = paths[0] if isinstance(paths, list) else paths
    if not fs.exists(path):
        return None
    with fs.open(path, "r") as f:
        j = json.load(f)
    return pd.Timestamp(j["last_timestamp"], tz="UTC")

def save_hwm(url: str, ts):
    fs, _, paths = fsspec.get_fs_token_paths(url)
    path = paths[0] if isinstance(paths, list) else paths
    with fs.open(path, "w") as f:
        json.dump({"last_timestamp": pd.Timestamp(ts).tz_convert("UTC").isoformat()}, f)

def floor_to_quarter(ts):
    ts = pd.Timestamp(ts, tz="UTC")
    m = (ts.minute // 15)*15
    return ts.replace(minute=m, second=0, microsecond=0)

def last_full_quarter(now_utc=None):
    now_utc = pd.Timestamp.utcnow().tz_localize("UTC") if now_utc is None else pd.Timestamp(now_utc, tz="UTC")
    # drop the current in-progress quarter-hour
    return floor_to_quarter(now_utc) - pd.Timedelta(minutes=15)