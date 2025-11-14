# %%
import json
from pathlib import Path
import pandas as pd

def load_hwm(path: str | Path):
    p = Path(path)
    if not p.exists():
        return None
    with open(p, "r") as f:
        j = json.load(f)
    return pd.Timestamp(j["last_timestamp"], tz="UTC")

def save_hwm(path: str | Path, ts):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(
            {"last_timestamp": pd.Timestamp(ts).tz_convert("UTC").isoformat()},
            f,
        )

def floor_to_quarter(ts):
    ts = pd.Timestamp(ts, tz="UTC")
    m = (ts.minute // 15) * 15
    return ts.replace(minute=m, second=0, microsecond=0)

def last_full_quarter(now_utc=None):
    now_utc = pd.Timestamp.utcnow().tz_localize("UTC") if now_utc is None else pd.Timestamp(now_utc, tz="UTC")
    return floor_to_quarter(now_utc) - pd.Timedelta(minutes=15)