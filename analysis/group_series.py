# group_series.py 
# %% 
import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.read_data import load_filter_history
from power.fetch_power.smard_filters import FILTER_GROUPS

# We already have generation/forecast/consumption filter groups defined
# in smard_filters.FILTER_GROUPS

def load_group_long(
    filter_group_name: str,
    root: Path = PROJECT_ROOT,
) -> pd.DataFrame:
    """
    Generic loader for a SMARD filter group (generation, forecast, consumption).

    Returns a long DataFrame with columns:
        time (UTC datetime)
        series (technology / type, human-readable label)
        value (MW or whatever SMARD provides)

    For now, assumes only one region (DE) is used.
    """
    filters = FILTER_GROUPS[filter_group_name]
    frames = []

    for filter_id, label in filters.items():
        df = load_filter_history(filter_id, region="DE", root=root)
        if df is None or df.empty:
            continue
        tmp = df.copy()
        tmp = tmp.rename(columns={"time_utc": "time"})
        tmp["series"] = label
        if "value" not in tmp.columns:
            continue
        tmp = tmp[["time", "series", "value"]]
        frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=["time", "series", "value"])

    out = pd.concat(frames, ignore_index=True)
    out["time"] = pd.to_datetime(out["time"], utc=True)
    out = out.sort_values(["series", "time"]).reset_index(drop=True)
    return out


def filter_by_window(df: pd.DataFrame, window: str) -> pd.DataFrame:
    """
    Simple time-window filter (1D, 7D, 30D, 90D, 1Y, max).
    """
    if df.empty or window == "max":
        return df

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True)
    end = df["time"].max()
    if window == "1D":
        delta = pd.Timedelta(days=1)
    elif window == "3D":
        delta = pd.Timedelta(days=3)
    elif window == "7D":
        delta = pd.Timedelta(days=7)
    elif window == "30D":
        delta = pd.Timedelta(days=30)
    elif window == "90D":
        delta = pd.Timedelta(days=90)
    elif window == "1Y":
        delta = pd.Timedelta(days=365)
    else:
        return df

    start = end - delta
    return df[df["time"].between(start, end)]