# %%

import seaborn as sb 
import pandas as pd 
from pathlib import Path 
import sys 
PROJECT_ROOT = Path(__file__).resolve().parent.parent #TRADING-POWER/ 
if str(PROJECT_ROOT) not in sys.path: 
    sys.path.insert(0, str(PROJECT_ROOT)) 

from analysis.read_data import load_filter_history 
from power.fetch_power.smard_filters import FILTER_GROUPS

# Time windows for your dashboard
WINDOWS = {
    "1D": "1D",
    "7D": "7D",
    "30D": "30D",
    "1Y": "365D",
    "max": None,  # full history
}


def load_market_price_series(filter_group_name: str = "market_price",
                             root: Path = PROJECT_ROOT) -> dict:
    """
    Load all SMARD market price series into a dict of DataFrames.
    Keys are filter_ids from FILTER_GROUPS[filter_group_name].
    """
    filters = FILTER_GROUPS[filter_group_name]
    all_series = {}
    for filter_id in filters.keys():
        df = load_filter_history(filter_id, root=root)
        all_series[filter_id] = df
    return all_series


def series_to_long(all_series: dict) -> pd.DataFrame:
    """
    Convert dict of DataFrames {filter_id: df} into a long DataFrame with:
        time, zone, price

    Assumes each df has 'time_utc' and 'value' columns.

    """

    FILTER_ID_TO_COUNTRY = {
    "4169": "DE",
    "4170": "NL",
    "256": "BE",
}
    frames = []

    for filter_id, df in all_series.items():
        if df is None or df.empty:
            continue

        tmp = df.copy()
        # Ensure datetime
        tmp = tmp.rename(columns={"time_utc": "time"})

        # Use filter_id as "zone" label for now (DE, AT, BE, etc, or SMARD id)
        tmp["zone"] = FILTER_ID_TO_COUNTRY.get(filter_id)

        # Standardise price column name
        if "value" in tmp.columns:
            tmp = tmp.rename(columns={"value": "price"})

        frames.append(tmp[[ "zone", "time", "price"]])

    if not frames:
        return pd.DataFrame(columns=["time", "zone", "price"])

    out = pd.concat(frames, axis = 0, ignore_index=True )
    out = out.sort_values(["zone", "time"])
    return out


def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add simple percentage returns per zone:
        return_t = price_t / price_{t-1} - 1
    """
    df = df.sort_values(["zone", "time"]).copy()
    df["return"] = df.groupby("zone")["price"].pct_change()
    return df


def filter_by_window(df: pd.DataFrame, window_key: str) -> pd.DataFrame:
    """
    Restrict df to the selected time window (1D, 7D, 30D, 1Y, max).
    """
    if df.empty:
        return df

    df = df.copy()

    if window_key == "max" or WINDOWS.get(window_key) is None:
        return df

    end = df["time"].max()
    start = end - pd.Timedelta(WINDOWS[window_key])
    return df[df["time"].between(start, end)]


def compute_return_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summary stats of returns per zone over the current window.
    """
    if "return" not in df.columns:
        raise ValueError("DataFrame must contain a 'return' column.")

    tmp = df.dropna(subset=["return"])
    if tmp.empty:
        return pd.DataFrame()

    rolling_std = tmp.groupby("zone")["return"].rolling(3).std()
    return rolling_std


def compute_spreads(df: pd.DataFrame, ref_zone: str) -> pd.DataFrame:
    """
    Compute price spreads vs reference zone:
        spread(zone) = price(zone) - price(ref_zone)

    Returns a long DataFrame with:
        time, zone, spread
    where zone != ref_zone.
    """
    if df.empty:
        return pd.DataFrame(columns=["time", "zone", "spread"])

    pivot = df.pivot(index="time", columns="zone", values="price")

    if ref_zone not in pivot.columns:
        raise ValueError(f"Reference zone {ref_zone!r} not in DataFrame.")

    spreads = pivot.subtract(pivot[ref_zone], axis=0)
    spreads = spreads.drop(columns=[ref_zone])

    spreads_long = (
        spreads
        .reset_index()
        .melt(id_vars="time", var_name="zone", value_name="spread")
        .sort_values(["zone", "time"])
    )
    return spreads_long
# %%
