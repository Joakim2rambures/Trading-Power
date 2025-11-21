# stats_backfill.py

# %%
import os, pandas as pd
from pathlib import Path

from analysis.market_price import (
    load_prices_with_returns,
    compute_multi_window_stats,
)

from power.fetch_power.parquet_convert import merge_incoming_data, to_parquet_bytes
from power.fetch_power.state import save_hwm, floor_to_quarter, load_hwm_map
from power.fetch_power.io_s3 import write_atomic 
from power.fetch_power.smard_filters import FILTER_GROUPS

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"

DATA_HWM_PATH = STATE_ROOT / "high_watermark.json"          # raw data HWM (per filter)
STATS_HWM_PATH = STATE_ROOT / "stats_high_watermark.json"

STATS_SLOW_PATH = DATA_ROOT / "stats" / "market_price_stats_slow.parquet"
SLOW_WINDOWS = ["7D", "30D", "1Y"]  # heavy windows; tweak as needed

REGION_CODE = "DE"
RESOLUTION = "quarterhour"
VERIFY = False   # kept for symmetry; not used in stats_range

"""
Stats backfill:
Compute initial chunk of stats you're interested in over [start, end],
for all filters in a group, and store them under STATS_ROOT.
"""

def main(start, end, filter_group_name=None, resolution: str = RESOLUTION):
    if filter_group_name is None:
        filter_group_name = os.environ.get("FILTER_GROUP", "market_price")

    # 1) Data HWM (per filter) -> group cutoff
    hwm_map = load_hwm_map(DATA_HWM_PATH)
    if not hwm_map:
        print("No data HWM found; nothing to do.")
        return    
    
    data_hwm = min(hwm_map.values())
    data_hwm = floor_to_quarter(data_hwm)
    print(f"Data HWM (min over filters) = {data_hwm}")

    # If START/END envs are provided, respect them but don't go past data_hwm
    start_env = os.environ.get("START")
    end_env = os.environ.get("END")

    if start_env is not None:
        start_ts = pd.to_datetime(start_env, utc=True)
    else:
        start_ts = None

    if end_env is not None:
        end_ts = pd.to_datetime(end_env, utc=True)
        end_ts = min(end_ts, data_hwm)
    else:
        end_ts = data_hwm


    # 2) Load all prices + returns for the group
    prices = load_prices_with_returns(filter_group_name=filter_group_name)
    if prices.empty:
        print("No prices data available; aborting stats backfill.")
        return

    # apply start/end filter if provided
    prices = prices.copy()
    if start_ts is not None:
        prices = prices[prices["time"] >= start_ts]
    prices = prices[prices["time"] <= end_ts]

    if prices.empty:
        print("No prices left after applying start/end; aborting.")
        return

    # 3) Compute heavy-window stats for the group (all zones together)
    stats_slow = compute_multi_window_stats(prices, SLOW_WINDOWS)
    if stats_slow.empty:
        print("No stats produced for heavy windows.")
        return

    data_bytes = to_parquet_bytes(stats_slow)
    write_atomic(STATS_SLOW_PATH, data_bytes)
    print(f"Saved slow-window stats to {STATS_SLOW_PATH}")

    # 4) Update stats HWM to end_ts (group-level)
    save_hwm(STATS_HWM_PATH, end_ts)
    print(f"Stats HWM -> {end_ts.isoformat()}")


if __name__ == "__main__":
    start = os.environ["START"]
    end = os.environ["END"]
    filter_group_name = os.environ.get("FILTER_GROUP", "market_price")
    main(start, end, filter_group_name)
