# stats_incremental.py

# %%
import os
from pathlib import Path
import pandas as pd

from analysis.market_price import (
    load_prices_with_returns,
    compute_multi_window_stats,
)
from power.fetch_power.state import load_hwm_map, load_hwm, save_hwm, floor_to_quarter
from power.fetch_power.parquet_convert import to_parquet_bytes
from power.fetch_power.io_s3 import write_atomic

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"

DATA_HWM_PATH = STATE_ROOT / "high_watermark.json"          # raw data HWM (per filter)
STATS_HWM_PATH = STATE_ROOT / "stats_high_watermark.json"   # stats HWM (single)

STATS_FAST_PATH = DATA_ROOT / "stats" / "market_price_stats_fast.parquet"
FAST_WINDOWS = ["1D", "3D"]  # light windows


def main(filter_group_name: str | None = None):
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

    # 2) Stats HWM (single)
    stats_hwm = load_hwm(STATS_HWM_PATH)
    if isinstance(stats_hwm, pd.Timestamp) and data_hwm <= stats_hwm:
        print("Stats already up to date; exiting.")
        return

    # 3) Load all prices + returns up to data_hwm
    prices = load_prices_with_returns(filter_group_name=filter_group_name)
    if prices.empty:
        print("No prices data; aborting stats incremental.")
        return

    prices = prices.copy()
    prices["time"] = pd.to_datetime(prices["time"], utc=True)
    prices = prices[prices["time"] <= data_hwm]

    if prices.empty:
        print("No prices up to data_hwm; aborting.")
        return

    # 4) Compute fast-window stats for the group
    stats_fast = compute_multi_window_stats(prices, FAST_WINDOWS)
    if stats_fast.empty:
        print("No stats produced for fast windows.")
        return

    # Option A: overwrite a single combined stats file with only 1D/3D
    # Option B (later): merge with slow stats, or write a separate file.
    data_bytes = to_parquet_bytes(stats_fast)
    write_atomic(STATS_FAST_PATH, data_bytes)
    print(f"Saved fast-window stats to {STATS_FAST_PATH}")

    # 5) Update stats HWM
    save_hwm(STATS_HWM_PATH, data_hwm)
    print(f"Stats HWM -> {data_hwm.isoformat()}")


if __name__ == "__main__":
    fg = os.environ.get("FILTER_GROUP", "market_price")
    main(filter_group_name=fg)
