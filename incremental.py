# %%

import os, pandas as pd
from pathlib import Path

from power.fetch_power.smard_fetch import smard_range
from power.fetch_power.parquet_convert import merge_write_partitions
from power.fetch_power.state import load_hwm, save_hwm, last_full_quarter
from power.fetch_power.smard_filters import FILTER_GROUPS

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"
HWM_PATH = STATE_ROOT / "high_watermark.json"

REGION_CODE = "DE"
FILTER_ID = "4071"
RESOLUTION = "quarterhour"
VERIFY = False

OVERLAP_HOURS = int(os.environ.get("OVERLAP_HOURS", "4"))

def main(filter_group_name=None):
    now_final = last_full_quarter()  # do not write partial quarters
    hwm = load_hwm(HWM_PATH)

    if hwm is None:
        start = now_final - pd.Timedelta(hours=24)
    else:
        start = hwm - pd.Timedelta(hours=OVERLAP_HOURS)
    end = now_final

    if filter_group_name is None:
        filter_group_name = 'market_price'
    
    filters = FILTER_GROUPS[filter_group_name]
    total_touched = 0

    for filter_id, desc in filters.items():
        print(f"incremental fetch for filter {filter_id} ({desc})")

        df = smard_range(
            filter_id=filter_id,
            region=REGION_CODE,
            resolution=RESOLUTION,
            start=start,
            end=end,
            verify=VERIFY,
        )

        if df.empty:
            print(" no rows fetched")
            continue


        touched = merge_write_partitions(DATA_ROOT, REGION_CODE, FILTER_ID, df)
    # merge_write_partitions = Merge df_new into existing daily Parquet files under root, dedupe by time_utc.
        total_touched += len(touched)
        print(f"  wrote {len(touched)} partitions")

    # Only update HWM if at least one filter wrote something (optional)
    if total_touched > 0:
        save_hwm(HWM_PATH, end)
        print(f"HWM -> {end.isoformat()}")
    else:
        print("no partitions written; HWM unchanged")

if __name__ == "__main__":
    main()