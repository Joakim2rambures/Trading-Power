# incremental.py
# %%

import os, pandas as pd
from pathlib import Path

from power.fetch_power.smard_fetch import smard_range
from power.fetch_power.parquet_convert import merge_incoming_data
from power.fetch_power.state import load_hwm_map, save_hwm_map, last_full_quarter
from power.fetch_power.smard_filters import FILTER_GROUPS

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"
HWM_PATH = STATE_ROOT / "high_watermark.json"

REGION_CODE = "DE"
RESOLUTION = "quarterhour"
VERIFY = False

OVERLAP_HOURS = int(os.environ.get("OVERLAP_HOURS", "2"))

def main(filter_group_name=None, resolution:str = RESOLUTION, region_code: str = 'DE', 
         verify=False, data_root: Path = DATA_ROOT, hmw_path: Path = HWM_PATH, overlap_hours: str = OVERLAP_HOURS):
    
    
    now_final = last_full_quarter()  # do not write partial quarters
    hwm_map  = load_hwm_map(hmw_path) 
    # gets the last timestamp for this filter_id : end point fo the data 
    # (e.g if we downloaded data from 01/01/2022 to 01/01/2025 for filter_id :11, it will returned 11 : 01/01/2025))

    if filter_group_name is None:
        filter_group_name = os.environ.get("FILTER_GROUP", "market_price")

    filters = FILTER_GROUPS[filter_group_name]
    total_touched = 0

    
    for filter_id, desc in filters.items():
        key = str(filter_id)
        hwm = hwm_map.get(key)

        if hwm is not None and now_final <= hwm:
            print(f"filter {filter_id} ({desc}): no new completed quarter-hour; skipping")
            return 
    
    # BELOW we set the start and end variables 
        if hwm is None:
            start = now_final - pd.Timedelta(hours=24) # start date for the API data pull
        else:
            start = hwm - pd.Timedelta(hours=overlap_hours) 
            # this will grab the lastest timestamp (minus 2 hours for safety reasons/in case data was missed) and set it as start 
        
        end = now_final

    for filter_id, desc in filters.items():
        print(f"incremental fetch for filter {filter_id} ({desc})")

        df = smard_range(
            filter_id=filter_id,
            region=region_code,
            resolution=resolution,
            start=start,
            end=end,
            verify=verify,
        )

        if df.empty:
            print("no rows fetched")
            continue


        touched = merge_incoming_data(data_root, region_code, filter_id, df)
    # merge_write_partitions = Merge df_new into existing daily Parquet files under root, and removes duplicates  by time_utc.
        
        total_touched += len(touched)
        print(f"  wrote {len(touched)} partitions")

                # update per-filter HWM if we wrote something
        if touched:
            hwm_map[key] = end
            print(f"  HWM[{key}] -> {end.isoformat()}")

    # Only update HWM if at least one filter wrote something (optional)
    if total_touched > 0:
        save_hwm_map(hmw_path, hwm_map)
        print(f"HWM -> {end.isoformat()}")
    else:
        print("no partitions written; HWM unchanged")

if __name__ == "__main__":
    main()