#backfill.py
# %%
import os, pandas as pd
from pathlib import Path

from power.fetch_power.smard_fetch import smard_range
from power.fetch_power.parquet_convert import merge_incoming_data
from power.fetch_power.state import save_hwm_map, floor_to_quarter, load_hwm_map
from power.fetch_power.smard_filters import FILTER_GROUPS

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"
HWM_PATH = STATE_ROOT / "high_watermark.json"

#the four lines above gets the data, state and high_watermark. path + file of interest everytime   

# Hardcode basic config for now
REGION_CODE = "DE"
RESOLUTION = "quarterhour"
VERIFY = False               # SMARD TLS verify (leave False if corp TLS is weird)

"""
Backfill allows you to get the big initial chunk of data you're interested about 
(conversely incremental.py tops up that big chunk of data witht the most recent one)
"""

def main(start, end, filter_group_name = None, 
         resolution:str = RESOLUTION, region_code: str = 'DE', 
         verify=False, data_root: Path = DATA_ROOT,  hmw_path: Path = HWM_PATH):
    
    if filter_group_name is None:
        filter_group_name = os.environ.get("FILTER_GROUP", "market_price")

    filters = FILTER_GROUPS[filter_group_name]
    hwm_map = load_hwm_map(hmw_path)
    end_ts = floor_to_quarter(pd.to_datetime(end, utc=True))

    for filter_id, desc in filters.items():
        print(f"backfilling filter {filter_id} ({desc})")

        df = smard_range(
            filter_id=filter_id,
            region=region_code,
            resolution=resolution,
            start=start,
            end=end_ts,
            verify=verify,
        )

        if df.empty:
            print("no data returned for backfill window")
            continue

        # merge_write_partitions = Merge df_new into existing daily Parquet files under root, dedupe by time_utc.
        merge_incoming_data(data_root, region_code, filter_id, df) 

        key = str(filter_id)
        hwm_map[key] = end_ts
        print(f"  HWM[{key}] -> {end_ts.isoformat()}")


    save_hwm_map(HWM_PATH, hwm_map) # save_hwm grabs a python object and turns it into a json file 
    print("backfill done")

if __name__ == "__main__":
    import os
    start = os.environ["START"]
    end = os.environ["END"]
    filter_group_name = os.environ.get("FILTER_GROUP")
    main(start, end, filter_group_name)
# %%
