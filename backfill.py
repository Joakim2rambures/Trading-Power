# %%
import os, pandas as pd
from pathlib import Path

from power.fetch_power.smard_fetch import smard_range
from power.fetch_power.parquet_convert import merge_write_partitions
from power.fetch_power.state import save_hwm, floor_to_quarter
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

def main(filter_group_name = None):
    # START/END still passed from workflow as env (not secrets, just inputs)
    START = os.environ["START"]
    END = os.environ["END"]
    
    if filter_group_name is None:
        filter_group_name = os.environ.get("FILTER_GROUP", "market_price")

    filters = FILTER_GROUPS[filter_group_name]

    for filter_id, desc in filters.items():
        print(f"backfilling filter {filter_id} ({desc})")

        df = smard_range(
            filter_id=filter_id,
            region=REGION_CODE,
            resolution=RESOLUTION,
            start=START,
            end=END,
            verify=VERIFY,
        )

        if df.empty:
            print("no data returned for backfill window")
            continue

        # merge_write_partitions = Merge df_new into existing daily Parquet files under root, dedupe by time_utc.
        merge_write_partitions(DATA_ROOT, REGION_CODE, filter_id, df) 

    # set HWM to END floored to last full quarter
    ftq = floor_to_quarter(pd.to_datetime(END, utc=True)) # ensure that utc is included, and 

    save_hwm(HWM_PATH, ftq) # save_hwm grabs a python object and turns it into a json file 
    print("backfill done")

if __name__ == "__main__":
    main()
# %%
