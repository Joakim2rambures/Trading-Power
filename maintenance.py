# %%
import pandas as pd
from pathlib import Path
import os 

from power.fetch_power.io_s3 import list_paths
from power.fetch_power.parquet_convert import read_parquet_if_exists, to_parquet_bytes
from power.fetch_power.io_s3 import write_atomic
from power.fetch_power.smard_filters import FILTER_GROUPS

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"
HWM_PATH = STATE_ROOT / "high_watermark.json"

REGION_CODE = "DE"
RESOLUTION = "quarterhour"
VERIFY = False

def main(filter_group_name=None):

    if filter_group_name is None: 
        filter_group_name = os.environ.get("FILTER_GROUP", "market_price")

    filter_ids = FILTER_GROUPS[filter_group_name]
    
    for filter_id in filter_ids :
        prefix = DATA_ROOT / f"region={REGION_CODE}" / f"filter={filter_id}"
        parts = sorted(
            {
                p.split("/date=", 1)[1].split("/", 1)[0]
                for p in list_paths(prefix)
                if "/date=" in p
            }
        )

        # parts  keeps only the dates, sorted, from the parquet files 

        for day in parts:
            path = DATA_ROOT / f"region={REGION_CODE}" / f"filter={filter_id}" / f"date={day}" / "data.parquet"
            df = read_parquet_if_exists(path)
            if df is None or df.empty:
                continue
            tpb = to_parquet_bytes(df)
            write_atomic(path,tpb)
            print(f"compacted {day}")

if __name__ == "__main__":
    main()
# %%
