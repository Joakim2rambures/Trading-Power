# %%
import os, pandas as pd
from power.fetch_power.smard_fetch import smard_range
from power.fetch_power.io_s3 import get_fs_and_url
from power.fetch_power.parquet_convert import merge_write_partitions
from power.fetch_power.state import save_hwm

# env (set via workflow inputs/secrets)
BUCKET_URL    = os.environ["BUCKET_URL"]      # e.g., s3://my-bucket/parquet_root
REGION_CODE   = os.environ.get("REGION_CODE","DE")
FILTER_ID     = os.environ["FILTER_ID"]       # e.g., 4071
RESOLUTION    = os.environ.get("RESOLUTION","quarterhour")
START         = os.environ["START"]           # ISO
END           = os.environ["END"]             # ISO
VERIFY        = os.environ.get("TLS_VERIFY","false").lower()=="true"
HWM_URL       = os.environ["HWM_URL"]         # e.g., s3://my-bucket/state/high_watermark.json

def main():
    fs, root = get_fs_and_url(BUCKET_URL)
    df = smard_range(filter_id=FILTER_ID, region=REGION_CODE, resolution=RESOLUTION,
                     start=START, end=END, verify=VERIFY)
    if df.empty:
        print("no data returned for backfill window")
        return
    merge_write_partitions(fs, root, REGION_CODE, FILTER_ID, df)
    # set HWM to END floored to last full quarter (since backfill is complete)
    from smard.state import floor_to_quarter
    save_hwm(HWM_URL, floor_to_quarter(pd.to_datetime(END, utc=True)))
    print("backfill done")

if __name__ == "__main__":
    main()