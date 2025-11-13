# %%

import os, pandas as pd
from datetime import timedelta
from smard.smard_fetch import smard_range
from smard.io_s3 import get_fs_and_url
from smard.parquet_table import merge_write_partitions
from smard.state import load_hwm, save_hwm, last_full_quarter

BUCKET_URL  = os.environ["BUCKET_URL"]
REGION_CODE = os.environ.get("REGION_CODE","DE")
FILTER_ID   = os.environ["FILTER_ID"]
RESOLUTION  = os.environ.get("RESOLUTION","quarterhour")
HWM_URL     = os.environ["HWM_URL"]
OVERLAP_HRS = int(os.environ.get("OVERLAP_HOURS","4"))
VERIFY      = os.environ.get("TLS_VERIFY","false").lower()=="true"

def main():
    now_final = last_full_quarter()  # do not write partial quarters
    hwm = load_hwm(HWM_URL)
    if hwm is None:
        # bootstrap a small initial window
        start = now_final - pd.Timedelta(hours=24)
    else:
        start = hwm - pd.Timedelta(hours=OVERLAP_HRS)
    end = now_final

    df = smard_range(filter_id=FILTER_ID, region=REGION_CODE,
                     resolution=RESOLUTION, start=start, end=end, verify=VERIFY)

    if df.empty:
        print("no rows fetched")
        return

    fs, root = get_fs_and_url(BUCKET_URL)
    touched = merge_write_partitions(fs, root, REGION_CODE, FILTER_ID, df)
    print(f"wrote {len(touched)} partitions")

    # only advance HWM after successful writes
    save_hwm(HWM_URL, end)
    print(f"HWM -> {end.isoformat()}")

if __name__ == "__main__":
    main()