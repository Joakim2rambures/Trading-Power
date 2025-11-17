# %%

import os, pandas as pd
from pathlib import Path

from power.fetch_power.smard_fetch import smard_range
from power.fetch_power.parquet_convert import merge_write_partitions
from power.fetch_power.state import load_hwm, save_hwm, last_full_quarter

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"
HWM_PATH = STATE_ROOT / "high_watermark.json"

REGION_CODE = "DE"
FILTER_ID = "4071"
RESOLUTION = "quarterhour"
VERIFY = False

OVERLAP_HOURS = int(os.environ.get("OVERLAP_HOURS", "4"))

def main():
    now_final = last_full_quarter()  # do not write partial quarters
    hwm = load_hwm(HWM_PATH)

    if hwm is None:
        start = now_final - pd.Timedelta(hours=24)
    else:
        start = hwm - pd.Timedelta(hours=OVERLAP_HOURS)
    end = now_final

    df = smard_range(
        filter_id=FILTER_ID,
        region=REGION_CODE,
        resolution=RESOLUTION,
        start=start,
        end=end,
        verify=VERIFY,
    )

    if df.empty:
        print("no rows fetched")
        return

    touched = merge_write_partitions(DATA_ROOT, REGION_CODE, FILTER_ID, df)
    # merge_write_partitions = Merge df_new into existing daily Parquet files under root, dedupe by time_utc.
    print(f"wrote {len(touched)} partitions")

    save_hwm(HWM_PATH, end)
    print(f"HWM -> {end.isoformat()}")

if __name__ == "__main__":
    main()