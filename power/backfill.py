# %%
import os, pandas as pd
from pathlib import Path

from power.fetch_power.smard_fetch import smard_range
from power.fetch_power.parquet_convert import merge_write_partitions
from power.fetch_power.state import save_hwm, floor_to_quarter

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"
HWM_PATH = STATE_ROOT / "high_watermark.json"

# Hardcode basic config for now
REGION_CODE = "DE"
FILTER_ID = "4071"           # later: list of IDs
RESOLUTION = "quarterhour"
VERIFY = False               # SMARD TLS verify (leave False if corp TLS is weird)

def main():
    # START/END still passed from workflow as env (not secrets, just inputs)
    START = os.environ["START"]
    END = os.environ["END"]

    df = smard_range(
        filter_id=FILTER_ID,
        region=REGION_CODE,
        resolution=RESOLUTION,
        start=START,
        end=END,
        verify=VERIFY,
    )
    if df.empty:
        print("no data returned for backfill window")
        return

    merge_write_partitions(DATA_ROOT, REGION_CODE, FILTER_ID, df)

    # set HWM to END floored to last full quarter
    save_hwm(HWM_PATH, floor_to_quarter(pd.to_datetime(END, utc=True)))
    print("backfill done")

if __name__ == "__main__":
    main()