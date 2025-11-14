# %%
import pandas as pd
from pathlib import Path

from power.fetch_power.io_s3 import list_paths
from power.fetch_power.parquet_convert import read_parquet_if_exists, to_parquet_bytes
from power.fetch_power.io_s3 import write_atomic

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
STATE_ROOT = PROJECT_ROOT / "state"
HWM_PATH = STATE_ROOT / "high_watermark.json"

REGION_CODE = "DE"
FILTER_ID = "4071"
RESOLUTION = "quarterhour"
VERIFY = False

def main():
    prefix = DATA_ROOT / f"region={REGION_CODE}" / f"filter={FILTER_ID}"
    parts = sorted(
        {
            p.split("/date=", 1)[1].split("/", 1)[0]
            for p in list_paths(prefix)
            if "/date=" in p
        }
    )

    for day in parts:
        path = DATA_ROOT / f"region={REGION_CODE}" / f"filter={FILTER_ID}" / f"date={day}" / "data.parquet"
        df = read_parquet_if_exists(path)
        if df is None or df.empty:
            continue
        write_atomic(path, to_parquet_bytes(df))
        print(f"compacted {day}")

if __name__ == "__main__":
    main()