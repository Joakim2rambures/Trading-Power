import os, pandas as pd
from smard.io_s3 import get_fs_and_url, list_paths
from smard.parquet_table import read_parquet_if_exists, to_parquet_bytes
from smard.io_s3 import write_atomic

BUCKET_URL  = os.environ["BUCKET_URL"]
REGION_CODE = os.environ.get("REGION_CODE","DE")
FILTER_ID   = os.environ["FILTER_ID"]

def main():
    fs, root = get_fs_and_url(BUCKET_URL)
    prefix = f"{root}/region={REGION_CODE}/filter={FILTER_ID}/"
    parts = sorted({p.rsplit("/date=",1)[1].split("/",1)[0] for p in list_paths(fs, prefix) if "/date=" in p})
    # for each date, ensure a single compacted Parquet exists (noop if already single)
    for day in parts:
        path = f"{root}/region={REGION_CODE}/filter={FILTER_ID}/date={day}/data.parquet"
        df = read_parquet_if_exists(fs, path)
        if df is None or df.empty: continue
        # rewrite once (snappy) — also a chance to normalize
        write_atomic(fs, path, to_parquet_bytes(df))
        print(f"compacted {day}")

if __name__ == "__main__":
    main()