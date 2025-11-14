# %%
import io, pandas as pd
from datetime import datetime, timezone
from pyarrow import Table as PaTable
import pyarrow.parquet as pq

def day_partitions_for(df: pd.DataFrame):
    # returns sorted list of yyyy-mm-dd strings present in df['time_utc']
    return sorted(pd.to_datetime(df["time_utc"], utc=True).dt.strftime("%Y-%m-%d").unique())

def partition_path(root: str, region: str, filter_id: str, day: str):
    return f"{root}/region={region}/filter={filter_id}/date={day}/data.parquet"

def dedupe_by_ts(df: pd.DataFrame):
    # last value wins per timestamp, drop duplicates except for the last occurence 
    out = df.sort_values("time_utc").drop_duplicates(subset=["time_utc"], keep="last")
    return out.sort_values("time_utc").reset_index(drop=True)

def read_parquet_if_exists(fs, path: str) -> pd.DataFrame:
    if not fs.exists(path): 
        return None
    with fs.open(path, "rb") as f:
        table = pq.read_table(f)
    return table.to_pandas()

def to_parquet_bytes(df: pd.DataFrame):
    #### Checks
    cols = ["time_utc","value"]
    for c in cols:
        if c not in df.columns: 
            raise ValueError(f"missing column {c}")
    ####   
    table = PaTable.from_pandas(df[cols])
    sink = io.BytesIO() 
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()

def merge_write_partitions(fs, root: str, region: str, filter_id: str, df_new: pd.DataFrame):
    touched = []
    for day in day_partitions_for(df_new):
        p = partition_path(root, region, filter_id, day)
        df_old = read_parquet_if_exists(fs, p)
        if df_old is not None:
            merged = pd.concat([df_old, df_new[(df_new["time_utc"].dt.strftime("%Y-%m-%d")==day)]],
                               ignore_index=True)
        else:
            merged = df_new[(df_new["time_utc"].dt.strftime("%Y-%m-%d")==day)].copy()
        merged = dedupe_by_ts(merged)
        from .io_s3 import write_atomic
        write_atomic(fs, p, to_parquet_bytes(merged))
        touched.append(p)
    return touched