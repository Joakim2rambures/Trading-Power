# %%
import io, pandas as pd
from datetime import datetime, timezone
from pyarrow import Table as PaTable
import pyarrow.parquet as pq
from pathlib import Path

def day_partitions_for(df: pd.DataFrame):
    return sorted(pd.to_datetime(df["time_utc"], utc=True).dt.strftime("%Y-%m-%d").unique())
# sorts per datetime, converts it in the desired format, and removes duplcaites/keep unique value

def partition_path(root: Path, region: str, filter_id: str, day: str) -> Path:
    return root / f"region={region}" / f"filter={filter_id}" / f"date={day}" / "data.parquet"
# this gives the path you will use to save your parquet.data : hen using the library pathlib, you use / to join the bits : (e.g. Path(file_name)/ 'name_of_the_file')

def dedupe_by_ts(df: pd.DataFrame):
    out = df.sort_values("time_utc").drop_duplicates(subset=["time_utc"], keep="last")
    return out.sort_values("time_utc").reset_index(drop=True)

def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_parquet(path)

def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    cols = ["time_utc", "value"]
    for c in cols:
        if c not in df.columns:
            raise ValueError(f"missing column {c}")
    table = PaTable.from_pandas(df[cols]) # constructs a table using a adatframe (from_pandas)
    sink = io.BytesIO() # calls io.Bytes : used to turn a file from ... to bytes, such that instead of the file being stored on the memory, it will be stored on the ram (making the compute faster)
    pq.write_table(table, sink, compression="snappy") 
    return sink.getvalue()

def merge_write_partitions(root: Path, region: str, filter_id: str, df_new: pd.DataFrame):
    """
    Merge df_new into existing daily Parquet files under root, dedupe by time_utc.
    """
    from .io_s3 import write_atomic  # we will repurpose this for local FS

    touched = []
    df_new = df_new.copy()
    df_new["date_str"] = pd.to_datetime(df_new["time_utc"], utc=True).dt.strftime("%Y-%m-%d")

    for day in day_partitions_for(df_new):
        p = partition_path(root, region, filter_id, day)
        # rows for this day
        df_day = df_new[df_new["date_str"] == day].drop(columns=["date_str"])

        df_old = read_parquet_if_exists(p)
        if df_old is not None:
            merged = pd.concat([df_old, df_day], ignore_index=True) # merge old dataset and new dataset
        else:
            merged = df_day

        merged = dedupe_by_ts(merged)
        data_bytes = to_parquet_bytes(merged)
        write_atomic(p, data_bytes) # this will overwrite the previous dataset with the new dataset and create teh file (thats why we don't need to return anything)
        touched.append(str(p)) 
    return touched 