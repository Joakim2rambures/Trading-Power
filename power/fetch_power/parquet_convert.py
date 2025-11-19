# %%
import io, pandas as pd
from datetime import datetime, timezone
from pyarrow import Table as PaTable
import pyarrow.parquet as pq
from pathlib import Path

def return_path(root: Path, region: str, filter_id: str, day: str) -> Path:
    return root / f"region={region}" / f"filter={filter_id}" / f"date={day}" / "data.parquet"
# this gives the path you will use to save your parquet.data : hen using the library pathlib, you use / to join the bits : (e.g. Path(file_name)/ 'name_of_the_file')

def drop_by_timecol(df: pd.DataFrame):
    out = df.sort_values("time_utc").drop_duplicates(subset=["time_utc"], keep="last")
    return out.sort_values("time_utc").reset_index(drop=True)

"""
drop_by_ts() drops duplicates and sort by time values
"""

def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_parquet(path)

"""
read_parquet_if_exists() read parquet files if file exists 
"""

def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    cols = ["time_utc", "value"]
    for c in cols:
        if c not in df.columns:
            raise ValueError(f"missing column {c}")
    sink = io.BytesIO() # calls io.Bytes : used to turn a file from ... to bytes, such that instead of the file being stored on the memory, it will be stored on the ram (making the compute faster)
    df[cols].to_parquet(sink, compression="snappy", engine='pyarrow') 
    return sink.getvalue()

"""""
Aim of to_parquet_bytes is it grabs the df -> create a temporary memory file (sink) that lives on the RAM (instead of memory)
then you save the parquet binary foramt file into that memory file (to_parquet with path = sink). You do that because the file will be processed
much faster on the RAM than actual memory. 
.getvalue() at the end is used to get what is written on that memory file (read bytes), so that i can be saved by the next function 
write_atomic() 
so path is df -> parquet bytes format RAM (processed faster) -> parquet_data bytes saved in memory by next function (write_atomic())
"""""

def merge_incoming_data(root: Path, region: str, filter_id: str, df: pd.DataFrame):
    """
    Merge df_new into existing daily Parquet files under root, remove duplicates by time_utc.
    """
    from .io_s3 import write_atomic  # we will repurpose this for local FS

    touched = []
    df_new = df.copy()
    df_new["date_str"] = pd.to_datetime(df_new["time_utc"], utc=True).dt.strftime("%Y-%m-%d")
    days = sorted( df_new["date_str"].unique())
    #above reformats the days in a standardized day format, drops duplicate dates

    for day in days:
        data_path = return_path(root, region, filter_id, day)
        # creates a path for this day 
        
        df_day = df_new[df_new["date_str"] == day].drop(columns=["date_str"])
        # this grabs the rows/time series data points for each date 

        df_old = read_parquet_if_exists(data_path)
        if df_old is not None:
            merged = pd.concat([df_old, df_day], ignore_index=True) # merge old dataset and new dataset
        else:
            merged = df_day
        # this concats the data that we have just received with the data we have already received from the beginning of the day until now.
        # else if this is the first data point fo the day, it assigns it as merged and saves it below (officially creating the file for that day)

        merged = drop_by_timecol(merged)
        data_bytes = to_parquet_bytes(merged)
        write_atomic(data_path, data_bytes) # this will overwrite the previous dataset with the new dataset and create the file (thats why we don't need to return anything)
        touched.append(str(data_path)) 
    return touched 
# %%
