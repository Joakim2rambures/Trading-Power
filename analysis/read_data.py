#read_data.py
# %%
from pathlib import Path
import pandas as pd
import sys 

PROJECT_ROOT = Path(__file__).resolve().parent.parent #__file__ is the path to the current file, .parent means we're targeting the file before
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from power.fetch_power.parquet_convert import return_path, read_parquet_if_exists, drop_by_timecol
from power.fetch_power.io_s3 import list_paths
from power.fetch_power.smard_filters import FILTER_GROUPS

DATA_ROOT = PROJECT_ROOT / "data"
REGION_CODE = "DE"

def load_filter_history(filter_id: str, region: str = 'DE', root = PROJECT_ROOT) -> pd.DataFrame:
    """Load all daily Parquet files for one filter_id into a single DataFrame."""
    DATA_ROOT =  root / "data"
    REGION_CODE = region
    prefix = DATA_ROOT / f"region={REGION_CODE}" / f"filter={filter_id}"

    # list_paths(prefix) returns all paths under that prefix (as strings)
    parts = sorted(
        {
            p.split("\\date=", 1)[1].split("\\", 1)[0] for p in list_paths(prefix) if "\\date=" in p
        }
        
    )

    dfs = []
    for day in parts:
        path = return_path(root = DATA_ROOT, region = REGION_CODE, filter_id=filter_id, day=day)
        df_day = read_parquet_if_exists(path)
        if df_day is None or df_day.empty:
            continue
        dfs.append(df_day)

    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)

    # optional but recommended:
    # - dedupe by time_utc
    # - sort by time_utc
    # - set as index if you like time-series operations
    # all of the above is included in the function droop_by_timecol
    if "time_utc" in merged.columns:
        merged = drop_by_timecol(merged)

    return merged

