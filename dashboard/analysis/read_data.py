# %%
from pathlib import Path
import pandas as pd
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent #__file__ is the path to the current file, .parent means we're targeting the file before

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from power.fetch_power.io_s3 import list_paths
from power.fetch_power.smard_filters import FILTER_GROUPS

DATA_ROOT = PROJECT_ROOT / "data"
REGION_CODE = "DE"

def load_filter_history(filter_id: str) -> pd.DataFrame:
    """Load all daily Parquet files for one filter_id into a single DataFrame."""
    prefix = DATA_ROOT / f"region={REGION_CODE}" / f"filter={filter_id}"

    # list_paths(prefix) returns all paths under that prefix (as strings)
    parts = sorted(
        {
            p.split("/date=", 1)[1].split("/", 1)[0]
            for p in list_paths(prefix)
            if "/date=" in p
        }
    )

    dfs = []
    for day in parts:
        path = DATA_ROOT / f"region={REGION_CODE}" / f"filter={filter_id}" / f"date={day}" / "data.parquet"
        if not path.exists():
            continue
        df_day = pd.read_parquet(path)
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
    if "time_utc" in merged.columns:
        merged = (
            merged
            .drop_duplicates(subset=["time_utc"])
            .sort_values("time_utc")
            .set_index("time_utc")
        )

    return merged

# example: loop over a whole group and build a dict of DataFrames
filter_ids = FILTER_GROUPS["market_price"].keys()

all_series = {}
for filter_id in filter_ids:
    df = load_filter_history(filter_id)
    all_series[filter_id] = df
    print(filter_id, df.shape)



# %%
