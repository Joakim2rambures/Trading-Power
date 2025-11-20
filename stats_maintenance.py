#%%
# stats_maintenance.py

from pathlib import Path

from power.fetch_power.parquet_convert import read_parquet_if_exists, to_parquet_bytes
from power.fetch_power.io_s3 import write_atomic

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"

FAST_PATH = DATA_ROOT / "stats" / "market_price_stats_fast.parquet"
SLOW_PATH = DATA_ROOT / "stats" / "market_price_stats_slow.parquet"

def compact_one(path: Path):
    df = read_parquet_if_exists(path)
    if df is None or df.empty:
        return
    tpb = to_parquet_bytes(df)
    write_atomic(path, tpb)
    print(f"Compacted {path}")


def main():
    for p in (FAST_PATH, SLOW_PATH):
        compact_one(p)


if __name__ == "__main__":
    main()

