# %%
import seaborn as sb 
import pandas as pd 
from pathlib import Path 
import sys 
PROJECT_ROOT = Path(__file__).resolve().parent.parent #TRADING-POWER/ 
if str(PROJECT_ROOT) not in sys.path: 
    sys.path.insert(0, str(PROJECT_ROOT)) 

from analysis.read_data import load_filter_history 
from power.fetch_power.smard_filters import FILTER_GROUPS

from analysis.market_price import (
    load_market_price_series,
    series_to_long,
    add_returns,
    filter_by_window,
    compute_return_stats,
    compute_spreads,
)

all_series = load_market_price_series()
prices = series_to_long(all_series)


# %%
