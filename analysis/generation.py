# analysis/generation.py

#%%
from pathlib import Path
import sys
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from power.fetch_power.smard_filters import (
    POWER_GENERATION_FILTER_IDS,
    FORECAST_FILTER_IDS,
    CONS_FILTER_IDS,
)
from analysis.group_series import load_group_series, series_dict_to_long


def load_generation_long(root: Path = PROJECT_ROOT) -> pd.DataFrame:
    """
    Long df with columns: time, tech, value (MW).
    """
    all_series = load_group_series("generation", root=root)
    df = series_dict_to_long(
        all_series,
        id_to_label=POWER_GENERATION_FILTER_IDS,
        label_col="tech",
    )
    df = df.rename(columns={"value": "generation_mw"})
    return df


def load_forecast_long(root: Path = PROJECT_ROOT) -> pd.DataFrame:
    """
    Long df: time, tech, forecast_mw
    """
    all_series = load_group_series("forecast", root=root)
    df = series_dict_to_long(
        all_series,
        id_to_label=FORECAST_FILTER_IDS,
        label_col="tech",
    )
    df = df.rename(columns={"value": "forecast_mw"})
    return df


def load_consumption_long(root: Path = PROJECT_ROOT) -> pd.DataFrame:
    """
    Long df: time, metric, consumption_mw (e.g. total, residual load, pumped storage).
    """
    all_series = load_group_series("consumption", root=root)
    df = series_dict_to_long(
        all_series,
        id_to_label=CONS_FILTER_IDS,
        label_col="metric",
    )
    df = df.rename(columns={"value": "consumption_mw"})
    return df