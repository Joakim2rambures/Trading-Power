#market_price.py
# %%

import pandas as pd 
from pathlib import Path 
import sys 
import numpy as np
PROJECT_ROOT = Path(__file__).resolve().parent.parent #TRADING-POWER/ 
if str(PROJECT_ROOT) not in sys.path: 
    sys.path.insert(0, str(PROJECT_ROOT)) 

from analysis.read_data import load_filter_history 
from power.fetch_power.smard_filters import FILTER_GROUPS

# Time windows for your dashboard
WINDOWS = {
    "1D": "1D",
    "3D": "3D",
    "7D": "7D",
    "30D": "30D",
    "90D": "90D",
    "1Y": "365D",
    "max": None,  # full history
}


def load_market_price_series(filter_group_name: str = "market_price",
                             root: Path = PROJECT_ROOT) -> dict:
    """
    Load all SMARD market price series into a dict of DataFrames.
    Keys are filter_ids from FILTER_GROUPS[filter_group_name].
    """
    filters = FILTER_GROUPS[filter_group_name]
    all_series = {}
    for filter_id in filters.keys():
        df = load_filter_history(filter_id, root=root)
        all_series[filter_id] = df
    return all_series


def series_to_long(all_series: dict) -> pd.DataFrame:
    """
    Convert dict of DataFrames {filter_id: df} into a long DataFrame with:
        time, zone, price

    Assumes each df has 'time_utc' and 'value' columns.

    """

    FILTER_ID_TO_COUNTRY = {
    "4169": "DE",
    "256": "NL",
    "4996": "BE",
}
    frames = []

    for filter_id, df in all_series.items():
        if df is None or df.empty:
            continue

        tmp = df.copy()
        # Ensure datetime
        tmp = tmp.rename(columns={"time_utc": "time"})

        # Use filter_id as "zone" label for now (DE, AT, BE, etc, or SMARD id)
        zone_label = FILTER_ID_TO_COUNTRY.get(filter_id, str(filter_id))
        tmp["zone"] = zone_label

        # Standardise price column name
        if "value" in tmp.columns:
            tmp = tmp.rename(columns={"value": "price"})

        frames.append(tmp[[ "zone", "time", "price"]])

    if not frames:
        return pd.DataFrame(columns=["time", "zone", "price"])

    out = pd.concat(frames, axis = 0, ignore_index=True )
    out = out.sort_values(["zone", "time"])
    return out


def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add simple percentage returns per zone:
        return_t = price_t / price_{t-1} - 1
    """
    df = df.sort_values(["zone", "time"]).copy()
    df["return"] = df.groupby("zone")["price"].pct_change()
    return df

def filter_by_window(df: pd.DataFrame, window_key: str) -> pd.DataFrame:
    """
    Restrict df to the selected time window (1D, 3D, 7D, 30D, 1Y, max).
    """
    if df.empty:
        return df

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True)

    if window_key == "max" or WINDOWS.get(window_key) is None:
        return df

    end = df["time"].max()
    start = end - pd.Timedelta(WINDOWS[window_key])
    return df[df["time"].between(start, end)]

def compute_return_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summary stats of returns per zone over the current window.
    Returns DataFrame indexed by zone with columns: mean, std, skew, kurt.
    """
    if "return" not in df.columns:
        raise ValueError("DataFrame must contain a 'return' column.")

    tmp = df.dropna(subset=["return"])
    if tmp.empty:
        return pd.DataFrame()

    stats = (
        tmp.groupby("zone")["return"]
        .agg(["mean", "std", "skew"])
    )
    return stats


def compute_spreads(df: pd.DataFrame, ref_zone: str) -> pd.DataFrame:
    """
    Compute price spreads vs reference zone:
        spread(zone) = price(zone) - price(ref_zone)

    Returns a long DataFrame with:
        time, zone, spread
    where zone != ref_zone.
    """
    if df.empty:
        return pd.DataFrame(columns=["time", "zone", "spread"])

    pivot = df.pivot(index="time", columns="zone", values="price")

    if ref_zone not in pivot.columns:
        raise ValueError(f"Reference zone {ref_zone!r} not in DataFrame.")

    spreads = pivot.subtract(pivot[ref_zone], axis=0)
    spreads = spreads.drop(columns=[ref_zone])

    spreads_long = (
        spreads
        .reset_index()
        .melt(id_vars="time", var_name="zone", value_name="spread")
        .sort_values(["zone", "time"])
    )
    return spreads_long


def load_prices_with_returns(
    filter_group_name: str = "market_price",
    root: Path = PROJECT_ROOT,
) -> pd.DataFrame:
    """
    Convenience: load all market price data for a group and add returns.
    Returns a long DataFrame with columns at least: time, zone, price, return.
    """
    all_series = load_market_price_series(filter_group_name=filter_group_name,
                                          root=root)
    prices = series_to_long(all_series)
    if prices.empty:
        return prices

    prices["time"] = pd.to_datetime(prices["time"], utc=True)
    prices = add_returns(prices)
    return prices


def compute_multi_window_stats(
    prices: pd.DataFrame,
    windows: list[str],
) -> pd.DataFrame:
    """
    Compute return stats for multiple windows.
    Returns a long DataFrame with columns:
        zone, window, as_of, mean, std, skew, kurt
    """
    if prices.empty:
        return pd.DataFrame(
            columns=["zone", "window", "as_of", "mean", "std", "skew"]
        )

    frames: list[pd.DataFrame] = []

    for w in windows:
        df_w = filter_by_window(prices, w)
        if df_w.empty:
            continue

        stats = compute_return_stats(df_w)
        if stats.empty:
            continue

        as_of = df_w["time"].max()
        stats["window"] = w
        stats["as_of"] = as_of

        frames.append(stats.reset_index())  # zone back to a column

    if not frames:
        return pd.DataFrame(
            columns=["zone", "window", "as_of", "mean", "std", "skew"]
        )

    return pd.concat(frames, ignore_index=True)

def add_rolling_volatility(
    df: pd.DataFrame,
    periods: int = 96,
) -> pd.DataFrame:
    """
    Add rolling std dev of returns per zone.
    `periods` is the rolling window size in number of samples.
    For quarter-hourly data, 96 ~ 1 day.
    """
    if "return" not in df.columns:
        df = add_returns(df)

    df = df.sort_values(["zone", "time"]).copy()
    df["rolling_std"] = (
        df.groupby("zone")["return"]
        .rolling(window=periods, min_periods=periods // 2)
        .std()
        .reset_index(level=0, drop=True)
    )
    return df


def add_technical_indicators(
    df: pd.DataFrame,
    price_col: str = "price",
    group_col: str = "zone",
    ma_short: int = 24,
    ma_long: int = 96,
    rsi_period: int = 14,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
) -> pd.DataFrame:
    """
    Add simple technical indicators per zone on `price_col`:
    - Moving averages (short / long)
    - RSI
    - MACD + signal line
    - Bollinger bands (around long MA)
    All periods are in number of samples (quarter-hours).
    """
    df = df.sort_values([group_col, "time"]).copy()

    def _per_group(g: pd.DataFrame) -> pd.DataFrame:
        p = g[price_col]

        # Moving averages
        g["ma_short"] = p.rolling(ma_short, min_periods=ma_short // 2).mean()
        g["ma_long"] = p.rolling(ma_long, min_periods=ma_long // 2).mean()

        # RSI
        delta = p.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(rsi_period, min_periods=rsi_period).mean()
        avg_loss = loss.rolling(rsi_period, min_periods=rsi_period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        g["rsi"] = 100 - (100 / (1 + rs))

        # MACD
        ema_fast = p.ewm(span=macd_fast, adjust=False).mean()
        ema_slow = p.ewm(span=macd_slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal = macd.ewm(span=macd_signal, adjust=False).mean()
        g["macd"] = macd
        g["macd_signal"] = signal

        # Bollinger bands around long MA
        rolling_std = p.rolling(ma_long, min_periods=ma_long // 2).std()
        g["bb_upper"] = g["ma_long"] + 2 * rolling_std
        g["bb_lower"] = g["ma_long"] - 2 * rolling_std

        return g

    df = df.groupby(group_col, group_keys=False).apply(_per_group)
    return df


def make_heatmap_frame(
    df: pd.DataFrame,
    value_col: str = "price",
) -> pd.DataFrame:
    """
    Prepare data for heatmaps:
        x = date, y = hour-of-day, color = value_col
    Returns columns: date, hour, value, zone
    """
    tmp = df.copy()
    tmp["time"] = pd.to_datetime(tmp["time"], utc=True)
    tmp["date"] = tmp["time"].dt.date
    tmp["hour"] = tmp["time"].dt.hour
    tmp = tmp.dropna(subset=[value_col])
    return tmp[["zone", "date", "hour", value_col]].rename(
        columns={value_col: "value"}
    )
# %%
