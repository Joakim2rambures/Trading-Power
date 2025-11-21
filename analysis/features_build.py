# analysis/features_build.py
#%%

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.market_price import (
    load_market_price_series,
    series_to_long,
    add_returns,
)
from analysis.group_series import load_group_long


def build_de_features() -> pd.DataFrame:
    """Build DE-only feature set for price modelling & correlation.

    Returns DataFrame indexed by time with columns, e.g.:
        price_de, ret_de,
        gen_total, wind_gen, solar_gen,
        load_total, residual_load,
        wind_share, solar_share, res_share,
        wind_fc, solar_fc, res_fc,
        error_wind, error_solar, error_res,
        hour, dow
    """
    # -------------------
    # Prices (DE only)
    # -------------------
    all_series = load_market_price_series()
    prices_long = series_to_long(all_series)
    prices_de = prices_long[prices_long["zone"] == "DE"].copy()
    if prices_de.empty:
        return pd.DataFrame()

    prices_de["time"] = pd.to_datetime(prices_de["time"], utc=True)
    prices_de = add_returns(prices_de)
    prices_de = (
        prices_de
        .set_index("time")
        .sort_index()
        .rename(columns={"price": "price_de", "return": "ret_de"})
    )
    prices_de = prices_de[["price_de", "ret_de"]]

    # -------------------
    # Helper to load & pivot SMARD groups
    # -------------------
    def _load_pivot(group_name: str) -> pd.DataFrame:
        df = load_group_long(group_name)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df["time"] = pd.to_datetime(df["time"], utc=True)
        pivot = (
            df
            .pivot_table(index="time", columns="series", values="value", aggfunc="mean")
            .sort_index()
        )
        return pivot

    gen_pivot = _load_pivot("generation")
    cons_pivot = _load_pivot("consumption")
    fc_pivot = _load_pivot("forecast")

    # -------------------
    # Join everything on price index
    # -------------------
    feat = prices_de.copy()
    for extra in (gen_pivot, cons_pivot, fc_pivot):
        if extra is not None and not extra.empty:
            feat = feat.join(extra, how="left")

    # -------------------
    # Generation aggregates
    # -------------------
    gen_cols = []
    if gen_pivot is not None and not gen_pivot.empty:
        gen_cols = list(gen_pivot.columns)

    if gen_cols:
        gen_block = feat[gen_cols]
        feat["gen_total"] = gen_block.sum(axis=1)

        wind_cols = [c for c in gen_cols if "wind" in c.lower()]
        solar_cols = [c for c in gen_cols if "photovoltaic" in c.lower() or "solar" in c.lower()]

        if wind_cols:
            feat["wind_gen"] = feat[wind_cols].sum(axis=1)
        else:
            feat["wind_gen"] = np.nan

        if solar_cols:
            feat["solar_gen"] = feat[solar_cols].sum(axis=1)
        else:
            feat["solar_gen"] = np.nan
    else:
        feat["gen_total"] = np.nan
        feat["wind_gen"] = np.nan
        feat["solar_gen"] = np.nan

    # -------------------
    # Consumption aggregates
    # -------------------
    cons_cols = []
    if cons_pivot is not None and not cons_pivot.empty:
        cons_cols = list(cons_pivot.columns)

    if cons_cols:
        load_total_col = None
        residual_load_col = None
        for c in cons_cols:
            lc = c.lower()
            if "total" in lc or "grid load" in lc:
                load_total_col = c
            if "residual" in lc:
                residual_load_col = c

        if load_total_col is not None:
            feat["load_total"] = feat[load_total_col]
        else:
            feat["load_total"] = np.nan

        if residual_load_col is not None:
            feat["residual_load"] = feat[residual_load_col]
        else:
            # fallback residual load ≈ total load − wind − solar
            if "load_total" in feat.columns and "wind_gen" in feat.columns and "solar_gen" in feat.columns:
                feat["residual_load"] = (
                    feat["load_total"]
                    - feat["wind_gen"].fillna(0.0)
                    - feat["solar_gen"].fillna(0.0)
                )
            else:
                feat["residual_load"] = np.nan
    else:
        feat["load_total"] = np.nan
        if "wind_gen" in feat.columns and "solar_gen" in feat.columns:
            feat["residual_load"] = (
                feat["load_total"]
                - feat["wind_gen"].fillna(0.0)
                - feat["solar_gen"].fillna(0.0)
            )
        else:
            feat["residual_load"] = np.nan

    # -------------------
    # Forecast aggregates
    # -------------------
    fc_cols = []
    if fc_pivot is not None and not fc_pivot.empty:
        fc_cols = list(fc_pivot.columns)

    if fc_cols:
        wind_fc_cols = [c for c in fc_cols if "wind" in c.lower()]
        solar_fc_cols = [c for c in fc_cols if "photovoltaic" in c.lower() or "solar" in c.lower()]

        if wind_fc_cols:
            feat["wind_fc"] = feat[wind_fc_cols].sum(axis=1)
        else:
            feat["wind_fc"] = np.nan

        if solar_fc_cols:
            feat["solar_fc"] = feat[solar_fc_cols].sum(axis=1)
        else:
            feat["solar_fc"] = np.nan

        feat["res_fc"] = feat["wind_fc"].fillna(0.0) + feat["solar_fc"].fillna(0.0)
    else:
        feat["wind_fc"] = np.nan
        feat["solar_fc"] = np.nan
        feat["res_fc"] = np.nan

    # -------------------
    # Shares (generation mix)
    # -------------------
    with np.errstate(divide="ignore", invalid="ignore"):
        feat["wind_share"] = feat["wind_gen"] / feat["gen_total"]
        feat["solar_share"] = feat["solar_gen"] / feat["gen_total"]
        feat["res_share"] = (feat["wind_gen"] + feat["solar_gen"]) / feat["gen_total"]

    # -------------------
    # Forecast errors (actual − forecast)
    # -------------------
    feat["error_wind"] = feat["wind_gen"] - feat["wind_fc"]
    feat["error_solar"] = feat["solar_gen"] - feat["solar_fc"]
    feat["error_res"] = (
        (feat["wind_gen"] + feat["solar_gen"]) - feat["res_fc"]
    )

    # -------------------
    # Time features
    # -------------------
    idx = feat.index
    feat["hour"] = idx.hour
    feat["dow"] = idx.dayofweek  # 0=Mon

    return feat.sort_index()
