# app_pages/forecast_page.py

#%%
import sys
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.group_series import (
    load_group_long as load_group_series_long,
    filter_by_window as filter_group_window,
)


@st.cache_data(ttl=300)
def get_generation_df() -> pd.DataFrame:
    return load_group_series_long("generation")


@st.cache_data(ttl=300)
def get_forecast_df() -> pd.DataFrame:
    return load_group_series_long("forecast")


def render_forecast_page():
    st.title("Forecast vs Actual – By Technology")

    df_gen = get_generation_df()
    df_fc = get_forecast_df()

    if df_gen.empty or df_fc.empty:
        st.warning("Need both generation and forecast data.")
        return

    fc_series = sorted(df_fc["series"].unique())
    gen_series = sorted(df_gen["series"].unique())

    col1, col2 = st.columns(2)
    with col1:
        fc_choice = st.selectbox("Forecast series", fc_series)
    with col2:
        gen_choice = st.selectbox("Actual generation series", gen_series)

    window = st.selectbox("Time window", ["7D", "30D", "90D", "1Y", "max"], index=1)

    df_fc_sel = filter_group_window(df_fc[df_fc["series"] == fc_choice], window)
    df_gen_sel = filter_group_window(df_gen[df_gen["series"] == gen_choice], window)

    if df_fc_sel.empty or df_gen_sel.empty:
        st.warning("No overlapping data for selection.")
        return

    merged = pd.merge_asof(
        df_gen_sel.sort_values("time"),
        df_fc_sel.sort_values("time"),
        on="time",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=15),
        suffixes=("_actual", "_forecast"),
    ).dropna(subset=["value_actual", "value_forecast"])

    if merged.empty:
        st.warning("No matched forecast/actual points within 15-minute tolerance.")
        return

    st.subheader("Forecast vs actual (time-series)")
    df_ts = merged[["time", "value_actual", "value_forecast"]].set_index("time")
    st.line_chart(df_ts)

    merged["error"] = merged["value_actual"] - merged["value_forecast"]

    st.subheader("Forecast error = actual – forecast")

    col_mae, col_rmse, col_bias = st.columns(3)
    mae = merged["error"].abs().mean()
    rmse = np.sqrt((merged["error"] ** 2).mean())
    bias = merged["error"].mean()
    col_mae.metric("MAE", f"{mae:.2f}")
    col_rmse.metric("RMSE", f"{rmse:.2f}")
    col_bias.metric("Bias (mean error)", f"{bias:.2f}")

    hist_err = alt.Chart(merged).mark_bar().encode(
        x=alt.X("error:Q", bin=alt.Bin(maxbins=50), title="Forecast error"),
        y="count():Q",
    )
    st.altair_chart(hist_err, use_container_width=True)

    merged["hour"] = pd.to_datetime(merged["time"], utc=True).dt.hour
    st.markdown("**Forecast error by hour-of-day**")
    box_err_hour = alt.Chart(merged).mark_boxplot().encode(
        x="hour:O",
        y="error:Q",
    )
    st.altair_chart(box_err_hour, use_container_width=True)
