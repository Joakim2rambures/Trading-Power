# app_pages/consumption_page.py
#%%

import sys
from pathlib import Path

import altair as alt
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
def get_consumption_df() -> pd.DataFrame:
    return load_group_series_long("consumption")


def render_consumption_page():
    st.title("Consumption – Load & Residual Load")

    df_cons = get_consumption_df()
    if df_cons.empty:
        st.warning("No consumption data found.")
        return

    window = st.selectbox("Time window", ["7D", "30D", "90D", "1Y", "max"], index=1)
    df_view = filter_group_window(df_cons, window)

    if df_view.empty:
        st.warning("No data for selected window.")
        return

    types = sorted(df_view["series"].unique())
    selected_types = st.multiselect("Series", types, default=types)
    df_view = df_view[df_view["series"].isin(selected_types)]

    if df_view.empty:
        st.warning("No consumption series selected.")
        return

    st.subheader("Consumption time-series")
    pivot = (
        df_view
        .pivot_table(index="time", columns="series", values="value", aggfunc="mean")
        .sort_index()
    )
    st.line_chart(pivot)

    st.subheader("Average diurnal profile")
    df_d = df_view.copy()
    df_d["time"] = pd.to_datetime(df_d["time"], utc=True)
    df_d["hour"] = df_d["time"].dt.hour

    prof = (
        df_d
        .groupby(["series", "hour"])["value"]
        .mean()
        .reset_index()
    )

    line = alt.Chart(prof).mark_line(point=True).encode(
        x=alt.X("hour:O", title="Hour of day"),
        y=alt.Y("value:Q", title="Average consumption"),
        color="series:N",
    )
    st.altair_chart(line, use_container_width=True)
