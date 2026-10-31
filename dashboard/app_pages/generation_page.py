# app_pages/generation_page.py
# %%

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.group_series import (
    load_group_long as load_group_series_long,
    filter_by_window as filter_group_window,
)


@st.cache_data(ttl=300)
def get_generation_df() -> pd.DataFrame:
    return load_group_series_long("generation")


def render_generation_page():
    st.title("Generation – By Technology")

    df_gen = get_generation_df()
    if df_gen.empty:
        st.warning("No generation data found.")
        return

    window = st.selectbox("Time window", ["7D", "30D", "90D", "1Y", "max"], index=1)
    df_view = filter_group_window(df_gen, window)

    if df_view.empty:
        st.warning("No data for selected window.")
        return

    techs = sorted(df_view["series"].unique())
    selected_techs = st.multiselect("Technologies", techs, default=techs)
    df_view = df_view[df_view["series"].isin(selected_techs)]

    if df_view.empty:
        st.warning("No technologies selected.")
        return

    normalize = st.checkbox("Normalize to % of total (generation mix)", value=False)

    pivot = (
        df_view
        .pivot_table(index="time", columns="series", values="value", aggfunc="mean")
        .fillna(0.0)
        .sort_index()
    )

    if normalize:
        row_sum = pivot.sum(axis=1)
        pivot = pivot.div(row_sum.where(row_sum != 0), axis=0) * 100.0
        y_title = "% of total generation"
    else:
        y_title = "Generation (MW)"

    st.subheader("Generation by technology (stacked area)")
    df_plot = pivot.reset_index().melt("time", var_name="series", value_name="value")

    area = alt.Chart(df_plot).mark_area().encode(
        x=alt.X("time:T"),
        y=alt.Y("value:Q", stack="normalize" if normalize else "zero", title=y_title),
        color=alt.Color("series:N"),
        tooltip=["time:T", "series:N", "value:Q"],
    )
    st.altair_chart(area, use_container_width=True)

    st.subheader("Average diurnal profile")

    df_diurnal = df_view.copy()
    df_diurnal["time"] = pd.to_datetime(df_diurnal["time"], utc=True)
    df_diurnal["hour"] = df_diurnal["time"].dt.hour

    prof = (
        df_diurnal
        .groupby(["series", "hour"])["value"]
        .mean()
        .reset_index()
    )

    line = alt.Chart(prof).mark_line(point=True).encode(
        x=alt.X("hour:O", title="Hour of day"),
        y=alt.Y("value:Q", title="Average generation"),
        color="series:N",
    )
    st.altair_chart(line, use_container_width=True)
