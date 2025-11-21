# app_pages/correlation_page.py
# %%

import sys
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app_pages.market_prices_page import get_market_price_df
from analysis.de_features import build_de_features
from analysis.market_price import filter_by_window, WINDOWS


def _corr_heatmap(corr: pd.DataFrame, title: str):
    if corr.empty:
        st.info(f"No data for {title}.")
        return

    df_corr = (
        corr.reset_index()
        .melt("zone", var_name="zone2", value_name="corr")
    )

    chart = alt.Chart(df_corr).mark_rect().encode(
        x=alt.X("zone2:N", title="Zone"),
        y=alt.Y("zone:N", title="Zone"),
        color=alt.Color("corr:Q", title="Correlation", scale=alt.Scale(domain=[-1, 1])),
        tooltip=["zone", "zone2", alt.Tooltip("corr:Q", format=".2f")],
    ).properties(title=title)

    st.altair_chart(chart, use_container_width=True)


def render_correlation_page():
    st.title("Correlation & Analytics")

    prices = get_market_price_df()
    if prices.empty:
        st.warning("No market price data available.")
        return

    feat_de = build_de_features()
    if feat_de.empty:
        st.warning("DE feature set is empty; DE-specific plots will be limited.")
    else:
        feat_de = feat_de.copy()

    tab_corr, tab_roll, tab_scatter = st.tabs(
        [
            "Cross-country correlations",
            "Rolling correlations",
            "Price vs fundamentals (DE)",
        ]
    )

    # ============================
    # TAB 1 – Cross-country correlations
    # ============================
    with tab_corr:
        st.subheader("Price & return correlation heatmaps")

        window_key = st.selectbox(
            "Time window",
            list(WINDOWS.keys()),
            index=2,
            help="Window used for correlation computation.",
        )

        df_w = filter_by_window(prices, window_key)
        if df_w.empty:
            st.warning("No data in selected window.")
        else:
            price_wide = (
                df_w
                .pivot_table(index="time", columns="zone", values="price")
                .dropna(how="all", axis=1)
            )
            ret_wide = (
                df_w
                .pivot_table(index="time", columns="zone", values="return")
                .dropna(how="all", axis=1)
            )

            if not price_wide.empty:
                corr_price = price_wide.corr()
                corr_price.index.name = "zone"
                _corr_heatmap(corr_price, "Price level correlations")

            if not ret_wide.empty:
                corr_ret = ret_wide.corr()
                corr_ret.index.name = "zone"
                _corr_heatmap(corr_ret, "Return correlations")

    # ============================
    # TAB 2 – Rolling correlations
    # ============================
    with tab_roll:
        st.subheader("Rolling correlation between two zones")

        zones = sorted(z for z in prices["zone"].unique() if pd.notna(z))
        if len(zones) < 2:
            st.warning("Need at least two zones for rolling correlations.")
        else:
            col1, col2, col3 = st.columns(3)
            with col1:
                z1 = st.selectbox("Zone 1", zones)
            with col2:
                z2 = st.selectbox("Zone 2", [z for z in zones if z != z1])
            with col3:
                series_type = st.radio("Series", ["Price", "Returns"], horizontal=True)

            window_days = st.slider(
                "Rolling window (days)",
                min_value=1,
                max_value=60,
                value=7,
                step=1,
                help="Window length converted to 15-min observations.",
            )
            steps = window_days * 96  # 96 quarter-hours per day

            df_pair = prices[prices["zone"].isin([z1, z2])].copy()
            if df_pair.empty:
                st.warning("No data for selected zones.")
            else:
                val_col = "price" if series_type == "Price" else "return"
                wide = (
                    df_pair
                    .pivot_table(index="time", columns="zone", values=val_col)
                    .sort_index()
                    .dropna()
                )
                if z1 not in wide.columns or z2 not in wide.columns:
                    st.warning("Missing data for one of the zones.")
                else:
                    rolling_corr = (
                        wide[z1]
                        .rolling(window=steps, min_periods=max(steps // 2, 1))
                        .corr(wide[z2])
                    )
                    df_roll = rolling_corr.dropna().to_frame("rolling_corr")
                    if df_roll.empty:
                        st.info("Not enough data for rolling correlation.")
                    else:
                        chart = alt.Chart(
                            df_roll.reset_index()
                        ).mark_line().encode(
                            x=alt.X("time:T", title="Time"),
                            y=alt.Y("rolling_corr:Q", title=f"Rolling corr {z1}–{z2}"),
                        )
                        st.altair_chart(chart, use_container_width=True)

    # ============================
    # TAB 3 – Price vs fundamentals (DE)
    # ============================
    with tab_scatter:
        st.subheader("DE price vs residual load / RES shares / forecasts")

        if feat_de is None or feat_de.empty:
            st.warning("DE feature set not available.")
            return

        # Align window to last N days
        window_days = st.slider(
            "Window (days, last N days of DE data)",
            min_value=5,
            max_value=365,
            value=60,
            step=5,
        )
        idx = feat_de.index
        end = idx.max()
        start = end - pd.Timedelta(days=window_days)
        feat_view = feat_de.loc[(idx >= start) & (idx <= end)].copy()

        target_type = st.radio("Target", ["Price (price_de)", "Returns (ret_de)"], horizontal=True)
        y_col = "price_de" if target_type.startswith("Price") else "ret_de"

        # candidate fundamentals
        candidates = [
            "residual_load",
            "load_total",
            "gen_total",
            "wind_gen",
            "solar_gen",
            "wind_share",
            "solar_share",
            "res_share",
            "wind_fc",
            "solar_fc",
            "res_fc",
            "error_wind",
            "error_solar",
            "error_res",
        ]
        candidates = [c for c in candidates if c in feat_view.columns]

        if not candidates:
            st.warning("No fundamental features found in DE feature set.")
            return

        x_col = st.selectbox("Fundamental (X-axis)", candidates, index=0)

        df_plot = feat_view[[y_col, x_col]].dropna()
        if df_plot.empty:
            st.info("No overlapping data for selected variables.")
            return

        corr = np.corrcoef(df_plot[x_col], df_plot[y_col])[0, 1]

        st.markdown(
            f"**Pearson correlation between {x_col} and {y_col}:** `{corr:.3f}`"
        )

        scatter = alt.Chart(df_plot.reset_index()).mark_circle(size=20, opacity=0.5).encode(
            x=alt.X(f"{x_col}:Q", title=x_col),
            y=alt.Y(f"{y_col}:Q", title=y_col),
            tooltip=[x_col, y_col],
        )
        st.altair_chart(scatter, use_container_width=True)