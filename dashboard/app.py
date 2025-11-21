#dashboard.py
#%%
import sys
from pathlib import Path
import numpy as np
import altair as alt
import streamlit as st
import pandas as pd

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.market_price import (
    load_market_price_series,
    series_to_long,
    add_returns,
    filter_by_window,
    compute_return_stats,
    compute_spreads,
    WINDOWS,
    add_rolling_volatility,
    add_technical_indicators,
    make_heatmap_frame,
)


@st.cache_data(ttl=300)
def get_market_price_df() -> pd.DataFrame:
    """
    Load data once and reuse (cached by Streamlit).
    """
    all_series = load_market_price_series()
    df = series_to_long(all_series)
    df = add_returns(df)
    return df

@st.cache_data(ttl=300)
def load_precomputed_stats() -> pd.DataFrame:
    stats_fast_path = PROJECT_ROOT / "data" / "stats" / "market_price_stats_fast.parquet"
    stats_slow_path = PROJECT_ROOT / "data" / "stats" / "market_price_stats_slow.parquet"

    frames = []
    if stats_fast_path.exists():
        frames.append(pd.read_parquet(stats_fast_path))
    if stats_slow_path.exists():
        frames.append(pd.read_parquet(stats_slow_path))

    if not frames:
        return pd.DataFrame()

    stats_all = pd.concat(frames, ignore_index=True)

    # Optional: ensure consistent column order
    cols = ["zone", "window", "as_of", "mean", "std", "skew"]
    existing = [c for c in cols if c in stats_all.columns]
    return stats_all[existing + [c for c in stats_all.columns if c not in existing]]


def main():
    st.title("Market Prices – Multi-Country Overview")

    df = get_market_price_df()
    if df.empty:
        st.warning("No market price data found.")
        return  # important: stop here if no data

    # Create tabs
    tab_overview, tab_deep = st.tabs(["Overview", "Market Prices – Deep Dive"])

    # =========================
    # Tab 1 – Overview
    # =========================
    with tab_overview:
        # --- Sidebar controls ---
        st.sidebar.header("Filters")

        window_key = st.sidebar.selectbox(
            "Time window",
            list(WINDOWS.keys()),
            index=2,  # adjust if you changed WINDOWS ordering
        )

        zones_raw = df["zone"].unique()
        zones = sorted(z for z in zones_raw if pd.notna(z))

        default_zones = zones  # start with all visible
        selected_zones = st.sidebar.multiselect(
            "Zones",
            zones,
            default=default_zones,
        )

        ref_zone = st.sidebar.selectbox(
            "Reference zone for spreads",
            zones,
            index=0,
        )

        df_view = df[df["zone"].isin(selected_zones)]
        df_view = filter_by_window(df_view, window_key)

        if df_view.empty:
            st.warning("No data for selected window / zones.")
            return

        # Prices chart
        st.subheader(f"Prices by zone ({window_key})")
        price_pivot = (
            df_view
            .pivot(index="time", columns="zone", values="price")
            .sort_index()
        )
        st.line_chart(price_pivot)

        # Returns chart
        st.subheader(f"Returns by zone ({window_key})")
        return_pivot = (
            df_view
            .pivot(index="time", columns="zone", values="return")
            .sort_index()
        )
        return_pivot = return_pivot.dropna(how="all", axis=1)
        if return_pivot.empty:
            st.info("Not enough data points to compute returns in this window.")
        else:
            st.line_chart(return_pivot)

        # Spreads
        st.subheader(f"Spreads vs {ref_zone} ({window_key})")
        try:
            df_spreads = compute_spreads(df_view, ref_zone)
            keep_zones = [z for z in selected_zones if z != ref_zone]
            df_spreads = df_spreads[df_spreads["zone"].isin(keep_zones)]
            if df_spreads.empty:
                st.info("No other zones selected to compute spreads.")
            else:
                spread_pivot = (
                    df_spreads
                    .pivot(index="time", columns="zone", values="spread")
                    .sort_index()
                )
                st.line_chart(spread_pivot)
        except ValueError as e:
            st.info(str(e))

        # Return statistics table
        st.subheader(f"Return statistics ({window_key})")
        stats_all = load_precomputed_stats()
        if stats_all.empty:
            st.info("No precomputed stats available yet.")
        else:
            stats_view = stats_all[stats_all["window"] == window_key]
            if selected_zones:
                stats_view = stats_view[stats_view["zone"].isin(selected_zones)]
            if stats_view.empty:
                st.info("No stats available for this selection.")
            else:
                cols = [
                    c for c in
                    ["zone", "window", "as_of", "mean", "std", "skew", "kurt"]
                    if c in stats_view.columns
                ]
                st.dataframe(
                    stats_view[cols].style.format(
                        {
                            "mean": "{:.4f}",
                            "std": "{:.4f}",
                            "skew": "{:.4f}",
                            "kurt": "{:.4f}",
                        }
                    )
                )

    # =========================
    # Tab 2 – Deep Dive
    # =========================
    with tab_deep:
        st.header("Market Prices – Deep Dive")

        zones = sorted(z for z in df["zone"].unique() if pd.notna(z))
        if not zones:
            st.warning("No zones available.")
            return

        col1, col2, col3 = st.columns(3)
        with col1:
            deep_zone = st.selectbox("Country / zone", zones)
        with col2:
            deep_window = st.selectbox("Time window", ["7D", "30D", "90D", "1Y"])
        with col3:
            view_mode = st.radio("View", ["Levels", "Returns"], horizontal=True)

        log_scale = st.checkbox("Use log scale for prices", value=False)

        ref_zone_deep = st.selectbox(
            "Reference zone for spreads",
            zones,
            index=0,
            help="Used for spread plots and heatmap.",
        )

        spread_thresh = st.number_input(
            "Highlight spreads above threshold (absolute, same units as price)",
            min_value=0.0,
            value=20.0,
            step=1.0,
        )

        # Filter data for selected zone + window
        df_zone = df[df["zone"] == deep_zone].copy()
        df_zone = filter_by_window(df_zone, deep_window)

        if df_zone.empty:
            st.warning("No data for this zone/window.")
            return

        # ---- Time-series: levels vs returns ----
        st.subheader("Time-series")

        if view_mode == "Levels":
            series = df_zone[["time", "price"]].set_index("time")
            if log_scale:
                series = np.log(series)
            st.line_chart(series)
        else:
            series = df_zone[["time", "return"]].set_index("time").dropna()
            st.line_chart(series)

        # ---- Spreads vs reference zone ----
        st.subheader(f"Spreads vs {ref_zone_deep}")

        df_window = filter_by_window(df, deep_window)
        try:
            df_spreads_all = compute_spreads(df_window, ref_zone_deep)
            df_spreads_zone = df_spreads_all[df_spreads_all["zone"] == deep_zone].copy()
        except ValueError:
            df_spreads_zone = pd.DataFrame(columns=["time", "zone", "spread"])

        if df_spreads_zone.empty:
            st.info("No spreads available for this selection.")
        else:
            spread_series = (
                df_spreads_zone[["time", "spread"]]
                .set_index("time")
                .sort_index()
            )
            st.line_chart(spread_series)

            # Highlight large spreads in a simple table
            big_spreads = df_spreads_zone[
                df_spreads_zone["spread"].abs() >= spread_thresh
            ].copy()
            if not big_spreads.empty:
                st.markdown("**Periods with large spreads**")
                st.dataframe(
                    big_spreads.sort_values("time").set_index("time")
                )

        # ---- Distribution / risk view ----
        st.subheader("Distribution of returns")

        returns_zone = df_zone["return"].dropna()
        if returns_zone.empty:
            st.info("Not enough data for return distribution.")
        else:
            # Histogram
            hist = alt.Chart(
                pd.DataFrame({"return": returns_zone})
            ).mark_bar().encode(
                alt.X("return:Q", bin=alt.Bin(maxbins=50)),
                alt.Y("count():Q"),
            )
            st.altair_chart(hist, use_container_width=True)

            # Boxplots by hour-of-day and day-of-week
            df_zone_dist = df_zone.copy()
            df_zone_dist["time"] = pd.to_datetime(df_zone_dist["time"], utc=True)
            df_zone_dist["hour"] = df_zone_dist["time"].dt.hour
            df_zone_dist["dow"] = df_zone_dist["time"].dt.day_name()

            df_zone_dist = df_zone_dist.dropna(subset=["return"])

            if not df_zone_dist.empty:
                st.markdown("**Boxplot of returns by hour-of-day**")
                box_hour = alt.Chart(df_zone_dist).mark_boxplot().encode(
                    x="hour:O",
                    y="return:Q",
                )
                st.altair_chart(box_hour, use_container_width=True)

                st.markdown("**Boxplot of returns by day-of-week**")
                box_dow = alt.Chart(df_zone_dist).mark_boxplot().encode(
                    x=alt.X("dow:O", sort=list(df_zone_dist["dow"].unique())),
                    y="return:Q",
                )
                st.altair_chart(box_dow, use_container_width=True)

        # ---- Volatility / regime view ----
        st.subheader("Rolling volatility")

        df_zone_vol = add_rolling_volatility(df_zone, periods=96)
        vol_series = (
            df_zone_vol[["time", "rolling_std"]]
            .set_index("time")
            .sort_index()
            .dropna()
        )
        if vol_series.empty:
            st.info("Not enough data for rolling volatility.")
        else:
            st.line_chart(vol_series)

        # ---- Technical indicators ----
        st.subheader("Technical indicators")

        df_zone_ta = add_technical_indicators(df_zone)
        ta = df_zone_ta.set_index("time").sort_index()

        # Price + moving averages
        if {"price", "ma_short", "ma_long"} <= set(ta.columns):
            ta_ma = ta[["price", "ma_short", "ma_long"]].dropna()
            st.line_chart(ta_ma)

        # RSI
        if "rsi" in ta.columns:
            st.markdown("**RSI**")
            rsi_series = ta[["rsi"]].dropna()
            st.line_chart(rsi_series)

        # MACD
        if {"macd", "macd_signal"} <= set(ta.columns):
            st.markdown("**MACD**")
            macd_df = ta[["macd", "macd_signal"]].dropna()
            st.line_chart(macd_df)

        # ---- Heatmaps ----
        st.subheader("Heatmaps")

        # Price heatmap for selected zone
        df_zone_heat = make_heatmap_frame(df_zone, value_col="price")
        df_zone_heat = df_zone_heat[df_zone_heat["zone"] == deep_zone]

        if df_zone_heat.empty:
            st.info("No data for price heatmap.")
        else:
            heat = alt.Chart(df_zone_heat).mark_rect().encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("hour:O", title="Hour of day"),
                color=alt.Color("value:Q", title="Price"),
            )
            st.markdown("**Price heatmap**")
            st.altair_chart(heat, use_container_width=True)

        # Spread heatmap vs reference
        if not df_spreads_zone.empty:
            df_spread_heat = df_spreads_zone.copy()
            df_spread_heat["time"] = pd.to_datetime(df_spread_heat["time"], utc=True)
            df_spread_heat["date"] = df_spread_heat["time"].dt.date
            df_spread_heat["hour"] = df_spread_heat["time"].dt.hour

            heat_spread = alt.Chart(df_spread_heat).mark_rect().encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("hour:O", title="Hour of day"),
                color=alt.Color("spread:Q", title=f"Spread vs {ref_zone_deep}"),
            )
            st.markdown("**Spread heatmap**")
            st.altair_chart(heat_spread, use_container_width=True)


if __name__ == "__main__":
    main()