import sys
from pathlib import Path

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
)


@st.cache_data
def get_market_price_df() -> pd.DataFrame:
    """
    Load data once and reuse (cached by Streamlit).
    """
    all_series = load_market_price_series()
    df = series_to_long(all_series)
    df = add_returns(df)
    return df


def main():
    st.title("Market Prices – Multi-Country Overview")

    df = get_market_price_df()

    if df.empty:
        st.warning("No market price data found.")
        return

    # --- Sidebar controls ---
    st.sidebar.header("Filters")

    window_key = st.sidebar.selectbox(
        "Time window",
        list(WINDOWS.keys()),
        index=2,  # default "30D"
    )

    zones = sorted(df["zone"].unique())
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

    # --- Filtered view ---
    df_view = df[df["zone"].isin(selected_zones)]
    df_view = filter_by_window(df_view, window_key)

    if df_view.empty:
        st.warning("No data for selected window / zones.")
        return

    # --- Prices chart ---
    st.subheader(f"Prices by zone ({window_key})")

    price_pivot = (
        df_view
        .pivot(index="time", columns="zone", values="price")
        .sort_index()
    )

    st.line_chart(price_pivot)

    # --- Returns chart ---
    st.subheader(f"Returns by zone ({window_key})")

    return_pivot = (
        df_view
        .pivot(index="time", columns="zone", values="return")
        .sort_index()
    )

    # Drop all-NaN columns (e.g. if very short window)
    return_pivot = return_pivot.dropna(how="all", axis=1)

    if return_pivot.empty:
        st.info("Not enough data points to compute returns in this window.")
    else:
        st.line_chart(return_pivot)

    # --- Spreads vs reference zone ---
    st.subheader(f"Spreads vs {ref_zone} ({window_key})")

    try:
        df_spreads = compute_spreads(df_view, ref_zone)
        # Only keep spreads for selected zones (excluding ref)
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

    # --- Return statistics table ---
    st.subheader(f"Return statistics ({window_key})")

    stats = compute_return_stats(df_view)
    if stats.empty:
        st.info("No returns stats available for this selection.")
    else:
        st.dataframe(stats.style.format("{:.4f}"))


if __name__ == "__main__":
    main()