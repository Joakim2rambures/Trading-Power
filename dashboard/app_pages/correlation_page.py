# app_pages/correlation_page.py
#%%

import streamlit as st


def render_correlation_page():
    st.title("Correlation & Analytics")

    st.info(
        "This page will include:\n"
        "- Correlation heatmaps (prices, returns, generation, residual load)\n"
        "- Rolling correlations (e.g. DE–NL, DE–BE)\n"
        "- Price vs residual load / RES share scatter plots\n"
        "- ACF/PACF and stationarity tests (ADF/KPSS).\n\n"
        "Data is already available via the existing loaders; we just need to add "
        "the statistical views here next."
    )
