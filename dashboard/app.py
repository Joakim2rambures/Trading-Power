#app.py
#%%
import streamlit as st

from app_pages.market_prices_page import render_market_prices_page
from app_pages.generation_page import render_generation_page
from app_pages.forecast_page import render_forecast_page
from app_pages.consumption_page import render_consumption_page
from app_pages.correlation_page import render_correlation_page
from app_pages.sarimax_page import render_sarimax_page


def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Page",
        [
            "Market Prices",
            "Generation",
            "Forecast",
            "Consumption",
            "Correlation & Analytics",
            "SARIMAX Forecasting",
        ],
    )

    if page == "Market Prices":
        render_market_prices_page()
    elif page == "Generation":
        render_generation_page()
    elif page == "Forecast":
        render_forecast_page()
    elif page == "Consumption":
        render_consumption_page()
    elif page == "Correlation & Analytics":
        render_correlation_page()
    elif page == "SARIMAX Forecasting":
        render_sarimax_page()


if __name__ == "__main__":
    main()