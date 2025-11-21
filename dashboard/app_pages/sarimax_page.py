# app_pages/sarimax_page.py
#%%

import sys
from pathlib import Path
import warnings

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.statespace.sarimax import SARIMAX

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.de_features import build_de_features, add_lags


def _filter_last_n_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty:
        return df
    idx = df.index
    end = idx.max()
    start = end - pd.Timedelta(days=days)
    return df.loc[(idx >= start) & (idx <= end)].copy()


def _fourier_terms(index: pd.DatetimeIndex, period: int, order: int) -> pd.DataFrame:
    """Fourier terms for a given period and order (for seasonality)."""
    if order <= 0:
        return pd.DataFrame(index=index)

    t = np.arange(len(index))
    data = {}
    for k in range(1, order + 1):
        data[f"sin_{period}_{k}"] = np.sin(2 * np.pi * k * t / period)
        data[f"cos_{period}_{k}"] = np.cos(2 * np.pi * k * t / period)
    return pd.DataFrame(data, index=index)


def render_sarimax_page():
    st.title("SARIMAX Forecasting – DE Market Prices")

    feat = build_de_features()
    if feat is None or feat.empty:
        st.warning("DE feature set is empty; cannot build SARIMAX model.")
        return

    st.markdown(
        "This page fits a SARIMAX model to DE prices, using exogenous variables:\n"
        "- Generation (wind/solar/total) and their lags\n"
        "- Consumption / residual load and lags\n"
        "- Forecasts (wind/solar/RES) and forecast errors\n"
        "- Daily & weekly seasonality via Fourier terms"
    )

    # ---------------------------
    # Data window & target choice
    # ---------------------------
    col1, col2 = st.columns(2)
    with col1:
        window_days = st.selectbox(
            "Training window (last N days)",
            [30, 90, 180, 365],
            index=1,
        )
    with col2:
        target_type = st.selectbox(
            "Target series",
            ["Returns (ret_de)", "Price levels (price_de)"],
            index=0,
        )

    df_train = _filter_last_n_days(feat, window_days)
    target_col = "ret_de" if target_type.startswith("Returns") else "price_de"

    if target_col not in df_train.columns:
        st.warning(f"Target column {target_col} not found.")
        return

    # ---------------------------
    # Exogenous variables selection
    # ---------------------------
    st.subheader("Exogenous variables (fundamentals)")

    base_candidates = [
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
    base_candidates = [c for c in base_candidates if c in df_train.columns]

    if not base_candidates:
        st.warning("No fundamental exogenous variables available.")
        return

    selected_exogs = st.multiselect(
        "Select exogenous variables",
        base_candidates,
        default=[
            c
            for c in ["residual_load", "wind_share", "solar_share", "gen_total", "load_total"]
            if c in base_candidates
        ],
    )

    if not selected_exogs:
        st.warning("Select at least one exogenous variable.")
        return

    max_lag = st.slider(
        "Max lag for exogenous variables (in 15-min steps)",
        min_value=0,
        max_value=96,
        value=4,
        help="0 = no lags, 96 = up to 1 day of lags.",
    )

    df_model = df_train[[target_col] + selected_exogs].copy()
    if max_lag > 0:
        df_model = add_lags(df_model, selected_exogs, max_lag)

    # Drop rows with NaNs introduced by lags
    df_model = df_model.dropna()
    if df_model.empty:
        st.warning("No data left after applying lags and dropping NaNs.")
        return

    y = df_model[target_col]
    X = df_model.drop(columns=[target_col])

    # ---------------------------
    # Fourier seasonality
    # ---------------------------
    st.subheader("Seasonality (Fourier terms)")

    k_daily = st.slider(
        "Daily Fourier order (period = 96)",
        min_value=0,
        max_value=5,
        value=2,
        help="0 disables daily Fourier terms.",
    )
    k_weekly = st.slider(
        "Weekly Fourier order (period = 96*7)",
        min_value=0,
        max_value=3,
        value=1,
        help="0 disables weekly Fourier terms.",
    )

    idx = y.index
    daily_terms = _fourier_terms(idx, period=96, order=k_daily)
    weekly_terms = _fourier_terms(idx, period=96 * 7, order=k_weekly)

    if not daily_terms.empty:
        X = X.join(daily_terms)
    if not weekly_terms.empty:
        X = X.join(weekly_terms)

    # Ensure alignment and no NaNs
    data = pd.concat([y, X], axis=1).dropna()
    y = data[target_col]
    X = data.drop(columns=[target_col])

    st.write(f"Final training sample size: `{len(y)}` observations, `{X.shape[1]}` exogenous features.")

    # ---------------------------
    # SARIMAX order selection
    # ---------------------------
    st.subheader("SARIMAX order")

    colp, cold, colq = st.columns(3)
    with colp:
        p = st.number_input("AR order (p)", min_value=0, max_value=5, value=1, step=1)
    with cold:
        default_d = 0 if target_col == "ret_de" else 1
        d = st.number_input("Integration (d)", min_value=0, max_value=2, value=default_d, step=1)
    with colq:
        q = st.number_input("MA order (q)", min_value=0, max_value=5, value=1, step=1)

    forecast_steps = st.slider(
        "Forecast horizon (steps, 15-min intervals)",
        min_value=0,
        max_value=96 * 2,
        value=96,
        help="0 = no forecast, just in-sample fit; 96 = 1 day ahead.",
    )

    if st.button("Fit SARIMAX model"):
        with st.spinner("Fitting SARIMAX (this may take a bit)..."):
            warnings.filterwarnings("ignore", category=ConvergenceWarning)

            try:
                model = SARIMAX(
                    endog=y,
                    exog=X,
                    order=(int(p), int(d), int(q)),
                    trend="c",
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                )
                res = model.fit(disp=False)
            except Exception as e:
                st.error(f"Model fitting failed: {e}")
                return

        st.success("Model fitted.")

        # ---------------------------
        # Summary & coefficients
        # ---------------------------
        st.subheader("Model diagnostics")

        st.write(f"**AIC:** `{res.aic:.2f}`  –  **BIC:** `{res.bic:.2f}`")

        with st.expander("Full SARIMAX summary"):
            st.text(res.summary().as_text())

        params = res.params.to_frame("coef")
        exog_params = params.loc[[p for p in params.index if p in X.columns]]
        if not exog_params.empty:
            exog_params["abs_coef"] = exog_params["coef"].abs()
            exog_params = exog_params.sort_values("abs_coef", ascending=False)
            st.markdown("**Exogenous variable coefficients (sorted by |coef|):**")
            st.dataframe(exog_params[["coef"]].style.format({"coef": "{:.4f}"}))

        # ---------------------------
        # In-sample fit vs actual
        # ---------------------------
        st.subheader("In-sample fit vs actual")

        fitted = res.fittedvalues
        df_fit = pd.DataFrame(
            {
                "time": y.index,
                "actual": y.values,
                "fitted": fitted.values,
            }
        ).set_index("time")

        # Show last N points for clarity
        n_show = min(len(df_fit), 96 * 10)  # e.g. last ~10 days
        df_fit_tail = df_fit.iloc[-n_show:].reset_index()

        chart_fit = alt.Chart(df_fit_tail).mark_line().encode(
            x=alt.X("time:T", title="Time"),
            y=alt.Y("value:Q", title=target_col),
            color="series:N",
        ).transform_fold(
            ["actual", "fitted"], as_=["series", "value"]
        )

        st.altair_chart(chart_fit, use_container_width=True)

        # ---------------------------
        # Simple forecast (flat exog assumption)
        # ---------------------------
        if forecast_steps > 0:
            st.subheader("Out-of-sample forecast (flat exog assumption)")

            last_exog = X.iloc[-1:]
            exog_future = pd.concat(
                [last_exog] * forecast_steps,
                ignore_index=True,
            )

            try:
                forecast_res = res.get_forecast(steps=forecast_steps, exog=exog_future)
                forecast_mean = forecast_res.predicted_mean

                freq = pd.infer_freq(y.index) or "15T"
                start = y.index[-1] + pd.Timedelta(freq)
                idx_forecast = pd.date_range(
                    start=start,
                    periods=forecast_steps,
                    freq=freq,
                )

                df_forecast = pd.DataFrame(
                    {
                        "time": idx_forecast,
                        "forecast": forecast_mean.values,
                    }
                )

                df_plot = pd.concat(
                    [
                        df_fit_tail[["time", "actual"]].assign(series="actual"),
                        df_forecast.assign(series="forecast"),
                    ],
                    ignore_index=True,
                )

                chart_forecast = alt.Chart(df_plot).mark_line().encode(
                    x=alt.X("time:T", title="Time"),
                    y=alt.Y("value:Q", title=target_col),
                    color="series:N",
                ).transform_fold(
                    ["actual", "forecast"], as_=["series", "value"]
                )

                st.altair_chart(chart_forecast, use_container_width=True)

            except Exception as e:
                st.warning(f"Forecast failed: {e}")

