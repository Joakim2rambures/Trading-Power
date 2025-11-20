Possible filters:
  * `1223` – Power generation: Lignite (brown coal)
  * `1224` – Power generation: Nuclear
  * `1225` – Power generation: Wind offshore
  * `1226` – Power generation: Hydropower
  * `1227` – Power generation: Other conventional
  * `1228` – Power generation: Other renewables
  * `4066` – Power generation: Biomass
  * `4067` – Power generation: Wind onshore
  * `4068` – Power generation: Photovoltaics
  * `4069` – Power generation: Hard coal
  * `4070` – Power generation: Pumped storage
  * `4071` – Power generation: Natural gas
  * `410` – Power consumption: Total (grid load)
  * `4359` – Power consumption: Residual load
  * `4387` – Power consumption: Pumped storage
  * `4169` – Market price: Germany/Luxembourg
  * `5078` – Market price: Neighbouring DE/LU
  * `4996` – Market price: Belgium
  * `4997` – Market price: Norway 2
  * `4170` – Market price: Austria
  * `252` – Market price: Denmark 1
  * `253` – Market price: Denmark 2
  * `254` – Market price: France
  * `255` – Market price: Italy (North)
  * `256` – Market price: Netherlands
  * `257` – Market price: Poland
  * `258` – Market price: Poland
  * `259` – Market price: Switzerland
  * `260` – Market price: Slovenia
  * `261` – Market price: Czechia (Czech Republic)
  * `262` – Market price: Hungary
  * `3791` – Forecast generation: Offshore
  * `123` – Forecast generation: Onshore
  * `125` – Forecast generation: Photovoltaics
  * `715` – Forecast generation: Other
  * `5097` – Forecast generation: Wind and photovoltaics
  * `122` – Forecast generation: Total


Ideas : 
Dashboard Design Overview This dashboard is built on SMARD power market data (spot prices, generation, and forecasts). 

Goal : exploratory analysis + visual monitoring of: Market prices by country Power generation by technology Forecasts vs actuals Cross-asset and cross-country relationships (Optional) Weather and simple forecasting models 

1. Tech Stack / Visualization Libraries Python web dashboard options Streamlit – simplest to build, great for: Tabs Controls (select boxes, sliders) Quick plots and layout Plotly + Dash – more control for: Time-series charts Interactive zoom / pan / hover Multiple linked charts Bokeh / Panel / hvPlot Good for linked brushing (select on one chart, highlight on another) If going front-end heavy React + Plotly.js or ECharts More “production” feel, more work Vega-Lite / Altair (from Python) Declarative, very good for statistical visuals For a first version, Streamlit or Dash is the natural choice.

2. High-Level Tab Structure Planned dashboard sections: Overview / Snapshot Market Prices Generation & Forecast Correlation / Analytics (Optional) Weather & Forecasting Models

3. Tab 1 – Overview / Snapshot Goal: “What’s going on right now?” Ideas: Latest price by country EU mini-map or bar chart: country vs last price Color scale by price level (green = cheap, red = expensive) Spreads vs reference country (e.g. DE) Horizontal bar chart: Price(country) – Price(DE) Quick view of who is “rich/cheap” vs Germany Today’s intraday profile Multi-line chart of today’s prices by country Toggle countries on/off System balance snapshot KPI cards: Total generation Residual load Share of renewables (RES %) Possibly net exports/imports (if available)

4. Tab 2 – Market Prices (Deep Dive) Per country and spreads; Time-series plots Prices over a selected window (e.g. last 7 / 30 / 90 days); Toggle: Levels vs returns Linear vs log scale Spreads DE–FR, DE–NL, AT–DE, etc. as: Separate lines, or Small multiples; Highlight when spread exceeds a chosen threshold Distribution / risk view Histograms / KDE of returns (not raw levels); Boxplots by: Hour-of-day Day-of-week Show summary stats (by asset / country): Mean, std Skewness Kurtosis (Optionally percentiles, VaR) Volatility / regime view Rolling std dev of returns (volatility); “Regime” shading: High-vol vs low-vol periods Technical indicators; Moving averages (short / long) on prices or returns RSI / MACD / Bollinger bands on returns Simple overbought/oversold markers Heatmaps; Price heatmap x = date, y = hour-of-day, color = price Spread heatmap; Same layout, but for spreads vs a reference

6. Tab 3 – Generation & Forecast Focus: power generation by technology and forecast quality. Per asset (wind, solar, gas, coal, nuclear, etc.) Stacked area chart Generation by technology over time Option to normalize to % of total (mix view) Capacity factor & profiles Time-series of capacity factor for wind / solar Average diurnal profile: x = hour-of-day, y = average generation or capacity factor Forecast vs actual Lines: forecast vs actual per technology Separate chart: forecast error = actual – forecast Histograms + summary stats for errors: MAE RMSE Bias (mean error) Possibly distribution by hour-of-day Residual load & price Scatter plot: x = residual load y = price Color = hour-of-day or RES share Rolling correlation between price and: Residual load Wind share Solar share

7. Tab 4 – Correlation & Analytics More statistical / cross-asset analysis. Correlation heatmaps Price levels across countries Price returns across countries Price vs generation (by technology) Price vs forecast error Rolling correlations e.g. 30-day rolling correlation: DE–FR prices DE price vs DE wind share Price vs residual load Display as: Line charts, or Small multiples by pair ACF / PACF For: Price returns (per country) Forecast errors (per asset) Use to inspect: Mean reversion Memory / lag structure Stationarity tests Show ADF / KPSS results for: Price levels (typically non-stationary) Price returns (ideally stationary) Spreads (often more stationary than levels) Simple visual indicator: Green = likely stationary Red = likely non-stationary

8. Tab 5 – Weather & Forecasting (Later Extension) When weather data is added (wind speed, temperature, irradiance): Weather Maps / heatmaps Wind speed over Germany / Europe Temperature maps by region Weather vs generation Scatter: wind speed vs wind generation Scatter: irradiance proxy vs PV generation Weather vs price Scatter: temperature vs load / price Heatmap: price as function of wind share and demand Forecasting models Start simple with baseline models: Naive: Forecast = last value Seasonal naive: Forecast = same time yesterday / last week Then extend to: ARIMA / SARIMA Prophet Simple tree or gradient boosting model on: Lagged prices Load Wind / solar Hour-of-day, weekday, etc. Model visualizations Backtest chart Actual vs predicted over last X days Shaded error band (e.g. ±1 or ±2σ) Error metric cards MAE, RMSE, MAPE Hit rate on sign of returns (directional accuracy) Feature relationship plots Price vs wind share Price vs residual load Partial dependence–style views

9. UX / Interaction Ideas Controls / filters Date range selector Dropdowns: Country / bidding zone Technology (wind, solar, gas, etc.) Forecast type (intraday, day-ahead, etc.) Toggle: Raw vs normalized (z-scores, per-MWh, per capacity) Download options Export current view / filtered data as CSV or Parquet Tooltips / hover info On chart hover, show: Timestamp Price Generation mix Forecast error (where relevant)

1. High-level architecture (how your pieces fit together)
a) Raw data ingestion
Backfill (power/fetch_power/incremental.py – actually backfill)
Purpose: big historical load.
For each filter_id in FILTER_GROUPS[filter_group_name]:
Calls smard_range(start, end_ts, ...).
Merges new data into daily parquet files using merge_incoming_data(...).
Updates high_watermark.json via save_hwm_map with per-filter end timestamp.

Path layout:
data/region=DE/filter=<filter_id>/date=<YYYY-MM-DD>/data.parquet
Incremental (incremental.py – the earlier one)

Purpose: periodic “top-up” of recent data.
Uses last_full_quarter() and load_hwm_map() to know where to resume.
Overlaps by OVERLAP_HOURS (default 2h) to avoid gaps.

For each filter:
Calls smard_range on [start, end].
Uses merge_incoming_data to append + dedupe by time_utc.
If anything was written, updates global HWM (currently via save_hwm(hwm_path, end)).
Storage details

merge_incoming_data:
Splits incoming rows by day via date_str.
Reads existing data.parquet (if present).
Concats old+new → drop_by_timecol to dedupe & sort by time_utc.
Writes back atomically via write_atomic.

State (state.py)
high_watermark.json = per-filter map: { "4169": "...", "256": "...", ... }.
stats_high_watermark.json = single timestamp { "last_timestamp": ... }.
ensure_utc, floor_to_quarter, last_full_quarter keep everything UTC & on 15-min grid.

Maintenance (maintenance.py)
Iterates over all daily parquet files for each filter and “compacts” them (read & rewrite) using to_parquet_bytes + write_atomic (good for cleaning up fragmentation, compression, etc.).

b) Analysis & stats
Read raw history (read_data.py)

load_filter_history(filter_id, root=PROJECT_ROOT):
Lists data/region=DE/filter=<id>/date=YYYY-MM-DD/... using list_paths.
Reads each day, concats, then drop_by_timecol for clean time series.

Market price analysis (analysis/market_price.py)
FILTER_GROUPS["market_price"] = DE / NL / BE market price filters.
load_market_price_series() → dict {filter_id: df}.
series_to_long(all_series) → long df with columns: time, zone, price.

FILTER_ID_TO_COUNTRY maps filter IDs to zone labels.
add_returns adds per-zone % returns.

filter_by_window implements your windows (1D, 3D, 7D, 30D, 1Y, max).
compute_return_stats → per-zone stats: mean, std, skew.
compute_spreads → spreads vs reference zone.
compute_multi_window_stats:

For each window: filter, compute return stats, tag with window + as_of.
Returns stacked long df: zone, window, as_of, mean, std, skew.
Stats generation
Backfill stats (stats_backfill.py)
Loads all prices up to some end_ts (from env or data HWM).

Computes “slow” window stats (SLOW_WINDOWS = [7D, 30D, 1Y]).
Writes only those stats to data/stats/market_price_stats.parquet.
Sets stats_high_watermark.json to end_ts.

Incremental stats (stats_incremental.py)
Looks at high_watermark.json → gets minimal per-filter HWM → data_hwm.
Compares with stats_high_watermark.json.
If data_hwm > stats_hwm, loads prices up to data_hwm.
Computes “fast” window stats (FAST_WINDOWS = ["1D", "3D"]).
Writes only those stats to the same market_price_stats.parquet (overwriting).
Updates stats_high_watermark.json.

Stats maintenance (stats_maintenance.py)
Reads stats parquet and rewrites it (compaction).

c) Dashboard (dashboard.py)
Uses Streamlit.

get_market_price_df():
Loads all market price series, converts to long, adds returns.

Cached with @st.cache_data(ttl=300).
load_precomputed_stats():

Reads data/stats/market_price_stats.parquet if present.

UI:
Sidebar:
Time window (1D, 3D, 7D, 30D, 1Y, max) → used to filter data.
Zones multiselect.
Reference zone for spreads.

Main:
Line chart of prices.
Line chart of returns.
Line chart of spreads vs reference zone.
Stats table (from precomputed stats) filtered by window & zones.
