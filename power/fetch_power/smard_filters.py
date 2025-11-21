# %%
# power/fetch_power/smard_filters.py

# --- Power generation (Stromerzeugung) ---
POWER_GENERATION_FILTER_IDS = {
    "1223": "Lignite (brown coal)",
    "1224": "Nuclear",
    "1225": "Wind offshore",
    "1226": "Hydropower",
    "1227": "Other conventional",
    "1228": "Other renewables",
    "4066": "Biomass",
    "4067": "Wind onshore",
    "4068": "Photovoltaic (solar PV)",
    "4069": "Hard coal",
    "4070": "Pumped storage (generation side)",
    "4071": "Natural gas",
}

# --- Market prices (Marktpreis) ---
MARKET_PRICE_FILTER_IDS = {
    "4169": "Market price: DE",
    "4996": "Market price: Belgium",
    "256":  "Market price: Netherlands",
}

CONS_FILTER_IDS = {
    "410":  "Power consumption: Total (grid load)",
    "4359": "Power consumption: Residual load",
    "4387": "Power consumption: Pumped storage",  # NEW
}


# --- Forecast generation (Prognostizierte Erzeugung) ---
FORECAST_FILTER_IDS = {
    "3791": "Offshore wind (forecast)",
    "123":  "Onshore wind (forecast)",
    "125":  "Photovoltaic (forecast)",
    "715":  "Other (forecast)",
    "5097": "Wind + PV (combined forecast)",
    "122":  "Total generation (forecast)",
}

# Optional: mapping by group name, to make it easy to select
FILTER_GROUPS = {
    "generation": POWER_GENERATION_FILTER_IDS,
    "market_price": MARKET_PRICE_FILTER_IDS,
    "forecast": FORECAST_FILTER_IDS,
    'consumption' : CONS_FILTER_IDS
}

# %%
