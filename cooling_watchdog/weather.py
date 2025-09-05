"""Weather forecast module for fetching and processing weather data."""

import pandas as pd
import requests
from zoneinfo import ZoneInfo
from typing import Tuple, Dict, Optional

from .url_builder import build_open_meteo_url
from .config import load_site_data

def get_weather_forecast(
    lat: float,
    lon: float,
    site_name: str,
    config_path: str,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[Dict], Optional[str]]:
    """
    Fetch and prepare hourly forecast for this site.

    Args:
        lat (float): Latitude of the location
        lon (float): Longitude of the location
        site_name (str): Name of the site
        config_path (str): Path to the configuration file

    Returns:
        Tuple containing:
            df_all: full DataFrame
            df_horizon: next-N-hours slice
            thresholds: dict
            effective_tz_string: str
    """
    sites_df, horizon_hours, default_tz, site_index = load_site_data(config_path)
    if sites_df is None or site_name not in site_index:
        print(f"ERROR: Could not load config or site not found for {site_name}")
        return None, None, None, None

    srow = site_index[site_name]
    thresholds = {
        "max_temp_f": srow["max_temp_f"],
        "max_wind_mph": srow["max_wind_mph"],
        "min_relative_humidity_pct": srow["min_relative_humidity_pct"],
    }

    # Decide tz for API
    effective_tz = srow.get("timezone") or default_tz or "auto"
    tz_for_api = effective_tz if effective_tz != "auto" else "auto"

    url = build_open_meteo_url(lat, lon, tz_for_api, horizon_hours)
    print(f"\n[{site_name}] Open-Meteo URL:\n{url}")

    # Fetch
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    try:
        times = data["hourly"]["time"]
        temps = data["hourly"]["temperature_2m"]
        rhs = data["hourly"]["relative_humidity_2m"]
        winds = data["hourly"]["wind_speed_10m"]
    except KeyError as e:
        print(f"ERROR: Missing key in API response for {site_name}: {e}")
        return None, None, None, None

    # Convert API response from SI to US units
    temps_f = [t * 9.0/5.0 + 32.0 for t in temps]  # Convert °C to °F
    winds_mph = [w * 2.2369362921 for w in winds]  # Convert m/s to mph
    
    df = pd.DataFrame(
        {
            "Time": pd.to_datetime(times),
            "Temperature (°F)": temps_f,
            "Humidity (%)": rhs,
            "Wind Speed (mph)": winds_mph,
        }
    )

    # Timezone handling & horizon slice
    if df["Time"].dt.tz is not None:
        # Already tz-aware (API localized)
        local_tz = df["Time"].dt.tz
        now_local = pd.Timestamp.now(tz=local_tz)
    else:
        # Not tz-aware → try to localize if we have a concrete tz name
        if effective_tz != "auto":
            local_tz = ZoneInfo(effective_tz)
        else:
            local_tz = ZoneInfo("UTC")  # safe fallback
        df["Time"] = df["Time"].dt.tz_localize(local_tz)
        now_local = pd.Timestamp.now(tz=local_tz)

    df_horizon = df[df["Time"] > now_local].iloc[:horizon_hours]

    print(f"\n[{site_name}] Current conditions (first rows):")
    print(df.head())
    print(f"\n[{site_name}] Next {horizon_hours} hours forecast:")
    print(df_horizon)

    return df, df_horizon, thresholds, str(local_tz)