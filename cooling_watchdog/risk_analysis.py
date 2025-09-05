"""Risk analysis module for processing weather data and identifying risk windows."""

import os
from datetime import datetime
import pandas as pd
from typing import Dict

from .config import load_site_data
from .weather import get_weather_forecast

def attach_risk_flags(forecast_df: pd.DataFrame, site_name: str, thresholds: dict) -> pd.DataFrame:
    """
    Attach thresholds and risk booleans to the forecast slice for one site.

    Args:
        forecast_df (pd.DataFrame): DataFrame containing forecast data
        site_name (str): Name of the site
        thresholds (dict): Dictionary containing threshold values

    Returns:
        pd.DataFrame: DataFrame with added risk flags
    """
    tmax = thresholds["max_temp_f"]
    wmax = thresholds["max_wind_mph"]
    rmin = thresholds["min_relative_humidity_pct"]

    out = forecast_df.assign(
        site_name=site_name,
        temp_threshold=tmax,
        wind_threshold=wmax,
        humidity_threshold=rmin,
    )
    out["temperature_risk"] = out["Temperature (°F)"] >= tmax
    out["wind_risk"] = out["Wind Speed (mph)"] >= wmax
    out["humidity_risk"] = out["Humidity (%)"] <= rmin
    out["any_risk"] = out["temperature_risk"] | out["wind_risk"] | out["humidity_risk"]

    # for grouping windows later
    out["risk_group"] = (out["any_risk"] != out["any_risk"].shift()).cumsum()
    out["risk_triggers"] = out.apply(
        lambda r: ", ".join(
            [k for k, v in {"Temperature": r["temperature_risk"], "Wind": r["wind_risk"], "Humidity": r["humidity_risk"]}.items() if v]
        ),
        axis=1,
    )
    return out

def analyze_risk_windows(config_path: str, save_excel: bool = True) -> pd.DataFrame:
    """
    For each site in the configuration: fetch, slice to horizon, flag risks, and optionally save Excel.

    Args:
        config_path (str): Path to the configuration file
        save_excel (bool): Whether to save results to Excel

    Returns:
        pd.DataFrame: Combined per-hour DataFrame for all sites
    """
    print("\nReading configuration...")
    sites_df, horizon_hours, default_tz, _index = load_site_data(config_path)
    if sites_df is None or sites_df.empty:
        print("No sites found; aborting.")
        return pd.DataFrame()

    all_rows = []
    for _, row in sites_df.iterrows():
        site_name = row["site_name"]
        lat, lon = row["lat"], row["lon"]

        print(f"\n--- Processing {site_name} ---")
        df_all, df_slice, thresholds, _tz = get_weather_forecast(
            lat, lon, site_name, config_path
        )
        if df_all is None or df_slice is None or df_slice.empty:
            print(f"[{site_name}] No forecast slice available; skipping.")
            continue

        flagged = attach_risk_flags(df_slice, site_name, thresholds)
        all_rows.append(flagged)

    if not all_rows:
        print("No data produced for any site.")
        return pd.DataFrame()

    combined = pd.concat(all_rows, ignore_index=True)

    # Summarize contiguous risk windows per site
    risk_only = combined[combined["any_risk"]].copy()
    if risk_only.empty:
        summary = pd.DataFrame()
        print("\nNo risk windows found.")
    else:
        grp = (risk_only["risk_group"] != risk_only["risk_group"].shift()).cumsum()
        risk_only["window_group"] = grp
        summary = (
            risk_only.groupby(["site_name", "window_group"], as_index=False)
            .agg(
                start_time=("Time", "min"),
                end_time=("Time", "max"),
                duration_h=("Time", lambda s: int((s.max() - s.min()).total_seconds() // 3600 + 1)),
                peak_temp_f=("Temperature (°F)", "max"),
                peak_wind_mph=("Wind Speed (mph)", "max"),
                min_rh_pct=("Humidity (%)", "min"),
                triggers=("risk_triggers", lambda s: ", ".join(sorted(set(", ".join(s).split(", ")))).strip(", ")),
            )
            .sort_values(["site_name", "start_time"])
            .reset_index(drop=True)
        )

    # Optional: Excel outputs
    if save_excel:
        os.makedirs("reports", exist_ok=True)
        xlsx_path = os.path.join("reports", f"Cooling_Watchdog_Risk_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            combined.to_excel(writer, index=False, sheet_name="Detailed Risks")
            if not summary.empty:
                summary.to_excel(writer, index=False, sheet_name="Risk Summary")
        print(f"\nAnalysis complete. Excel saved:\n{xlsx_path}")

    return combined

def print_risk_preview(df: pd.DataFrame):
    """
    Print a preview of risk data.

    Args:
        df (pd.DataFrame): DataFrame containing risk data
    """
    if df.empty:
        print("\nNo weather risks detected.")
        return
    cols = [
        "site_name",
        "Time",
        "Temperature (°F)",
        "Wind Speed (mph)",
        "Humidity (%)",
        "temperature_risk",
        "wind_risk",
        "humidity_risk",
        "any_risk",
    ]
    present = [c for c in cols if c in df.columns]
    print("\n=== Detailed Risk Rows (first 30) ===")
    print(df[present].head(30))