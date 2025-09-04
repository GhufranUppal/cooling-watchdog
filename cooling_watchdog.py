#!/usr/bin/env python3
# cooling_watch.py
# Proactive weather risk analysis (keeps your structure, fixes URL/horizon handling, site names, and tz slicing)

import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests


# ----------------------------- URL builder (sized by horizon) -----------------------------
def build_open_meteo_url(lat: float, lon: float, tz: str, horizon_hours: int) -> str:
    """
    Build an Open-Meteo URL sized just large enough for the requested horizon.
    1 day covers up to 24h, 2 days up to 48h, etc.
    """
    safe_h = max(1, int(horizon_hours))
    forecast_days = max(1, (safe_h + 23) // 24)  # ceil(h/24) without math.ceil

    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        f"&timezone={tz}"
        f"&forecast_days={forecast_days}"
    )


# ----------------------------- Config loader -----------------------------
def load_site_data(file_path):
    """
    Load and validate Sites.json. Returns:
      sites_df: DataFrame with site rows
      horizon_hours: int
      default_timezone: str  (e.g., "auto" or "America/Denver")
      site_index: dict name->row
    """
    try:
        with open(file_path, "r") as f:
            cfg = json.load(f)

        horizon_hours = int(cfg.get("horizon_hours", 72))
        default_tz = cfg.get("timezone", "auto")

        rows, index = [], {}
        for site in cfg.get("sites", []):
            row = {
                "site_name": site["name"],
                "lat": float(site["lat"]),
                "lon": float(site["lon"]),
                "max_temp_c": float(site["thresholds"]["max_temp_c"]),
                "max_wind_mps": float(site["thresholds"]["max_wind_mps"]),
                "min_relative_humidity_pct": float(site["thresholds"]["min_relative_humidity_pct"]),
                "timezone": site.get("timezone"),  # optional per-site override
            }
            rows.append(row)
            index[row["site_name"]] = row

        sites_df = pd.DataFrame(rows)
        if sites_df.empty:
            raise ValueError("Sites list is empty in Sites.json")

        print("\nProcessed Site Data:")
        print(
            sites_df[
                ["site_name", "lat", "lon", "max_temp_c", "max_wind_mps", "min_relative_humidity_pct"]
            ]
        )
        print(f"\nHorizon Hours: {horizon_hours}   Default TZ: {default_tz}")

        return sites_df, horizon_hours, default_tz, index

    except FileNotFoundError:
        print(f"ERROR: Configuration file not found: {file_path}")
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON format in {file_path}")
    except Exception as e:
        print(f"ERROR processing site data: {e}")
    return None, None, None, None


# ----------------------------- Fetch + process forecast -----------------------------
def get_weather_forecast(
    lat: float,
    lon: float,
    tz_hint: str,
    site_name: str,
    config_path: str,
    horizon_hours: int,
):
    """
    Fetch and prepare hourly forecast for this site.
    Returns:
      df_all: full DataFrame
      df_horizon: next-N-hours slice
      thresholds: dict
      effective_tz_string: str
    """
    sites_df, _, default_tz, site_index = load_site_data(config_path)
    if sites_df is None or site_name not in site_index:
        print(f"ERROR: Could not load config or site not found for {site_name}")
        return None, None, None, None

    srow = site_index[site_name]
    thresholds = {
        "max_temp_c": srow["max_temp_c"],
        "max_wind_mps": srow["max_wind_mps"],
        "min_relative_humidity_pct": srow["min_relative_humidity_pct"],
    }

    # Decide tz for API
    effective_tz = srow.get("timezone") or tz_hint or default_tz or "auto"
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

    df = pd.DataFrame(
        {
            "Time": pd.to_datetime(times),
            "Temperature (°C)": temps,
            "Humidity (%)": rhs,
            "Wind Speed (m/s)": winds,
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


# ----------------------------- Risk columns builder -----------------------------
def attach_risk_flags(forecast_df: pd.DataFrame, site_name: str, thresholds: dict) -> pd.DataFrame:
    """Attach thresholds and risk booleans to the forecast slice for one site."""
    tmax = thresholds["max_temp_c"]
    wmax = thresholds["max_wind_mps"]
    rmin = thresholds["min_relative_humidity_pct"]

    out = forecast_df.assign(
        site_name=site_name,
        temp_threshold=tmax,
        wind_threshold=wmax,
        humidity_threshold=rmin,
    )
    out["temperature_risk"] = out["Temperature (°C)"] >= tmax
    out["wind_risk"] = out["Wind Speed (m/s)"] >= wmax
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


# ----------------------------- Main analysis -----------------------------
def analyze_risk_windows(config_path: str = "Sites.json", save_excel: bool = True) -> pd.DataFrame:
    """
    For each site in Sites.json: fetch, slice to horizon, flag risks, and optionally save Excel.
    Returns a combined per-hour DataFrame for all sites.
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
        tz_hint = row.get("timezone") or default_tz

        print(f"\n--- Processing {site_name} ---")
        df_all, df_slice, thresholds, _tz = get_weather_forecast(
            lat, lon, tz_hint, site_name, config_path, horizon_hours
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
                peak_temp_c=("Temperature (°C)", "max"),
                peak_wind_mps=("Wind Speed (m/s)", "max"),
                min_rh_pct=("Humidity (%)", "min"),
                triggers=("risk_triggers", lambda s: ", ".join(sorted(set(", ".join(s).split(", ")))).strip(", ")),
            )
            .sort_values(["site_name", "start_time"])
            .reset_index(drop=True)
        )

    # Optional: Excel outputs (same idea you had)
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
    if df.empty:
        print("\nNo weather risks detected.")
        return
    cols = [
        "site_name",
        "Time",
        "Temperature (°C)",
        "Wind Speed (m/s)",
        "Humidity (%)",
        "temperature_risk",
        "wind_risk",
        "humidity_risk",
        "any_risk",
    ]
    present = [c for c in cols if c in df.columns]
    print("\n=== Detailed Risk Rows (first 30) ===")
    print(df[present].head(30))


def main():
    print("\n=== Cooling Watch – Starting Analysis ===")
    DEFAULT_CONFIG_PATH = os.path.join(os.getcwd(), "Sites.json")
    combined = analyze_risk_windows(config_path=DEFAULT_CONFIG_PATH, save_excel=True)
    print_risk_preview(combined)
    return 0


if __name__ == "__main__":
    sys.exit(main())
