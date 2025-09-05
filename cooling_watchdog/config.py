import json
import pandas as pd

def load_site_data(file_path: str):
    """
    Load Sites.json and normalize site thresholds to US units (°F, mph).

    Returns:
      - sites_df: DataFrame with one row per site and columns:
          site_name, lat, lon, max_temp_f, max_wind_mph, min_relative_humidity_pct, timezone
      - horizon_hours: int (global horizon)
      - default_timezone: str (e.g. "auto" or an IANA TZ like "America/Denver")
      - site_index: dict mapping site_name -> row (same values as in DataFrame row)
    """

    # --- 1) Read JSON file into a Python dict ---
    with open(file_path, "r") as f:
        cfg = json.load(f)

    # --- 2) Pull top-level config values ---
    horizon_hours = int(cfg.get("horizon_hours", 72))
    default_timezone = cfg.get("timezone", "auto")

    # --- 3) Local helper to normalize thresholds to US (°F, mph) ---
    def _to_us_thresholds(thr: dict) -> dict:
        """
        Convert threshold values to US units based on the 'units' key.
        
        Args:
            thr (dict): Dictionary containing:
                - units: "US" or "SI"
                - max_temp: temperature value
                - max_wind: wind speed value
                - min_relative_humidity_pct: humidity percentage
        
        Returns:
            dict: Values converted to US units with keys:
                max_temp_f, max_wind_mph, min_relative_humidity_pct
                
        Raises:
            ValueError: If units is neither "US" nor "SI"
        """
        units = thr.get("units")
        if units not in ["US", "SI"]:
            raise ValueError(
                f"The 'units' field must be either 'US' or 'SI', but got: {units}"
            )
        
        if units == "US":
            # If already in US units, just return the values with correct keys
            return {
                "max_temp_f": float(thr["max_temp"]),
                "max_wind_mph": float(thr["max_wind"]),
                "min_relative_humidity_pct": int(thr["min_relative_humidity_pct"]),
            }
            
        # If in SI units, convert to US
        temp_c = float(thr["max_temp"])
        wind_mps = float(thr["max_wind"])
        
        # Convert from SI to US units
        temp_f = temp_c * 9.0 / 5.0 + 32.0  # °C to °F
        wind_mph = wind_mps * 2.2369362921   # m/s to mph
        
        return {
            "max_temp_f": temp_f,
            "max_wind_mph": wind_mph,
            "min_relative_humidity_pct": int(thr["min_relative_humidity_pct"]),
        }

    # --- 4) Build rows for a DataFrame and an index dict ---
    rows, site_index = [], {}
    for site in cfg.get("sites", []):
        thresholds = site["thresholds"]                      # nested dict (as you expected)
        thr_us = _to_us_thresholds(thresholds)               # normalize to US units

        row = {
            "site_name": site["name"],
            "lat": float(site["lat"]),
            "lon": float(site["lon"]),
            "max_temp_f": float(thr_us["max_temp_f"]),
            "max_wind_mph": float(thr_us["max_wind_mph"]),
            "min_relative_humidity_pct": int(thr_us["min_relative_humidity_pct"]),
            "timezone": site.get("timezone"),               # per-site override (optional)
        }
        rows.append(row)
        site_index[row["site_name"]] = row

    # --- 5) Pack into a DataFrame (one row per site) ---
    sites_df = pd.DataFrame(rows)
    if sites_df.empty:
        raise ValueError("Sites list is empty in Sites.json")

    # Optional: quick visibility
    print("\nProcessed Site Data (US units):")
    print(
        sites_df[
            ["site_name", "lat", "lon", "max_temp_f", "max_wind_mph", "min_relative_humidity_pct", "timezone"]
        ]
    )
    print(f"\nHorizon Hours: {horizon_hours}   Default TZ: {default_timezone}")

    return sites_df, horizon_hours, default_timezone, site_index
