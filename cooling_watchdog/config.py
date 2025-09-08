import json
import sys
import pandas as pd
from typing import Tuple, Dict, Optional

# Error codes and custom exception
class ConfigError(Exception):
    """Custom exception class for configuration errors"""
    SUCCESS = 0
    FILE_NOT_FOUND = 1
    INVALID_JSON = 2
    INVALID_UNITS = 3
    EMPTY_SITES = 4
    GENERAL_ERROR = 5
    
    def __init__(self, message: str, code: int):
        super().__init__(message)
        self.code = code

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

def load_site_data(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[int], Optional[str], Optional[Dict], int]:
    """
    Load Sites.json and normalize site thresholds to US units (°F, mph).

    Args:
        file_path (str): Path to the configuration file

    Returns:
        Tuple containing:
        - sites_df: DataFrame with one row per site and columns:
            site_name, lat, lon, max_temp_f, max_wind_mph, min_relative_humidity_pct, timezone
        - horizon_hours: int (global horizon)
        - default_timezone: str (e.g. "auto" or an IANA TZ like "America/Denver")
        - site_index: dict mapping site_name -> row
        - error_code: int indicating success (0) or specific error conditions
            - 0: Success
            - 1: File not found
            - 2: Invalid JSON
            - 3: Invalid units specification
            - 4: Empty sites list
            - 5: General error
    """

    # --- 1) Read JSON file into a Python dict ---
    with open(file_path, "r") as f:
        cfg = json.load(f)

    # --- 2) Pull top-level config values ---
    horizon_hours = int(cfg.get("horizon_hours", 72))
    default_timezone = cfg.get("timezone", "auto")

    # --- 3) Build rows for a DataFrame and an index dict ---
    rows, site_index = [], {}
    for site in cfg.get("sites", []):
        site_name = site["name"]
        thresholds = site["thresholds"]
        
        try:
            thr_us = _to_us_thresholds(thresholds)
            
            row = {
                "site_name": site_name,
                "lat": float(site["lat"]),
                "lon": float(site["lon"]),
                "max_temp_f": float(thr_us["max_temp_f"]),
                "max_wind_mph": float(thr_us["max_wind_mph"]),
                "min_relative_humidity_pct": int(thr_us["min_relative_humidity_pct"]),
                "timezone": site.get("timezone"),
            }
            rows.append(row)
            site_index[row["site_name"]] = row
            
        except ValueError as e:
            print(f"\nERROR: Invalid units specification for site '{site_name}':")
            print(f"  {str(e)}")
            return None, None, None, None, ConfigError.INVALID_UNITS

    # --- 5) Pack into a DataFrame (one row per site) ---
    try:
        sites_df = pd.DataFrame(rows)
        if sites_df.empty:
            print("\nERROR: No sites found in configuration file")
            return None, None, None, None, ConfigError.EMPTY_SITES

        # Optional: quick visibility
        print("\nProcessed Site Data (US units):")
        print(
            sites_df[
                ["site_name", "lat", "lon", "max_temp_f", "max_wind_mph", "min_relative_humidity_pct", "timezone"]
            ]
        )
        print(f"\nHorizon Hours: {horizon_hours}   Default TZ: {default_timezone}")

        return sites_df, horizon_hours, default_timezone, site_index, ConfigError.SUCCESS

    except Exception as e:
        print(f"\nERROR: Failed to process site data: {str(e)}")
        return None, None, None, None, ConfigError.GENERAL_ERROR
