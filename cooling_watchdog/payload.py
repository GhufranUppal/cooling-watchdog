# in your main or analysis script
from typing import Dict
import pandas as pd
from cooling_watchdog.sql_io import ensure_schema, upsert_risk_hourly, insert_risk_windows, upsert_risk_now

def write_hourly_to_db(combined: pd.DataFrame):
    """
    combined columns (from your pipeline):
      site_name, Time, Temperature (°F), Wind Speed (mph), Humidity (%),
      temperature_risk, wind_risk, humidity_risk, any_risk
    """
    if combined is None or combined.empty:
        return
    rows = []
    for _, r in combined.iterrows():
        rows.append((
            r["site_name"],
            pd.Timestamp(r["Time"]).to_pydatetime(),  # tz-aware
            float(r["Temperature (°F)"]) if pd.notna(r["Temperature (°F)"]) else None,
            float(r["Wind Speed (mph)"]) if pd.notna(r["Wind Speed (mph)"]) else None,
            int(r["Humidity (%)"]) if pd.notna(r["Humidity (%)"]) else None,
            bool(r["temperature_risk"]),
            bool(r["wind_risk"]),
            bool(r["humidity_risk"]),
            bool(r["any_risk"]),
        ))
    upsert_risk_hourly(rows)

def write_windows_to_db(summary: pd.DataFrame):
    """
    summary columns:
      site_name, start_time, end_time, duration_h, peak_temp_f, peak_wind_mph, min_rh_pct, triggers, risk_score
    """
    if summary is None or summary.empty:
        return
    rows = []
    for _, r in summary.iterrows():
        rows.append((
            r["site_name"],
            pd.Timestamp(r["start_time"]).to_pydatetime(),
            pd.Timestamp(r["end_time"]).to_pydatetime(),
            int(r["duration_h"]),
            float(r["peak_temp_f"]) if pd.notna(r["peak_temp_f"]) else None,
            float(r["peak_wind_mph"]) if pd.notna(r["peak_wind_mph"]) else None,
            int(r["min_rh_pct"]) if pd.notna(r["min_rh_pct"]) else None,
            str(r.get("triggers", "")),
            int(r.get("risk_score", 0)),
        ))
    insert_risk_windows(rows)

def write_now_to_db(site_payloads: Dict[str, Dict]):
    """
    site_payloads example:
      {"Montgomery-Edge": {"risk_score": 2, "next_window_start_ts": Timestamp(...), "next_window_starts_in_h": 3}, ...}
    """
    for site, payload in site_payloads.items():
        upsert_risk_now(
            site=site,
            risk_score=int(payload.get("risk_score", 0)),
            next_window_start_ts=(
                pd.Timestamp(payload.get("next_window_start_ts")).to_pydatetime()
                if payload.get("next_window_start_ts") is not None else None
            ),
            next_window_starts_in_h=(
                int(payload.get("next_window_starts_in_h"))
                if payload.get("next_window_starts_in_h") is not None else None
            ),
        )

# Example orchestration
def persist_all(combined: pd.DataFrame, summary: pd.DataFrame, site_payloads: Dict[str, Dict]):
    ensure_schema()            # safe no-op if already created
    write_hourly_to_db(combined)
    write_windows_to_db(summary)
    write_now_to_db(site_payloads)
