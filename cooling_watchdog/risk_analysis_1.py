import os
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd

# If you keep these helpers elsewhere, import them instead.
def window_triggers_label_from_str(triggers_str: str) -> str:
    """Normalize triggers to a tidy, de-duplicated, sorted comma list."""
    if not triggers_str:
        return ""
    uniq = {t.strip().title() for t in str(triggers_str).split(",") if t.strip()}
    allowed = {"Temperature", "Wind", "Humidity"}  # guard against typos/unexpected tokens
    uniq = uniq & allowed
    return ", ".join(sorted(uniq))

def risk_score_simple(triggers_str: str) -> int:
    """Score by number of distinct trigger types present: 0..3."""
    if not triggers_str:
        return 0
    uniq = {t.strip().title() for t in str(triggers_str).split(",") if t.strip()}
    allowed = {"Temperature", "Wind", "Humidity"}
    return min(3, len(uniq & allowed))

def build_site_payload(site: str, summary_for_site: pd.DataFrame) -> Dict:
    """
    Build the 'risk_now' payload (one row per site):
      - risk_score (0..3)
      - next_window_start_ts (tz-aware)
      - next_window_starts_in_h (int hours; 0 if already started/now)
    """
    if summary_for_site is None or summary_for_site.empty:
        return {"site": site, "risk_score": 0, "next_window_start_ts": None, "next_window_starts_in_h": None}

    nxt = summary_for_site.sort_values("start_time").iloc[0]
    score = int(nxt.get("risk_score", 0))

    tzinfo = nxt["start_time"].tz
    now_aware = pd.Timestamp.now(tz=tzinfo)
    starts_in_h = max(0, int((nxt["start_time"] - now_aware).total_seconds() // 3600))

    return {
        "site": site,
        "risk_score": score,
        "next_window_start_ts": nxt["start_time"],
        "next_window_starts_in_h": starts_in_h,
    }


def analyze_risk_windows(
    config_path: str,
    save_excel: bool = True,
    persist_to_db: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Dict], int]:
    """
    End-to-end analysis:
      1) Load sites + horizons from JSON
      2) Pull forecast per site + slice to horizon
      3) Flag hourly risks (Temperature/Wind/Humidity + any_risk)
      4) Group contiguous risky hours into windows, build clean triggers, compute risk_score (0..3)
      5) Build 'risk_now' payloads per site
      6) (Optional) Save Excel report
      7) (Optional) Persist to PostgreSQL

    Returns:
      combined_hourly_df, summary_windows_df, site_payloads_dict, error_code
    """

    # Imports from your package/local files (adjust as needed)
    try:
        from cooling_watchdog.config import load_site_data, ConfigError
        from cooling_watchdog.weather import get_weather_forecast
    except ImportError:
        from config import load_site_data, ConfigError
        from weather import get_weather_forecast

    print("\nReading configuration...")
    sites_df, horizon_hours, default_tz, _index, error_code = load_site_data(config_path)

    if error_code != ConfigError.SUCCESS:
        return pd.DataFrame(), pd.DataFrame(), {}, error_code
    if sites_df is None or sites_df.empty:
        print("No sites found; aborting.")
        return pd.DataFrame(), pd.DataFrame(), {}, ConfigError.EMPTY_SITES

    all_rows = []

    for _, row in sites_df.iterrows():
        site_name = row["site_name"]
        lat, lon = row["lat"], row["lon"]
        print(f"\n--- Processing {site_name} ---")

        # Your weather function should already respect horizon from JSON internally
        # and return (df_all, df_slice, thresholds, local_tz_str) for the site.
        df_all, df_slice, thresholds, _tz = get_weather_forecast(
            lat, lon, site_name, config_path
        )
        if df_all is None or df_slice is None or df_slice.empty:
            print(f"[{site_name}] No forecast slice available; skipping.")
            continue

        # Attach booleans and risk triggers at the HOURLY level
        flagged = attach_risk_flags(df_slice, site_name, thresholds)
        # Keep tz awareness handy for debug/exports
        flagged["Time Zone"] = flagged["Time"].dt.tz
        all_rows.append(flagged)

    if not all_rows:
        print("No data produced for any site.")
        return pd.DataFrame(), pd.DataFrame(), {}, ConfigError.EMPTY_SITES

    combined = pd.concat(all_rows, ignore_index=True)
    print(f"\nCombined hourly shape: {combined.shape}")

    # ---- Windowing: contiguous risky hours per site ----
    risk_only = combined[combined["any_risk"]].copy()
    if risk_only.empty:
        print("No risk windows found.")
        summary = pd.DataFrame(columns=[
            "site_name","start_time","end_time","duration_h",
            "peak_temp_f","peak_wind_mph","min_rh_pct","triggers","risk_score"
        ])
    else:
        # New group starts each time the risk state toggles within site
        risk_only.sort_values(["site_name", "Time"], inplace=True)
        # Ensure groups reset per site
        risk_only["site_block"] = (risk_only["site_name"] != risk_only["site_name"].shift()).cumsum()
        risk_only["risk_group_toggle"] = (risk_only["any_risk"] != risk_only["any_risk"].shift()).cumsum()
        # window_group within a site: combine both so groups don't bleed across sites
        risk_only["window_group"] = (risk_only["site_block"].astype(str) + "_" +
                                     risk_only["risk_group_toggle"].astype(str))

        # Aggregate to windows
        summary = (
            risk_only
            .groupby(["site_name", "window_group"], as_index=False)
            .agg(
                start_time=("Time", "min"),
                end_time=("Time", "max"),
                duration_h=("Time", lambda s: int((s.max() - s.min()).total_seconds() // 3600 + 1)),
                peak_temp_f=("Temperature (°F)", "max"),
                peak_wind_mph=("Wind Speed (mph)", "max"),
                min_rh_pct=("Humidity (%)", "min"),
                triggers_raw=("risk_triggers", lambda s: ", ".join(s.dropna().astype(str))),
            )
            .sort_values(["site_name", "start_time"])
            .reset_index(drop=True)
        )

        # Normalize triggers and compute risk_score
        summary["triggers"] = summary["triggers_raw"].apply(window_triggers_label_from_str)
        summary["risk_score"] = summary["triggers"].apply(risk_score_simple)
        # Keep tz visible if you want in Excel: tz from start_time
        summary["Timezone"] = summary["start_time"].dt.tz
        summary.drop(columns=["triggers_raw"], inplace=True)

    # ---- Build 'risk_now' payloads (earliest upcoming window per site) ----
    site_payloads: Dict[str, Dict] = {}
    if not summary.empty:
        for site, sdf in summary.groupby("site_name"):
            site_payloads[site] = build_site_payload(site, sdf)

    # ---- Optional Excel report ----
    if save_excel:
        reports_dir = os.path.join(os.getcwd(), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        xlsx_path = os.path.join(
            reports_dir, f"Cooling_Watchdog_Risk_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        )

        try:
            detailed = combined.copy()
            # Friendly date/time columns for the detailed sheet
            detailed["Date"] = pd.to_datetime(detailed["Time"], errors="coerce").dt.strftime("%Y-%m-%d")
            detailed["Time of Day"] = pd.to_datetime(detailed["Time"], errors="coerce").dt.strftime("%I:%M %p")

            detailed_cols = {
                "Date": "Date",
                "Time of Day": "Time",
                "Time Zone": "Time Zone",
                "site_name": "Site",
                "Temperature (°F)": "Temperature (°F)",
                "Humidity (%)": "Humidity (%)",
                "Wind Speed (mph)": "Wind Speed (mph)",
                "temperature_risk": "Temperature Risk",
                "wind_risk": "Wind Risk",
                "humidity_risk": "Humidity Risk",
                "any_risk": "Any Risk Condition",
                "risk_triggers": "Risk Triggers",
            }
            detailed_excel = detailed[list(detailed_cols.keys())].rename(columns=detailed_cols)

            with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                detailed_excel.to_excel(writer, index=False, sheet_name="Detailed Risks")

                if not summary.empty:
                    summary_excel = summary.copy()
                    summary_excel["Start Date"] = summary_excel["start_time"].dt.strftime("%Y-%m-%d")
                    summary_excel["Start Time"] = summary_excel["start_time"].dt.strftime("%I:%M %p")
                    summary_excel["End Date"] = summary_excel["end_time"].dt.strftime("%Y-%m-%d")
                    summary_excel["End Time"] = summary_excel["end_time"].dt.strftime("%I:%M %p")

                    summary_cols = {
                        "site_name": "Site",
                        "Start Date": "Start Date",
                        "Start Time": "Start Time",
                        "End Date": "End Date",
                        "End Time": "End Time",
                        "Timezone": "Timezone",
                        "duration_h": "Duration (hours)",
                        "peak_temp_f": "Peak Temperature (°F)",
                        "peak_wind_mph": "Peak Wind Speed (mph)",
                        "min_rh_pct": "Minimum Humidity (%)",
                        "triggers": "Risk Triggers",
                        "risk_score": "Risk Score",
                    }
                    summary_excel = summary_excel[list(summary_cols.keys())].rename(columns=summary_cols)
                    summary_excel.to_excel(writer, index=False, sheet_name="Risk Summary")

                # (Optional) Auto-size columns
                for ws in writer.sheets.values():
                    for col_cells in ws.columns:
                        max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
                        col_letter = col_cells[0].column_letter
                        ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

            print(f"\nAnalysis complete. Excel saved:\n{xlsx_path}")
        except Exception as e:
            print(f"\nError saving Excel file: {e}")

    # ---- Optional DB persistence (uncomment to enable) ----
    if persist_to_db:
        try:
            from cooling_watchdog.sql_io import ensure_schema, upsert_risk_hourly, insert_risk_windows, upsert_risk_now_from_summary
        except Exception:
            # Backward compatible: if you don’t have the _from_summary helper, we’ll compose rows ourselves.
            from cooling_watchdog.sql_io import ensure_schema, upsert_risk_hourly, insert_risk_windows, upsert_risk_now

            def upsert_risk_now_from_summary(summary_df: pd.DataFrame):
                if summary_df is None or summary_df.empty:
                    return
                for site, sdf in summary_df.groupby("site_name"):
                    payload = build_site_payload(site, sdf)
                    upsert_risk_now(
                        site=site,
                        risk_score=int(payload.get("risk_score", 0)),
                        next_window_start_ts=payload.get("next_window_start_ts"),
                        next_window_starts_in_h=payload.get("next_window_starts_in_h"),
                    )

        try:
            ensure_schema()

            # Hourly
            rows_h = []
            for _, r in combined.iterrows():
                rows_h.append((
                    r["site_name"],
                    pd.Timestamp(r["Time"]).to_pydatetime(),
                    float(r["Temperature (°F)"]) if pd.notna(r["Temperature (°F)"]) else None,
                    float(r["Wind Speed (mph)"]) if pd.notna(r["Wind Speed (mph)"]) else None,
                    int(r["Humidity (%)"]) if pd.notna(r["Humidity (%)"]) else None,
                    bool(r["temperature_risk"]),
                    bool(r["wind_risk"]),
                    bool(r["humidity_risk"]),
                    bool(r["any_risk"]),
                ))
            upsert_risk_hourly(rows_h)

            # Windows
            if not summary.empty:
                rows_w = []
                for _, r in summary.iterrows():
                    rows_w.append((
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
                insert_risk_windows(rows_w)
                # risk_now (one per site)
                upsert_risk_now_from_summary(summary)

            print("✅ Persisted hourly/windows/now to PostgreSQL.")
        except Exception as e:
            print(f"⚠️ DB persistence failed: {e}")

    # Done
    return combined, summary, site_payloads, ConfigError.SUCCESS
