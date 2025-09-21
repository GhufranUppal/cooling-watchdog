# cooling_watchdog_consolidated.py
"""
Cooling Watchdog (consolidated)
- Loads sites from JSON (load_site_data)
- Gets weather forecast per site (get_weather_forecast)
- Flags hourly risks
- Groups contiguous risky hours into windows, normalizes triggers, computes risk_score (0..3)
- Writes to PostgreSQL:
    * risk_windows
    * risk_now (earliest upcoming window per site)
    * risk_hourly STRICTLY from summary windows (synthesized hourly stamps, no fallback)
- Optional Excel export
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, Iterable, Tuple, List

import pandas as pd
import psycopg2
import psycopg2.extras as extras


# ============================================================================
# 0) PostgreSQL DSN (hard-coded for lab/dev; prefer env vars in prod)
# ============================================================================
DSN = (
    "dbname=ignitiondb "
    "user=ignition_user "
    "password=1234567 "
    "host=localhost "
    "port=5432"
)


# ============================================================================
# 1) Config + Weather imports (package-first, local fallback)
# ============================================================================
try:
    from cooling_watchdog.config import load_site_data, ConfigError  # type: ignore
    from cooling_watchdog.weather import get_weather_forecast        # type: ignore
except ImportError:
    from config import load_site_data, ConfigError  # type: ignore
    from weather import get_weather_forecast        # type: ignore


# ============================================================================
# 2) DB LAYER (schema + writers)
# ============================================================================

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS public.risk_now (
  site TEXT PRIMARY KEY,
  risk_score INT NOT NULL,
  next_window_start_ts TIMESTAMPTZ,
  next_window_starts_in_h INT,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.risk_windows (
  id BIGSERIAL PRIMARY KEY,
  site TEXT NOT NULL,
  start_ts TIMESTAMPTZ NOT NULL,
  end_ts TIMESTAMPTZ NOT NULL,
  duration_h INT NOT NULL,
  peak_temp NUMERIC,
  peak_wind NUMERIC,
  min_rh_pct INT,
  triggers TEXT,
  risk_score INT NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.risk_hourly (
  site TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  temp NUMERIC,
  wind NUMERIC,
  rh_pct INT,
  temperature_risk BOOLEAN,
  wind_risk BOOLEAN,
  humidity_risk BOOLEAN,
  any_risk BOOLEAN,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (site, ts)
);

CREATE INDEX IF NOT EXISTS idx_windows_site_start ON public.risk_windows(site, start_ts DESC);
CREATE INDEX IF NOT EXISTS idx_hourly_site_ts    ON public.risk_hourly(site, ts DESC);
"""

def get_conn():
    """Open a psycopg2 connection using the global DSN."""
    return psycopg2.connect(DSN)

def ensure_schema():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET search_path TO public;")
        cur.execute(SCHEMA_DDL)
        conn.commit()

def upsert_risk_now(
    site: str,
    risk_score: int,
    next_window_start_ts,  # tz-aware datetime or None
    next_window_starts_in_h: Optional[int],
):
    sql = """
    INSERT INTO public.risk_now (site, risk_score, next_window_start_ts, next_window_starts_in_h)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (site) DO UPDATE
      SET risk_score = EXCLUDED.risk_score,
          next_window_start_ts = EXCLUDED.next_window_start_ts,
          next_window_starts_in_h = EXCLUDED.next_window_starts_in_h,
          generated_at = now();
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET search_path TO public;")
        cur.execute(sql, (site, int(risk_score), next_window_start_ts, next_window_starts_in_h))
        conn.commit()

def insert_risk_windows(rows: Iterable[Tuple]):
    """
    Rows: (site, start_ts, end_ts, duration_h, peak_temp, peak_wind, min_rh_pct, triggers, risk_score)
    Each timestamp must be timezone-aware. NaT/None timestamps will be filtered out.
    """
    rows = list(rows)
    if not rows:
        return
    sql = """
    INSERT INTO public.risk_windows
    (site, start_ts, end_ts, duration_h, peak_temp, peak_wind, min_rh_pct, triggers, risk_score)
    VALUES %s
    """
    # Filter out rows with NaT/None timestamps
    rows = [r for r in rows if r[1] is not None and r[2] is not None]
    if not rows:
        return

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET search_path TO public;")
        extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()

def upsert_risk_hourly(rows: Iterable[Tuple]):
    """
    Rows: (site, ts, temp, wind, rh_pct, temperature_risk, wind_risk, humidity_risk, any_risk)
    """
    rows = list(rows)
    if not rows:
        return
    sql = """
    INSERT INTO public.risk_hourly
    (site, ts, temp, wind, rh_pct, temperature_risk, wind_risk, humidity_risk, any_risk)
    VALUES %s
    ON CONFLICT (site, ts) DO UPDATE SET
      temp = EXCLUDED.temp,
      wind = EXCLUDED.wind,
      rh_pct = EXCLUDED.rh_pct,
      temperature_risk = EXCLUDED.temperature_risk,
      wind_risk = EXCLUDED.wind_risk,
      humidity_risk = EXCLUDED.humidity_risk,
      any_risk = EXCLUDED.any_risk,
      generated_at = now();
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET search_path TO public;")
        extras.execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


# ============================================================================
# 3) RISK HELPERS
# ============================================================================

def _ts_or_none(v) -> Optional[datetime]:
    """Convert pandas/np NaT/NaN/None → None; else return Python datetime (tz preserved)."""
    if v is None:
        return None
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()

def window_triggers_label_from_str(triggers_str: str) -> str:
    """Normalize triggers: split, strip, dedupe, title case, sort."""
    if not triggers_str:
        return ""
    uniq = {t.strip().title() for t in str(triggers_str).split(",") if t.strip()}
    allowed = {"Temperature", "Wind", "Humidity"}
    return ", ".join(sorted(uniq & allowed))

def risk_score_simple(triggers_str: str) -> int:
    """Score = number of distinct triggers (Temperature/Wind/Humidity) → 0..3."""
    if not triggers_str:
        return 0
    uniq = {t.strip().title() for t in str(triggers_str).split(",") if t.strip()}
    allowed = {"Temperature", "Wind", "Humidity"}
    return min(3, len(uniq & allowed))

def attach_risk_flags(forecast_df: pd.DataFrame, site_name: str, thresholds: dict) -> pd.DataFrame:
    """
    Add risk flags to hourly forecast.
    Requires columns: Time, Temperature (°F), Humidity (%), Wind Speed (mph)
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
    out["wind_risk"]        = out["Wind Speed (mph)"] >= wmax
    out["humidity_risk"]    = out["Humidity (%)"] <= rmin
    out["any_risk"]         = out["temperature_risk"] | out["wind_risk"] | out["humidity_risk"]

    # Toggle-based grouping id (for later windowing)
    out["risk_group"] = (out["any_risk"] != out["any_risk"].shift()).cumsum()

    # Hour-level label like "Temperature, Wind"
    out["risk_triggers"] = out.apply(
        lambda r: ", ".join(
            [k for k, v in {
                "Temperature": r["temperature_risk"],
                "Wind":        r["wind_risk"],
                "Humidity":    r["humidity_risk"],
            }.items() if v]
        ),
        axis=1,
    )
    return out


# ============================================================================
# 4) STRICT hourly upsert from summary (no fallback)
# ============================================================================

def _flags_from_triggers(triggers: str) -> Tuple[bool, bool, bool]:
    """
    Infer hourly boolean flags from a window's normalized triggers string.
    Expected tokens: 'Temperature', 'Wind', 'Humidity' (case-insensitive ok).
    """
    t = (triggers or "").lower()
    tr = "temperature" in t
    wr = "wind" in t
    hr = "humidity" in t
    return tr, wr, hr

def upsert_risk_hourly_from_summary_strict(summary: pd.DataFrame) -> None:
    """
    STRICT writer: only uses the window 'summary' DataFrame to upsert risk_hourly.

    Expects per-row:
      - site_name (str)
      - start_time (tz-aware preferred)
      - end_time
      - triggers (comma list like 'Temperature, Wind', already normalized)

    Writes one row per hour in each window with:
      - temp/wind/rh = NULL
      - temperature_risk / wind_risk / humidity_risk from 'triggers'
      - any_risk = OR of the three flags
    """
    if summary is None or summary.empty:
        return

    s = summary.copy()
    s["start_time"] = pd.to_datetime(s["start_time"], errors="coerce")
    s["end_time"]   = pd.to_datetime(s["end_time"], errors="coerce")
    s = s.dropna(subset=["start_time", "end_time"])
    if s.empty:
        return

    rows: List[Tuple] = []
    for _, w in s.iterrows():
        site = w["site_name"]
        start = w["start_time"]
        end   = w["end_time"]
        trig  = str(w.get("triggers", ""))

        tr, wr, hr = _flags_from_triggers(trig)
        anyr = tr or wr or hr

        # Find weather data for this site
        site_data = combined[combined['site_name'] == site].copy()
        
        # Inclusive hourly stamps
        hours = pd.date_range(start=start, end=end, freq="H")
        if len(hours) == 0:
            continue

        for ts in hours:
            ts_py = _ts_or_none(ts)
            if ts_py is None:
                continue
                
            # Get weather data for this timestamp
            ts_data = site_data[site_data['Time'] == ts]
            if len(ts_data) > 0:
                temp = float(ts_data['Temperature (°F)'].iloc[0])
                wind = float(ts_data['Wind Speed (mph)'].iloc[0])
                rh = float(ts_data['Humidity (%)'].iloc[0])
            else:
                # If no data found for this timestamp, skip it
                continue
                
            rows.append((
                site,        # site
                ts_py,       # ts (TIMESTAMPTZ)
                temp,        # temp
                wind,        # wind
                rh,         # rh_pct
                bool(tr),    # temperature_risk
                bool(wr),    # wind_risk
                bool(hr),    # humidity_risk
                bool(anyr),  # any_risk
            ))

    if not rows:
        return

    # Deduplicate by PK (site, ts)
    rows.sort(key=lambda t: (t[0], t[1]))
    deduped: List[Tuple] = []
    last_key = None
    for row in rows:
        key = (row[0], row[1])
        if key != last_key:
            deduped.append(row)
            last_key = key

    ensure_schema()
    upsert_risk_hourly(deduped)


# ============================================================================
# 5) MAIN ANALYSIS
# ============================================================================

def analyze_risk_windows(config_path: str, save_excel: bool = True) -> tuple[pd.DataFrame, int]:
    """
    For each site in the configuration: fetch, slice to horizon, flag risks, and optionally save Excel.

    Args:
        config_path (str): Path to the configuration file
        save_excel (bool): Whether to save results to Excel

    Returns:
        tuple containing:
            - pd.DataFrame: Combined per-hour DataFrame for all sites
            - int: Error code (0 for success, non-zero for errors)
    """
    print("\nReading configuration...")
    sites_df, horizon_hours, default_tz, _index, error_code = load_site_data(config_path)

    if error_code != ConfigError.SUCCESS:
        return pd.DataFrame(), pd.DataFrame(), error_code
    if sites_df is None or sites_df.empty:
        print("No sites found; aborting.")
        return pd.DataFrame(), pd.DataFrame(), ConfigError.EMPTY_SITES

    all_rows = []
    for _, row in sites_df.iterrows():
        site_name = row["site_name"]
        lat, lon = row["lat"], row["lon"]
        print(f"\n--- Processing {site_name} ---")

        df_all, df_slice, thresholds, _tz = get_weather_forecast(lat, lon, site_name, config_path)
        if df_all is None or df_slice is None or df_slice.empty:
            print(f"[{site_name}] No forecast slice available; skipping.")
            continue

        flagged = attach_risk_flags(df_slice, site_name, thresholds)
        flagged["Time Zone"] = flagged["Time"].dt.tz
        all_rows.append(flagged)

    if not all_rows:
        print("No data produced for any site.")
        return pd.DataFrame(), pd.DataFrame(), ConfigError.EMPTY_SITES

    combined = pd.concat(all_rows, ignore_index=True)

    # ---- Build risk windows ----
    risk_only = combined[combined["any_risk"]].copy()
    if risk_only.empty:
        summary = pd.DataFrame(
            columns=[
                "site_name", "start_time", "end_time", "duration_h",
                "peak_temp_f", "peak_wind_mph", "min_rh_pct", "triggers", "risk_score"
            ]
        )
        print("No risk windows found.")
    else:
        risk_only.sort_values(["site_name", "Time"], inplace=True)
        risk_only["site_block"] = (risk_only["site_name"] != risk_only["site_name"].shift()).cumsum()
        risk_only["risk_group_toggle"] = (risk_only["any_risk"] != risk_only["any_risk"].shift()).cumsum()
        risk_only["window_group"] = risk_only["site_block"].astype(str) + "_" + risk_only["risk_group_toggle"].astype(str)

        summary = (
            risk_only.groupby(["site_name", "window_group"], as_index=False)
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

        # Normalize times & triggers and compute score
        summary["start_time"] = pd.to_datetime(summary["start_time"], errors="coerce")
        summary["end_time"]   = pd.to_datetime(summary["end_time"], errors="coerce")
        summary["triggers"]   = summary["triggers_raw"].apply(window_triggers_label_from_str)
        summary["risk_score"] = summary["triggers"].apply(risk_score_simple)
        summary.drop(columns=["triggers_raw"], inplace=True)

        # Drop invalid windows (safety)
        summary = summary.dropna(subset=["start_time", "end_time"]).copy()

    # ---- DB persistence ----
    ensure_schema()

    # Windows
    rows_w = []
    for _, r in summary.iterrows():
        start_dt = _ts_or_none(r["start_time"])
        end_dt   = _ts_or_none(r["end_time"])
        if start_dt is None or end_dt is None:
            continue
        rows_w.append((
            r["site_name"],
            start_dt,
            end_dt,
            int(r["duration_h"]),
            float(r["peak_temp_f"]) if pd.notna(r["peak_temp_f"]) else None,
            float(r["peak_wind_mph"]) if pd.notna(r["peak_wind_mph"]) else None,
            int(r["min_rh_pct"]) if pd.notna(r["min_rh_pct"]) else None,
            str(r.get("triggers", "")),
            int(r.get("risk_score", 0)),
        ))
    if rows_w:
        insert_risk_windows(rows_w)

    # risk_now snapshot (earliest upcoming per site)
    for site, sdf in summary.groupby("site_name"):
        sdf = sdf.sort_values("start_time")
        if sdf.empty:
            upsert_risk_now(site, 0, None, None)
            continue
        nxt = sdf.iloc[0]
        start_dt = _ts_or_none(nxt["start_time"])
        if start_dt is None:
            upsert_risk_now(site, 0, None, None)
            continue
        tzinfo = nxt["start_time"].tz
        now_aware = pd.Timestamp.now(tz=tzinfo)
        starts_in_h = max(0, int((nxt["start_time"] - now_aware).total_seconds() // 3600))
        upsert_risk_now(site, int(nxt.get("risk_score", 0)), start_dt, starts_in_h)

    # hourly (with weather data from combined)
    rows: List[Tuple] = []
    for _, w in summary.iterrows():
        site = w["site_name"]
        start = w["start_time"]
        end = w["end_time"]
        trig = str(w.get("triggers", ""))

        tr, wr, hr = _flags_from_triggers(trig)
        anyr = tr or wr or hr

        # Find weather data for this site
        site_data = combined[combined['site_name'] == site].copy()
        
        # Inclusive hourly stamps
        hours = pd.date_range(start=start, end=end, freq="h")  # Using 'h' instead of 'H' to avoid warning
        if len(hours) == 0:
            continue

        for ts in hours:
            ts_py = _ts_or_none(ts)
            if ts_py is None:
                continue
                
            # Get weather data for this timestamp
            ts_data = site_data[site_data['Time'] == ts]
            if len(ts_data) > 0:
                temp = float(ts_data['Temperature (°F)'].iloc[0])
                wind = float(ts_data['Wind Speed (mph)'].iloc[0])
                rh = float(ts_data['Humidity (%)'].iloc[0])
            
                rows.append((
                    site,        # site
                    ts_py,       # ts (TIMESTAMPTZ)
                    temp,        # temp
                    wind,        # wind
                    rh,         # rh_pct
                    bool(tr),    # temperature_risk
                    bool(wr),    # wind_risk
                    bool(hr),    # humidity_risk
                    bool(anyr),  # any_risk
                ))

    if rows:
        upsert_risk_hourly(rows)

    # ---- Optional Excel export ----
    if save_excel and not combined.empty:
        reports_dir = os.path.join(os.getcwd(), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        xlsx_path = os.path.join(
            reports_dir, f"Cooling_Watchdog_Risk_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        )
        try:
            detailed = combined.copy()
            detailed["Date"]        = pd.to_datetime(detailed["Time"], errors="coerce").dt.strftime("%Y-%m-%d")
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
                    summary_excel["End Date"]   = summary_excel["end_time"].dt.strftime("%Y-%m-%d")
                    summary_excel["End Time"]   = summary_excel["end_time"].dt.strftime("%I:%M %p")
                    summary_excel["Timezone"]   = summary_excel["start_time"].dt.tz

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

                # Auto-size
                for ws in writer.sheets.values():
                    for col_cells in ws.columns:
                        max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
                        col_letter = col_cells[0].column_letter
                        ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

            print(f"\nAnalysis complete. Excel saved:\n{xlsx_path}")
        except Exception as e:
            print(f"\nError saving Excel file: {e}")

    return combined, summary, ConfigError.SUCCESS


# ============================================================================
# 6) Preview utility
# ============================================================================

def print_risk_preview(df: pd.DataFrame):
    """Print a quick preview of hourly risk rows."""
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


# ============================================================================
# 7) Main
# ============================================================================

if __name__ == "__main__":
    import sys
    cfg = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "Sites.json")
    print(f"Using config: {cfg}")
    combined, summary, code = analyze_risk_windows(cfg, save_excel=True)
    print("Exit code:", code)
    print_risk_preview(combined)
