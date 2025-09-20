# risk_analysis_consolidated.py
"""
Consolidated Cooling Watchdog analysis:
- Loads sites/config
- Gets weather forecast per site (uses your existing `weather.get_weather_forecast`)
- Flags hourly risks, groups windows, computes triggers & risk_score (0..3)
- Writes to PostgreSQL (safe for NaT -> NULL)
- Optional Excel report
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Optional, Tuple, Iterable, Tuple as Tup

import pandas as pd
import psycopg2
import psycopg2.extras as extras

# ============================================================================
# 0) CONFIG: PostgreSQL DSN
#    (Hard-code here for lab/dev; for prod, use env vars or .env)
# ============================================================================
DSN = (
    "dbname=ignitiondb "
    "user=ignition_user "
    "password=1234567 "
    "host=localhost "
    "port=5432"
)

# ============================================================================
# 1) TRY IMPORTS FOR CONFIG + WEATHER
# ============================================================================
try:
    # Package-style (if you installed your package or run from package root)
    from cooling_watchdog.config import load_site_data, ConfigError
    from cooling_watchdog.weather import get_weather_forecast
except ImportError:
    # Local fallback
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

def insert_risk_windows(rows: Iterable[Tup]):
    """
    Rows: (site, start_ts, end_ts, duration_h, peak_temp, peak_wind, min_rh_pct, triggers, risk_score)
    """
    rows = list(rows)
    if not rows:
        return
    sql = """
    INSERT INTO public.risk_windows
    (site, start_ts, end_ts, duration_h, peak_temp, peak_wind, min_rh_pct, triggers, risk_score)
    VALUES %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET search_path TO public;")
        extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()

def upsert_risk_hourly(rows: Iterable[Tup]):
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

def build_site_payload(site: str, summary_for_site: pd.DataFrame) -> dict:
    """
    Create risk_now payload (earliest upcoming window per site).
    """
    if summary_for_site is None or summary_for_site.empty:
        return {
            "site": site,
            "risk_score": 0,
            "next_window_start_ts": None,
            "next_window_starts_in_h": None,
        }
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

def upsert_risk_now_from_summary(summary: pd.DataFrame):
    """UPSERT one snapshot row per site from the summary windows."""
    if summary is None or summary.empty:
        return
    ensure_schema()
    summary["start_time"] = pd.to_datetime(summary["start_time"], errors="coerce")
    for site, sdf in summary.groupby("site_name"):
        sdf = sdf.dropna(subset=["start_time"]).sort_values("start_time")
        if sdf.empty:
            upsert_risk_now(site, 0, None, None)
            continue
        nxt = sdf.iloc[0]
        start_dt = _ts_or_none(nxt["start_time"])
        tzinfo = nxt["start_time"].tz
        now_aware = pd.Timestamp.now(tz=tzinfo)
        starts_in_h = max(0, int((nxt["start_time"] - now_aware).total_seconds() // 3600))
        upsert_risk_now(site, int(nxt.get("risk_score", 0)), start_dt, starts_in_h)


# ============================================================================
# 4) MAIN ANALYSIS
# ============================================================================

def analyze_risk_windows(config_path: str, save_excel: bool = True) -> Tuple[pd.DataFrame, int]:
    """
    - Load sites from JSON (via load_site_data)
    - Fetch weather per site (via get_weather_forecast) which already slices to horizon
    - Attach hourly risk flags
    - Build contiguous risk windows per site
    - Normalize triggers + compute risk_score (0..3)
    - Persist: windows, hourly, risk_now
    - Optional Excel export

    Returns: (combined_hourly_df, error_code)
    """
    print("\nReading configuration...")
    sites_df, horizon_hours, default_tz, _index, error_code = load_site_data(config_path)

    if error_code != ConfigError.SUCCESS:
        return pd.DataFrame(), error_code
    if sites_df is None or sites_df.empty:
        print("No sites found; aborting.")
        return pd.DataFrame(), ConfigError.EMPTY_SITES

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
        return pd.DataFrame(), ConfigError.EMPTY_SITES

    combined = pd.concat(all_rows, ignore_index=True)

    # Ensure Time is valid before any DB writes
    combined["Time"] = pd.to_datetime(combined["Time"], errors="coerce")
    bad_hours = combined[combined["Time"].isna()]
    if not bad_hours.empty:
        print("⚠️ Dropping hourly rows with invalid Time (NaT):", len(bad_hours))
        combined = combined.dropna(subset=["Time"]).copy()

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

        # Drop invalid windows
        bad_windows = summary[summary[["start_time", "end_time"]].isna().any(axis=1)]
        if not bad_windows.empty:
            print("⚠️ Dropping windows with invalid timestamps (NaT):", len(bad_windows))
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
    upsert_risk_now_from_summary(summary)

    # Hourly
    rows_h = []
    for _, r in combined.iterrows():
        ts = _ts_or_none(r["Time"])
        if ts is None:
            continue
        rows_h.append((
            r["site_name"],
            ts,
            float(r["Temperature (°F)"]) if pd.notna(r["Temperature (°F)"]) else None,
            float(r["Wind Speed (mph)"]) if pd.notna(r["Wind Speed (mph)"]) else None,
            int(r["Humidity (%)"]) if pd.notna(r["Humidity (%)"]) else None,
            bool(r["temperature_risk"]),
            bool(r["wind_risk"]),
            bool(r["humidity_risk"]),
            bool(r["any_risk"]),
        ))
    print('Number of hourly rows to upsert:', len(rows_h))
    if rows_h:
        upsert_risk_hourly(rows_h)

    # ---- Optional Excel export ----
    if save_excel and not combined.empty:
        reports_dir = os.path.join(os.getcwd(), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        xlsx_path = os.path.join(
            reports_dir, f"Cooling_Watchdog_Risk_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        )
        try:
            detailed = combined.copy()
            detailed["Date"]       = pd.to_datetime(detailed["Time"], errors="coerce").dt.strftime("%Y-%m-%d")
            detailed["Time of Day"]= pd.to_datetime(detailed["Time"], errors="coerce").dt.strftime("%I:%M %p")

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

                # Auto-size columns
                for ws in writer.sheets.values():
                    for col_cells in ws.columns:
                        max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
                        col_letter = col_cells[0].column_letter
                        ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

            print(f"\nAnalysis complete. Excel saved:\n{xlsx_path}")
        except Exception as e:
            print(f"\nError saving Excel file: {e}")

    return combined, ConfigError.SUCCESS


# ============================================================================
# 5) UTILITY: print preview
# ============================================================================

def print_risk_preview(df: pd.DataFrame):
    """Print a quick preview of risk rows."""
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
# 6) MAIN (run: python risk_analysis_consolidated.py [Sites.json])
# ============================================================================

if __name__ == "__main__":
    import sys
    cfg = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "Sites.json")
    print(f"Using config: {cfg}")
    df, code = analyze_risk_windows(cfg, save_excel=True)
    print("Exit code:", code)
    print_risk_preview(df)
