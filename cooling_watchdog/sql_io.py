# sql_io.py
from __future__ import annotations
import os
from contextlib import contextmanager
from typing import Iterable, Sequence, Tuple, Optional

import psycopg2
import psycopg2.extras as extras

# ---------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------


DSN_1 = (
    "dbname=ignitiondb "
    "user=ignition_user "
    "password=1234567 "
    "host=localhost "
    "port=5432"
)

def _dsn_from_env() -> str:
    """Build a psycopg2 DSN from env vars, preferring PG_DSN."""
    if os.getenv("PG_DSN"):
        return os.environ["PG_DSN"]

    parts = {
        "dbname": os.getenv("PG_DB", "ignitiondb"),
        "user": os.getenv("PG_USER", "ignition_user"),
        "password": os.getenv("PG_PASSWORD", ""),
        "host": os.getenv("PG_HOST", "localhost"),
        "port": os.getenv("PG_PORT", "5432"),
    }
    return " ".join(f"{k}={v}" for k, v in parts.items() if v != "")

@contextmanager
def get_conn(autocommit: bool = True):
    """Context manager that yields a psycopg2 connection."""
    conn = psycopg2.connect(DSN_1)
    try:
        conn.autocommit = autocommit
        # Optional: fix search_path & timezone for session (helps in Designer too)
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public;")
            # Current timestamp handling is TIMESTAMPTZ, no need to SET TIME ZONE
        yield conn
    finally:
        conn.close()

# ---------------------------------------------------------------------
# Schema bootstrap (safe to run multiple times)
# ---------------------------------------------------------------------
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

def ensure_schema():
    """Create tables/indexes if they don't exist."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DDL)

# ---------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------
def upsert_risk_now(
    site: str,
    risk_score: int,
    next_window_start_ts,  # tz-aware datetime (pandas.Timestamp or datetime)
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
        cur.execute(sql, (site, int(risk_score), next_window_start_ts, next_window_starts_in_h))

def insert_risk_windows(rows: Iterable[Tuple]):
    """
    Insert window rows. Each row must be:
      (site, start_ts, end_ts, duration_h, peak_temp, peak_wind, min_rh_pct, triggers, risk_score)
    """
    sql = """
    INSERT INTO public.risk_windows
    (site, start_ts, end_ts, duration_h, peak_temp, peak_wind, min_rh_pct, triggers, risk_score)
    VALUES %s
    """
    values = list(rows)
    if not values:
        return
    with get_conn() as conn, conn.cursor() as cur:
        extras.execute_values(cur, sql, values, page_size=500)

def upsert_risk_hourly(rows: Iterable[Tuple]):
    """
    Upsert hourly rows. Each row must be:
      (site, ts, temp, wind, rh_pct, temperature_risk, wind_risk, humidity_risk, any_risk)
    """
    values = list(rows)
    if not values:
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
        extras.execute_values(cur, sql, values, page_size=1000)
