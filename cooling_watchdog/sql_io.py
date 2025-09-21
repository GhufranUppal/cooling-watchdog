# sql_io.py
from __future__ import annotations
import os
from contextlib import contextmanager
from typing import Iterable, Sequence, Tuple, Optional

import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ---------------------------------------------------------------------
# Connection settings & helpers
# ---------------------------------------------------------------------

# PostgreSQL connection parameters
DB_PARAMS = {
    "dbname": "ignitiondb",     # Our application database
    "user": "ignition_user",    # Our application user
    "password": "1234567",      # User's password
    "host": "localhost",
    "port": "5432"
}

def verify_connection(conn) -> tuple[bool, str]:
    """
    Verify database connection and permissions.
    Returns (success, message) tuple.
    """
    try:
        with conn.cursor() as cur:
            # Check connection is alive
            cur.execute("SELECT 1")
            if not cur.fetchone():
                return False, "Connection test failed"
            
            # Check we can create tables in public schema
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public._test_permissions (
                    id serial PRIMARY KEY,
                    test_col text
                );
            """)
            
            # Clean up test table
            cur.execute("DROP TABLE IF EXISTS public._test_permissions")
            
            return True, "Connection and permissions verified"
            
    except psycopg2.Error as e:
        return False, f"Database error: {str(e).strip()}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

@contextmanager
def get_conn(autocommit: bool = True):
    """
    Context manager that yields a verified psycopg2 connection.
    Raises RuntimeError if connection or permissions verification fails.
    """
    conn = None
    try:
        # Build DSN string
        dsn = " ".join(f"{k}={v}" for k, v in DB_PARAMS.items())
        
        # Connect with error checking
        try:
            conn = psycopg2.connect(dsn)
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to connect to database: {str(e).strip()}")
            
        # Set autocommit and verify connection
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT if autocommit else conn.isolation_level)
        ok, msg = verify_connection(conn)
        if not ok:
            raise RuntimeError(f"Database verification failed: {msg}")
            
        # Connection good, set search path and yield
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public")
        yield conn
        
    finally:
        if conn is not None:
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
