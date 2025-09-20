"""Risk analysis module for processing weather data and identifying risk windows."""

import os
from datetime import datetime
import pandas as pd
from typing import Dict


try:
    # Try package-style import first
    from cooling_watchdog.config import load_site_data, ConfigError
    from cooling_watchdog.weather import get_weather_forecast
    from cooling_watchdog.Helpers import window_triggers_label_from_str, risk_score_simple
    from cooling_watchdog.sql_io import upsert_risk_hourly, insert_risk_windows, upsert_risk_now,contextmanager,get_conn 
    
except ImportError:
    # Fall back to local imports if running directly
    from config import load_site_data, ConfigError
    from weather import get_weather_forecast
    from Helpers import window_triggers_label_from_str, risk_score_simple
    from cooling_watchdog.sql_io import upsert_risk_hourly, insert_risk_windows, upsert_risk_now,contextmanager,get_conn


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
    get_conn()
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
    get_conn()

    insert_risk_windows(rows)




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
        # Configuration error occurred, return empty DataFrame and the error code
        return pd.DataFrame(), error_code
    
    if sites_df is None:
        print("No sites found; aborting.")
        return pd.DataFrame(), ConfigError.EMPTY_SITES

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
        flagged['Time Zone'] = flagged['Time'].dt.tz
        all_rows.append(flagged)

    if not all_rows:
        print("No data produced for any site.")
        return pd.DataFrame()
    for df in all_rows:
        print('DataFrame  Time columns:', df['Time'].dt.tz)

    combined = pd.concat(all_rows, ignore_index=True)
    print('combined columns:', combined.columns)
    print(combined.head())
    print('Number of Time zones:', combined['Time Zone'].nunique())



    # Summarize contiguous risk windows per site
    risk_only = combined[combined["any_risk"]].copy()

    print('risk_only columns:', risk_only.columns)
    print('Number of Time zones in risk_only:', risk_only['Time Zone'].nunique())
    if risk_only.empty:
        summary = pd.DataFrame()
        print("\nNo risk windows found.")
    else:
        grp = (risk_only["risk_group"] != risk_only["risk_group"].shift()).cumsum()
        risk_only["window_group"] = grp
        print('The type of Group:', type(grp))
        summary = (
            risk_only.groupby(["site_name", "window_group"], as_index=False)
            .agg(
                start_time=("Time", "min"),
                end_time=("Time", "max"),
                duration_h=("Time", lambda s: int((s.max() - s.min()).total_seconds() // 3600 + 1)),
                peak_temp_f=("Temperature (°F)", "max"),
                peak_wind_mph=("Wind Speed (mph)", "max"),
                min_rh_pct=("Humidity (%)", "min"),
                 #collect all hourly trigger strings into one big string (we'll normalize next)
                triggers_raw=("risk_triggers", lambda s: ", ".join(s.dropna().astype(str))),
                
            )
            .sort_values(["site_name", "start_time"])
            .reset_index(drop=True)
        )
    
      # 2) Normalize triggers and compute risk_score (0..3)
    summary["triggers"] = summary["triggers_raw"].apply(window_triggers_label_from_str)
    summary1=summary[['triggers_raw','triggers']]
    #print (summary1.to_excel('C:\GHUFRAN\Old\PythonScripting\CoolingWatchdog\Test\summary.xlsx'))
    summary["risk_score"] = summary["triggers"].apply(risk_score_simple)
    summary2=summary[['triggers_raw','triggers','risk_score']]
    #summary2.to_excel('C:\GHUFRAN\Old\PythonScripting\CoolingWatchdog\Test\summary2.xlsx')



    # (Optional) drop the raw column now that we've normalized
    summary.drop(columns=["triggers_raw"], inplace=True)

    print('The type of summary:', type(summary))
    print('the columns of summary:', summary.columns)
    print('Types of summary columns:', summary.dtypes)
    summary3=summary[['site_name','duration_h','peak_temp_f','peak_wind_mph','min_rh_pct','triggers','risk_score']]
    summary['start_time'] = pd.to_datetime(summary['start_time'], errors='coerce')
    summary['end_time'] = pd.to_datetime(summary['end_time'], errors='coerce')
    print('Types of summary columns after start_time and end_time conversion:', summary.dtypes)


    #summary3.to_excel('C:\GHUFRAN\Old\PythonScripting\CoolingWatchdog\Test\summary3.xlsx')

    # Write results to the database
    write_windows_to_db(summary)

    # Optional: Excel outputs

    if save_excel and not combined.empty:
        # Prepare detailed risks data with formatted dates
        detailed_risks = combined.copy()
        print ('size of detailed_risks:', detailed_risks.shape)
        print ('columns for detailed risks:')
        print( detailed_risks.columns)
        print('Number of Time zones:', detailed_risks['Time Zone'].nunique())
        print(detailed_risks['Time Zone'].unique())
        #df['time'] = pd.to_datetime(df['time'], errors='coerce')
        detailed_risks['Time'] = pd.to_datetime(detailed_risks['Time'], errors='coerce')
        detailed_risks['Date'] = detailed_risks['Time'].dt.strftime('%Y-%m-%d')
        detailed_risks['Time of Day'] = detailed_risks['Time'].dt.strftime('%I:%M %p')
        print ('size of detailed_risks:', detailed_risks.shape)
        print ('columns for detailed risks:')
        print( detailed_risks.columns)

        #detailed_risks['Local Timezone'] = detailed_risks['Time'].dt.tz
        #print ('Number of Time zones:', detailed_risks['Local Timezone'].nunique())
        
        # Select and rename columns for detailed risks sheet
        detailed_cols = {
            'Date': 'Date',
            'Time of Day': 'Time',
            'Time Zone': 'Time Zone',
            'site_name': 'Site',
            'Temperature (°F)': 'Temperature (°F)',
            'Humidity (%)': 'Humidity (%)',
            'Wind Speed (mph)': 'Wind Speed (mph)',
            'temperature_risk': 'Temperature Risk',
            'wind_risk': 'Wind Risk',
            'humidity_risk': 'Humidity Risk',
            'any_risk': 'Any Risk Condition',
            'risk_triggers': 'Risk Triggers'
            
        }

        

        detailed_excel = detailed_risks[list(detailed_cols.keys())].rename(columns=detailed_cols)
        print('detailed_excel columns and types:', detailed_excel.columns)
        

        # Prepare risk summary data with formatted dates
        summary_excel = None
        if not summary.empty:
            summary_excel = summary.copy()
            summary_excel['Start Date'] = summary_excel['start_time'].dt.strftime('%Y-%m-%d')
            summary_excel['Start Time'] = summary_excel['start_time'].dt.strftime('%I:%M %p')
            summary_excel['End Date'] = summary_excel['end_time'].dt.strftime('%Y-%m-%d')
            summary_excel['End Time'] = summary_excel['end_time'].dt.strftime('%I:%M %p')
            summary_excel['Timezone'] = summary_excel['start_time'].dt.tz
            print('summary_excel columns:', summary_excel.columns)
           


            # Select and rename columns for summary sheet
            summary_cols = {
                'site_name': 'Site',
                'Start Date': 'Start Date',
                'Start Time': 'Start Time',
                'End Date': 'End Date',
                'End Time': 'End Time',
                'Timezone': 'Timezone',
                'duration_h': 'Duration (hours)',
                'peak_temp_f': 'Peak Temperature (°F)',
                'peak_wind_mph': 'Peak Wind Speed (mph)',
                'min_rh_pct': 'Minimum Humidity (%)',
                'triggers': 'Risk Triggers',
                'risk_score': 'Risk Score'           # <-- add this line
            }
            summary_excel = summary_excel[list(summary_cols.keys())].rename(columns=summary_cols)

        # Create reports directory and save Excel file
        reports_dir = os.path.join(os.getcwd(), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        xlsx_path = os.path.join(reports_dir, f"Cooling_Watchdog_Risk_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")
        
        try:
            with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                # Write detailed risks sheet
                detailed_excel.to_excel(writer, index=False, sheet_name="Detailed Risks")
                
                # Write summary sheet if available
                if summary_excel is not None:
                    summary_excel.to_excel(writer, index=False, sheet_name="Risk Summary")
                    
                # Auto-adjust column widths in both sheets
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for idx, col in enumerate(worksheet.columns, 1):
                        max_length = 0
                        column = worksheet.column_dimensions[chr(64 + idx)]  # Get column letter
                        for cell in col:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = (max_length + 2)
                        column.width = min(adjusted_width, 50)  # Cap width at 50
                        
            print(f"\nAnalysis complete. Excel saved:\n{xlsx_path}")
            return combined, ConfigError.SUCCESS
        except Exception as e:
            print(f"\nError saving Excel file: {str(e)}")
            return combined, ConfigError.GENERAL_ERROR
        print(f"\nAnalysis complete. Excel saved:\n{xlsx_path}")

    return combined, ConfigError.SUCCESS

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