# Cooling Watchdog 🌡️🌀

**A proactive weather-driven risk detection service for data center cooling systems.**  
Continuously monitors weather forecasts, evaluates against custom thresholds, and surfaces upcoming **risk windows** to operators via **Ignition SCADA (Vision Dashboard + PostgreSQL integration).**

---

## ⚡ Motivation

Cooling systems in data centers are typically controlled by **reactive, rule-based strategies**.  
While effective at preventing immediate failure, this **reactive approach** has major drawbacks:  

- ⚡ **Abrupt responses** — sudden staging of chillers, rapid valve/damper movements.  
- 💸 **Energy inefficiency** — large spikes in energy consumption from quick reactions.  
- 🛠️ **Mechanical stress** — equipment endures unnecessary wear due to abrupt load changes.  

Cooling Watchdog introduces **forecast-based awareness** into the data center. By monitoring external weather and predicting risk windows:  
- Operators gain **advance visibility** into upcoming stress events (heat spikes, low humidity, high winds).  
- With this insight, they can **proactively adjust setpoints** (e.g., pre-stage chilled water) ahead of the event.  
- This enables a **gradual and measured response** instead of abrupt rule-based reactions.  

✅ Benefits:  
- **Improved energy efficiency**  
- **Lower mechanical stress**  
- **Enhanced resiliency**  

---

## 💡 Solution Overview

Cooling Watchdog is a Python-based service that:

1. **Fetches weather forecasts** via [Open-Meteo API](https://open-meteo.com/)  
2. **Evaluates site thresholds** (temperature, humidity, wind speed)  
3. **Detects and merges risk windows**  
4. **Writes results into PostgreSQL** (`risk_now`, `risk_windows`, `risk_hourly`)  
5. **Exposes results in Ignition Vision Dashboard**  

---

## 🏗️ System Architecture

![Cooling Watchdog Architecture](CoolingWatchdog_Architecture.png)

---

## 📊 Example Excel Report

Reports are saved under the `reports/` folder.  
Each report includes hourly forecasts, risk flags, and summarized risk windows.  

Example report:  
[Cooling_Watchdog_Risk_Report_20250922_1324.xlsx](reports/Cooling_Watchdog_Risk_Report_20250922_1324.xlsx)

---

## 📊 Ignition Vision Dashboard

The **Cooling Risk Dashboard** has been implemented in **Ignition Vision**.

### Features
- **Risk Now Card** → Current site risk score  
- **Next Window Card** → Upcoming risk window start + countdown  
- **Risk Windows Table** → List of upcoming windows (start, end, triggers, score)  
- **Risk Hourly Easy Chart** → Graph of hourly risk score  

### Screenshot 
![CoolingWatchdog Dashboard](CoolingWatchdog.png)

### Notes
- Dropdown for site selection  
- Named Queries drive Vision bindings (`getRiskNow`, `getWindows`, `getHourlySeries`)  
- Dynamic cell coloring in Vision tables for `Risk Score`  

---
