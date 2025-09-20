"""
Cooling Watchdog Package
A proactive weather risk analysis system for monitoring temperature, humidity, and wind conditions.
"""

from .url_builder import build_open_meteo_url
from .config import load_site_data
from .weather import get_weather_forecast
from .risk_analysis_2 import (
    attach_risk_flags,
    analyze_risk_windows,
    print_risk_preview
)

__all__ = [
    'build_open_meteo_url',
    'load_site_data',
    'get_weather_forecast',
    'attach_risk_flags',
    'analyze_risk_windows',
    'print_risk_preview'
]