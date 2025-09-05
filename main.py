#!/usr/bin/env python3
"""
Main module for the Cooling Watchdog application.
A proactive weather risk analysis system for monitoring temperature, humidity, and wind conditions.
"""

import os
import sys
from cooling_watchdog.risk_analysis import analyze_risk_windows, print_risk_preview

def main():
    """Main entry point for the Cooling Watchdog application."""
    print("\n=== Cooling Watch – Starting Analysis ===")
    DEFAULT_CONFIG_PATH = os.path.join(os.getcwd(), "Sites.json")
    combined = analyze_risk_windows(config_path=DEFAULT_CONFIG_PATH, save_excel=True)
    print_risk_preview(combined)
    return 0

if __name__ == "__main__":
    sys.exit(main())