#!/usr/bin/env python3
"""
Main module for the Cooling Watchdog application.
A proactive weather risk analysis system for monitoring temperature, humidity, and wind conditions.
"""

import os
import sys
from cooling_watchdog.risk_analysis import analyze_risk_windows, print_risk_preview
from cooling_watchdog.config import ConfigError


def main():
    """Main entry point for the Cooling Watchdog application."""
    try:
        print("\n=== Cooling Watch – Starting Analysis ===")
        DEFAULT_CONFIG_PATH = os.path.join(os.getcwd(), "Sites.json")
        
        # Analyze risk windows and handle potential errors
        result, summary, error_code = analyze_risk_windows(config_path=DEFAULT_CONFIG_PATH, save_excel=True)
        
        # Check if we received an error code
        if error_code != ConfigError.SUCCESS:
            error_messages = {
                ConfigError.FILE_NOT_FOUND: "Error: No sites found in configuration",
                ConfigError.INVALID_JSON: "Error: Invalid site data format",
                ConfigError.INVALID_UNITS: "Error: Invalid unit specification",
                ConfigError.EMPTY_SITES: "Error: Empty sites list",
                ConfigError.GENERAL_ERROR: "Error: Failed to process configuration"
            }
            error_msg = error_messages.get(error_code, f"Unknown error occurred (code: {error_code})")
            print(f"\nAnalysis failed: {error_msg}")
            return error_code
            
        # If we got here, result contains valid risk data
        print_risk_preview(result)
        return 0
        
    except Exception as e:
        print(f"\nUnexpected error occurred: {str(e)}")
        return -1

if __name__ == "__main__":
    sys.exit(main())