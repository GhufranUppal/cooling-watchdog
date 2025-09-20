#!/usr/bin/env python3
"""
Entry point script for running the Cooling Watchdog analysis directly.
"""

import os
import sys
from cooling_watchdog.risk_analysis_2 import analyze_risk_windows, print_risk_preview

def main():
    """Main entry point for running the analysis."""
    print("\n=== Cooling Watch – Starting Analysis ===")
    config_path = os.path.join(os.getcwd(), "Sites.json")
    
    combined, error_code = analyze_risk_windows(config_path=config_path, save_excel=True)
    if error_code == 0:
        print_risk_preview(combined)
        return 0
    return error_code

if __name__ == "__main__":
    sys.exit(main())