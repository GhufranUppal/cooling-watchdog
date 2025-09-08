"""URL builder module for the Open-Meteo API."""

def build_open_meteo_url(lat: float, lon: float, tz: str, horizon_hours: int) -> str:
    """
    Build an Open-Meteo URL sized just large enough for the requested horizon.
    1 day covers up to 24h, 2 days up to 48h, etc. Returns data in US units.

    Args:
        lat (float): Latitude of the location
        lon (float): Longitude of the location
        tz (str): Timezone string
        horizon_hours (int): Number of hours to forecast

    Returns:
        str: The complete Open-Meteo API URL configured for US units
              (temperature in °F, wind speed in mph)
    """
    safe_h = max(1, int(horizon_hours))
    forecast_days = max(1, (safe_h + 23) // 24)  # ceil(h/24) without math.ceil

    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        "&temperature_unit=fahrenheit"
        "&windspeed_unit=mph"
        "&precipitation_unit=inch"
        f"&timezone={tz}"
        f"&forecast_days={forecast_days}"
    )