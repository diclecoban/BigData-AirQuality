"""Shared schemas for raw and normalized ingestion records."""

RAW_AIR_QUALITY_FIELDS = [
    "station_id",
    "station_name",
    "district",
    "timestamp",
    "pm10",
    "pm25",
    "no2",
    "so2",
    "co",
    "o3",
    "aqi",
    "latitude",
    "longitude",
    "source",
]

WEATHER_FIELDS = [
    "timestamp",
    "temperature",
    "humidity",
    "wind_speed",
    "wind_direction",
    "pressure",
    "precipitation",
    "visibility",
    "cloud_cover",
]
