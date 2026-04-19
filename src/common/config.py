"""Shared configuration helpers for the project.

All magic numbers, API endpoints, and tunable parameters live here.
Modules import from this file instead of hard-coding values.
"""

import os
from pathlib import Path

# Auto-load .env from project root (if python-dotenv is installed)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass  # python-dotenv optional; set env vars manually if not installed

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

PROJECT_ROOT   = Path(__file__).resolve().parents[2]
CONFIG_DIR     = PROJECT_ROOT / "config"
RAW_DIR        = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR  = PROJECT_ROOT / "data" / "processed"
MODEL_DIR      = PROJECT_ROOT / "data" / "models"
REPORT_DIR     = PROJECT_ROOT / "data" / "reports"
MLFLOW_DIR     = PROJECT_ROOT / "data" / "mlruns"


def get_config_path(filename: str) -> Path:
    """Return an absolute path under the config directory."""
    return CONFIG_DIR / filename


# ---------------------------------------------------------------------------
# IBB (Istanbul Metropolitan Municipality) Air Quality API
# ---------------------------------------------------------------------------

IBB_STATIONS_URL    = "https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIStations"
IBB_MEASUREMENTS_URL = (
    "https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId"
)

# Timeout for IBB API calls (seconds). IBB endpoints can be slow.
IBB_REQUEST_TIMEOUT = 30

# ---------------------------------------------------------------------------
# OpenAQ v3 API
# ---------------------------------------------------------------------------

OPENAQ_BASE_URL = "https://api.openaq.org/v3"

# Free API key — get yours at https://explore.openaq.org/register
# Set OPENAQ_API_KEY in your environment or .env file.
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")

# Istanbul bounding box used when searching locations
ISTANBUL_LAT    = 41.0082
ISTANBUL_LON    = 29.0230
ISTANBUL_RADIUS = 60_000   # metres — covers the whole province

# Max records per OpenAQ page request
OPENAQ_PAGE_LIMIT = 1000

# Timeout for OpenAQ API calls (seconds)
OPENAQ_REQUEST_TIMEOUT = 20

# ---------------------------------------------------------------------------
# Data quality thresholds
# Values outside these ranges are treated as sensor errors → set to NaN.
# ---------------------------------------------------------------------------

MAX_PM25 = 500.0    # µg/m³
MAX_PM10 = 600.0    # µg/m³
MAX_NO2  = 2000.0   # µg/m³
MAX_SO2  = 2000.0   # µg/m³
MAX_CO   = 50.0     # mg/m³
MAX_O3   = 1000.0   # µg/m³
MAX_AQI  = 500.0

MIN_PM25 = 0.0
MIN_PM10 = 0.0
MIN_NO2  = 0.0
MIN_SO2  = 0.0
MIN_CO   = 0.0
MIN_O3   = 0.0
MIN_AQI  = 0.0

# ---------------------------------------------------------------------------
# Feature engineering parameters
# Used by engineer_* functions in feature_engineering.py and data_merger.py.
# The existing add_* functions in feature_engineering.py keep their own
# module-level constants for backward compatibility; these are the values
# consumed by the new engineer_* API.
# ---------------------------------------------------------------------------

LAG_HOURS       = [1, 2, 3, 6, 24]     # hours to lag for engineer_lag_features
ROLLING_WINDOWS = [3, 6, 24]            # window sizes (hours) for engineer_rolling_features

# ---------------------------------------------------------------------------
# ML settings (mirrors app.yaml)
# ---------------------------------------------------------------------------

FORECAST_HORIZONS_H = [1, 3, 6]
TARGET_COLUMNS      = ["aqi", "pm25", "no2"]
PRIMARY_MODEL       = "GBTRegressor"
MLFLOW_EXPERIMENT   = "istanbul-aqi-gbt"

# ---------------------------------------------------------------------------
# Spark runtime settings
# ---------------------------------------------------------------------------

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "6g")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")


def configure_windows_hadoop_env() -> None:
    """Set HADOOP_HOME/PATH automatically when local winutils is installed."""
    hadoop_home = Path(os.getenv("HADOOP_HOME", "C:/hadoop"))
    if os.name != "nt" or not hadoop_home.exists():
        return

    os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
    os.environ.setdefault("hadoop.home.dir", str(hadoop_home))

    bin_dir = str(hadoop_home / "bin")
    path_parts = os.environ.get("PATH", "").split(os.pathsep) if os.environ.get("PATH") else []
    if bin_dir not in path_parts:
        os.environ["PATH"] = f"{bin_dir}{os.pathsep}{os.environ['PATH']}" if os.environ.get("PATH") else bin_dir
