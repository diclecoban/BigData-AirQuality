"""Feature engineering pipeline for AQI forecasting.

Input  : enriched Spark DataFrame with air quality + weather columns
Output : model-ready DataFrame with lag, rolling, temporal, and spatial features

All transformations use PySpark Window functions so this module works
identically in local mode and on a Spark cluster.
"""

import math

from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from src.ingestion.schema import WEATHER_FIELDS
from src.common.config import (
    MAX_PM25, MAX_PM10, MAX_NO2, MAX_SO2, MAX_CO, MAX_O3, MAX_AQI,
    MIN_PM25, MIN_PM10, MIN_NO2, MIN_SO2, MIN_CO, MIN_O3, MIN_AQI,
)


# ---------------------------------------------------------------------------
# Column groups
# ---------------------------------------------------------------------------

POLLUTANT_COLS = ["pm10", "pm25", "no2", "so2", "co", "o3", "aqi"]
ROLLING_COLS   = ["pm25", "no2", "aqi"]          # heavier rolling stats only on key cols
LAG_HOURS      = [1, 3, 6, 12, 24]
ROLLING_WINDOWS = {"3h": 3, "6h": 6, "24h": 24}

# Target variables (aligned with app.yaml ml.target_columns)
TARGET_COLS = ["aqi", "pm25", "no2"]


_BASE_FEATURE_COLS = [
    "station_id",
    "district",
    "timestamp",
    "latitude",
    "longitude",
    *POLLUTANT_COLS,
    *[col for col in WEATHER_FIELDS if col != "timestamp"],
]


# ---------------------------------------------------------------------------
# 0. Data validation and cleaning (for real IBB / OpenAQ data)
# ---------------------------------------------------------------------------

_POLLUTANT_BOUNDS = {
    "pm25": (MIN_PM25, MAX_PM25),
    "pm10": (MIN_PM10, MAX_PM10),
    "no2":  (MIN_NO2,  MAX_NO2),
    "so2":  (MIN_SO2,  MAX_SO2),
    "co":   (MIN_CO,   MAX_CO),
    "o3":   (MIN_O3,   MAX_O3),
    "aqi":  (MIN_AQI,  MAX_AQI),
}

_WEATHER_BOUNDS = {
    "temperature":   (-30.0, 50.0),
    "humidity":      (0.0,   100.0),
    "wind_speed":    (0.0,   100.0),
    "wind_direction":(0.0,   360.0),
    "pressure":      (900.0, 1100.0),
    "precipitation": (0.0,   500.0),
    "visibility":    (0.0,   100.0),
    "cloud_cover":   (0.0,   100.0),
}


def validate_and_clean(df: DataFrame) -> DataFrame:
    """Clamp out-of-range sensor readings to NULL for real IBB/OpenAQ data.

    Values outside the configured thresholds (config.py) are sensor errors
    and are set to NULL so the downstream Imputer can fill them with medians.
    Also drops rows missing both station_id and timestamp, which are
    unrecoverable.
    """
    for col, (lo, hi) in {**_POLLUTANT_BOUNDS, **_WEATHER_BOUNDS}.items():
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(
                    (F.col(col) < lo) | (F.col(col) > hi), None
                ).otherwise(F.col(col))
            )

    # Drop rows that can't be identified or placed on the timeline
    df = df.filter(
        F.col("station_id").isNotNull() & F.col("timestamp").isNotNull()
    )
    return df


# ---------------------------------------------------------------------------
# 1. Lag features
# ---------------------------------------------------------------------------

def add_lag_features(df: DataFrame) -> DataFrame:
    """Add t-N hour lag predictors for every pollutant column.

    Uses a Window partitioned by station so lags never bleed across stations.
    Rows with NULL lags (start of each station's history) are kept; callers
    should call dropna() at the end of build_feature_dataset.
    """
    w = (
        Window
        .partitionBy("station_id")
        .orderBy(F.col("timestamp").cast(T.LongType()))
    )
    for col in POLLUTANT_COLS:
        for lag in LAG_HOURS:
            df = df.withColumn(f"{col}_lag_{lag}h", F.lag(col, lag).over(w))
    return df


# ---------------------------------------------------------------------------
# 2. Rolling statistics
# ---------------------------------------------------------------------------

def add_rolling_statistics(df: DataFrame) -> DataFrame:
    """Add rolling mean and std for key pollutants over 3 h / 6 h / 24 h."""
    w_base = (
        Window
        .partitionBy("station_id")
        .orderBy(F.col("timestamp").cast(T.LongType()))
    )
    for col in ROLLING_COLS:
        for name, n_rows in ROLLING_WINDOWS.items():
            w = w_base.rowsBetween(-n_rows, 0)
            df = df.withColumn(f"{col}_mean_{name}", F.avg(col).over(w))
            df = df.withColumn(f"{col}_std_{name}",  F.stddev(col).over(w))
    return df


# ---------------------------------------------------------------------------
# 3. Temporal features
# ---------------------------------------------------------------------------

def add_time_features(df: DataFrame) -> DataFrame:
    """Encode hour, day-of-week, month, weekend flag, and cyclical signals."""
    df = (
        df
        .withColumn("hour",        F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))   # 1=Sun … 7=Sat
        .withColumn("month",       F.month("timestamp"))
        .withColumn("is_weekend",  (F.dayofweek("timestamp").isin(1, 7)).cast("int"))
    )

    # Cyclical sin/cos so the model understands hour 23 ≈ hour 0
    two_pi = 2.0 * math.pi
    df = (
        df
        .withColumn("hour_sin",  F.sin(F.col("hour")  * (two_pi / 24)))
        .withColumn("hour_cos",  F.cos(F.col("hour")  * (two_pi / 24)))
        .withColumn("month_sin", F.sin(F.col("month") * (two_pi / 12)))
        .withColumn("month_cos", F.cos(F.col("month") * (two_pi / 12)))
        .withColumn("dow_sin",   F.sin(F.col("day_of_week") * (two_pi / 7)))
        .withColumn("dow_cos",   F.cos(F.col("day_of_week") * (two_pi / 7)))
    )
    return df


# ---------------------------------------------------------------------------
# 4. Spatial features
# ---------------------------------------------------------------------------

def add_spatial_features(df: DataFrame) -> DataFrame:
    """Add district index encoding and distance from Istanbul city centre.

    City centre approximation: Taksim Meydanı (41.0369 N, 28.9850 E).
    Distance is Euclidean in degree-space (sufficient for relative ranking).
    """
    CENTER_LAT = 41.0369
    CENTER_LON = 28.9850

    # Numeric district index (needed by MLlib VectorAssembler)
    indexer = StringIndexer(
        inputCol="district",
        outputCol="district_index",
        handleInvalid="keep",
    )
    df = indexer.fit(df).transform(df)

    df = df.withColumn(
        "dist_from_center",
        F.sqrt(
            (F.col("latitude")  - CENTER_LAT) ** 2 +
            (F.col("longitude") - CENTER_LON) ** 2
        ),
    )
    return df


# ---------------------------------------------------------------------------
# 5. Forecast target columns (future labels)
# ---------------------------------------------------------------------------

def add_forecast_targets(df: DataFrame, horizons_h=(1, 3, 6)) -> DataFrame:
    """Append future AQI/PM2.5 values as supervised-learning targets.

    target_aqi_1h  = aqi value 1 h later for the same station
    target_aqi_3h  = aqi value 3 h later
    target_aqi_6h  = aqi value 6 h later
    Same pattern for pm25.
    """
    w = (
        Window
        .partitionBy("station_id")
        .orderBy(F.col("timestamp").cast(T.LongType()))
    )
    for h in horizons_h:
        for col in ["aqi", "pm25"]:
            df = df.withColumn(f"target_{col}_{h}h", F.lead(col, h).over(w))
    return df


# ---------------------------------------------------------------------------
# 6. Compose final model-ready table
# ---------------------------------------------------------------------------

def build_feature_dataset(df: DataFrame, horizons_h=(1, 3, 6)) -> DataFrame:
    """Compose the full feature table.

    Pipeline:
      1. Cast timestamp to TimestampType if it arrived as string.
      2. Add all feature groups.
      3. Add forecast targets.
      4. Drop rows with any NULL (caused by lag / lead at series boundaries).

    Returns a DataFrame ready to be split into train / val / test and fed
    into a VectorAssembler → MLlib Pipeline.
    """
    # Ensure timestamp column is proper Timestamp
    if dict(df.dtypes).get("timestamp") == "string":
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    # Clamp sensor outliers to NULL before feature engineering
    df = validate_and_clean(df)

    keep_cols = [col for col in _BASE_FEATURE_COLS if col in df.columns]
    df = df.select(*keep_cols)
    if "station_id" in df.columns:
        df = df.repartition("station_id")

    df = add_lag_features(df)
    df = add_rolling_statistics(df)
    df = add_time_features(df)
    df = add_spatial_features(df)
    df = add_forecast_targets(df, horizons_h)

    # Drop boundary rows that have NULLs from lag/lead windows
    df = df.dropna()
    return df


# ---------------------------------------------------------------------------
# 7. Return the list of feature column names used by VectorAssembler
# ---------------------------------------------------------------------------

def get_feature_columns() -> list:
    """Return the ordered list of input feature column names.

    This list must stay in sync with build_feature_dataset output so that
    train_baseline_models and train_gbt_model can import it instead of
    hard-coding column names.
    """
    cols = []

    # Raw pollutants + weather (present after join in historical_analysis)
    cols += POLLUTANT_COLS
    cols += [
        "temperature", "humidity", "wind_speed", "wind_direction",
        "pressure", "precipitation", "visibility", "cloud_cover",
    ]

    # Lag features
    for c in POLLUTANT_COLS:
        for lag in LAG_HOURS:
            cols.append(f"{c}_lag_{lag}h")

    # Rolling features
    for c in ROLLING_COLS:
        for name in ROLLING_WINDOWS:
            cols.append(f"{c}_mean_{name}")
            cols.append(f"{c}_std_{name}")

    # Temporal
    cols += [
        "hour", "day_of_week", "month", "is_weekend",
        "hour_sin", "hour_cos", "month_sin", "month_cos",
        "dow_sin", "dow_cos",
    ]

    # Spatial
    cols += ["district_index", "dist_from_center", "latitude", "longitude"]

    return cols


# ---------------------------------------------------------------------------
# 8. engineer_* API — thin wrappers consumed by data_merger workflow
#
# These functions have a different signature from the add_* functions above:
#   - lag_hours / windows are explicit parameters (read from config.py defaults)
#   - engineer_lag_features targets only pm25 + aqi (not all pollutants)
#   - engineer_rolling_features accepts a plain list of ints, not a dict
#
# They delegate to the add_* functions to avoid code duplication.
# ---------------------------------------------------------------------------

def engineer_lag_features(
    df: DataFrame,
    lag_hours: list | None = None,
) -> DataFrame:
    """Add lag features for PM2.5 and AQI at configurable horizons.

    Thin wrapper around add_lag_features() restricted to pm25 and aqi columns.
    Reads default lag_hours from src/common/config.py when not supplied.

    Args:
        df:        PySpark DataFrame with station_id, timestamp, pm25, aqi.
        lag_hours: List of integer hour offsets.  Defaults to config.LAG_HOURS.

    Returns:
        DataFrame with added columns ``pm25_lag_{n}h`` and ``aqi_lag_{n}h``.
    """
    from src.common.config import LAG_HOURS as _DEFAULT_LAG_HOURS

    hours = lag_hours if lag_hours is not None else _DEFAULT_LAG_HOURS

    w = (
        Window
        .partitionBy("station_id")
        .orderBy(F.col("timestamp").cast(T.LongType()))
    )
    for col in ("pm25", "aqi"):
        if col in df.columns:
            for lag in hours:
                df = df.withColumn(f"{col}_lag_{lag}h", F.lag(col, lag).over(w))
    return df


def engineer_rolling_features(
    df: DataFrame,
    windows: list | None = None,
) -> DataFrame:
    """Add rolling mean and std for PM2.5 and AQI at configurable window sizes.

    Thin wrapper around add_rolling_statistics() with explicit window list.
    Reads default windows from src/common/config.py when not supplied.

    Args:
        df:      PySpark DataFrame with station_id, timestamp, pm25, aqi.
        windows: List of integer window sizes in hours.  Defaults to
                 config.ROLLING_WINDOWS.

    Returns:
        DataFrame with added columns
        ``{col}_mean_{n}h`` and ``{col}_std_{n}h`` for each window.
    """
    from src.common.config import ROLLING_WINDOWS as _DEFAULT_WINDOWS

    win_list = windows if windows is not None else _DEFAULT_WINDOWS

    w_base = (
        Window
        .partitionBy("station_id")
        .orderBy(F.col("timestamp").cast(T.LongType()))
    )
    for col in ("pm25", "aqi"):
        if col in df.columns:
            for n in win_list:
                w = w_base.rowsBetween(-n, 0)
                df = df.withColumn(f"{col}_mean_{n}h", F.avg(col).over(w))
                df = df.withColumn(f"{col}_std_{n}h",  F.stddev(col).over(w))
    return df


def engineer_temporal_features(df: DataFrame) -> DataFrame:
    """Add hour_sin, hour_cos, month_sin, month_cos, and is_weekend columns.

    Delegates to add_time_features(); provided as a named entry-point for
    the data_merger workflow so callers don't need to know the internal name.

    Args:
        df: PySpark DataFrame with a timestamp column.

    Returns:
        DataFrame with cyclical temporal columns added.
    """
    return add_time_features(df)


# ---------------------------------------------------------------------------
# Smoke-test (run directly: python -m src.processing.feature_engineering)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("feature-engineering-test")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    from pathlib import Path
    raw = Path(__file__).resolve().parents[2] / "data" / "raw"

    aq = spark.read.csv(str(raw / "airquality_historical.csv"), header=True, inferSchema=True)
    wx = spark.read.csv(str(raw / "weather_historical.csv"),    header=True, inferSchema=True)

    # Join weather onto air quality by hour
    wx_ts = wx.withColumn("timestamp", F.to_timestamp("timestamp"))
    aq_ts = aq.withColumn("timestamp", F.to_timestamp("timestamp"))
    joined = aq_ts.join(wx_ts.drop("timestamp"), on=F.date_trunc("hour", aq_ts.timestamp) == F.date_trunc("hour", wx_ts.timestamp), how="left")

    features = build_feature_dataset(joined)
    features.printSchema()
    print(f"Feature rows: {features.count():,}")
    spark.stop()
