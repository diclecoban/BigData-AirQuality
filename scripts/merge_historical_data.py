"""Merge IBB + OpenAQ historical air quality data into a Parquet dataset.

Workflow:
  1. Fetch IBB data  (optional, controlled by --source)
  2. Fetch OpenAQ data  (optional, controlled by --source)
  3. Normalise both to unified schema
  4. Merge and deduplicate (IBB priority)
  5. Apply data-quality filters
  6. Load into PySpark
  7. Add lag / rolling / temporal features via feature_engineering.py
  8. Write Parquet to --output-path, partitioned by date

Usage examples:
  # Last 7 days from both sources
  python scripts/merge_historical_data.py

  # Custom date range, IBB only
  python scripts/merge_historical_data.py \\
      --start-date 2024-01-01 --end-date 2024-03-31 --source ibb

  # Full 2024, both sources, custom output
  python scripts/merge_historical_data.py \\
      --start-date 2024-01-01 --end-date 2024-12-31 \\
      --output-path data/processed/full_2024.parquet
"""

from __future__ import annotations

import argparse
import math
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Windows: set HADOOP_HOME before importing PySpark
# ---------------------------------------------------------------------------
_HADOOP_HOME = Path("C:/hadoop")
if sys.platform == "win32" and _HADOOP_HOME.exists():
    os.environ.setdefault("HADOOP_HOME",     str(_HADOOP_HOME))
    os.environ.setdefault("hadoop.home.dir", str(_HADOOP_HOME))
    _bin = str(_HADOOP_HOME / "bin")
    if _bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _bin + ";" + os.environ.get("PATH", "")

# Add project root to path so src.* imports work
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.batch.data_merger import (
    IBBDataFetcher,
    OpenAQDataFetcher,
    apply_data_quality_filters,
    merge_and_deduplicate,
    normalize_ibb_schema,
    normalize_openaq_schema,
)
from src.common.config import LAG_HOURS, PROCESSED_DIR, ROLLING_WINDOWS
from src.common.logger import get_logger
from src.processing.feature_engineering import (
    engineer_lag_features,
    engineer_rolling_features,
    engineer_temporal_features,
)

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    today     = datetime.now(tz=timezone.utc).date()
    week_ago  = today - timedelta(days=7)

    parser = argparse.ArgumentParser(
        description="Merge IBB + OpenAQ historical air quality data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-date",
        default=str(week_ago),
        help="Inclusive start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=str(today),
        help="Inclusive end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--source",
        choices=["ibb", "openaq", "both"],
        default="both",
        help="Which API(s) to fetch from.",
    )
    parser.add_argument(
        "--output-path",
        default=str(PROCESSED_DIR / "merged_historical.parquet"),
        help="Destination Parquet path.",
    )
    parser.add_argument(
        "--lag-hours",
        nargs="+",
        type=int,
        default=LAG_HOURS,
        help="Lag offsets in hours for feature engineering.",
    )
    parser.add_argument(
        "--rolling-windows",
        nargs="+",
        type=int,
        default=ROLLING_WINDOWS,
        help="Rolling window sizes in hours.",
    )
    parser.add_argument(
        "--skip-features",
        action="store_true",
        help="Write raw merged data without feature engineering (faster).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Step 1 + 2: fetch from APIs
# ---------------------------------------------------------------------------

def _fetch_ibb(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch all IBB measurements; return empty DataFrame on failure."""
    logger.info("Fetching IBB data %s → %s", start.date(), end.date())
    fetcher = IBBDataFetcher()
    raw = fetcher.fetch_all_measurements(start, end)
    if raw.empty:
        logger.warning("IBB returned no data — continuing without IBB source")
        return pd.DataFrame()
    normalised = normalize_ibb_schema(raw)
    logger.info("IBB: %d rows after normalisation", len(normalised))
    return normalised


def _fetch_openaq(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch all OpenAQ measurements; return empty DataFrame on failure."""
    logger.info("Fetching OpenAQ data %s → %s", start.date(), end.date())
    fetcher = OpenAQDataFetcher()
    raw = fetcher.fetch_all_measurements(start, end)
    if raw.empty:
        logger.warning("OpenAQ returned no data — continuing without OpenAQ source")
        return pd.DataFrame()
    normalised = normalize_openaq_schema(raw)
    logger.info("OpenAQ: %d rows after normalisation", len(normalised))
    return normalised


# ---------------------------------------------------------------------------
# Step 6 + 7: Spark feature engineering
# ---------------------------------------------------------------------------

def _build_spark() -> SparkSession:
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

    return (
        SparkSession.builder
        .appName("merge-historical-data")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .getOrCreate()
    )


def _apply_features(
    spark: SparkSession,
    pandas_df: pd.DataFrame,
    lag_hours: list[int],
    rolling_windows: list[int],
) -> "pyspark.sql.DataFrame":
    """Convert pandas → Spark and apply engineer_* feature functions.

    Args:
        spark:           Active SparkSession.
        pandas_df:       Merged, quality-filtered pandas DataFrame.
        lag_hours:       Lag offsets for engineer_lag_features.
        rolling_windows: Window sizes for engineer_rolling_features.

    Returns:
        Spark DataFrame with feature columns added.
    """
    sdf = spark.createDataFrame(pandas_df)
    sdf = sdf.withColumn("timestamp", F.to_timestamp("timestamp"))

    sdf = engineer_lag_features(sdf,       lag_hours=lag_hours)
    sdf = engineer_rolling_features(sdf,   windows=rolling_windows)
    sdf = engineer_temporal_features(sdf)

    return sdf


def _apply_features_pandas(
    pandas_df: pd.DataFrame,
    lag_hours: list[int],
    rolling_windows: list[int],
) -> pd.DataFrame:
    """Apply lag, rolling, and temporal features directly in pandas."""
    df = pandas_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values(["station_id", "timestamp"]).reset_index(drop=True)
    grouped = df.groupby("station_id", sort=False)

    for col in ("pm25", "aqi"):
        if col not in df.columns:
            continue
        for lag in lag_hours:
            df[f"{col}_lag_{lag}h"] = grouped[col].shift(lag)

    for col in ("pm25", "aqi"):
        if col not in df.columns:
            continue
        for window in rolling_windows:
            rolling = grouped[col].rolling(window=window + 1, min_periods=1)
            df[f"{col}_mean_{window}h"] = rolling.mean().reset_index(level=0, drop=True)
            df[f"{col}_std_{window}h"] = rolling.std().reset_index(level=0, drop=True)

    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = ((df["timestamp"].dt.dayofweek + 1) % 7) + 1
    df["month"] = df["timestamp"].dt.month
    df["is_weekend"] = df["day_of_week"].isin([1, 7]).astype(int)

    two_pi = 2.0 * math.pi
    df["hour_sin"] = (df["hour"] * (two_pi / 24)).map(math.sin)
    df["hour_cos"] = (df["hour"] * (two_pi / 24)).map(math.cos)
    df["month_sin"] = (df["month"] * (two_pi / 12)).map(math.sin)
    df["month_cos"] = (df["month"] * (two_pi / 12)).map(math.cos)
    df["dow_sin"] = (df["day_of_week"] * (two_pi / 7)).map(math.sin)
    df["dow_cos"] = (df["day_of_week"] * (two_pi / 7)).map(math.cos)

    return df


# ---------------------------------------------------------------------------
# Step 8: write Parquet
# ---------------------------------------------------------------------------

def _write_parquet(sdf: "pyspark.sql.DataFrame", output_path: str) -> None:
    """Write Spark DataFrame to Parquet partitioned by date.

    Args:
        sdf:         Feature-enriched Spark DataFrame.
        output_path: Destination path string.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    sdf = sdf.withColumn("date", F.to_date("timestamp"))

    logger.info("Writing %d rows → %s (partitioned by date)", sdf.count(), output_path)
    (
        sdf.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(output_path)
    )
    logger.info("Parquet written successfully.")


def _write_parquet_pandas(df: pd.DataFrame, output_path: str) -> None:
    """Write a partitioned Parquet dataset directly with pandas/pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        shutil.rmtree(out)

    data = df.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    data["date"] = data["timestamp"].dt.date.astype("string")

    logger.info("Writing %d rows â†’ %s (partitioned by date)", len(data), output_path)
    table = pa.Table.from_pandas(data, preserve_index=False)
    pq.write_to_dataset(table, root_path=str(out), partition_cols=["date"])
    logger.info("Parquet written successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    logger.info(
        "merge_historical_data: source=%s  %s → %s  output=%s",
        args.source, args.start_date, args.end_date, args.output_path,
    )

    # --- Step 1 & 2: fetch ---------------------------------------------------
    ibb_df    = pd.DataFrame()
    openaq_df = pd.DataFrame()

    if args.source in ("ibb", "both"):
        ibb_df = _fetch_ibb(start_dt, end_dt)

    if args.source in ("openaq", "both"):
        openaq_df = _fetch_openaq(start_dt, end_dt)

    # --- Step 3 & 4: merge + dedup -------------------------------------------
    merged = merge_and_deduplicate(ibb_df, openaq_df)

    if merged.empty:
        logger.error(
            "No data fetched from any source.  "
            "Check API connectivity and OPENAQ_API_KEY."
        )
        sys.exit(1)

    # --- Step 5: quality filters ---------------------------------------------
    clean = apply_data_quality_filters(merged)
    logger.info("After quality filters: %d rows remain", len(clean))

    # --- Step 6 + 7 + 8: feature engineering + Parquet write -----------------
    if sys.platform == "win32":
        logger.info("Using pandas execution path on Windows for stability.")
        if args.skip_features:
            output_df = clean.copy()
            output_df["timestamp"] = pd.to_datetime(output_df["timestamp"], errors="coerce")
        else:
            output_df = _apply_features_pandas(
                clean, args.lag_hours, args.rolling_windows
            )
        _write_parquet_pandas(output_df, args.output_path)
    else:
        spark = _build_spark()
        spark.sparkContext.setLogLevel("WARN")

        try:
            if args.skip_features:
                sdf = spark.createDataFrame(clean)
                sdf = sdf.withColumn("timestamp", F.to_timestamp("timestamp"))
            else:
                sdf = _apply_features(
                    spark, clean, args.lag_hours, args.rolling_windows
                )

            _write_parquet(sdf, args.output_path)
        finally:
            spark.stop()

    n_days = (end_dt.date() - start_dt.date()).days + 1
    logger.info(
        "Done.  %d days × ~37 stations = expected ~%d rows  (actual: %d)",
        n_days, n_days * 24 * 37, len(clean),
    )
    print(f"\nOutput written to: {args.output_path}")


if __name__ == "__main__":
    main()
