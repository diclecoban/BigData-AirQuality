"""Batch analytics on historical Istanbul air quality data.

Reads raw CSV files from data/raw/, joins with weather, runs district-level
aggregations, identifies temporal trends, and writes reports to data/reports/.

Run:
  spark-submit src/batch/historical_analysis.py
  # or in local mode:
  python -m src.batch.historical_analysis
"""

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT       = Path(__file__).resolve().parents[2]
RAW_DIR    = ROOT / "data" / "raw"
REPORT_DIR = ROOT / "data" / "reports"

AQ_CSV  = str(RAW_DIR / "airquality_historical.csv")
WX_CSV  = str(RAW_DIR / "weather_historical.csv")


# ---------------------------------------------------------------------------
# AQI category helper
# ---------------------------------------------------------------------------

def _add_aqi_category(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "aqi_category",
        F.when(F.col("aqi") <= 50,  "Good")
         .when(F.col("aqi") <= 100, "Moderate")
         .when(F.col("aqi") <= 150, "Unhealthy for Sensitive Groups")
         .when(F.col("aqi") <= 200, "Unhealthy")
         .when(F.col("aqi") <= 300, "Very Unhealthy")
         .otherwise("Hazardous"),
    )


# ---------------------------------------------------------------------------
# 1. Load historical data
# ---------------------------------------------------------------------------

def load_historical_data(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Read and cast raw CSVs.  Returns (air_quality_df, weather_df)."""
    aq = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(AQ_CSV)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )
    wx = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(WX_CSV)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )
    return aq, wx


def join_weather(aq: DataFrame, wx: DataFrame) -> DataFrame:
    """Left-join weather onto air quality by truncated hour."""
    aq_h = aq.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    wx_h = wx.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    weather_cols = [c for c in wx.columns if c != "timestamp"]
    return aq_h.join(wx_h.select(["ts_hour"] + weather_cols), on="ts_hour", how="left")


# ---------------------------------------------------------------------------
# 2. District-level statistics
# ---------------------------------------------------------------------------

def compute_district_statistics(df: DataFrame) -> DataFrame:
    """Build district-level historical summary metrics.

    Returns one row per district with:
      - mean / max / min of AQI, PM2.5, NO2
      - proportion of hours in each AQI category
      - correlation of AQI with temperature and wind speed
    """
    df = _add_aqi_category(df)

    summary = df.groupBy("district").agg(
        # counts
        F.count("*").alias("total_hours"),

        # AQI stats
        F.round(F.avg("aqi"),  2).alias("aqi_mean"),
        F.round(F.max("aqi"),  2).alias("aqi_max"),
        F.round(F.min("aqi"),  2).alias("aqi_min"),
        F.round(F.stddev("aqi"), 2).alias("aqi_std"),

        # PM2.5 stats
        F.round(F.avg("pm25"), 2).alias("pm25_mean"),
        F.round(F.max("pm25"), 2).alias("pm25_max"),

        # NO2 stats
        F.round(F.avg("no2"),  2).alias("no2_mean"),
        F.round(F.max("no2"),  2).alias("no2_max"),

        # Bad-air hours (AQI > 100)
        F.round(
            F.sum((F.col("aqi") > 100).cast("int")) / F.count("*") * 100, 2
        ).alias("pct_unhealthy_hours"),

        # Weather correlations (approx via avg of products)
        F.round(F.corr("aqi", "temperature"), 3).alias("corr_aqi_temperature"),
        F.round(F.corr("aqi", "wind_speed"),  3).alias("corr_aqi_wind_speed"),
        F.round(F.corr("aqi", "humidity"),    3).alias("corr_aqi_humidity"),
    )
    return summary.orderBy(F.desc("aqi_mean"))


# ---------------------------------------------------------------------------
# 3. Temporal trends
# ---------------------------------------------------------------------------

def identify_trends(df: DataFrame) -> dict[str, DataFrame]:
    """Analyse seasonal, hourly, and long-term trends.

    Returns a dict of DataFrames, each keyed by trend type.
    """
    trends = {}

    # --- Hourly average across all stations ---
    trends["hourly"] = (
        df.withColumn("hour", F.hour("timestamp"))
          .groupBy("hour")
          .agg(
              F.round(F.avg("aqi"),  2).alias("aqi_mean"),
              F.round(F.avg("pm25"), 2).alias("pm25_mean"),
              F.round(F.avg("no2"),  2).alias("no2_mean"),
          )
          .orderBy("hour")
    )

    # --- Monthly (seasonal) average ---
    trends["monthly"] = (
        df.withColumn("month", F.month("timestamp"))
          .groupBy("month")
          .agg(
              F.round(F.avg("aqi"),  2).alias("aqi_mean"),
              F.round(F.avg("pm25"), 2).alias("pm25_mean"),
              F.round(F.avg("no2"),  2).alias("no2_mean"),
          )
          .orderBy("month")
    )

    # --- Day-of-week pattern ---
    trends["day_of_week"] = (
        df.withColumn("dow", F.dayofweek("timestamp"))
          .groupBy("dow")
          .agg(
              F.round(F.avg("aqi"),  2).alias("aqi_mean"),
              F.round(F.avg("pm25"), 2).alias("pm25_mean"),
          )
          .orderBy("dow")
    )

    # --- Weekly rolling trend (long-term) ---
    trends["weekly"] = (
        df.withColumn("week", F.weekofyear("timestamp"))
          .groupBy("week")
          .agg(
              F.round(F.avg("aqi"),  2).alias("aqi_mean"),
              F.round(F.avg("pm25"), 2).alias("pm25_mean"),
              F.round(F.avg("temperature"), 2).alias("temp_mean"),
          )
          .orderBy("week")
    )

    return trends


# ---------------------------------------------------------------------------
# 4. Export reports
# ---------------------------------------------------------------------------

def export_reports(
    district_stats: DataFrame,
    trends: dict[str, DataFrame],
    spark: SparkSession,
) -> None:
    """Write analysis outputs to data/reports/ as CSV files via pandas.

    Using toPandas() + pandas.to_csv() avoids the HADOOP_HOME dependency
    that Spark's native .write.csv() requires on Windows.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # District summary
    ds_path = REPORT_DIR / "district_statistics.csv"
    district_stats.toPandas().to_csv(ds_path, index=False)
    print(f"  Saved district_statistics → {ds_path}")

    # Trend tables
    for name, tdf in trends.items():
        path = REPORT_DIR / f"trend_{name}.csv"
        tdf.toPandas().to_csv(path, index=False)
        print(f"  Saved trend_{name} → {path}")

    # JSON summary of top-5 most polluted districts for dashboards
    top5 = (
        district_stats
        .select("district", "aqi_mean", "pm25_mean", "pct_unhealthy_hours")
        .limit(5)
        .toPandas()
        .to_dict(orient="records")
    )
    summary_path = REPORT_DIR / "top5_polluted_districts.json"
    summary_path.write_text(json.dumps(top5, indent=2, ensure_ascii=False))
    print(f"  Saved top5_polluted_districts.json → {summary_path}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(spark: SparkSession) -> None:
    print("Loading historical data...")
    aq, wx = load_historical_data(spark)
    df = join_weather(aq, wx)
    print(f"  Joined dataset: {df.count():,} rows")

    print("Computing district statistics...")
    district_stats = compute_district_statistics(df)
    district_stats.show(10, truncate=False)

    print("Identifying temporal trends...")
    trends = identify_trends(df)
    for name, tdf in trends.items():
        print(f"\n--- {name} trend ---")
        tdf.show(truncate=False)

    print("Exporting reports...")
    export_reports(district_stats, trends, spark)
    print("Done.")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("historical-analysis")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()
