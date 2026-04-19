"""Inference pipeline for real-time AQI prediction.

Loads the best saved GBT model and scores incoming feature rows.
Output records are formatted for the Grafana dashboard (Engineer 3's schema).

Usage (standalone smoke-test):
  python -m src.ml.inference
"""

from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT      = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "data" / "models"

# Default horizon to serve (1 h gives the most accurate predictions)
DEFAULT_HORIZON = 1


# ---------------------------------------------------------------------------
# AQI category helper (keeps output human-readable for the dashboard)
# ---------------------------------------------------------------------------

def _add_aqi_category(df: DataFrame, aqi_col: str = "predicted_aqi") -> DataFrame:
    return df.withColumn(
        "aqi_category",
        F.when(F.col(aqi_col) <= 50,  "Good")
         .when(F.col(aqi_col) <= 100, "Moderate")
         .when(F.col(aqi_col) <= 150, "Unhealthy for Sensitive Groups")
         .when(F.col(aqi_col) <= 200, "Unhealthy")
         .when(F.col(aqi_col) <= 300, "Very Unhealthy")
         .otherwise("Hazardous"),
    )


# ---------------------------------------------------------------------------
# 1. Load the active model artifact
# ---------------------------------------------------------------------------

def load_latest_model(horizon_h: int = DEFAULT_HORIZON) -> PipelineModel:
    """Load the trained GBT model for the given horizon from local storage.

    Falls back to the baseline Random Forest if the GBT is not found.
    """
    preferred = MODEL_DIR / f"gbt_{horizon_h}h"
    fallback  = MODEL_DIR / "baseline_random_forest"

    if preferred.exists():
        print(f"  Loading model: {preferred}")
        return PipelineModel.load(str(preferred))

    if fallback.exists():
        print(f"  GBT not found; falling back to: {fallback}")
        return PipelineModel.load(str(fallback))

    raise FileNotFoundError(
        f"No model found at {preferred} or {fallback}. "
        "Run train_gbt_model.py first."
    )


def load_model_from_mlflow(
    run_name: str = None,
    registered_name: str = "istanbul-aqi-gbt-1h",
    stage: str = "Production",
) -> PipelineModel:
    """Load model from MLflow Model Registry (requires active MLflow server).

    Fallback: call load_latest_model() instead for file-system loading.
    """
    import mlflow
    from pathlib import Path as _Path
    mlflow.set_tracking_uri(str(ROOT / "data" / "mlruns"))
    model_uri = f"models:/{registered_name}/{stage}"
    return mlflow.spark.load_model(model_uri)


# ---------------------------------------------------------------------------
# 2. Score a micro-batch
# ---------------------------------------------------------------------------

def score_micro_batch(
    feature_df: DataFrame,
    model: PipelineModel,
) -> DataFrame:
    """Generate AQI predictions for incoming feature rows.

    Args:
        feature_df: DataFrame whose columns include those returned by
                    feature_engineering.get_feature_columns().
        model:      Loaded PipelineModel.

    Returns:
        DataFrame with an added `prediction` column (raw model output).
    """
    return model.transform(feature_df)


# ---------------------------------------------------------------------------
# 3. Format prediction output for Grafana / downstream consumers
# ---------------------------------------------------------------------------

def format_prediction_output(
    predictions_df: DataFrame,
    horizon_h: int = DEFAULT_HORIZON,
) -> DataFrame:
    """Convert model outputs into dashboard-ready records.

    Output schema (aligns with Kafka topic airquality.predictions):
      station_id      string
      district        string
      latitude        double
      longitude       double
      timestamp       timestamp   — observation time
      predicted_at    timestamp   — wall-clock time of inference
      horizon_h       int         — forecast horizon in hours
      predicted_aqi   double      — model prediction
      aqi_category    string      — human-readable category
    """
    return (
        predictions_df
        .withColumn("predicted_aqi",  F.round(F.col("prediction"), 2))
        .withColumn("predicted_at",   F.current_timestamp())
        .withColumn("horizon_h",      F.lit(horizon_h))
        .transform(_add_aqi_category)
        .select(
            "station_id",
            "district",
            "latitude",
            "longitude",
            "timestamp",
            "predicted_at",
            "horizon_h",
            "predicted_aqi",
            "aqi_category",
        )
    )


# ---------------------------------------------------------------------------
# 4. Write predictions to Kafka topic (used by streaming job)
# ---------------------------------------------------------------------------

def write_predictions_to_kafka(
    output_df: DataFrame,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "airquality.predictions",
) -> None:
    """Serialize output rows as JSON and publish to Kafka.

    Called by structured_streaming_job.py; not needed for batch inference.
    """
    kafka_df = output_df.select(
        F.col("station_id").alias("key"),
        F.to_json(F.struct(*output_df.columns)).alias("value"),
    )
    (
        kafka_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .save()
    )


# ---------------------------------------------------------------------------
# 3b. Score new raw data (real-time / streaming entry point)
# ---------------------------------------------------------------------------

def score_new_data(
    spark,
    aq_df: DataFrame,
    wx_df: DataFrame,
    horizon_h: int = DEFAULT_HORIZON,
) -> DataFrame:
    """Score raw air quality + weather data without needing a pre-built test split.

    This is the entry point for Engineer 1's real streaming data.  Pass the
    raw Kafka-sourced DataFrames directly; feature engineering runs inside.

    Args:
        spark:     Active SparkSession.
        aq_df:     Raw air quality DataFrame (station_id, timestamp, pm25, …).
        wx_df:     Raw weather DataFrame (timestamp, temperature, …).
        horizon_h: Forecast horizon in hours (1, 3, or 6).

    Returns:
        Dashboard-ready DataFrame from format_prediction_output().
    """
    from pyspark.sql import functions as F
    from src.processing.feature_engineering import build_feature_dataset

    target_col = f"target_aqi_{horizon_h}h"

    # Join weather by truncated hour
    aq_h = aq_df.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    wx_h = wx_df.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    weather_cols = [c for c in wx_df.columns if c != "timestamp"]
    joined = aq_h.join(wx_h.select(["ts_hour"] + weather_cols), on="ts_hour", how="left")

    # Feature engineering (includes validate_and_clean for real data)
    features_df = build_feature_dataset(joined, horizons_h=(horizon_h,))

    model = load_latest_model(horizon_h)
    preds = score_micro_batch(features_df, model)
    return format_prediction_output(preds, horizon_h)


# ---------------------------------------------------------------------------
# Standalone smoke-test
# ---------------------------------------------------------------------------

def run_batch_inference(spark: SparkSession, horizon_h: int = DEFAULT_HORIZON) -> DataFrame:
    """Score the test split and print a sample."""
    from src.ml.train_baseline_models import load_training_dataset

    print(f"Loading test data and GBT-{horizon_h}h model...")
    _, _, test = load_training_dataset(spark)
    model = load_latest_model(horizon_h)

    target_col = f"target_aqi_{horizon_h}h"
    test_h = test.filter(F.col(target_col).isNotNull())

    preds  = score_micro_batch(test_h, model)
    output = format_prediction_output(preds, horizon_h)

    output.show(20, truncate=False)
    print(f"Total predictions: {output.count():,}")

    # Write sample to reports for inspection
    out_path = ROOT / "data" / "reports" / f"sample_predictions_{horizon_h}h"
    output.limit(1000).coalesce(1).write.mode("overwrite").option("header", "true").csv(str(out_path))
    print(f"Sample predictions → {out_path}")

    return output


if __name__ == "__main__":
    from src.common.config import (
        configure_windows_hadoop_env,
        SPARK_DRIVER_MEMORY,
        SPARK_MASTER,
        SPARK_SQL_SHUFFLE_PARTITIONS,
    )
    configure_windows_hadoop_env()
    spark = (
        SparkSession.builder
        .appName("inference")
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_batch_inference(spark, horizon_h=DEFAULT_HORIZON)
    finally:
        spark.stop()
