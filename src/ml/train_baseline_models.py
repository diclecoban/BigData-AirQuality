"""Baseline model training: Linear Regression and Random Forest.

Trains two baseline MLlib models for AQI forecasting (1-hour horizon).
Models are saved to data/models/ for later comparison against GBT.

Run:
  python -m src.ml.train_baseline_models
"""

from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    configure_windows_hadoop_env,
    SPARK_DRIVER_MEMORY,
    SPARK_MASTER,
    SPARK_SQL_SHUFFLE_PARTITIONS,
)
from src.processing.feature_engineering import (
    build_feature_dataset,
    get_feature_columns,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT       = Path(__file__).resolve().parents[2]
RAW_DIR    = ROOT / "data" / "raw"
MODEL_DIR  = ROOT / "data" / "models"
REPORT_DIR = ROOT / "data" / "reports"

TARGET_COL  = "target_aqi_1h"   # predict AQI 1 hour ahead
FEATURE_COLS = get_feature_columns()

configure_windows_hadoop_env()


def _temporal_split_by_timestamp(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split by timestamp cutoffs to avoid sorting the full wide dataset."""
    timestamps = [
        row["timestamp"]
        for row in (
            df.select("timestamp")
            .where(F.col("timestamp").isNotNull())
            .distinct()
            .orderBy("timestamp")
            .collect()
        )
    ]
    if not timestamps:
        empty = df.limit(0)
        return empty, empty, empty

    train_idx = min(len(timestamps) - 1, max(0, int(len(timestamps) * 0.70) - 1))
    val_idx = min(len(timestamps) - 1, max(train_idx, int(len(timestamps) * 0.85) - 1))

    train_end = timestamps[train_idx]
    val_end = timestamps[val_idx]

    train = df.filter(F.col("timestamp") <= F.lit(train_end))
    val = df.filter(
        (F.col("timestamp") > F.lit(train_end)) & (F.col("timestamp") <= F.lit(val_end))
    )
    test = df.filter(F.col("timestamp") > F.lit(val_end))
    return train, val, test


# ---------------------------------------------------------------------------
# 1. Load and split dataset
# ---------------------------------------------------------------------------

def load_training_dataset(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Load raw CSVs, build features, split 70 / 15 / 15.

    Returns (train_df, val_df, test_df).
    """
    aq = (
        spark.read
        .option("header", "true").option("inferSchema", "true")
        .csv(str(RAW_DIR / "airquality_historical.csv"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )
    wx = (
        spark.read
        .option("header", "true").option("inferSchema", "true")
        .csv(str(RAW_DIR / "weather_historical.csv"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )

    # Join weather by hour
    aq_h = aq.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    wx_h = wx.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    weather_cols = [c for c in wx.columns if c != "timestamp"]
    joined = aq_h.join(wx_h.select(["ts_hour"] + weather_cols), on="ts_hour", how="left")

    # Build full feature table (drops NULLs from lags)
    features_df = build_feature_dataset(joined)

    # Keep only rows where the target exists
    features_df = features_df.filter(F.col(TARGET_COL).isNotNull())

    train, val, test = _temporal_split_by_timestamp(features_df)

    print(f"  Split -> train: {train.count():,}  val: {val.count():,}  test: {test.count():,}")
    return train, val, test


# ---------------------------------------------------------------------------
# 2. Build shared ML stages
# ---------------------------------------------------------------------------

def _build_preprocessor() -> list:
    """Return Imputer + VectorAssembler stages shared by all models."""
    # Imputer fills any remaining NULLs with column median
    imputer = Imputer(
        inputCols=FEATURE_COLS,
        outputCols=[f"{c}_imp" for c in FEATURE_COLS],
        strategy="median",
    )
    imputed_cols = [f"{c}_imp" for c in FEATURE_COLS]
    assembler = VectorAssembler(
        inputCols=imputed_cols,
        outputCol="features",
        handleInvalid="skip",
    )
    return [imputer, assembler]


# ---------------------------------------------------------------------------
# 3. Linear Regression baseline
# ---------------------------------------------------------------------------

def train_linear_regression(train_df: DataFrame) -> Pipeline:
    """Train a LinearRegression model wrapped in a Pipeline.

    Elastic-net regularisation (alpha=0.5) is used to avoid overfitting on
    the large feature set.
    """
    stages = _build_preprocessor()
    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COL,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5,
        predictionCol="prediction",
    )
    stages.append(lr)
    pipeline = Pipeline(stages=stages)
    print("  Fitting LinearRegression...")
    model = pipeline.fit(train_df)
    return model


# ---------------------------------------------------------------------------
# 4. Random Forest baseline
# ---------------------------------------------------------------------------

def train_random_forest(train_df: DataFrame) -> Pipeline:
    """Train a RandomForestRegressor model wrapped in a Pipeline."""
    stages = _build_preprocessor()
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COL,
        numTrees=50,
        maxDepth=8,
        seed=42,
        predictionCol="prediction",
    )
    stages.append(rf)
    pipeline = Pipeline(stages=stages)
    print("  Fitting RandomForestRegressor...")
    model = pipeline.fit(train_df)
    return model


# ---------------------------------------------------------------------------
# 5. Save baseline artifacts
# ---------------------------------------------------------------------------

def save_baseline_artifacts(models: dict) -> None:
    """Persist each baseline model to data/models/<name>.

    Requires HADOOP_HOME to be set on Windows (see scripts/setup_winutils.py).
    The run_pipeline.py sets it automatically if C:/hadoop exists.
    """
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    for name, model in models.items():
        path = str(MODEL_DIR / name)
        model.write().overwrite().save(path)
        print(f"  Saved {name} -> {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(spark: SparkSession) -> dict:
    """Train baselines and return (models, test_df) dict for evaluation."""
    print("Loading dataset...")
    train, val, test = load_training_dataset(spark)

    print("Training Linear Regression baseline...")
    lr_model = train_linear_regression(train)

    print("Training Random Forest baseline...")
    rf_model = train_random_forest(train)

    models = {
        "baseline_linear_regression": lr_model,
        "baseline_random_forest":     rf_model,
    }

    print("Saving baseline models...")
    save_baseline_artifacts(models)

    return models, test


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("train-baseline-models")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
        print("Baseline training complete.")
    finally:
        spark.stop()
